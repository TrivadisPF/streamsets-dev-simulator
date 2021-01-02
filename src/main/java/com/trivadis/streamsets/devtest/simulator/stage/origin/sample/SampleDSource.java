/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.trivadis.streamsets.devtest.simulator.stage.origin.sample;

import com.codahale.metrics.Timer;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.api.lineage.EndPointType;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageEventType;
import com.streamsets.pipeline.api.lineage.LineageSpecificAttribute;
import com.trivadis.streamsets.devtest.simulator.stage.lib.sample.Errors;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.files.PathMatcherMode;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.files.PathMatcherModeChooserValues;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format.CsvConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format.DataFormatType;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format.InputDataFormatChooserValues;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time.DateUtil;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time.EventTimeConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time.TimestampModeType;
import com.trivadis.streamsets.sdc.csv.CsvParser;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@GenerateResourceBundle
@StageDef(
        version = 1,
        label = "Dev Simulator",
        description = "Reads files with data and simulates it as messages considering the timestamp for replaying. For development only",
        icon = "dev.png",
        execution = {ExecutionMode.STANDALONE, ExecutionMode.EDGE},
        producesEvents = true,
        recordsByRef = true,
        onlineHelpRefUrl = ""
)
@ConfigGroups(value = Groups.class)
public class SampleDSource extends BasePushSource {

    private static final Logger LOG = LoggerFactory.getLogger(SampleDSource.class);

    private final int EVENT_VERSION = 1;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "",
            label = "Files Directory",
            description = "Use a local directory",
            displayPosition = 10,
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "FILES"
    )
    public String filesDirectory;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.BOOLEAN,
            defaultValue = "true",
            label = "Include Subdirectories",
            description = "Include files in subdirectories of Files Directory.  " +
                    "Only file names matching File Name Pattern will be processed.",
            displayPosition = 15,
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "FILES"
    )
    public boolean includeSubdirectories = true;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "",
            label = "File Name Pattern",
            description = "A glob or regular expression that defines the pattern of the file names in the directory.",
            displayPosition = 20,
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "FILES"
    )
    public String fileNamePattern;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.MODEL,
            label = "File Name Pattern Mode",
            description = "Select whether the File Name Pattern specified uses wildcard pattern syntax or regex syntax.",
            defaultValue = "WILDCARD",
            displayPosition = 15,
            displayMode = ConfigDef.DisplayMode.ADVANCED,
            group = "FILES"
    )
    @ValueChooserModel(PathMatcherModeChooserValues.class)
    public PathMatcherMode pathMatcherMode = PathMatcherMode.WILDCARD;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.NUMBER,
            defaultValue = "1000",
            label = "Batch size",
            description = "Number of records that will be generated for single batch.",
            min = 1,
            max = Integer.MAX_VALUE,
            displayMode = ConfigDef.DisplayMode.ADVANCED,
            group = "FILES"
    )
    public int batchSize;

    @ValueChooserModel(InputDataFormatChooserValues.class)
    @ConfigDef(
            required = true,
            type = ConfigDef.Type.MODEL,
            defaultValue = "AS_DELIMITED",
            label = "Input Data Format",
            description = "How should the output be produced.",
            group = "DATA_FORMAT",
            displayPosition = 0
    )
    public DataFormatType inputDataFormat;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.BOOLEAN,
            defaultValue = "false",
            label = "Multiple Record Types",
            description = "Support multiple, different Record Types.",
            group = "FILES",
            displayPosition = 30
    )
    public boolean useMultiRecordType;

    @ConfigDefBean()
    public EventTimeConfig eventTimeConfig;

    @ConfigDefBean()
    public CsvConfig csvConfig;

    /**
     * Counter for LONG_SEQUENCE type
     */
    private long counter;
    private long maxWaitTime;

    public int numThreads;

    private ExecutorService executorService = null;

    private Timer dataGeneratorTimer;

    private List<CsvParser> csvParsers = new ArrayList<CsvParser>();
    private BufferedDataStreamFileReader bufferedDataStream = null;

    @Override
    protected List<ConfigIssue> init() {
        counter = 0;

        //preview settings
        if (getContext().isPreview()) {
            maxWaitTime = 1000;
        }

        LineageEvent event = getContext().createLineageEvent(LineageEventType.ENTITY_READ);
        event.setSpecificAttribute(LineageSpecificAttribute.ENDPOINT_TYPE, EndPointType.DEVDATA.name());
        event.setSpecificAttribute(LineageSpecificAttribute.DESCRIPTION, getContext().getStageInfo().getName());

        Collection<File> files = new ArrayList<>();

        files = getAllFilesThatMatchFilenameExtension(filesDirectory, fileNamePattern, includeSubdirectories, pathMatcherMode);
        LOG.info("Using the following files " + files);
        numThreads = 1;

        try {
            bufferedDataStream = BufferedDataStreamFileReader.create()
                    .withContext(getContext())
                    .withConfig(eventTimeConfig, csvConfig)
                    .withFiles(files)
                    .withMinBufferSize(10)
                    .withMaxBufferSize(100);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return super.init();
    }

    @Override
    public void destroy() {
        super.destroy();
    }

    @Override
    public int getNumberOfThreads() {
        return numThreads;
    }

    @Override
    public void produce(Map<String, String> offsets, int maxBatchSize) throws StageException {
        int batchSize = Math.min(this.batchSize, maxBatchSize);
        if (!getContext().isPreview() && this.batchSize > maxBatchSize) {
            getContext().reportError(Errors.DEV_SIMULATOR_027, maxBatchSize);
        }

        this.executorService = Executors.newFixedThreadPool(numThreads);
        List<Future<Runnable>> futures = new ArrayList<>(numThreads);

        StageException propagateException = null;

        long startTimeMs = 0;
        long deltaMs = 0;
        long currentTimestampMs = System.currentTimeMillis();
        if (this.eventTimeConfig.simulationStartNow) {
            startTimeMs = currentTimestampMs;
        } else {
            startTimeMs = DateUtil.parseCustomFormat(eventTimeConfig.simulationStartTimestampDateFormat.getFormat(), eventTimeConfig.simulationStartTimestamp) * 1000;
            deltaMs = currentTimestampMs - startTimeMs;
        }

        try {
            Future future = executorService.submit(new GeneratorRunnable( 1, bufferedDataStream, csvConfig, batchSize, startTimeMs, deltaMs));
            futures.add(future);

            // Wait for proper execution finish
            for (Future<Runnable> f : futures) {
                try {
                    f.get();
                } catch (InterruptedException | ExecutionException e) {
                    LOG.error("InterruptedException when attempting to wait for all runnables to complete, after context " +
                            "was stopped: {}", e.getMessage(), e);
                    Thread.currentThread().interrupt();
                }
            }
        } finally {
            // Terminate executor that will also clear up threads that were created
            shutdownExecutorIfNeeded();
        }

        if (propagateException != null) {
            throw propagateException;
        }
    }

    private void shutdownExecutorIfNeeded() {
        Optional.ofNullable(executorService).ifPresent(executor -> {
            if (!executor.isTerminated()) {
                LOG.info("Shutting down executor service");
                executor.shutdown();
            }
        });
    }


    public class GeneratorRunnable implements Runnable {
        int threadId;
        private BufferedDataStreamFileReader bufferedDataStream = null;
        private long recordCount;
        private long startTimestampMs;
        private long deltaMs;
        private List<Field> headers;

        private int batchSize;

        public GeneratorRunnable(int threadId, BufferedDataStreamFileReader bufferedDataStream, CsvConfig csvConfig, int batchSize, long startTimestampMs, long deltaMs) {
            this.threadId = threadId;
            this.bufferedDataStream = bufferedDataStream;
            this.batchSize = batchSize;
            this.startTimestampMs = startTimestampMs;
            this.deltaMs = deltaMs;
            this.recordCount = 0;
        }

        @Override
        public void run() {
            String oldThreadName = Thread.currentThread().getName();
            Thread.currentThread().setName("DevSimulator" + threadId + "::" + getContext().getPipelineId());

            try {
                boolean cont = true;
                // Create new batch
                BatchContext batchContext = null;

                while (!getContext().isStopped() && cont) {
                    try {
                        if (batchContext == null) {
                            batchContext = getContext().startBatch();
                        }

                        cont = produce(batchSize, batchContext, startTimestampMs, deltaMs);

                        // send the batch
                        getContext().processBatch(batchContext);
                        batchContext = getContext().startBatch();
                    } finally {

                    }
                }
            } finally {
                Thread.currentThread().setName(oldThreadName);
            }
        }

        public boolean produce(int batchSize, BatchContext batchContext, long startTime, long deltaMs) {
            BatchMaker batchMaker = batchContext.getBatchMaker();

            int i = 0;
            while (i < batchSize) {
                try {
                    List<Record> records = bufferedDataStream.pollFromBuffer();
                    for (Record record : records) {
                        long currentEventTimeMs = System.currentTimeMillis() - deltaMs;

                        if (record != null) {
                            long eventTimestampMs = 0;
                            if (record.has(eventTimeConfig.timestampField)) {
                                long recordTimeMs = record.get(eventTimeConfig.timestampField).getValueAsLong();
                                if (eventTimeConfig.timestampMode.equals(TimestampModeType.RELATIVE)) {
                                    eventTimestampMs = this.startTimestampMs + recordTimeMs;
                                    if (eventTimestampMs > currentEventTimeMs) {
                                        try {
                                            Thread.sleep(Math.max(eventTimestampMs - currentEventTimeMs, 0));
                                            //Thread.sleep(Math.max(eventTimestampMs - System.currentTimeMillis(), 0));
                                        } catch (InterruptedException e) {
                                            break;
                                        }
                                    }
                                } else if (eventTimeConfig.timestampMode.equals(TimestampModeType.ABSOLUTE)) {

                                } else if (eventTimeConfig.timestampMode.equals(TimestampModeType.ABSOLUTE_RELATIVE)) {

                                } else if (eventTimeConfig.timestampMode.equals(TimestampModeType.FIXED)) {

                                }
                            }

                            record.set(eventTimeConfig.eventTimestampField, Field.create(eventTimestampMs));

                            batchMaker.addRecord(record);
                            i++;
                        } else {
                            return false;
                        }
                    }
                } finally {

                }
            }
            return true;
        }

    }

    private Collection getAllFilesThatMatchFilenameExtension(String directoryName, String pattern, boolean includeSubdirectories, PathMatcherMode pathMatcherMode) {
        File directory = new File(directoryName);

        return FileUtils.listFilesAndDirs(directory
                                        , pathMatcherMode.equals(PathMatcherMode.WILDCARD) ? new WildcardFileFilter(pattern) : new RegexFileFilter(pattern)
                                        , includeSubdirectories ? TrueFileFilter.INSTANCE : null)
                .stream().filter(f -> f.isFile()).collect(Collectors.toList());
    }


}
