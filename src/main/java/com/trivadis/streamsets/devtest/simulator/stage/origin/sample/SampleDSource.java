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
import com.streamsets.pipeline.api.impl.Utils;
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
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time.EventTimeConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time.TimestampModeType;
import com.trivadis.streamsets.sdc.csv.CsvParser;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sound.midi.Patch;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    private Map<String, Object> recordHeaderAttr;

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
            for (File file : files) {
                csvParsers.add(createParser(new FileReader(file)));
                recordHeaderAttr = generateHeaderAttrs(file);
            }

            bufferedDataStream = new BufferedDataStreamFileReader(getContext(), eventTimeConfig, csvConfig, csvParsers, 0, 10, 100);
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

    private CsvParser createParser(Reader reader) throws IOException {
        CsvParser csvParser = null;

        CSVFormat csvFormat = null;
        switch (csvConfig.csvFileFormat) {
            case CUSTOM:
                csvFormat = CSVFormat.DEFAULT.withDelimiter(csvConfig.csvCustomDelimiter)
                        .withEscape(csvConfig.csvCustomEscape)
                        .withQuote(csvConfig.csvCustomQuote)
                        .withIgnoreEmptyLines(csvConfig.csvIgnoreEmptyLines);

                if (csvConfig.csvEnableComments) {
                    csvFormat = csvFormat.withCommentMarker(csvConfig.csvCommentMarker);
                }
                break;
            case MULTI_CHARACTER:
                //
                break;
            default:
                csvFormat = csvConfig.csvFileFormat.getFormat();
                break;
        }

        switch (csvConfig.csvHeader) {
            case USE_HEADER:
            case IGNORE_HEADER:
                csvFormat = csvFormat.withHeader((String[]) null).withSkipHeaderRecord(true);
                break;
            case NO_HEADER:
                csvFormat = csvFormat.withHeader((String[]) null).withSkipHeaderRecord(false);
                break;
            default:
                throw new RuntimeException(Utils.format("Unknown Header error: {}", csvConfig.csvHeader));
        }
        csvParser = new CsvParser(reader, csvFormat, 1000);

        return csvParser;
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
        long startTime = System.currentTimeMillis();

        try {
            Future future = executorService.submit(new GeneratorRunnable( 1, bufferedDataStream, csvConfig, recordHeaderAttr, batchSize, startTime));
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

        /*
        try {
            // Run all the threads
            int i = 0;
            for (CsvParser csvParser : csvParsers) {
                Future future = executorService.submit(new GeneratorRunnable(i + 1, bufferedDataStream, csvConfig, recordHeaderAttr, batchSize, startTime));
                futures.add(future);
                i++;
            }

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
        */

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
        private long startTime;
        private List<Field> headers;
        //        private CsvConfig csvConfig;
        private int batchSize;
        //        private CSVIterator csvIterator = null;
        private Map<String, Object> recordHeaderAttr = null;


        public GeneratorRunnable(int threadId, BufferedDataStreamFileReader bufferedDataStream, CsvConfig csvConfig, Map<String, Object> recordHeaderAttr, int batchSize, long startTime) {
            this.threadId = threadId;
            this.bufferedDataStream = bufferedDataStream;
//            this.csvConfig = csvConfig;
            this.recordHeaderAttr = recordHeaderAttr;
            this.batchSize = batchSize;
            this.startTime = startTime;
            this.recordCount = 0;
/*
            if (csvConfig.csvHeader != CsvHeader.NO_HEADER) {
                String[] hs = bufferedDataStream.getHeaders();
                if (csvConfig.csvHeader != CsvHeader.IGNORE_HEADER && hs != null) {
                    headers = new ArrayList<>();
                    for (String h : hs) {
                        headers.add(Field.create(h));
                    }
                }
            }
*/
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

                        cont = produce(batchSize, batchContext, startTime);

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

        public boolean produce(int batchSize, BatchContext batchContext, long startTime) {
            BatchMaker batchMaker = batchContext.getBatchMaker();

            int i = 0;
            while (i < batchSize) {
                try {
                    List<Record> records = bufferedDataStream.pollFromBuffer();
                    for (Record record : records) {

                        // nextLine[] is an array of values from the line
                        // System.out.println(nextLine[0]  + "::" + nextLine[1]  + " etc...");

                        if (record != null) {
//                            LinkedHashMap<String, Field> map = new LinkedHashMap<>();

                            if (record.has(eventTimeConfig.timestampField)) {
                                long eventTime = record.get(eventTimeConfig.timestampField).getValueAsLong();
                                if (eventTimeConfig.timestampMode.equals(TimestampModeType.RELATIVE)) {
                                    long absoluteEventTime = this.startTime + eventTime;
                                    if (absoluteEventTime > System.currentTimeMillis()) {
                                        try {
                                            Thread.sleep(Math.max(absoluteEventTime - System.currentTimeMillis(), 0));
                                            //Thread.sleep(Math.max(absoluteEventTime - System.currentTimeMillis(), 0));
                                        } catch (InterruptedException e) {
                                            break;
                                        }
                                    }
                                } else if (eventTimeConfig.timestampMode.equals(TimestampModeType.ABSOLUTE)) {

                                }
                            }

                            record.set(eventTimeConfig.eventTimestampField, Field.create(System.currentTimeMillis()));
                            setHeaders(record, recordHeaderAttr);

                            batchMaker.addRecord(record);
                            i++;
                        } else {
                            return false;
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {

                }
            }
            return true;
        }

    }
/*
    protected Record createRecord(PushSource.Context context, long offset, List<Field> headers, String[] columns) throws DataParserException {
        Record record = context.createRecord("test" + "::" + offset);

        // In case that the number of columns does not equal the number of expected columns from header, report the
        // parsing error as recoverable issue - it's safe to continue reading the stream.
        if (headers != null && columns.length > headers.size()) {
            record.set(Field.create(Field.Type.MAP, ImmutableMap.builder()
                    .put("columns", getListField(columns))
                    .put("headers", Field.create(Field.Type.LIST, headers))
                    .build()
            ));

            throw new RecoverableDataParserException(record, Errors.DEV_SIMULATOR_001, offset, columns.length, headers.size());
        }

        if (csvConfig.csvRecordType == CsvRecordType.LIST) {
            List<Field> row = new ArrayList<>();
            for (int i = 0; i < columns.length; i++) {
                Map<String, Field> cell = new HashMap<>();
                Field header = (headers != null) ? headers.get(i) : null;
                if (header != null) {
                    cell.put("header", header);
                }
                Field value = getField(columns[i]);
                cell.put("value", value);
                row.add(Field.create(cell));
            }
            record.set(Field.create(row));
        } else {
            LinkedHashMap<String, Field> listMap = new LinkedHashMap<>();
            for (int i = 0; i < columns.length; i++) {
                String key;
                Field header = (headers != null) ? headers.get(i) : null;
                if (header != null) {
                    key = header.getValueAsString();
                } else {
                    key = Integer.toString(i);
                }
                listMap.put(key, getField(columns[i]));
            }
            record.set(Field.createListMap(listMap));
        }

        return record;
    }
*/

    private Map<String, Object> generateHeaderAttrs(File file) throws IOException {
        Map<String, Object> recordHeaderAttr = new HashMap<>();
        recordHeaderAttr.put("file", file.getAbsolutePath());
        recordHeaderAttr.put("filename", file.getName());
        recordHeaderAttr.put("baseDir", filesDirectory);
        return recordHeaderAttr;
    }

    private void setHeaders(Record record, Map<String, Object> recordHeaderAttr) throws IOException {
        recordHeaderAttr.forEach((k, v) -> record.getHeader().setAttribute(k, v.toString()));
    }


    private Collection getAllFilesThatMatchFilenameExtension(String directoryName, String pattern, boolean includeSubdirectories, PathMatcherMode pathMatcherMode) {
        File directory = new File(directoryName);

        return FileUtils.listFilesAndDirs(directory
                                        , pathMatcherMode.equals(PathMatcherMode.WILDCARD) ? new WildcardFileFilter(pattern) : new RegexFileFilter(pattern)
                                        , includeSubdirectories ? TrueFileFilter.INSTANCE : null)
                .stream().filter(f -> f.isFile()).collect(Collectors.toList());
    }


}
