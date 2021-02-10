package com.trivadis.streamsets.devtest.simulator.stage.origin.sample;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.api.lineage.EndPointType;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageEventType;
import com.streamsets.pipeline.api.lineage.LineageSpecificAttribute;
import com.trivadis.streamsets.devtest.simulator.stage.lib.sample.Errors;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.DevSimulatorConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.files.PathMatcherMode;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format.CsvConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.multitype.MultiTypeConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time.DateUtil;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time.EventTimeConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time.TimestampModeType;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class DevSimulatorSource extends BasePushSource {
    private static final Logger LOG = LoggerFactory.getLogger(DevSimulatorSource.class);

    private final DevSimulatorConfig basicConfig;
    private final EventTimeConfig eventTimeConfig;
    private final MultiTypeConfig multiTypeConfig;
    private final CsvConfig csvConfig;

    private BufferedDataStreamFileReader bufferedDataStream = null;

    //    private long counter;
    private long maxWaitTime;

    private long startTimestampMs = 0;

    private Collection getAllFilesThatMatchFilenameExtension(String directoryName, String pattern, boolean includeSubdirectories, PathMatcherMode pathMatcherMode) {
        File directory = new File(directoryName);

        return FileUtils.listFilesAndDirs(directory
                , pathMatcherMode.equals(PathMatcherMode.WILDCARD) ? new WildcardFileFilter(pattern) : new RegexFileFilter(pattern)
                , includeSubdirectories ? TrueFileFilter.INSTANCE : null)
                .stream().filter(f -> f.isFile()).collect(Collectors.toList());
    }

    public DevSimulatorSource(
            DevSimulatorConfig basicConfig,
            EventTimeConfig eventTimeConfig,
            MultiTypeConfig multiTypeConfig,
            CsvConfig csvConfig) {
        this.basicConfig = Preconditions.checkNotNull(basicConfig, "basicConfig cannot be null");
        this.eventTimeConfig = Preconditions.checkNotNull(eventTimeConfig, "eventTimeConfig cannot be null");
        this.multiTypeConfig = Preconditions.checkNotNull(multiTypeConfig, "multiTypeConfig cannot be null");
        this.csvConfig = Preconditions.checkNotNull(csvConfig, "csvConfig cannot be null");
    }

    @Override
    protected List<ConfigIssue> init() {
//        counter = 0;

        //preview settings
        if (getContext().isPreview()) {
            maxWaitTime = 1000;
        }

        LineageEvent event = getContext().createLineageEvent(LineageEventType.ENTITY_READ);
        event.setSpecificAttribute(LineageSpecificAttribute.ENDPOINT_TYPE, EndPointType.DEVDATA.name());
        event.setSpecificAttribute(LineageSpecificAttribute.DESCRIPTION, getContext().getStageInfo().getName());

        Collection<File> files = new ArrayList<>();

        files = getAllFilesThatMatchFilenameExtension(basicConfig.filesDirectory, basicConfig.fileNamePattern, basicConfig.includeSubdirectories, basicConfig.pathMatcherMode);
        LOG.info("Using the following files " + files);

        try {
            bufferedDataStream = BufferedDataStreamFileReader.create()
                    .withContext(getContext())
                    .withConfig(eventTimeConfig, csvConfig)
                    .withFiles(files)
                    .withMinBufferSize(basicConfig.minBufferSize)
                    .withMaxBufferSize(basicConfig.maxBufferSize);
            bufferedDataStream.fillBuffer();
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
        return 1;
    }

    @Override
    public void produce(Map<String, String> map, int i) throws StageException {

        //long deltaMs = 0;
        long currentTimestampMs = System.currentTimeMillis();

        // set the start timestamp and delta if RELATIVE timestamp mode
        if (this.eventTimeConfig.timestampMode.equals(TimestampModeType.RELATIVE)) {
            if (this.eventTimeConfig.simulationStartNow) {
                startTimestampMs = currentTimestampMs;
            } else {
                startTimestampMs = DateUtil.parseCustomFormat(eventTimeConfig.simulationStartTimestampDateFormat.getFormat(), eventTimeConfig.simulationStartTimestamp) * 1000;
                //deltaMs = currentTimestampMs - startTimestampMs;
            }
        }

        long machineStartTimestampMs = System.currentTimeMillis();

        try {
            boolean cont = true;
            // Create new batch
            BatchContext batchContext = null;

            while (!getContext().isStopped() && cont) {
                try {
                    if (batchContext == null) {
                        batchContext = getContext().startBatch();
                    }

                    cont = produceRecords(batchContext, machineStartTimestampMs);

                    // send the batch
                    getContext().processBatch(batchContext);
                    batchContext = getContext().startBatch();
                } finally {

                }
            }
        } finally {
        }

    }

    public boolean produceRecords(BatchContext batchContext, long machineStartTimestampMs) {
        BatchMaker batchMaker = batchContext.getBatchMaker();

        try {
            Map.Entry<Long, List<Record>> timestampWithRecords = bufferedDataStream.pollFromBuffer();
            if (timestampWithRecords != null) {
                long recordTimeMs = timestampWithRecords.getKey();
                if (startTimestampMs == 0) {
                    startTimestampMs = recordTimeMs;
                }

                for (Record record : timestampWithRecords.getValue()) {
                    long currentEventTimeMs = TimeUtil.generateTimestamp(System.currentTimeMillis(), machineStartTimestampMs, startTimestampMs, eventTimeConfig.speedup);
                    if (record != null) {
                        long eventTimestampMs = 0;

                        if (eventTimeConfig.timestampMode.equals(TimestampModeType.RELATIVE)) {
                            eventTimestampMs = startTimestampMs + recordTimeMs;
                        } else if (eventTimeConfig.timestampMode.equals(TimestampModeType.ABSOLUTE)) {
                            eventTimestampMs = recordTimeMs;
                        } else if (eventTimeConfig.timestampMode.equals(TimestampModeType.ABSOLUTE_RELATIVE)) {

                        } else if (eventTimeConfig.timestampMode.equals(TimestampModeType.FIXED)) {

                        }

                        if (eventTimestampMs > currentEventTimeMs) {
                            try {
                                Thread.sleep(Math.max(eventTimestampMs - currentEventTimeMs, 0));
                                //Thread.sleep(Math.max(eventTimestampMs - System.currentTimeMillis(), 0));
                            } catch (InterruptedException e) {
                                break;
                            }
                        }

                        record.set(eventTimeConfig.eventTimestampOutputField, Field.create(eventTimestampMs));

                        batchMaker.addRecord(record);
                    } else {
                        return false;
                    }
                }
            }
        } finally {

        }
        return true;
    }

}
