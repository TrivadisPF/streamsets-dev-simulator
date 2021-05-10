package com.trivadis.streamsets.devtest.simulator.stage.origin.sample;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.lineage.EndPointType;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageEventType;
import com.streamsets.pipeline.api.lineage.LineageSpecificAttribute;
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
import java.util.*;
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

    private boolean publishStartEventSent = false;

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
                    .withConfig(basicConfig, eventTimeConfig, csvConfig, multiTypeConfig)
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
        if (this.eventTimeConfig.timestampMode.equals(TimestampModeType.RELATIVE_FROM_ANCHOR)
                || this.eventTimeConfig.timestampMode.equals(TimestampModeType.RELATIVE_FROM_PREVIOUS)) {
            if (this.eventTimeConfig.anchorTimeNow) {
                startTimestampMs = currentTimestampMs;
            } else {
                startTimestampMs = DateUtil.parseCustomFormat(eventTimeConfig.anchorTimestampDateFormat.getFormat(), eventTimeConfig.anchorTimestamp) * 1000;
                //deltaMs = currentTimestampMs - startTimestampMs;
            }
        }

        // set the machineStartTimestampMs to be the actual time + configurable delay
        long machineStartTimestampMs = System.currentTimeMillis() + eventTimeConfig.delayMs;

        try {
            boolean cont = true;
            // Create new batch
            BatchContext batchContext = null;

            while (!getContext().isStopped() && cont) {
                try {
                    if (batchContext == null) {
                        batchContext = getContext().startBatch();

                        // publish event for simulation start
                        String eventRecordSourceId = Utils.format("event:{}", machineStartTimestampMs);
                        EventRecord eventRecord = getContext().createEventRecord("START", 1, eventRecordSourceId);
                        Map<String, Field> eventMap = new HashMap<>();
                        eventMap.put("timestampMs", Field.create(machineStartTimestampMs));
                        eventMap.put("event", Field.create("START"));
                        eventRecord.set(Field.create(eventMap));
                        batchContext.toEvent(eventRecord);
                        getContext().processBatch(batchContext);
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
                    String outputLane = record.getHeader().getAttribute("_outputLane");
                    long currentEventTimeMs = TimeUtil.generateTimestamp(System.currentTimeMillis(), machineStartTimestampMs, startTimestampMs, eventTimeConfig.speedup);
                    if (record != null) {
                        long eventTimestampMs = 0;

                        if (eventTimeConfig.timestampMode.equals(TimestampModeType.RELATIVE_FROM_ANCHOR)) {
                            eventTimestampMs = startTimestampMs + recordTimeMs;
                        } else if (eventTimeConfig.timestampMode.equals(TimestampModeType.RELATIVE_FROM_PREVIOUS)) {
                            eventTimestampMs = startTimestampMs + recordTimeMs;
                        } else if (eventTimeConfig.timestampMode.equals(TimestampModeType.ABSOLUTE)) {
                            eventTimestampMs = recordTimeMs;
                        } else if (eventTimeConfig.timestampMode.equals(TimestampModeType.FIXED)) {
                            // t.b.d.
                        }

                        if (eventTimestampMs > currentEventTimeMs) {
                            try {
                                Thread.sleep(Math.max(eventTimestampMs - currentEventTimeMs, 0));
                                //Thread.sleep(Math.max(eventTimestampMs - System.currentTimeMillis(), 0));
                            } catch (InterruptedException e) {
                                break;
                            }
                        }

                        // if a delay was used, send another event upon start of sending messages
                        if (eventTimeConfig.delayMs > 0 && !publishStartEventSent) {
                            String eventRecordSourceId = Utils.format("event:{}", eventTimestampMs);
                            EventRecord eventRecord = getContext().createEventRecord("publishing-start", 1, eventRecordSourceId);
                            Map<String, Field> eventMap = new HashMap<>();
                            eventMap.put("timestampMs", Field.create(eventTimestampMs));
                            eventRecord.set(Field.create(eventMap));
                            batchContext.toEvent(eventRecord);

                            publishStartEventSent = true;
                        }

                        record.set(eventTimeConfig.eventTimestampOutputField, Field.create(eventTimestampMs));
                        //record.set("/StartEventTimestamp", Field.create(startTimestampMs));
                        record.getHeader().setAttribute("startEventTimestamp", String.valueOf(startTimestampMs));

                        if (basicConfig.useMultiRecordType) {
                            batchMaker.addRecord(record, record.getHeader().getAttribute("_outputLane"));
                        } else {
                            batchMaker.addRecord(record);
                        }
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
