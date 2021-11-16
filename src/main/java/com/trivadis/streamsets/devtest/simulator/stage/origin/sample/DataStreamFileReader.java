package com.trivadis.streamsets.devtest.simulator.stage.origin.sample;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.service.dataformats.DataParserException;
import com.streamsets.pipeline.api.service.dataformats.RecoverableDataParserException;
import com.trivadis.streamsets.devtest.simulator.stage.lib.sample.Errors;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.DevSimulatorConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format.CsvConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format.CsvHeader;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format.CsvRecordType;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.multitype.MultiTypeConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time.*;
import com.trivadis.streamsets.sdc.csv.CsvParser;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class DataStreamFileReader {
    private static final Logger LOG = LoggerFactory.getLogger(DataStreamFileReader.class);

    /**
     * The DevSimulatro Config object
     */
    private DevSimulatorConfig devSimulatorConfig;

    /**
     * The CSV Config object
     */
    private CsvConfig csvConfig;

    /**
     * The Event Time Config object
     */
    private EventTimeConfig eventTimeConfig;

    /**
     * The Multi Type Config object
     */
    private MultiTypeConfig multiTypeConfig;

    /**
     * Reflects if the end of the file is already reached
     */
    private boolean fileEnd = false;

    /**
     * Map of current item for each CsvParser (and its file behind it)
     */
    private Map<String,Item> csvParsers = new HashMap<>();

    /**
     * Record Header Attributes
     */
    //private List<Map<String, Object>> recordHeaderAttrList = new ArrayList<>();

    private Map<String, Long> previousTimestampMsPerOutputLane = null;

    private PushSource.Context context;

    private int timestampPosition;

    private long lowestRecordTimeMs = Long.MAX_VALUE;

    private long startTimestampMs = 0;
    private long machineStartTimestampMs = 0;

    public long getStartTimestampMs() {
        return startTimestampMs;
    }

    public long getMachineStartTimestampMs() {
        return machineStartTimestampMs;
    }

    protected Record createRecord(PushSource.Context context, long offset, String filename, String[] columns) throws DataParserException, IOException {
        Record record = context.createRecord("test" + "::" + offset);

        // get the headers of this parser
        List<Field> headers = getHeaders(filename);

        // In case that the number of columns does not equal the number of expected columns from header, report the
        // parsing error as recoverable issue - it's safe to continue reading the stream.
        if (headers != null && columns.length > headers.size()) {
            record.set(Field.create(Field.Type.MAP, ImmutableMap.builder()
                    .put("columns", getListField(columns))
                    .put("headers", Field.create(Field.Type.LIST, headers))
                    .build()
            ));

            throw new RecoverableDataParserException(record, Errors.DEV_SIMULATOR_001, offset, columns.length, headers.size(), headers);
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
                String fieldKey;
                Field header = (headers != null) ? headers.get(i) : null;
                if (header != null) {
                    fieldKey = header.getValueAsString();
                } else {
                    fieldKey = Integer.toString(i);
                }
                listMap.put(fieldKey, getField(columns[i]));
            }
            record.set(Field.createListMap(listMap));
        }

        // set additional headers
        setHeaders(record, csvParsers.get(filename).recordHeaderAttr);

        return record;
    }

    private Field getListField(String... values) {
        ImmutableList.Builder<Field> listBuilder = ImmutableList.builder();
        for (String value : values) {
            listBuilder.add(Field.create(Field.Type.STRING, value));
        }

        return Field.create(Field.Type.LIST, listBuilder.build());
    }

    private Field getField(String value) {
        if (csvConfig.parseNull && csvConfig.nullConstant != null && csvConfig.nullConstant.equals(value)) {
            return Field.create(Field.Type.STRING, null);
        }

        return Field.create(Field.Type.STRING, value);
    }

    /**
     * Set headers to a record
     *
     * @param record
     * @param recordHeaderAttr
     * @throws IOException
     */
    private void setHeaders(Record record, Map<String, Object> recordHeaderAttr) throws IOException {
        recordHeaderAttr.forEach((k, v) -> record.getHeader().setAttribute(k, v.toString()));
    }

    private Map<String, Object> generateHeaderAttrs(File file) throws IOException {
        Map<String, Object> recordHeaderAttr = new HashMap<>();
        recordHeaderAttr.put("file", file.getAbsolutePath());
        recordHeaderAttr.put("filename", file.getName());
        recordHeaderAttr.put("baseDir", file.getParent()); //filesDirectory);
        return recordHeaderAttr;
    }

    private List<Field> getHeaders(String filename) throws IOException {
        List<Field> headers = null;
        if (csvConfig.csvHeader != CsvHeader.NO_HEADER) {
            String[] hs = csvParsers.get(filename).parser.getHeaders();
            if (csvConfig.csvHeader != CsvHeader.IGNORE_HEADER && hs != null) {
                headers = new ArrayList<>();
                for (String h : hs) {
                    headers.add(Field.create(h));
                }
            }
        }
        return headers;
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

    private DataStreamFileReader() throws IOException {
    }

    public static DataStreamFileReader create() throws IOException {
        return new DataStreamFileReader();
    }

    public DataStreamFileReader withContext(PushSource.Context context) {
        this.context = context;
        return this;
    }

    public DataStreamFileReader withConfig(DevSimulatorConfig devSimulatorConfig, EventTimeConfig eventTimeConfig, CsvConfig csvConfig, MultiTypeConfig multiTypeConfig) {
        this.eventTimeConfig = eventTimeConfig;
        this.csvConfig = csvConfig;
        this.multiTypeConfig = multiTypeConfig;
        this.devSimulatorConfig = devSimulatorConfig;

        if (eventTimeConfig.timestampMode.equals(TimestampModeType.RELATIVE_FROM_PREVIOUS)) {
            previousTimestampMsPerOutputLane = new HashMap<>();
        }
        return this;
    }

    public DataStreamFileReader withFiles(Collection<File> files) throws IOException {
        for (File file : files) {
            CsvParser parser = createParser(new BufferedReader(new FileReader(file)));
            csvParsers.put(file.getName(), Item.create(null, generateHeaderAttrs(file), parser));
        }
        return this;
    }

    public DataStreamFileReader withTimestamps(long machineStartTimestampMs) {
        this.machineStartTimestampMs = machineStartTimestampMs;
        long currentTimestampMs = System.currentTimeMillis();

        // set the start timestamp and delta if RELATIVE timestamp mode
        if (this.eventTimeConfig.timestampMode.equals(TimestampModeType.RELATIVE_FROM_ANCHOR)
                || this.eventTimeConfig.timestampMode.equals(TimestampModeType.RELATIVE_FROM_PREVIOUS)) {
            if (this.eventTimeConfig.anchorTimeNow) {
                startTimestampMs = currentTimestampMs;
            } else {
                String timestampFormat = eventTimeConfig.anchorTimestampDateFormat != AnchorDateFormat.OTHER ? eventTimeConfig.anchorTimestampDateFormat.getFormat() : eventTimeConfig.anchorTimestampOtherDateFormat;
                startTimestampMs = DateUtil.parseCustomFormat(eventTimeConfig.anchorTimestampDateFormat.getFormat(), eventTimeConfig.anchorTimestamp) * 1000;
                //deltaMs = currentTimestampMs - startTimestampMs;
            }
        } else if (this.eventTimeConfig.timestampMode.equals(TimestampModeType.ABSOLUTE_WITH_START)) {
            String timestampFormat = eventTimeConfig.simulationStartTimestampDateFormat != SimulationStartDateFormat.OTHER ? eventTimeConfig.simulationStartTimestampDateFormat.getFormat() : eventTimeConfig.simulatorStartTimestampOtherDateFormat;
            startTimestampMs = DateUtil.parseCustomFormat(timestampFormat, eventTimeConfig.simulationStartTimestamp) * 1000;
            System.out.println ("startTimestampMs: " + startTimestampMs);
        }

        return this;
    }

    public List<Record> read() {
        List<Record> result = new ArrayList<>();
        String[] line;

        boolean hasMore = true;
        lowestRecordTimeMs = Long.MAX_VALUE;

        while (hasMore){
            // loop for each file
            Set<String> keysToRemove = new HashSet<>();
            for (String key : csvParsers.keySet()) {
                Record record = csvParsers.get(key).record;

                try {
                    // if a record is available, check for the time, if no record is available, read the next one
                    if (record != null) {
                        lowestRecordTimeMs = Long.min(lowestRecordTimeMs, csvParsers.get(key).recordTimeMs);
                    } else {
                        line = csvParsers.get(key).parser.read();
                        if (line == null) {
                            // remove the parser, as it is at EOF
                            keysToRemove.add(key);
                            this.fileEnd = csvParsers.isEmpty();
                        } else {
                            record = createRecord(context, 1, key, line);

                            // either we are not interested in handling multi-record types or the record type is one we should handle
                            if (!devSimulatorConfig.useMultiRecordType ||
                                    (multiTypeConfig != null &&
                                            multiTypeConfig.contains(record.get(multiTypeConfig.discriminatorField).getValueAsString()))) {

                                // check if the timestamp field is available
                                if (!record.has(eventTimeConfig.timestampField)) {
                                    throw new RuntimeException("record does not contain field: " + eventTimeConfig.timestampField + " -> " + record);
                                }

                                // set the output lane to use
                                String outputLane = "1";
                                if (devSimulatorConfig.useMultiRecordType) {
                                    outputLane = multiTypeConfig.outputLane(record.get(multiTypeConfig.discriminatorField).getValueAsString());
                                    record.getHeader().setAttribute("_outputLane", outputLane);
                                }

                                // determine the record time milliseconds
                                long recordTimeMs = 0;
                                if (eventTimeConfig.timestampMode.equals(TimestampModeType.RELATIVE_FROM_ANCHOR)) {
                                    String recordTimeMsString = record.get(eventTimeConfig.timestampField).getValueAsString();
                                    recordTimeMs = Math.round(NumberUtils.toDouble(recordTimeMsString));
                                    recordTimeMs = recordTimeMs - eventTimeConfig.fastForwardToTimestamp;
                                    if (eventTimeConfig.relativeTimeResolution.equals(RelativeTimeResolution.SECONDS)) {
                                        recordTimeMs = recordTimeMs * 1000;
                                    }
                                } else if (eventTimeConfig.timestampMode.equals(TimestampModeType.RELATIVE_FROM_PREVIOUS)) {
                                    String recordTimeMsString = record.get(eventTimeConfig.timestampField).getValueAsString();
                                    recordTimeMs = Math.round(NumberUtils.toDouble(recordTimeMsString));

                                    Long previousEventTime = previousTimestampMsPerOutputLane.containsKey(outputLane) ? previousTimestampMsPerOutputLane.get(outputLane) : 0;
                                    Long previousPlusEventTime = previousEventTime + recordTimeMs;
                                    previousTimestampMsPerOutputLane.put(outputLane, previousPlusEventTime);

                                    if (eventTimeConfig.relativeTimeResolution.equals(RelativeTimeResolution.SECONDS)) {
                                        recordTimeMs = previousPlusEventTime * 1000;
                                    }
                                } else if (eventTimeConfig.timestampMode.equals(TimestampModeType.ABSOLUTE) || eventTimeConfig.timestampMode.equals(TimestampModeType.ABSOLUTE_WITH_START)) {
                                    String absoluteTimeString = record.get(eventTimeConfig.timestampField).getValueAsString();
                                    recordTimeMs = DateUtil.parseCustomFormat(eventTimeConfig.getDateMask(), absoluteTimeString) * 1000;
                                    System.out.println ("recordTimeMs: " + recordTimeMs);

                                }

                                csvParsers.get(key).record = record;
                                csvParsers.get(key).recordTimeMs = recordTimeMs;
                                lowestRecordTimeMs = Long.min(lowestRecordTimeMs, recordTimeMs);
                            } else {
                                csvParsers.get(key).record = null;
                                csvParsers.get(key).recordTimeMs = Long.MAX_VALUE;
                            }
                        }
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            // create the currentEventTime in MS which is applicable for this iteration
            long currentEventTimeMs = TimeUtil.generateTimestamp(System.currentTimeMillis(), machineStartTimestampMs, startTimestampMs, eventTimeConfig.speedup);

            // Wait until the lowestRecordTimeMs is reached
            if (lowestRecordTimeMs != Long.MAX_VALUE) {
                waitFor(lowestRecordTimeMs, currentEventTimeMs);
                currentEventTimeMs = TimeUtil.generateTimestamp(System.currentTimeMillis(), machineStartTimestampMs, startTimestampMs, eventTimeConfig.speedup);
            }

            // remove files which are at EOF
            for (String key : keysToRemove) {
                csvParsers.remove(key);
            }

            // process all items where recordTimeMs matches the lowestRecordTimeMs
            hasMore = false;
            for (String key : csvParsers.keySet()) {
                Item item = csvParsers.get(key);
                if (item.recordTimeMs == lowestRecordTimeMs) {
                    hasMore = true;
                    // check if the record with the given recordTimeMs is applicable for publishing
                    if (isApplicable(item.record, item.recordTimeMs, currentEventTimeMs)) {
                        result.add(item.record);
                        csvParsers.get(key).record = null;
                        csvParsers.get(key).recordTimeMs = Long.MAX_VALUE;
                    }
                }
            }
        }

        return result;
    }

    public boolean isApplicable(Record record, long recordTimeMs, long currentEventTimeMs) {
        //String outputLane = record.getHeader().getAttribute("_outputLane");

        if (startTimestampMs == 0) {
            startTimestampMs = recordTimeMs;
        }

        long eventTimestampMs = 0;
        if (eventTimeConfig.timestampMode.equals(TimestampModeType.RELATIVE_FROM_ANCHOR)) {
            eventTimestampMs = startTimestampMs + recordTimeMs;
        } else if (eventTimeConfig.timestampMode.equals(TimestampModeType.RELATIVE_FROM_PREVIOUS)) {
            eventTimestampMs = startTimestampMs + recordTimeMs;
        } else if (eventTimeConfig.timestampMode.equals(TimestampModeType.ABSOLUTE)) {
            eventTimestampMs = recordTimeMs;
        } else if (eventTimeConfig.timestampMode.equals(TimestampModeType.ABSOLUTE_WITH_START)) {
            eventTimestampMs = recordTimeMs;
        } else if (eventTimeConfig.timestampMode.equals(TimestampModeType.FIXED)) {
            // t.b.d.
        }

        record.set(eventTimeConfig.eventTimestampOutputField, Field.create(eventTimestampMs));
        //record.set("/StartEventTimestamp", Field.create(startTimestampMs));
        record.getHeader().setAttribute("startEventTimestamp", String.valueOf(startTimestampMs));

        if (currentEventTimeMs - eventTimestampMs > 500)
            LOG.trace("Lag of > 1s: " + Long.toString(currentEventTimeMs - eventTimestampMs));

        return (eventTimestampMs <= currentEventTimeMs);
    }

    public void waitFor(long recordTimeMs, long currentEventTimeMs) {
        if (startTimestampMs == 0) {
            startTimestampMs = recordTimeMs;
        }

        long eventTimestampMs = 0;
        if (eventTimeConfig.timestampMode.equals(TimestampModeType.RELATIVE_FROM_ANCHOR)) {
            eventTimestampMs = startTimestampMs + recordTimeMs;
        } else if (eventTimeConfig.timestampMode.equals(TimestampModeType.RELATIVE_FROM_PREVIOUS)) {
            eventTimestampMs = startTimestampMs + recordTimeMs;
        } else if (eventTimeConfig.timestampMode.equals(TimestampModeType.ABSOLUTE)) {
            eventTimestampMs = recordTimeMs;
        } else if (eventTimeConfig.timestampMode.equals(TimestampModeType.ABSOLUTE_WITH_START)) {
            eventTimestampMs = recordTimeMs;
        } else if (eventTimeConfig.timestampMode.equals(TimestampModeType.FIXED)) {
            // t.b.d.
        }

        // do we have to wait?
        if (eventTimestampMs > currentEventTimeMs) {
            try {
                Long sleepForMs = Math.max(eventTimestampMs - currentEventTimeMs, 0);
                LOG.trace("sleep for " + sleepForMs + "milliseconds");
                Thread.sleep(sleepForMs);
            } catch (InterruptedException e) {
                // do nothing
                lowestRecordTimeMs = Long.MAX_VALUE;
            }
        }
    }

    private static class Item {
        public Record record;
        public Map<String, Object> recordHeaderAttr;
        public CsvParser parser;
        public long recordTimeMs;

        protected Item (Record record, Map<String, Object> recordHeaderAttr, CsvParser parser) {
            this.record = record;
            this.recordHeaderAttr = recordHeaderAttr;
            this.parser = parser;
            this.recordTimeMs = 0;
        }

        public static Item create(Record record, Map<String, Object> recordHeaderAttr, CsvParser parser) {
            return new Item(record, recordHeaderAttr, parser);
        }
    }
}

