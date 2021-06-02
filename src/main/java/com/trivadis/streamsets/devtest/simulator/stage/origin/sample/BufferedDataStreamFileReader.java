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
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time.EventTimeConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time.RelativeTimeResolution;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time.TimestampModeType;
import com.trivadis.streamsets.sdc.csv.CsvParser;
import org.apache.commons.csv.CSVFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class BufferedDataStreamFileReader {
    private static final Logger LOG = LoggerFactory.getLogger(BufferedDataStreamFileReader.class);

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
     * a list of discriminators, if multiple types should be supported
     */
    //private List<String> discriminators = new ArrayList<>();

    /**
     * Minimal number of elements in the buffer (if the end of the file is not reached yet)
     */
    private int minBufferSize;

    /**
     * Maximal number of elements in the buffer
     */
    private int maxBufferSize;

    /**
     * Reflects if the end of the file is already reached
     */
    private boolean fileEnd = false;

    /**
     * CsvParser
     */
    private List<CsvParser> csvParsers = new ArrayList<>();

    /**
     * Record Header Attributes
     */
    private List<Map<String, Object>> recordHeaderAttrList = new ArrayList<>();

    /**
     * Buffer storing prefetched objects
     */
    private NavigableMap<Long, List<Record>> buffer;

    private Map<String, Long> previousTimestampMsPerOutputLane = null;

    private PushSource.Context context;

    private int timestampPosition;
    private SimpleDateFormat dateFormatter = null;

    private int recordCount = 0;

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

    private BufferedDataStreamFileReader() throws IOException {
        buffer = Collections.synchronizedNavigableMap(new TreeMap<>());
        this.minBufferSize = 10;
        this.maxBufferSize = 100;
    }

    public static BufferedDataStreamFileReader create() throws IOException {
        return new BufferedDataStreamFileReader();
    }

    public BufferedDataStreamFileReader withContext(PushSource.Context context) {
        this.context = context;
        return this;
    }

    public BufferedDataStreamFileReader withConfig(DevSimulatorConfig devSimulatorConfig, EventTimeConfig eventTimeConfig, CsvConfig csvConfig, MultiTypeConfig multiTypeConfig) {
        this.eventTimeConfig = eventTimeConfig;
        this.csvConfig = csvConfig;
        this.multiTypeConfig = multiTypeConfig;
        this.devSimulatorConfig = devSimulatorConfig;

        if (eventTimeConfig.getDateMask() != null) {
            dateFormatter = new SimpleDateFormat(eventTimeConfig.getDateMask());
        }

        if (eventTimeConfig.timestampMode.equals(TimestampModeType.RELATIVE_FROM_PREVIOUS)) {
            previousTimestampMsPerOutputLane = new HashMap<>();
        }
        return this;
    }

    public BufferedDataStreamFileReader withFiles(Collection<File> files) throws IOException {
        for (File file : files) {
            csvParsers.add(createParser(new FileReader(file)));
            recordHeaderAttrList.add(generateHeaderAttrs(file));
        }
        return this;
    }

    public BufferedDataStreamFileReader withMinBufferSize (int minBufferSize) {
        this.minBufferSize = minBufferSize;
        return this;
    }

    public BufferedDataStreamFileReader withMaxBufferSize (int maxBufferSize) {
        this.maxBufferSize = maxBufferSize;
        return this;
    }

    public List<Field> getHeaders(int parser) throws IOException {
        List<Field> headers = null;
        if (csvConfig.csvHeader != CsvHeader.NO_HEADER) {
            String[] hs = csvParsers.get(parser).getHeaders();
            if (csvConfig.csvHeader != CsvHeader.IGNORE_HEADER && hs != null) {
                headers = new ArrayList<>();
                for (String h : hs) {
                    headers.add(Field.create(h));
                }
            }
        }
        return headers;
    }

    public void fillBuffer() {
        LOG.info("fillBuffer() called buffer.size() = " + buffer.size() + ": maxBufferSize =" + this.maxBufferSize);
        if (!this.fileEnd && this.buffer.size() < this.maxBufferSize) {
            String[] line;
            try {
                while (!fileEnd && this.buffer.size() < this.maxBufferSize) {
                    try {
                        int actualParser = recordCount % csvParsers.size();
                        line = csvParsers.get(actualParser).read();

                        if (line == null) {
                            csvParsers.remove(actualParser);
                            this.fileEnd = csvParsers.isEmpty();

                            if (fileEnd)
                                LOG.info("fillBuffer() no more records, buffer.size() = " + buffer.size());
                        } else {
                            Record record = createRecord(context, 1, getHeaders(actualParser), line);
                            setHeaders(record, recordHeaderAttrList.get(actualParser));

                            // handle record types
                            if (!devSimulatorConfig.useMultiRecordType ||
                                    (multiTypeConfig != null &&
                                    multiTypeConfig.contains(record.get(multiTypeConfig.discriminatorField).getValueAsString()))) {

                                String outputLane = "1";
                                if (devSimulatorConfig.useMultiRecordType) {
                                    outputLane = multiTypeConfig.outputLane(record.get(multiTypeConfig.discriminatorField).getValueAsString());
                                    record.getHeader().setAttribute("_outputLane", outputLane);
                                }

                                if (!record.has(eventTimeConfig.timestampField)) {
                                    throw new RuntimeException("record does not contain field: " + eventTimeConfig.timestampField + " -> " + record);
                                }

                                long eventTime = 0;
                                if (eventTimeConfig.timestampMode.equals(TimestampModeType.RELATIVE_FROM_ANCHOR)) {
                                    eventTime = record.get(eventTimeConfig.timestampField).getValueAsLong() - eventTimeConfig.fastForwardToTimestamp;
                                    if (eventTimeConfig.relativeTimeResolution.equals(RelativeTimeResolution.SECONDS)) {
                                        eventTime = eventTime * 1000;
                                    }
                                } else if (eventTimeConfig.timestampMode.equals(TimestampModeType.RELATIVE_FROM_PREVIOUS)) {
                                    eventTime = record.get(eventTimeConfig.timestampField).getValueAsLong();

                                    Long previousEventTime = previousTimestampMsPerOutputLane.containsKey(outputLane) ? previousTimestampMsPerOutputLane.get(outputLane) : 0;
                                    Long previousPlusEventTime = previousEventTime + eventTime;
                                    previousTimestampMsPerOutputLane.put(outputLane, previousPlusEventTime);

                                    if (eventTimeConfig.relativeTimeResolution.equals(RelativeTimeResolution.SECONDS)) {
                                        eventTime = previousPlusEventTime * 1000;
                                    }
                                } else if (eventTimeConfig.timestampMode.equals(TimestampModeType.ABSOLUTE)) {
                                    String absoluteTimeString = record.get(eventTimeConfig.timestampField).getValueAsString();
                                    Date absoluteTime = dateFormatter.parse(absoluteTimeString);
                                    eventTime = absoluteTime.getTime();
                                }

                                if (!buffer.containsKey(eventTime)) {
                                    buffer.put(eventTime, new ArrayList<Record>());
                                }
                                buffer.get(eventTime).add(record);
                            }
                            recordCount++;
                        }
                    } catch (ParseException e) {
                        throw new StageException(Errors.DEV_SIMULATOR_002, e.toString(), e);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public List<Record> pollFromBuffer(Long beforeTimestamp) {
        if (buffer.size() > 0) {
            if (buffer.firstKey() < beforeTimestamp) {
                Map.Entry<Long, List<Record>> res = buffer.firstEntry();
                buffer.remove(res.getKey());

                if (!this.fileEnd && this.buffer.size() < this.minBufferSize) {
                    LOG.info("(1) invoke fillBuffer() ");

                    fillBuffer();
                }
                return res.getValue();
            }
        } else {

            // throw
        }
        return new ArrayList<Record>();
    }


    public Map.Entry<Long, List<Record>> pollFromBuffer() {
        if (buffer.size() > 0) {
            Map.Entry<Long, List<Record>> res = buffer.firstEntry();
            buffer.remove(res.getKey());

            if (!this.fileEnd && this.buffer.size() < this.minBufferSize) {
                LOG.info("(2) invoke fillBuffer() ");

                fillBuffer();
            }
            return res;
        } else {
            LOG.info("(3) invoke fillBuffer() ");

            fillBuffer();
            // throw
        }
        return null;
    }
}
