package com.trivadis.streamsets.devtest.simulator.stage.origin.sample;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.service.dataformats.DataParserException;
import com.streamsets.pipeline.api.service.dataformats.RecoverableDataParserException;
import com.trivadis.streamsets.devtest.simulator.stage.lib.sample.Errors;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format.CsvConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format.CsvHeader;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format.CsvRecordType;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time.EventTimeConfig;
import com.trivadis.streamsets.sdc.csv.CsvParser;

import java.io.IOException;
import java.util.*;

public class BufferedDataStreamFileReader {

    /**
     * The CSV Config object
     */
    private final CsvConfig csvConfig;

    /**
     * The Event Time Config object
     */
    private final EventTimeConfig eventTimeConfig;

    /**
     * Minimal number of elements in the buffer (if the end of the file is not reached yet)
     */
    private final int minBufferSize;

    /**
     * Maximal number of elements in the buffer
     */
    private final int maxBufferSize;

    /**
     * Reflects if the end of the file is already reached
     */
    private boolean fileEnd = false;

    /**
     * CsvParser
     */
    private List<CsvParser> csvParsers = new ArrayList<>();


    /**
     * Buffer storing prefetched objects
     */
    private NavigableMap<Long, List<Record>> buffer;

    private PushSource.Context context;

    private int timestampPosition;

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

    public BufferedDataStreamFileReader(PushSource.Context context, EventTimeConfig eventTimeConfig, CsvConfig csvConfig, CsvParser parser, int timestampPosition, int minBufferSize, int maxBufferSize) throws IOException {

        buffer = Collections.synchronizedNavigableMap(new TreeMap<>());

        this.context = context;
        this.csvConfig = csvConfig;
        this.eventTimeConfig = eventTimeConfig;
        this.csvParsers.add(parser);
        this.minBufferSize = minBufferSize;
        this.maxBufferSize = maxBufferSize;
        this.timestampPosition = timestampPosition;
    }

    public BufferedDataStreamFileReader(PushSource.Context context, EventTimeConfig eventTimeConfig, CsvConfig csvConfig, List<CsvParser> parsers, int timestampPosition, int minBufferSize, int maxBufferSize) throws IOException {

        buffer = Collections.synchronizedNavigableMap(new TreeMap<>());

        this.context = context;
        this.csvConfig = csvConfig;
        this.eventTimeConfig = eventTimeConfig;
        this.csvParsers = parsers;
        this.minBufferSize = minBufferSize;
        this.maxBufferSize = maxBufferSize;
        this.timestampPosition = timestampPosition;
    }


    public List<Field> getHeaders() throws IOException {
        List<Field> headers = null;
        if (csvConfig.csvHeader != CsvHeader.NO_HEADER) {
            String[] hs = csvParsers.get(0).getHeaders();
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

        if (!this.fileEnd && this.buffer.size() < this.maxBufferSize) {
            String[] line;

            try {
                while (this.buffer.size() < this.maxBufferSize) {
                    int actualParser = recordCount % csvParsers.size();
                    line = csvParsers.get(actualParser).read();

                    if (line == null) {
                        this.fileEnd = true;
                        return;
                    } else {
                        Record record = createRecord(context, 1, getHeaders(), line);
                        if (!record.has(eventTimeConfig.timestampField)) {
                            throw new RuntimeException("record does not contain field: " +  eventTimeConfig.timestampField + " -> " + record);
                        }
                        long eventTime = record.get(eventTimeConfig.timestampField).getValueAsLong();
                        if (!buffer.containsKey(eventTime)) {
                            buffer.put(eventTime, new ArrayList<Record>());
                        }
                        buffer.get(eventTime).add(record);
                    }
                    recordCount++;
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
                    fillBuffer();
                }
                return res.getValue();
            }
        } else {

            // throw
        }
        return new ArrayList<Record>();
    }


    public List<Record> pollFromBuffer() {
        if (buffer.size() > 0) {
            Map.Entry<Long, List<Record>> res = buffer.firstEntry();
            buffer.remove(res.getKey());

            if (!this.fileEnd && this.buffer.size() < this.minBufferSize) {
                fillBuffer();
            }
            return res.getValue();
        } else {
            fillBuffer();
            // throw
        }
        return new ArrayList<Record>();
    }
}
