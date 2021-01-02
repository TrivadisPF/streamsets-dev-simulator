import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.BufferedDataStreamFileReader;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format.CsvConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format.CsvHeader;
import com.trivadis.streamsets.sdc.csv.CsvParser;
import org.apache.commons.csv.CSVFormat;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestBufferedDataStreamFileReader {

    CsvParser parser = null;

    private void println(List<String[]> records) {
        for (String[] record: records) {
            System.out.println (Arrays.asList(record));
        }
    }
/*
    @Test
    public void testOneFile() throws IOException {
        CSVFormat format = CSVFormat.DEFAULT.withFirstRecordAsHeader().withSkipHeaderRecord(false);
        Reader reader = new FileReader(new File("/Users/gus/workspace/git/trivadispf/streamsets-dev-simulator/src/test/resources/data/ball/200.csv"));
        parser = new CsvParser(reader, format, 1000);

        CsvConfig csvConfig = new CsvConfig();
        csvConfig.csvCustomDelimiter = ',';
        csvConfig.csvHeader = CsvHeader.USE_HEADER;



        BufferedDataStreamFileReader bdsfr = new BufferedDataStreamFileReader(parser, 0, 10, 100);
        bdsfr.fillBuffer();
        List<String[]> records = bdsfr.pollFromBuffer(100L);
        println(records);
    }


    @Test
    public void testTwoFiles() throws IOException {
        List<CsvParser> parsers = new ArrayList<>();
        CSVFormat format = CSVFormat.DEFAULT.withFirstRecordAsHeader().withSkipHeaderRecord(false);
        Reader reader = new FileReader(new File("/Users/gus/workspace/git/trivadispf/streamsets-dev-simulator/src/test/resources/data/ball/200.csv"));
        parsers.add(new CsvParser(reader, format, 1000));

        Reader reader2 = new FileReader(new File("/Users/gus/workspace/git/trivadispf/streamsets-dev-simulator/src/test/resources/data/away/101.csv"));
        parsers.add(new CsvParser(reader2, format, 1000));

        BufferedDataStreamFileReader bdsfr = new BufferedDataStreamFileReader(parsers, 0, 10, 100);
        bdsfr.fillBuffer();
        List<String[]> records = bdsfr.pollFromBuffer(100L);
        println(records);
    }
*/
}
