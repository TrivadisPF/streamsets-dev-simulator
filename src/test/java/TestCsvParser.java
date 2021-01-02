import com.trivadis.streamsets.sdc.csv.CsvParser;
import org.apache.commons.csv.CSVFormat;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;

public class TestCsvParser {

    CsvParser parser = null;

    @Test
    public void testParse() throws Exception {
        CSVFormat format = CSVFormat.DEFAULT.withFirstRecordAsHeader().withSkipHeaderRecord(false);
        Reader reader = new FileReader(new File("/Users/gus/workspace/git/trivadispf/streamsets-dev-simulator/src/test/resources/data/ball/200.csv"));
        parser = new CsvParser(reader, format, 1000);

        String[] result = parser.read();
        //System.out.println(parser.getHeaders().toString());
        System.out.println(result[0]);
    }

}
