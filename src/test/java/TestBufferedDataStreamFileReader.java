import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.BaseStage;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.DevSimulatorDSource;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.DevSimulatorConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format.CsvConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format.CsvHeader;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format.CsvMode;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format.DataFormatType;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.multitype.MultiTypeConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time.EventTimeConfig;
import com.trivadis.streamsets.sdc.csv.CsvParser;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TestBufferedDataStreamFileReader {

    CsvParser parser = null;
    BaseStage stage;
    PushSourceRunner runner;

    private void println(List<String[]> records) {
        for (String[] record: records) {
            System.out.println (Arrays.asList(record));
        }
    }


    protected PushSourceRunner newStageRunner(String outputLane) {
        PushSourceRunner output = new PushSourceRunner.Builder(DevSimulatorDSource.class, (PushSource) this.stage).addOutputLane("output").build();
        return output;
    }

    @Test
    public void testOneFile() throws IOException {
        List<File> files = new ArrayList<>();
        files.add(new File("/Users/gus/workspace/GitHub/trivadispf/streamsets-dev-simulator/src/test/resources/data/product.csv"));

        DevSimulatorConfig devSimulatorConfig = new DevSimulatorConfig();
        devSimulatorConfig.inputDataFormat = DataFormatType.AS_DELIMITED;

        CsvConfig csvConfig = new CsvConfig();
        csvConfig.csvCustomDelimiter = ',';
        csvConfig.csvFileFormat = CsvMode.CSV;
        csvConfig.csvHeader = CsvHeader.USE_HEADER;

        EventTimeConfig eventTimeConfig = new EventTimeConfig();

        MultiTypeConfig multiTypeConfig = new MultiTypeConfig();
/*
        BufferedDataStreamFileReader bdsfr = BufferedDataStreamFileReader.create()
//                                                                .withContext(runner.getContext())
                                                                .withConfig(devSimulatorConfig, eventTimeConfig, csvConfig, multiTypeConfig)
                                                                .withFiles(files)
                                                                .withMinBufferSize(10).withMaxBufferSize(100);
        bdsfr.fillBuffer();
        Map.Entry<Long, List<Record>> entry = bdsfr.pollFromBuffer();
        System.out.println(entry);

 */
    }

/*
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
