package com.trivadis.streamsets.devtest.simulator.stage.origin.sample;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.lineage.EndPointType;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageEventType;
import com.streamsets.pipeline.api.lineage.LineageSpecificAttribute;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.DevSimulatorConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.files.PathMatcherMode;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format.CsvConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format.CsvHeader;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format.DataFormatType;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.multitype.MultiTypeConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time.AnchorDateFormat;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time.EventTimeConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time.SimulationStartDateFormat;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time.TimestampModeType;
import com.trivadis.streamsets.sdc.csv.CsvParser;
import org.apache.commons.csv.CSVFormat;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class TestDataStreamFileReader {

    static class ForTestDevSimulatorSource extends DevSimulatorSource {
        public ForTestDevSimulatorSource(DevSimulatorConfig devSimulatorConfig, EventTimeConfig eventTimeConfig, MultiTypeConfig multiTypeConfig, CsvConfig csvConfig) {
            super(devSimulatorConfig, eventTimeConfig, multiTypeConfig, csvConfig);
        }

        public PushSource.Context getContext() {
            return super.getContext();
        }

    }

    CsvParser parser = null;

    @Test
    public void testRead() throws Exception {

        DevSimulatorConfig basicConfig = new DevSimulatorConfig();
        basicConfig.fileNamePattern = ".+.csv";
        //basicConfig.minBufferSize = 10;
        //basicConfig.maxBufferSize = 100;
        basicConfig.inputDataFormat = DataFormatType.AS_DELIMITED;
        basicConfig.includeSubdirectories = true;
        basicConfig.pathMatcherMode = PathMatcherMode.REGEX;
        basicConfig.filesDirectory = "/Users/gus/workspace/git/trivadispf/streamsets-dev-simulator/src/test/resources/data";

        CsvConfig csvConfig = new CsvConfig();
        csvConfig.csvCustomDelimiter = ',';
        csvConfig.csvHeader = CsvHeader.NO_HEADER;

        MultiTypeConfig multiTypeConfig = new MultiTypeConfig();

        EventTimeConfig eventTimeConfig = new EventTimeConfig();
        eventTimeConfig.timestampField = "/0";
        eventTimeConfig.timestampMode = TimestampModeType.ABSOLUTE_WITH_START;
        eventTimeConfig.eventTimestampOutputField = "/EventTimestamp";
        eventTimeConfig.anchorTimestampDateFormat = AnchorDateFormat.YYYY_MM_DD_T_HH_MM_SS_Z;
        eventTimeConfig.simulationStartTimestamp = "2021-11-16T09:00:06+0100";
        eventTimeConfig.simulationStartTimestampDateFormat = SimulationStartDateFormat.YYYY_MM_DD_T_HH_MM_SS_Z;
        eventTimeConfig.speedup = 1.0;

        Collection<File> files = new ArrayList<>();
        files.add(new File ("/Users/guido.schmutz/Documents/GitHub/trivadispf/streamsets-dev-simulator/src/test/resources/data/absolute-without-header.csv"));

        long machineStartTimestampMs = System.currentTimeMillis() + eventTimeConfig.delayMs;

        ForTestDevSimulatorSource source = new ForTestDevSimulatorSource(basicConfig, eventTimeConfig, multiTypeConfig, csvConfig);

        source = Mockito.spy(source);
        PushSource.Context context = Mockito.mock(PushSource.Context.class);

        DataStreamFileReader dataStream = DataStreamFileReader.create()
                .withContext(context)
                .withConfig(basicConfig, eventTimeConfig, csvConfig, multiTypeConfig)
                .withFiles(files)
                .withTimestamps(machineStartTimestampMs);

        List<Record> records = dataStream.read();
    }

}
