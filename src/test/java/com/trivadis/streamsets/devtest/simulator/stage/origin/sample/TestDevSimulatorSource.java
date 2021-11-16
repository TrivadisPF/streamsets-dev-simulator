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

import _ss_com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.DevSimulatorConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.files.PathMatcherMode;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format.CsvConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format.CsvHeader;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format.DataFormatType;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.multitype.MultiTypeConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time.AnchorDateFormat;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time.EventTimeConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time.TimestampModeType;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestDevSimulatorSource {
  private static final int MAX_BATCH_SIZE = 5;

  private PushSourceRunner runner;
  @Test
  public void testOrigin() throws Exception {
    Configuration.setFileRefsBaseDir(new File("/Users/gus/workspace/git/trivadispf/streamsets-dev-simulator/src/test/resources"));

    DevSimulatorConfig basicConfig = new DevSimulatorConfig();
    basicConfig.fileNamePattern = ".+.csv";
    //basicConfig.minBufferSize = 10;
    //basicConfig.maxBufferSize = 100;
    basicConfig.inputDataFormat = DataFormatType.AS_DELIMITED;
    basicConfig.includeSubdirectories = true;
    basicConfig.pathMatcherMode = PathMatcherMode.REGEX;
    basicConfig.filesDirectory = "/Users/gus/workspace/git/trivadispf/streamsets-dev-simulator/src/test/resources/data/ball";

    CsvConfig csvConfig = new CsvConfig();
    csvConfig.csvCustomDelimiter = ',';
    csvConfig.csvHeader = CsvHeader.USE_HEADER;

    MultiTypeConfig multiTypeConfig = new MultiTypeConfig();

    EventTimeConfig eventTimeConfig = new EventTimeConfig();
    eventTimeConfig.timestampField = "/Timestamp";
    eventTimeConfig.timestampMode = TimestampModeType.RELATIVE_FROM_ANCHOR;
    eventTimeConfig.eventTimestampOutputField = "/EventTimestamp";
    eventTimeConfig.anchorTimeNow = true;
    eventTimeConfig.anchorTimestamp = "16:00:00";
    eventTimeConfig.anchorTimestampDateFormat = AnchorDateFormat.HH_MM_SS;
    eventTimeConfig.speedup = 1.0;

    DevSimulatorDSource origin = new DevSimulatorDSource();
    origin.basicConfig = basicConfig;
    origin.csvConfig = csvConfig;
    origin.eventTimeConfig = eventTimeConfig;
    origin.multiTypeConfig = multiTypeConfig;

    runner = new PushSourceRunner.Builder(DevSimulatorDSource.class, origin)
        .addOutputLane("lane")
        .build();

    try {
      runner.runInit();

      final List<Record> records = new ArrayList<>();
        runner.runProduce(Collections.<String, String>emptyMap(), 100, new PushSourceRunner.Callback() {
            @Override
            public void processBatch(StageRunner.Output output) {
                records.clear();
                records.addAll(output.getRecords().get("lane"));
                runner.setStop();
            }
        });
        runner.waitOnProduce();
        System.out.println(records);
        System.out.println(records.get(0).getHeader().getAttribute("filename"));
        System.out.println(records.get(0).getHeader().getAttribute("baseDir"));

    } finally {
      runner.runDestroy();
    }
  }

}
