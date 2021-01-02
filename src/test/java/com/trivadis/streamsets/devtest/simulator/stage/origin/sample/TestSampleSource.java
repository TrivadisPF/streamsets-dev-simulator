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
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format.CsvConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format.CsvHeader;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format.DataFormatType;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time.EventTimeConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time.TimestampModeType;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestSampleSource {
  private static final int MAX_BATCH_SIZE = 5;

  private PushSourceRunner runner;
  @Test
  public void testOrigin() throws Exception {
    Configuration.setFileRefsBaseDir(new File("/Users/gus/workspace/git/trivadispf/streamsets-dev-simulator/src/test/resources"));

    CsvConfig csvConfig = new CsvConfig();
    csvConfig.csvCustomDelimiter = ',';
    csvConfig.csvHeader = CsvHeader.USE_HEADER;

    EventTimeConfig eventTimeConfig = new EventTimeConfig();
    eventTimeConfig.timestampField = "/Timestamp";
    eventTimeConfig.timestampMode = TimestampModeType.RELATIVE;
    eventTimeConfig.eventTimestampField = "/EventTimestamp";
    eventTimeConfig.matchStartTimestamp = "now";

    SampleDSource origin = new SampleDSource();
    origin.csvConfig = csvConfig;
    origin.eventTimeConfig = eventTimeConfig;

    runner = new PushSourceRunner.Builder(SampleDSource.class, origin)
        .addConfiguration("fileNamePattern", ".+.csv")
            .addConfiguration("batchSize", 10)
            .addConfiguration("inputDataFormat", DataFormatType.AS_DELIMITED)
            .addConfiguration("filesDirectory", "/Users/gus/workspace/git/trivadispf/streamsets-dev-simulator/src/test/resources/data/ball")
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

    } finally {
      runner.runDestroy();
    }
  }

}
