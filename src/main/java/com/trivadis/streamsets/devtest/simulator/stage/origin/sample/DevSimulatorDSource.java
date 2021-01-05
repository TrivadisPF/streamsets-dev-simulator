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

import com.codahale.metrics.Timer;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.api.base.configurablestage.DPushSource;
import com.streamsets.pipeline.api.base.configurablestage.DSource;
import com.trivadis.streamsets.devtest.simulator.stage.lib.sample.Errors;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.DevSimulatorConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.files.PathMatcherMode;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format.CsvConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.multitype.MultiTypeConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time.DateUtil;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time.EventTimeConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time.TimestampModeType;
import com.trivadis.streamsets.sdc.csv.CsvParser;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@GenerateResourceBundle
@StageDef(
        version = 1,
        label = "Dev Simulator",
        description = "Reads files with data and simulates it as messages considering the timestamp for replaying. For development only",
        icon = "dev.png",
        execution = {ExecutionMode.STANDALONE, ExecutionMode.EDGE},
        producesEvents = true,
        recordsByRef = true,
        onlineHelpRefUrl = ""
)
@ConfigGroups(value = Groups.class)
public class DevSimulatorDSource extends DPushSource {

    private static final Logger LOG = LoggerFactory.getLogger(DevSimulatorDSource.class);

    private final int EVENT_VERSION = 1;

    @ConfigDefBean()
    public DevSimulatorConfig basicConfig;

    @ConfigDefBean(groups = {"MULTI_RECORD_TYPES"})
    public MultiTypeConfig multiTypeConfig;

    @ConfigDefBean(groups = {"EVENT_TIME"})
    public EventTimeConfig eventTimeConfig;

    @ConfigDefBean(groups = {"DATA_FORMAT"})
    public CsvConfig csvConfig;

    @Override
    protected PushSource createPushSource() {
        return new DevSimulatorSource(basicConfig, eventTimeConfig, multiTypeConfig, csvConfig);
    }
}
