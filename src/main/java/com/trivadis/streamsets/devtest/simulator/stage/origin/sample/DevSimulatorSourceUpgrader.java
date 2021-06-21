package com.trivadis.streamsets.devtest.simulator.stage.origin.sample;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.lineage.EndPointType;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageEventType;
import com.streamsets.pipeline.api.lineage.LineageSpecificAttribute;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.DevSimulatorConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.files.PathMatcherMode;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format.CsvConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.multitype.MultiTypeConfig;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time.EventTimeConfig;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Joiner;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class DevSimulatorSourceUpgrader implements StageUpgrader {
    private static final String BASIC_CONFIG = "basicConfig";

    private static final Joiner joiner = Joiner.on(".");

    private final List<Config> configsToRemove = new ArrayList<>();
    private final List<Config> configsToAdd = new ArrayList<>();

    @Override
    public List<Config> upgrade(String library, String stageName, String stageInstance, int fromVersion, int toVersion, List<Config> configs) throws StageException {
        switch (fromVersion) {
            case 1:
                upgradeV1ToV2(configs);
                if (toVersion == 2) {
                    break;
                }
            default:
                throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));

        }
        return configs;
    }


    private void upgradeV1ToV2(List<Config> configs) {
        configsToRemove.clear();

        for (Config config : configs) {
            if (joiner.join(BASIC_CONFIG, "minBufferSize").equals(config.getName())) {
                configsToRemove.add(config);
            }
            if (joiner.join(BASIC_CONFIG, "maxBufferSize").equals(config.getName())) {
                configsToRemove.add(config);
            }

        }


        configs.removeAll(configsToRemove);
        configs.addAll(configsToAdd);
    }
}
