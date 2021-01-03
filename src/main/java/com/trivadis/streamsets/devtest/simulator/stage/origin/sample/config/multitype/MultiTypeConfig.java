package com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.multitype;

import com.streamsets.pipeline.api.ConfigDef;

public class MultiTypeConfig {
    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "",
            label = "Message Type",
            displayPosition = 10,
            description = "The timestamp mode",
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "MULTI_RECORD_TYPES",
            dependsOn = "useMultiRecordType^",
            triggeredByValue = "true"
    )
    public String messageType;
}
