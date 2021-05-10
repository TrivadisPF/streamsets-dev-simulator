package com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.multitype;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;

public class MessageTypeConfig {

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "",
            label = "Stream",
            description = "Records that match the specified discriminator pass to the stream",
            displayPosition = 100,
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "MULTI_RECORD_TYPES"
    )
    public String outputLane;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "",
            label = "Descriminator value",
            description = "Value the discriminator field holds to determine a message type",
            displayPosition = 105,
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "MULTI_RECORD_TYPES"
    )
    public String discriminatorValue;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "",
            label = "Message Type Name",
            description = "The name of the message type",
            displayPosition = 110,
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "MULTI_RECORD_TYPES"
    )
    public String name;
}
