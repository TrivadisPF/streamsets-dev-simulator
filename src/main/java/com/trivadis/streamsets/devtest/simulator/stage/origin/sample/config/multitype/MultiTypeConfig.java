package com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.multitype;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ListBeanModel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class MultiTypeConfig {
    @ConfigDef(
            required = true,
            type = ConfigDef.Type.MODEL,
            defaultValue = "/",
            label = "Descriminator field",
            description = "Name of the field to derive the message type from",
            displayPosition = 10,
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "MULTI_RECORD_TYPES",
            dependsOn = "^basicConfig.useMultiRecordType",
            triggeredByValue = "true"
    )
    @FieldSelectorModel(singleValued = true)
    public String discriminatorField;

    @ConfigDef(
            label = "Data Types",
            required = false,
            type = ConfigDef.Type.MODEL,
            defaultValue="",
            description="Fields to generate of the indicated type",
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "MULTI_RECORD_TYPES",
            dependsOn = "^basicConfig.useMultiRecordType",
            triggeredByValue = "true"
    )
    @ListBeanModel
    public List<MessageTypeConfig> messageTypes = Collections.singletonList(new MessageTypeConfig());

    /**
     * Check if the configuration contains the given value
     * @param value
     * @return
     */
    public boolean contains(String value) {
        return messageTypes.stream().anyMatch(t -> t.discriminatorValue.equals(value));
    }

    public String outputLane(String value) {
        List<String> outputLanes = messageTypes.stream().filter(t -> t.discriminatorValue.equals(value)).map(t -> t.outputLane).collect(Collectors.toList());
        return outputLanes.get(0);
    }

}
