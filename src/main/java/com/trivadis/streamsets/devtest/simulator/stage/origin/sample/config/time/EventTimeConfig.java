package com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ValueChooserModel;

public class EventTimeConfig {

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.MODEL,
            defaultValue = "RELATIVE",
            label = "Timestamp Mode",
            displayPosition = 20,
            description = "The timestamp mode",
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "EVENT_TIME"
    )
    @ValueChooserModel(TimestampModeChooserValues.class)
    public TimestampModeType timestampMode = TimestampModeType.RELATIVE;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.MODEL,
            defaultValue = "",
            label = "Timestamp Field",
            displayPosition = 30,
            description = "Field to use as the timestamp.",
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "EVENT_TIME",
            dependsOn = "timestampMode",
            triggeredByValue = {"ABSOLUTE","RELATIVE","ABSOLUTE_RELATIVE"}
    )
    @FieldSelectorModel(singleValued = true)
    public String timestampField;

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.STRING,
            defaultValue = "",
            label = "Timestamp Format (if not yet in millisecond)",
            displayPosition = 35,
            description = "Timestamp Format if it is not yet an epoc in millisecond.",
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "EVENT_TIME"
    )
    @FieldSelectorModel(singleValued = true)
    public String timestampFormat = null;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.NUMBER,
            defaultValue = "",
            label = "Fixed Delay in milliseconds",
            displayPosition = 30,
            description = "Field to use as the timestamp.",
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "EVENT_TIME",
            dependsOn = "timestampMode",
            triggeredByValue = "FIXED"
    )
    public Integer fixedDelayInMs;

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.STRING,
            defaultValue = "now",
            label = "Match Start Tiemstamp",
            displayPosition = 45,
            description = ".",
            displayMode = ConfigDef.DisplayMode.ADVANCED,
            group = "EVENT_TIME"
    )
    public String matchStartTimestamp;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "/EventTimestamp",
            label = "Event Timestamp Output Field",
            displayPosition = 50,
            description = "The name of the field holding the event timestamp calculated from the starting time plus the value from the record.",
            displayMode = ConfigDef.DisplayMode.ADVANCED,
            group = "EVENT_TIME"
    )
    public String eventTimestampField;
}
