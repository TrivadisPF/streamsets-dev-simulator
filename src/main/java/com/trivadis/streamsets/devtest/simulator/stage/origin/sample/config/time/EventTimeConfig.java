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
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "/EventTimestamp",
            label = "Event Timestamp Output Field",
            displayPosition = 40,
            description = "The name of the field holding the event timestamp calculated from the starting time plus the value from the record.",
            displayMode = ConfigDef.DisplayMode.ADVANCED,
            group = "EVENT_TIME"
    )
    public String eventTimestampField;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.BOOLEAN,
            defaultValue = "true",
            label = "Simulation Start Time is Now",
            displayPosition = 45,
            description = "Should the simulation start time be now, i.e. current system time?",
            displayMode = ConfigDef.DisplayMode.ADVANCED,
            group = "EVENT_TIME",
            dependsOn = "timestampMode",
            triggeredByValue = {"RELATIVE"}
    )
    public boolean simulationStartNow;

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.STRING,
            defaultValue = "",
            label = "Simulation Start Timestamp",
            displayPosition = 45,
            description = "The timestamp to use as the start timestamp for the simulation, use now to specify current system time.",
            displayMode = ConfigDef.DisplayMode.ADVANCED,
            group = "EVENT_TIME",
            dependsOn = "simulationStartNow",
            triggeredByValue = {"false"}
    )
    public String simulationStartTimestamp;

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.MODEL,
            defaultValue="HH_MM_SS",
            label = "Date Format",
            description="Select or enter any valid date or datetime format",
            displayPosition = 50,
            displayMode = ConfigDef.DisplayMode.ADVANCED,
            group = "EVENT_TIME",
            dependsOn = "simulationStartNow",
            triggeredByValue = {"false"}
    )
    @ValueChooserModel(DateFormatChooserValues.class)
    public DateFormat simulationStartTimestampDateFormat;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "",
            label = "Other Date Format",
            displayPosition = 55,
            displayMode = ConfigDef.DisplayMode.ADVANCED,
            dependsOn = "simulationStartTimestampDateFormat",
            group = "EVENT_TIME",
            triggeredByValue = "OTHER"
    )
    public String simulationStartTimestampDateOtherDateFormat;

}
