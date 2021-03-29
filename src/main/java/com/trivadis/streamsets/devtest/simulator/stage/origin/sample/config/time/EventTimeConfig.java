package com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ValueChooserModel;

import java.time.format.DateTimeFormatter;

public class EventTimeConfig {

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.MODEL,
            defaultValue = "RELATIVE",
            label = "Timestamp Mode",
            description = "How to retrieve the timestamp of the message.",
            displayPosition = 20,
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
            description = "Field to use as the timestamp.",
            displayPosition = 30,
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "EVENT_TIME",
            dependsOn = "timestampMode",
            triggeredByValue = {"ABSOLUTE","RELATIVE","ABSOLUTE_RELATIVE"}
    )
    @FieldSelectorModel(singleValued = true)
    public String timestampField;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.NUMBER,
            defaultValue = "",
            label = "Fixed Time Delta (ms)",
            description = "A fixed Time Delta in milliseconds between each message.",
            displayPosition = 30,
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "EVENT_TIME",
            dependsOn = "timestampMode",
            triggeredByValue = {"FIXED"}
    )
    public Long fixedTimeDeltaMs;

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.MODEL,
            defaultValue="MILLISECONDS",
            label = "Relative Time Resolution",
            description="Select the relative time resolution",
            displayPosition = 35,
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "EVENT_TIME",
            dependsOn = "timestampMode",
            triggeredByValue = {"RELATIVE","ABSOLUTE_RELATIVE"}
    )
    @ValueChooserModel(RelativeTimeResolutionChooserValues.class)
    public RelativeTimeResolution relativeTimeResolution = RelativeTimeResolution.MILLISECONDS;

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.MODEL,
            defaultValue="",
            label = "Timestamp Format",
            description="Select or enter any valid date or datetime format",
            displayPosition = 37,
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "EVENT_TIME",
            dependsOn = "timestampMode",
            triggeredByValue = {"ABSOLUTE","ABSOLUTE_RELATIVE"}
    )
    @ValueChooserModel(TimestampDateFormatChooserValues.class)
    public TimestampDateFormat timestampDateFormat;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "",
            label = "Other Date Format",
            displayPosition = 39,
            displayMode = ConfigDef.DisplayMode.BASIC,
            dependsOn = "timestampDateFormat",
            group = "EVENT_TIME",
            triggeredByValue = "OTHER"
    )
    public String otherTimestampDateFormat;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "/EventTimestamp",
            label = "Event Timestamp Output Field",
            description = "Name of the field in the record to hold the calculated event timestamp.",
            displayPosition = 40,
            displayMode = ConfigDef.DisplayMode.ADVANCED,
            group = "EVENT_TIME"
    )
    public String eventTimestampOutputField;

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
            label = "Simulation Start Timestamp Format",
            description="Select or enter any valid date or datetime format",
            displayPosition = 50,
            displayMode = ConfigDef.DisplayMode.ADVANCED,
            group = "EVENT_TIME",
            dependsOn = "simulationStartNow",
            triggeredByValue = {"false"}
    )
    @ValueChooserModel(SimulationStartDateFormatChooserValues.class)
    public SimulationStartDateFormat simulationStartTimestampDateFormat = SimulationStartDateFormat.HH_MM_SS;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "",
            label = "Other Simulation Start Timestamp Format",
            displayPosition = 55,
            displayMode = ConfigDef.DisplayMode.ADVANCED,
            group = "EVENT_TIME",
            dependsOn = "simulationStartTimestampDateFormat",
            triggeredByValue = "OTHER"
    )
    public String simulationStartTimestampDateOtherDateFormat;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.NUMBER,
            defaultValue = "0",
            label = "Delay Sending Messages",
            displayPosition = 60,
            description = "Delay sending first message my milliseconds",
            displayMode = ConfigDef.DisplayMode.ADVANCED,
            group = "EVENT_TIME"
    )
    public long delayMs = 0;

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.NUMBER,
            defaultValue = "1",
            label = "Speedup Factor",
            description = "Speedup value for the match simulation (1.0 = normal speed, 0.1 = 10 times slower, 10.0 = 10 times faster).",
            displayPosition = 65,
            displayMode = ConfigDef.DisplayMode.ADVANCED,
            group = "EVENT_TIME"
    )
    public Double speedup;

    /**
     * Return configured date mask.
     */
    public String getDateMask() {
        if (timestampDateFormat != null)
            return timestampDateFormat != TimestampDateFormat.OTHER ? timestampDateFormat.getFormat() : otherTimestampDateFormat;
        else
            return null;
    }

}
