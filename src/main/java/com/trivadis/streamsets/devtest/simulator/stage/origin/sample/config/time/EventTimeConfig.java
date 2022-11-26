package com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ValueChooserModel;

public class EventTimeConfig {


    @ConfigDef(
            required = true,
            type = ConfigDef.Type.MODEL,
            defaultValue = "RELATIVE_FROM_ANCHOR",
            label = "Timestamp Mode",
            description = "How to retrieve the timestamp of the message.",
            displayPosition = 20,
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "EVENT_TIME"
    )
    @ValueChooserModel(TimestampModeChooserValues.class)
    public TimestampModeType timestampMode = TimestampModeType.RELATIVE_FROM_ANCHOR;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.MODEL,
            defaultValue = "",
            label = "Timestamp Field",
            description = "Field to use as the timestamp.",
            displayPosition = 25,
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "EVENT_TIME",
            dependsOn = "timestampMode",
            triggeredByValue = {"ABSOLUTE","ABSOLUTE_WITH_START", "RELATIVE_FROM_ANCHOR","RELATIVE_FROM_PREVIOUS"}
    )
    @FieldSelectorModel(singleValued = true)
    public String timestampField;

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.MODEL,
            defaultValue="MILLISECONDS",
            label = "Relative Time Resolution",
            description="Select the relative time resolution",
            displayPosition = 30,
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "EVENT_TIME",
            dependsOn = "timestampMode",
            triggeredByValue = {"RELATIVE_FROM_ANCHOR","RELATIVE_FROM_PREVIOUS"}
    )
    @ValueChooserModel(RelativeTimeResolutionChooserValues.class)
    public RelativeTimeResolution relativeTimeResolution = RelativeTimeResolution.MILLISECONDS;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.BOOLEAN,
            defaultValue = "true",
            label = "Anchor Time is Now?",
            displayPosition = 35,
            description = "Should the anchor time to use be now, i.e. current wallclock time?",
            group = "EVENT_TIME",
            dependsOn = "timestampMode",
            triggeredByValue = {"RELATIVE_FROM_ANCHOR","RELATIVE_FROM_PREVIOUS"}
    )
    public boolean anchorTimeNow;

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.STRING,
            defaultValue = "",
            label = "Anchor Time",
            displayPosition = 40,
            description = "The timestamp to use as the anchor for the relative from anchor mode.",
            group = "EVENT_TIME",
            dependsOn = "anchorTimeNow",
            triggeredByValue = {"false"}
    )
    public String anchorTimestamp;

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.MODEL,
            defaultValue="HH_MM_SS",
            label = "Anchor Timestamp Format",
            description="Select or enter any valid date or datetime format",
            displayPosition = 45,
            group = "EVENT_TIME",
            dependsOn = "anchorTimeNow",
            triggeredByValue = {"false"}
    )
    @ValueChooserModel(AnchorDateFormatChooserValues.class)
    public AnchorDateFormat anchorTimestampDateFormat = AnchorDateFormat.HH_MM_SS;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "",
            label = "Other Anchor Timestamp Format",
            displayPosition = 50,
            group = "EVENT_TIME",
            dependsOn = "anchorTimestampDateFormat",
            triggeredByValue = "OTHER"
    )
    public String anchorTimestampOtherDateFormat;

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.STRING,
            defaultValue = "",
            label = "Simulation Start Timestamp",
            displayPosition = 40,
            description = "The timestamp to use as the simulation start time in the absolute mode.",
            group = "EVENT_TIME",
            dependsOn = "timestampMode",
            triggeredByValue = {"ABSOLUTE_WITH_START"}
    )
    public String simulationStartTimestamp;

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.MODEL,
            defaultValue="HH_MM_SS",
            label = "Simulation Start Timestamp Format",
            description="Select or enter any valid date or datetime format",
            displayPosition = 45,
            group = "EVENT_TIME",
            dependsOn = "timestampMode",
            triggeredByValue = {"ABSOLUTE_WITH_START"}
    )
    @ValueChooserModel(AnchorDateFormatChooserValues.class)
    public SimulationStartDateFormat simulationStartTimestampDateFormat = SimulationStartDateFormat.HH_MM_SS;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "",
            label = "Other Simulation Start Timestamp Format",
            displayPosition = 50,
            group = "EVENT_TIME",
            dependsOn = "simulationStartTimestampDateFormat",
            triggeredByValue = "OTHER"
    )
    public String simulatorStartTimestampOtherDateFormat;

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
            defaultValue="",
            label = "Timestamp Format",
            description="Select or enter any valid date or datetime format",
            displayPosition = 37,
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "EVENT_TIME",
            dependsOn = "timestampMode",
            triggeredByValue = {"ABSOLUTE", "ABSOLUTE_WITH_START"}
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
            required = false,
            type = ConfigDef.Type.BOOLEAN,
            defaultValue = "false",
            label = "Fast forward in data",
            description = "Fast forward to a point in the data ",
            displayPosition = 50,
            displayMode = ConfigDef.DisplayMode.ADVANCED,
            dependsOn = "timestampMode",
            triggeredByValue = {"RELATIVE_FROM_ANCHOR"},
            group = "EVENT_TIME"
    )
    public boolean fastForwardByTimeSpan;

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.NUMBER,
            defaultValue = "0",
            label = "Fast forward by time span",
            description = "Fast forward by this time span (in seconds or milliseconds depending on Relative Time Resolution config setting) ",
            displayPosition = 52,
            displayMode = ConfigDef.DisplayMode.ADVANCED,
            dependsOn = "fastForwardByTimeSpan",
            triggeredByValue = "true",
            group = "EVENT_TIME"
    )
    public long fastForwardInitialTimeSpan;

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.BOOLEAN,
            defaultValue = "0",
            label = "Skip initial time span",
            description = "Skip the initial time span (in seconds or milliseconds depending on Relative Time Resolution config setting) ",
            displayPosition = 54,
            displayMode = ConfigDef.DisplayMode.ADVANCED,
            dependsOn = "fastForwardByTimeSpan",
            triggeredByValue = "true",
            group = "EVENT_TIME"
    )
    public boolean skipEarlierEvents;
    
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
