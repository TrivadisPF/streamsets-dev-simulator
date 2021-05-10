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
            triggeredByValue = {"ABSOLUTE","RELATIVE_FROM_ANCHOR","RELATIVE_FROM_PREVIOUS"}
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
            triggeredByValue = {"ABSOLUTE"}
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
            type = ConfigDef.Type.NUMBER,
            defaultValue = "0",
            label = "Fast forward to time",
            description = "Start at time (in seconds or milliseconds depending on Relative Time Resolution config setting) ",
            displayPosition = 57,
            displayMode = ConfigDef.DisplayMode.ADVANCED,
            dependsOn = "timestampMode",
            triggeredByValue = {"RELATIVE_FROM_ANCHOR"},
            group = "EVENT_TIME"
    )
    public long fastForwardToTimestamp;

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
