package com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ValueChooserModel;

public class CsvConfig {

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.MODEL,
            defaultValue = "CSV",
            label = "Delimiter Format Type",
            displayPosition = 310,
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "DATA_FORMAT",
            dependsOn = "^basicConfig.inputDataFormat",
            triggeredByValue = "AS_DELIMITED"
    )
    @ValueChooserModel(CsvModeChooserValues.class)
    public CsvMode csvFileFormat = CsvMode.CSV;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.MODEL,
            defaultValue = "NO_HEADER",
            label = "Header Line",
            description = "",
            displayPosition = 380,
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "DATA_FORMAT",
            dependsOn = "^basicConfig.inputDataFormat",
            triggeredByValue = "AS_DELIMITED"
    )
    @ValueChooserModel(CsvHeaderChooserValues.class)
    public CsvHeader csvHeader = CsvHeader.NO_HEADER;

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.CHARACTER,
            defaultValue = "|",
            label = "Delimiter Character",
            displayPosition = 400,
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "DATA_FORMAT",
            dependsOn = "csvFileFormat",
            triggeredByValue = "CUSTOM"
    )
    public char csvCustomDelimiter = '|';

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.CHARACTER,
            defaultValue = "\\",
            label = "Escape Character",
            description = "Character used to escape quote and delimiter characters. To disable select Other and enter " +
                    "\\u0000 (unicode codepoint for the NULL character).",
            displayPosition = 410,
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "DATA_FORMAT",
            dependsOn = "csvFileFormat",
            triggeredByValue = {"CUSTOM", "MULTI_CHARACTER"}
    )
    public char csvCustomEscape = '\\';

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.CHARACTER,
            defaultValue = "\"",
            label = "Quote Character",
            description = "Character used to quote string fields. To disable select Other and enter" +
                    " \\u0000 (unicode codepoint for the NULL character).",
            displayPosition = 420,
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "DATA_FORMAT",
            dependsOn = "csvFileFormat",
            triggeredByValue = {"CUSTOM", "MULTI_CHARACTER"}
    )
    public char csvCustomQuote = '\"';

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.BOOLEAN,
            defaultValue = "false",
            label = "Enable comments",
            displayPosition = 425,
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "DATA_FORMAT",
            dependsOn = "csvFileFormat",
            triggeredByValue = "CUSTOM"
    )
    public boolean csvEnableComments = false;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.CHARACTER,
            defaultValue = "#",
            label = "Comment marker",
            displayPosition = 426,
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "DATA_FORMAT",
            dependsOn = "csvEnableComments",
            triggeredByValue = "true"
    )
    public char csvCommentMarker;


    @ConfigDef(
            required = false,
            type = ConfigDef.Type.BOOLEAN,
            defaultValue = "true",
            label = "Ignore empty lines",
            displayPosition = 427,
            displayMode = ConfigDef.DisplayMode.ADVANCED,
            group = "DATA_FORMAT",
            dependencies = {
                    @Dependency(configName = "csvFileFormat", triggeredByValues = {"CUSTOM"})
            }
    )
    public boolean csvIgnoreEmptyLines = true;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.MODEL,
            defaultValue = "LIST_MAP",
            label = "Root Field Type",
            description = "",
            displayPosition = 432,
            displayMode = ConfigDef.DisplayMode.ADVANCED,
            group = "DATA_FORMAT",
            dependsOn = "^basicConfig.inputDataFormat",
            triggeredByValue = "AS_DELIMITED"
    )
    @ValueChooserModel(CsvRecordTypeChooserValues.class)
    public CsvRecordType csvRecordType = CsvRecordType.LIST_MAP;

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.NUMBER,
            defaultValue = "0",
            label = "Lines to Skip",
            description = "Number of lines to skip before reading",
            displayPosition = 428,
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "DATA_FORMAT",
            dependsOn = "^basicConfig.inputDataFormat",
            triggeredByValue = "AS_DELIMITED",
            min = 0
    )
    public int csvSkipStartLines;


    @ConfigDef(
            required = true,
            type = ConfigDef.Type.BOOLEAN,
            defaultValue = "false",
            label = "Parse NULLs",
            description = "When checked, configured string constant will be converted into NULL field.",
            displayPosition = 436,
            displayMode = ConfigDef.DisplayMode.ADVANCED,
            group = "DATA_FORMAT",
            dependsOn = "^basicConfig.inputDataFormat",
            triggeredByValue = "AS_DELIMITED"
    )
    public boolean parseNull;

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.STRING,
            defaultValue = "\\\\N",
            label = "NULL constant",
            description = "String constant that should be converted to a NULL rather then passed as it is.",
            displayPosition = 437,
            displayMode = ConfigDef.DisplayMode.ADVANCED,
            group = "DATA_FORMAT",
            dependsOn = "parseNull",
            triggeredByValue = "true"
    )
    public String nullConstant;

    /* Charset Related -- Shown last */
    @ConfigDef(
            required = true,
            type = ConfigDef.Type.MODEL,
            defaultValue = "UTF-8",
            label = "Charset",
            displayPosition = 999,
            displayMode = ConfigDef.DisplayMode.ADVANCED,
            group = "DATA_FORMAT",
            dependsOn = "^basicConfig.inputDataFormat",
            triggeredByValue = { "AS_DELIMITED"}
    )
    @ValueChooserModel(CharsetChooserValues.class)
    public String charset = "UTF-8";

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.BOOLEAN,
            defaultValue = "false",
            label = "Ignore Control Characters",
            displayPosition = 1000,
            description = "Use only if required as it impacts reading performance",
            dependsOn = "^basicConfig.inputDataFormat",
            triggeredByValue = {"AS_DELIMITED"},
            displayMode = ConfigDef.DisplayMode.ADVANCED,
            group = "DATA_FORMAT"
    )
    public boolean removeCtrlChars = false;

}
