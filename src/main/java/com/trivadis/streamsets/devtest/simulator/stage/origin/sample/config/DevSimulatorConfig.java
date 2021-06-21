package com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.files.PathMatcherMode;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.files.PathMatcherModeChooserValues;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format.DataFormatType;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format.InputDataFormatChooserValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DevSimulatorConfig {
    private static final Logger LOG = LoggerFactory.getLogger(DevSimulatorConfig.class);

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "",
            label = "Files Directory",
            description = "Use a local directory",
            displayPosition = 10,
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "FILES"
    )
    public String filesDirectory;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.BOOLEAN,
            defaultValue = "true",
            label = "Include Subdirectories",
            description = "Include files in subdirectories of Files Directory.  " +
                    "Only file names matching File Name Pattern will be processed.",
            displayPosition = 15,
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "FILES"
    )
    public boolean includeSubdirectories = true;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "",
            label = "File Name Pattern",
            description = "A glob or regular expression that defines the pattern of the file names in the directory.",
            displayPosition = 20,
            displayMode = ConfigDef.DisplayMode.BASIC,
            group = "FILES"
    )
    public String fileNamePattern;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.MODEL,
            label = "File Name Pattern Mode",
            description = "Select whether the File Name Pattern specified uses wildcard pattern syntax or regex syntax.",
            defaultValue = "WILDCARD",
            displayPosition = 15,
            displayMode = ConfigDef.DisplayMode.ADVANCED,
            group = "FILES"
    )
    @ValueChooserModel(PathMatcherModeChooserValues.class)
    public PathMatcherMode pathMatcherMode = PathMatcherMode.WILDCARD;

    @ValueChooserModel(InputDataFormatChooserValues.class)
    @ConfigDef(
            required = true,
            type = ConfigDef.Type.MODEL,
            defaultValue = "AS_DELIMITED",
            label = "Input Data Format",
            description = "How should the output be produced.",
            group = "DATA_FORMAT",
            displayPosition = 0
    )
    public DataFormatType inputDataFormat;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.BOOLEAN,
            defaultValue = "false",
            label = "Different Record Types?",
            description = "Support multiple, different Record Types.",
            group = "FILES",
            displayPosition = 30
    )
    public boolean useMultiRecordType;


}
