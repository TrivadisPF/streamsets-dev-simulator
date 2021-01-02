package com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.files;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum PathMatcherMode implements Label {
    WILDCARD("Wildcard"),
    REGEX("Regular Expression"),
    ;

    private final String label;

    PathMatcherMode(String label) {
        this.label = label;
    }

    @Override
    public String getLabel() {
        return label;
    }

}
