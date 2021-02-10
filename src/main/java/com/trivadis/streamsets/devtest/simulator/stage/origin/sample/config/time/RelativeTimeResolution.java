package com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum RelativeTimeResolution implements Label {

    SECONDS("seconds"),
    MILLISECONDS("milliseconds");

    private final String label;

    private RelativeTimeResolution(String label) {
        this.label = label;
    }

    @Override
    public String getLabel() {
        return label;
    }
}
