package com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum CsvRecordType implements Label {
    LIST("List"),
    LIST_MAP("List-Map"),
    ;

    private final String label;

    CsvRecordType(String label) {
        this.label = label;
    }

    @Override
    public String getLabel() {
        return label;
    }

}
