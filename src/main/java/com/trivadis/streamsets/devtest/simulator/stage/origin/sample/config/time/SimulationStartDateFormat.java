package com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum SimulationStartDateFormat implements Label {

    HH_MM("HH:mm", "HH:mm"),
    HH_MM_SS("HH:mm:ss", "HH:mm:ss"),
    YYYY_MM_DD_HH_MM_SS("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss"),
    YYYY_MM_DD_HH_MM_SS_SSS("yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss.SSS"),
    YYYY_MM_DD_HH_MM_SS_SSS_Z("yyyy-MM-dd HH:mm:ss.SSS Z", "yyyy-MM-dd HH:mm:ss.SSS Z"),
    YYYY_MM_DD_T_HH_MM_Z("yyyy-MM-dd'T'HH:mmZ", "yyyy-MM-dd'T'HH:mmZ"),
    YYYY_MM_DD_T_HH_MM_SS_Z("yyyy-MM-dd'T'HH:mm:ssZ", "yyyy-MM-dd'T'HH:mm:ssZ"),
    YYYY_MM_DD_T_HH_MM_SS_SSS_Z("yyyy-MM-dd'T'HH:mm:ss.SSSZ", "yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
    OTHER(null, "Other ...");

    private final String format;
    private final String label;

    private SimulationStartDateFormat(String format, String label) {
        this.format = format;
        this.label = label;
    }

    public String getFormat() {
        return format;
    }


    @Override
    public String getLabel() {
        return label;
    }
}
