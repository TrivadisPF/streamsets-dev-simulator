package com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum TimestampDateFormat implements Label {

    EPOCH_S("ss", "EPOCH time in seconds"),
    EPOCH_MS("ssSSS", "EPOCH time in milliseconds"),
    HH_MM("HH:mm", "HH:mm"),
    HH_MM_SS("HH:mm:ss", "HH:mm:ss"),
    YYYY_MM_DD_HH_MM_SS("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss"),
    YYYY_MM_DD_HH_MM_SS_SSS("yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss.SSS"),
    YYYY_MM_DD_HH_MM_SS_SSS_Z("yyyy-MM-dd HH:mm:ss.SSS Z", "yyyy-MM-dd HH:mm:ss.SSS Z"),
    YYYY_MM_DD_T_HH_MM_Z("yyyy-MM-dd'T'HH:mm'Z'", "yyyy-MM-dd'T'HH:mm'Z'"),
    YYYY_MM_DD_T_HH_MM_SS_SSS_Z("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
    OTHER(null, "Other ...");

    private final String format;
    private final String label;

    private TimestampDateFormat(String format, String label) {
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
