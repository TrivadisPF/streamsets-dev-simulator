package com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum CsvHeader implements Label {
	  USE_HEADER("With Header Line"),
	  IGNORE_HEADER("Ignore Header Line"),
	  NO_HEADER("No Header Line"),
	  ;

	  private final String label;

	  CsvHeader(String label) {
	    this.label = label;
	  }

	  @Override
	  public String getLabel() {
	    return label;
	}
}
