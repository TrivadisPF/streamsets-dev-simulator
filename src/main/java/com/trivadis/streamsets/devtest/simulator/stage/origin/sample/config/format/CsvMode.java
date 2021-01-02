package com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;
import org.apache.commons.csv.CSVFormat;

@GenerateResourceBundle
public enum CsvMode implements Label {
	CSV("Default CSV (ignores empty lines)", CSVFormat.DEFAULT),
	RFC4180("RFC4180 CSV", CSVFormat.RFC4180),
	EXCEL("MS Excel CSV", CSVFormat.EXCEL),
	MYSQL("MySQL CSV", CSVFormat.MYSQL),
	TDF("Tab Separated Values", CSVFormat.TDF),
	POSTGRES_CSV("PostgreSQL CSV", CSVFormat.POSTGRESQL_CSV),
	POSTGRES_TEXT("PostgreSQL Text", CSVFormat.POSTGRESQL_TEXT),
	CUSTOM("Custom", null),
	MULTI_CHARACTER("Multi Character Delimited", null)
	;

	  private final String label;
	  private CSVFormat format;


	CsvMode(String label, CSVFormat format) {
		this.label = label;
		this.format = format;
	}

	@Override
	public String getLabel() {
		return label;
	}

	public CSVFormat getFormat() {
		return format;
	}

}
