package com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Locale;

public class DateUtil {

    public static long  parseCustomFormat(String formatPattern, String dateTimeString) {
        LocalDate today = LocalDate.now();

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern(formatPattern);
        ZonedDateTime zdt = ZonedDateTime.parse(dateTimeString, dtf);

        return zdt.toEpochSecond();
    }

    public static long parseCustomFormatToEpocMs(String formatPattern, String dateTimeString) {
        LocalDate today = LocalDate.now();

        DateTimeFormatter dtf  = DateTimeFormatter.ofPattern(formatPattern);
        ZonedDateTime zdt  = ZonedDateTime.parse(dateTimeString,dtf);

        return zdt.toInstant().toEpochMilli();

    }
}
