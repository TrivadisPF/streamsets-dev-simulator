package com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Locale;

public class DateUtil {
    public static long  parseCustomFormat(String formatPattern, String dateTimeString) {
        LocalDate today = LocalDate.now();

        DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                .appendPattern(formatPattern)
                .parseDefaulting(ChronoField.YEAR_OF_ERA, today.getYear())
                .parseDefaulting(ChronoField.MONTH_OF_YEAR, today.getMonthValue())
                .parseDefaulting(ChronoField.DAY_OF_MONTH, today.getDayOfMonth())
                .toFormatter(Locale.ENGLISH);
        LocalDateTime parsedDateTime = LocalDateTime.parse(dateTimeString, formatter);
        ZoneId zoneId = ZoneId.systemDefault();
        long epoc = parsedDateTime.atZone(zoneId).toEpochSecond();
        return epoc;
    }
}
