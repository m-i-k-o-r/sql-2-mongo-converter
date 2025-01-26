package com.koroli.queryconverter.operators.date;

import lombok.experimental.UtilityClass;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Date;

/**
 * Utility class for parsing dates from various formats.
 */
@UtilityClass
public class DateParser {
    private static final ZoneId UTC = ZoneId.of("UTC");

    /**
     * Enum to represent different types of temporal data.
     */
    private enum TemporalType {
        INSTANT, DATETIME, DATE, UNKNOWN
    }

    /**
     * Parses a date string into a {@link Date} object.
     *
     * @param format the format of the date string.
     * @param value  the date string to parse.
     * @return a {@link Date} object representing the parsed date.
     */
    public static Date parse(String format, String value) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format).withZone(UTC);

        try {
            TemporalAccessor accessor = formatter.parse(value);

            return switch (getTemporalType(accessor)) {
                case INSTANT -> Date.from(Instant.from(accessor));
                case DATETIME -> Date.from(LocalDateTime.from(accessor).atZone(UTC).toInstant());
                case DATE -> Date.from(LocalDate.from(accessor).atStartOfDay(UTC).toInstant());
                default -> throw new IllegalArgumentException();
            };
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to parse date: " + value + " with format: " + format, e);
        }
    }

    /**
     * Determine the type of TemporalAccessor
     *
     * @param accessor the TemporalAccessor to analyze.
     * @return {@link TemporalType} TemporalType representing the type time.
     */
    private static TemporalType getTemporalType(TemporalAccessor accessor) {
        if (accessor.isSupported(ChronoField.INSTANT_SECONDS)) {
            return TemporalType.INSTANT;
        } else if (accessor.isSupported(ChronoField.HOUR_OF_DAY)) {
            return TemporalType.DATETIME;
        } else if (accessor.isSupported(ChronoField.DAY_OF_MONTH)) {
            return TemporalType.DATE;
        }
        return TemporalType.UNKNOWN;
    }
}
