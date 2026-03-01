package io.github.yok.flexdblink.util;

import java.sql.Connection;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * Abstraction for dialect-agnostic JDBC date/time formatting used by CSV dump/load processing.
 *
 * @author Yasuharu.Okawauchi
 */
public interface DateTimeFormatSupport {

    /**
     * Formats a JDBC temporal value into a CSV-friendly string.
     *
     * @param columnName target column name
     * @param value JDBC value
     * @param connection JDBC connection for driver-specific formatting if required
     * @return formatted value for CSV
     */
    String formatJdbcDateTime(String columnName, Object value, Connection connection);

    /**
     * Parses a CSV date string using the configured date format.
     *
     * @param value CSV date string
     * @return parsed {@link LocalDate}, or {@code null} if the value does not match
     */
    default LocalDate parseConfiguredDate(String value) {
        return null;
    }

    /**
     * Parses a CSV time string using the configured time format.
     *
     * @param value CSV time string
     * @return parsed {@link LocalTime}, or {@code null} if the value does not match
     */
    default LocalTime parseConfiguredTime(String value) {
        return null;
    }

    /**
     * Parses a CSV timestamp string using the configured dateTimeWithMillis format first, then
     * dateTime format.
     *
     * @param value CSV timestamp string
     * @return parsed {@link LocalDateTime}, or {@code null} if the value does not match either
     *         format
     */
    default LocalDateTime parseConfiguredTimestamp(String value) {
        return null;
    }
}
