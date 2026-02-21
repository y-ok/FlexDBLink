package io.github.yok.flexdblink.util;

import java.sql.Connection;

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
}
