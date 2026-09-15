package io.github.yok.flexdblink.util;

import io.github.yok.flexdblink.db.DbDialectHandler;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Generated;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.lang3.StringUtils;

/**
 * Utility class for reading and writing CSV files.
 *
 * <p>
 * This class currently provides a helper to write CSV files in UTF-8 using Apache Commons CSV with
 * quoting of non-null values. NULL is an unquoted empty field; an empty string is quoted. Quotes
 * are doubled, and records use the platform's default line separator.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
public final class CsvUtils {

    /**
     * Shared CSV syntax with RFC 4180 quoting and PostgreSQL-style NULL/empty distinction. Empty
     * records are retained so a one-column NULL row can be read back.
     */
    public static final CSVFormat FORMAT = CSVFormat.RFC4180.builder()
            .setQuoteMode(QuoteMode.ALL_NON_NULL).setRecordSeparator(System.lineSeparator()).get();

    /**
     * Prevents instantiation of this utility class.
     */
    @Generated
    private CsvUtils() {
        // Utility class; do not instantiate.
    }

    /**
     * Builds the list of column indices to use for sorting, given the header array and the primary
     * key column list.
     *
     * <p>
     * When {@code pkColumns} is empty, returns {@code [0]} (sort by first column). When duplicated
     * header names exist, the first occurrence wins.
     * </p>
     *
     * @param headers CSV header names
     * @param pkColumns PK column names (empty list means no primary key)
     * @return list of zero-based column indices to sort by, in PK order
     */
    public static List<Integer> buildSortIndices(String[] headers, List<String> pkColumns) {
        Map<String, Integer> headerIndex = IntStream.range(0, headers.length).boxed()
                .collect(Collectors.toMap(i -> headers[i].toUpperCase(Locale.ROOT), i -> i,
                        (a, b) -> a, LinkedHashMap::new));
        if (pkColumns.isEmpty()) {
            return List.of(0);
        }
        return pkColumns.stream().map(pk -> headerIndex.get(pk.toUpperCase(Locale.ROOT)))
                .collect(Collectors.toList());
    }

    /**
     * Returns a {@link Comparator} that sorts rows of CSV cells by the specified column indices.
     *
     * <p>
     * 32-bit integers sort before other strings, which sort lexicographically. Equal numeric values
     * use their text as a tie-breaker. NULL sorts before empty strings. Leading/trailing whitespace
     * is trimmed before comparison.
     * </p>
     *
     * @param sortIdx zero-based column indices to sort by, in priority order
     * @return comparator for in-memory row sorting
     */
    public static Comparator<List<String>> rowComparator(List<Integer> sortIdx) {
        Comparator<String> keyComparator =
                Comparator
                        .nullsFirst(
                                Comparator
                                        .comparing(CsvUtils::parseSortInteger,
                                                Comparator.nullsLast(
                                                        Comparator.<Integer>naturalOrder()))
                                        .thenComparing(Comparator.naturalOrder()));
        return (a, b) -> {
            for (int idx : sortIdx) {
                String sa = StringUtils.trim(a.get(idx));
                String sb = StringUtils.trim(b.get(idx));
                int cmp = keyComparator.compare(sa, sb);
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        };
    }

    /**
     * Classifies a sort key independently so mixed keys have a consistent ordering.
     *
     * @param value trimmed sort key
     * @return integer value, or null for a non-integer key
     */
    private static Integer parseSortInteger(String value) {
        try {
            return Integer.valueOf(value);
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    /**
     * Preserves significant character data while trimming values that require type conversion.
     *
     * @param value non-null CSV value
     * @param sqlType target JDBC type
     * @return original character data or trimmed non-character data
     */
    public static String trimNonTextValue(String value, int sqlType) {
        if (isCharacterType(sqlType)) {
            return value;
        }
        return value.trim();
    }

    /**
     * Identifies character columns whose empty strings and whitespace are meaningful data.
     *
     * @param sqlType target JDBC type
     * @return true for character and character LOB types
     */
    public static boolean isCharacterType(int sqlType) {
        switch (sqlType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
                return true;
            default:
                return false;
        }
    }

    /**
     * Resolves a typed temporal value from the current {@link ResultSet} row.
     *
     * <p>
     * Attempts to retrieve a more specific typed value ({@link java.sql.Date},
     * {@link java.sql.Time}, or {@link java.sql.Timestamp}) based on {@code sqlType} and
     * {@code typeName}. SQL Server {@code DATETIMEOFFSET} uses the driver-specific
     * {@code getDateTimeOffset(int)} when available and falls back to text when unavailable. Falls
     * back to {@code raw} if the typed read returns {@code null}.
     * </p>
     *
     * @param rs result set positioned at the target row
     * @param colIndex one-based column index
     * @param raw value already read via {@code rs.getObject(colIndex)}
     * @param sqlType JDBC SQL type from {@link java.sql.ResultSetMetaData#getColumnType}
     * @param typeName column type name (e.g., {@code "DATE"})
     * @return resolved temporal value, or {@code null} if both typed and raw values are null
     * @throws SQLException on column access error
     */
    public static Object resolveTemporalValue(ResultSet rs, int colIndex, Object raw, int sqlType,
            String typeName) throws SQLException {
        Object temporal = raw;
        if ("DATE".equalsIgnoreCase(typeName) || sqlType == Types.DATE) {
            Date typed = rs.getDate(colIndex);
            if (typed != null) {
                temporal = typed;
            }
        } else if ("DATETIMEOFFSET".equalsIgnoreCase(typeName)) {
            Object typed = resolveSqlServerDateTimeOffset(rs, colIndex);
            if (typed != null) {
                temporal = typed;
            }
        } else if (sqlType == Types.TIME) {
            Time typed = rs.getTime(colIndex);
            if (typed != null) {
                temporal = typed;
            }
        } else if (sqlType == Types.TIMESTAMP) {
            Timestamp typed = rs.getTimestamp(colIndex);
            if (typed != null) {
                temporal = typed;
            }
        }
        return temporal;
    }

    /**
     * Resolves SQL Server {@code DATETIMEOFFSET} using a driver-specific getter when available.
     *
     * @param rs result set positioned at the target row
     * @param colIndex one-based column index
     * @return SQL Server datetimeoffset value, or text fallback
     * @throws SQLException on column access error
     */
    private static Object resolveSqlServerDateTimeOffset(ResultSet rs, int colIndex)
            throws SQLException {
        try {
            return rs.getClass().getMethod("getDateTimeOffset", int.class).invoke(rs, colIndex);
        } catch (ReflectiveOperationException ex) {
            return rs.getString(colIndex);
        }
    }

    /**
     * Retrieves the list of primary key column names for the specified table.
     *
     * @param conn JDBC connection
     * @param schema schema name
     * @param table table name
     * @return list of PK column names (empty if none)
     * @throws SQLException on SQL error while reading PK metadata
     */
    public static List<String> fetchPrimaryKeyColumns(Connection conn, String schema, String table)
            throws SQLException {
        List<String> pkColumns = new ArrayList<>();
        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet rs = meta.getPrimaryKeys(conn.getCatalog(), schema, table)) {
            while (rs.next()) {
                pkColumns.add(rs.getString("COLUMN_NAME"));
            }
        }
        return pkColumns;
    }

    /**
     * Removes only trailing ASCII space characters from a value.
     *
     * <p>
     * This is used for fixed-width {@code CHAR}/{@code NCHAR} dump normalization. Other trailing
     * whitespace such as tabs or newlines is preserved because it may be significant user data.
     * </p>
     *
     * @param value input text
     * @return text without trailing space characters
     */
    public static String trimTrailingSpaces(String value) {
        int end = value.length();
        while (end > 0 && value.charAt(end - 1) == ' ') {
            end--;
        }
        return value.substring(0, end);
    }

    /**
     * Formats a single column value from a {@link ResultSet} into a CSV-compatible string.
     *
     * <p>
     * This method applies the same formatting rules as the dump path in {@code CsvTableExporter}:
     * binary columns are hex-encoded, datetime columns go through
     * {@link DbDialectHandler#formatDateTimeColumn}, CHAR/NCHAR values are right-trimmed, and
     * everything else is delegated to {@link DbDialectHandler#formatDbValueForCsv}.
     * </p>
     *
     * @param rs result set positioned on the current row
     * @param columnName column name to format
     * @param dialectHandler DB dialect handler
     * @param conn JDBC connection (passed to datetime formatting)
     * @return formatted string suitable for CSV comparison, or null for SQL NULL
     * @throws Exception on SQL or formatting error
     */
    public static String formatColumnValue(ResultSet rs, String columnName,
            DbDialectHandler dialectHandler, Connection conn) throws Exception {
        return formatColumnValue(rs, rs.findColumn(columnName), columnName, dialectHandler, conn);
    }

    /**
     * Formats a column by position using the shared CSV conversion rules.
     *
     * <p>
     * Positional access preserves distinct columns even when their labels are identical.
     * The supplied column name is passed unchanged to dialect-specific formatters.
     * </p>
     *
     * @param rs result set positioned on the current row
     * @param colIndex one-based column index to read
     * @param columnName column name passed to dialect-specific formatters
     * @param dialectHandler DB dialect handler
     * @param conn JDBC connection passed to datetime formatting
     * @return formatted string, or null for SQL NULL
     * @throws Exception on SQL or formatting error
     */
    public static String formatColumnValue(ResultSet rs, int colIndex, String columnName,
            DbDialectHandler dialectHandler, Connection conn) throws Exception {
        ResultSetMetaData md = rs.getMetaData();
        int sqlType = md.getColumnType(colIndex);
        String typeName = md.getColumnTypeName(colIndex);
        Object val = rs.getObject(colIndex);

        if (dialectHandler.isBinaryTypeForDump(sqlType, typeName)) {
            byte[] bytes = rs.getBytes(colIndex);
            if (bytes == null) {
                return null;
            }
            return org.apache.commons.codec.binary.Hex.encodeHexString(bytes).toUpperCase();
        } else if (dialectHandler.isDateTimeTypeForDump(sqlType, typeName)) {
            Object temporalValue = resolveTemporalValue(rs, colIndex, val, sqlType, typeName);
            if (temporalValue == null) {
                return null;
            }
            return dialectHandler.formatDateTimeColumn(columnName, temporalValue, conn);
        } else if (val == null) {
            return null;
        } else if (sqlType == Types.CHAR || sqlType == Types.NCHAR) {
            return trimTrailingSpaces(dialectHandler.formatDbValueForCsv(columnName, val));
        } else {
            return dialectHandler.formatDbValueForCsv(columnName, val);
        }
    }

    /**
     * Writes the given header and row data to a CSV file encoded in UTF-8.
     *
     * <p>
     * The CSV is written with:
     * </p>
     * <ul>
     * <li>Header row provided by {@code headers}</li>
     * <li>Quote mode: {@link QuoteMode#ALL_NON_NULL}</li>
     * <li>NULL: unquoted empty field; empty string: {@code ""}</li>
     * <li>Embedded quotes are doubled; backslashes are literal</li>
     * <li>Record separator: {@link System#lineSeparator()}</li>
     * </ul>
     *
     * @param csvFile the destination CSV file (will be created or overwritten)
     * @param headers the header columns to write as the first record
     * @param rows the data rows; each inner list represents one CSV record
     * @throws IOException if an I/O error occurs while writing the file
     */
    public static void writeCsvUtf8(File csvFile, String[] headers, List<List<String>> rows)
            throws IOException {
        CSVFormat fmt = FORMAT.builder().setHeader(headers).get();
        try (Writer w =
                new OutputStreamWriter(new FileOutputStream(csvFile), StandardCharsets.UTF_8);
                CSVPrinter printer = new CSVPrinter(w, fmt)) {
            for (List<String> row : rows) {
                printer.printRecord(row);
            }
        }
    }
}
