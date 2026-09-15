package io.github.yok.flexdblink.core;

import com.google.common.io.BaseEncoding;
import io.github.yok.flexdblink.config.FilePatternConfig;
import io.github.yok.flexdblink.db.DbDialectHandler;
import io.github.yok.flexdblink.util.CsvUtils;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

/**
 * Exports BLOB/CLOB columns from a database table to individual files, and updates the
 * corresponding CSV file with {@code file:<filename>} references.
 *
 * <p>
 * This class is extracted from {@code DataDumper} and handles the second pass over each table:
 * reading the already-written CSV, scanning the live DB rows for LOB columns, writing each LOB
 * value to a file under the {@code files/} directory, replacing the cell in memory, and finally
 * overwriting the CSV in sorted order.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
@Slf4j
class LobFileExporter {

    private static final Set<Integer> LOB_SQL_TYPES = Set.of(Types.BLOB, Types.CLOB, Types.NCLOB);

    private final FilePatternConfig filePatternConfig;

    /**
     * Creates an exporter using the configured LOB filename patterns.
     *
     * @param filePatternConfig filename patterns by table and column
     */
    LobFileExporter(FilePatternConfig filePatternConfig) {
        this.filePatternConfig = filePatternConfig;
    }

    /**
     * Scans all rows of the specified table, writes BLOB/CLOB columns to files, replaces the
     * corresponding CSV cells with {@code file:<filename>} references, and overwrites the CSV in
     * sorted order.
     *
     * @param conn JDBC connection
     * @param table table name
     * @param dbDir directory containing the CSV file
     * @param filesDir directory to write LOB files into
     * @param schema schema name
     * @param dialectHandler DB dialect handler
     * @return dump result (row count + number of LOB files written)
     * @throws Exception on SQL or file I/O error
     */
    DumpResult export(Connection conn, String table, File dbDir, File filesDir, String schema,
            DbDialectHandler dialectHandler) throws Exception {
        File csvFile = new File(dbDir, table + ".csv");
        if (!csvFile.exists()) {
            log.warn("CSV file not found: {}", csvFile.getAbsolutePath());
            return new DumpResult(0, 0);
        }

        List<String> headers = readHeaders(csvFile);
        List<List<String>> csvData = readCsvData(csvFile, headers);
        logFilePatterns(table);

        int fileCount = updateCsvData(conn, table, filesDir, dialectHandler, headers, csvData);

        List<String> pkColumns = CsvUtils.fetchPrimaryKeyColumns(conn, schema, table);
        String[] headerArray = headers.toArray(new String[0]);
        List<Integer> sortIndices = CsvUtils.buildSortIndices(headerArray, pkColumns);
        csvData.sort(CsvUtils.rowComparator(sortIndices));
        CsvUtils.writeCsvUtf8(csvFile, headerArray, csvData);

        return new DumpResult(csvData.size(), fileCount);
    }

    /**
     * Reads CSV headers, removing surrounding whitespace and quote characters.
     *
     * @param csvFile source CSV file
     * @return normalized headers in their original order, retaining duplicates
     * @throws IOException on CSV read error
     */
    private List<String> readHeaders(File csvFile) throws IOException {
        List<String> headers = new ArrayList<>();
        try (CSVParser parser = CSVParser.parse(csvFile, StandardCharsets.UTF_8, CsvUtils.FORMAT)) {
            CSVRecord headerRecord = parser.iterator().next();
            for (String header : headerRecord) {
                headers.add(StringUtils.strip(header.trim(), "\""));
            }
        }
        return headers;
    }

    /**
     * Reads CSV rows using header names to preserve existing duplicate-header lookup behavior.
     *
     * @param csvFile source CSV file
     * @param headers normalized column names in CSV order
     * @return mutable rows ready for replacement with database values
     * @throws IOException on CSV read error
     */
    private List<List<String>> readCsvData(File csvFile, List<String> headers) throws IOException {
        CSVFormat format = CsvUtils.FORMAT.builder().setHeader(headers.toArray(new String[0]))
                .setSkipHeaderRecord(true).get();
        List<CSVRecord> records;
        try (CSVParser parser = CSVParser.parse(csvFile, StandardCharsets.UTF_8, format)) {
            records = parser.getRecords();
        }

        List<List<String>> csvData = new ArrayList<>();
        for (CSVRecord record : records) {
            List<String> row = new ArrayList<>();
            for (String header : headers) {
                row.add(record.get(header));
            }
            csvData.add(row);
        }
        return csvData;
    }

    /**
     * Logs the filename patterns configured for a table.
     *
     * @param table table whose patterns are logged
     */
    private void logFilePatterns(String table) {
        Map<String, String> tablePatterns = filePatternConfig.getPatternsForTable(table);
        String joined = tablePatterns.entrySet().stream()
                .map(e -> e.getKey() + " : " + e.getValue()).collect(Collectors.joining(", "));
        log.debug("BLOB/CLOB output filename patterns: [{}]", joined);
    }

    /**
     * Replaces CSV cells from the query result and writes configured non-null LOB values.
     *
     * <p>
     * Temporal formatting takes precedence over filename patterns. Other columns with a pattern are
     * written as files; LOB columns without a pattern cause an error even when their value is null.
     * Columns absent from the CSV are ignored, and duplicate headers use the first index.
     * </p>
     *
     * @param conn JDBC connection used to read the table and format temporal values
     * @param table table to query and resolve filename patterns for
     * @param filesDir directory receiving LOB files
     * @param dialectHandler DB dialect handler for SQL, value formatting, and file writes
     * @param headers CSV headers used to locate replacement cells
     * @param csvData mutable CSV rows, updated in query order before sorting
     * @return number of completed LOB file writes, including repeated writes to the same path
     * @throws Exception on SQL, formatting, file I/O, or missing LOB pattern error
     */
    private int updateCsvData(Connection conn, String table, File filesDir,
            DbDialectHandler dialectHandler, List<String> headers, List<List<String>> csvData)
            throws Exception {
        int fileCount = 0;
        String quotedTable = dialectHandler.quoteIdentifier(table);
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + quotedTable)) {

            ResultSetMetaData metadata = rs.getMetaData();
            int columnCount = metadata.getColumnCount();

            int rowIndex = 0;
            while (rs.next()) {
                List<String> row = csvData.get(rowIndex++);

                for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
                    String columnName =
                            metadata.getColumnLabel(columnIndex).toUpperCase(Locale.ROOT);
                    int sqlType = metadata.getColumnType(columnIndex);
                    String typeName = metadata.getColumnTypeName(columnIndex);
                    int csvColumnIndex = headers.indexOf(columnName);
                    if (csvColumnIndex < 0) {
                        continue;
                    }

                    Object rawValue = rs.getObject(columnIndex);
                    Optional<String> filenamePattern =
                            filePatternConfig.getPattern(table, columnName);

                    if (dialectHandler.shouldUseRawTemporalValueForDump(columnName, sqlType,
                            typeName)) {
                        row.set(csvColumnIndex, formatRawTemporalValue(rs.getString(columnIndex),
                                columnName, dialectHandler));
                        continue;
                    }

                    if (dialectHandler.isDateTimeTypeForDump(sqlType, typeName)) {
                        Object temporalValue = CsvUtils.resolveTemporalValue(rs, columnIndex,
                                rawValue, sqlType, typeName);
                        row.set(csvColumnIndex, formatTemporalValue(temporalValue, columnName, conn,
                                dialectHandler));
                        continue;
                    }

                    if (filenamePattern.isPresent()) {
                        String cell = null;
                        if (rawValue != null) {
                            String pattern = filenamePattern.get();
                            Map<String, Object> keyMap = buildKeyMap(rs, pattern);
                            String filename = applyPlaceholders(pattern, keyMap);
                            Path outputPath = filesDir.toPath().resolve(filename);
                            dialectHandler.writeLobFile(table, columnName, rawValue, outputPath);
                            fileCount++;
                            cell = "file:" + filename;
                        }
                        row.set(csvColumnIndex, cell);
                        continue;
                    }

                    if (isLobSqlType(sqlType)) {
                        throw new IllegalStateException("No definition for \"" + table + "\" / \""
                                + columnName + "\" in file-patterns.");
                    }

                    if (dialectHandler.isBinaryTypeForDump(sqlType, typeName)) {
                        row.set(csvColumnIndex, formatBinaryValue(rs.getBytes(columnIndex)));
                        continue;
                    }

                    row.set(csvColumnIndex,
                            formatScalarValue(rawValue, columnName, sqlType, dialectHandler));
                }
            }
        }

        return fileCount;
    }

    /**
     * Normalizes a raw temporal string while preserving SQL NULL.
     *
     * @param value temporal string read from the database, or null
     * @param columnName uppercase column label
     * @param dialectHandler DB-specific temporal normalizer
     * @return normalized string, or null for SQL NULL
     */
    private String formatRawTemporalValue(String value, String columnName,
            DbDialectHandler dialectHandler) {
        if (value == null) {
            return null;
        }
        return dialectHandler.normalizeRawTemporalValueForDump(columnName, value);
    }

    /**
     * Formats a resolved temporal value while preserving SQL NULL.
     *
     * @param value typed temporal value or raw fallback, or null
     * @param columnName uppercase column label
     * @param conn JDBC connection passed to the formatter
     * @param dialectHandler DB-specific temporal formatter
     * @return formatted temporal value, or null for SQL NULL
     * @throws SQLException on temporal formatting error
     */
    private String formatTemporalValue(Object value, String columnName, Connection conn,
            DbDialectHandler dialectHandler) throws SQLException {
        if (value == null) {
            return null;
        }
        return dialectHandler.formatDateTimeColumn(columnName, value, conn);
    }

    /**
     * Encodes binary data as uppercase hexadecimal while preserving SQL NULL.
     *
     * @param bytes binary column value, or null
     * @return uppercase hexadecimal string, or null for SQL NULL
     */
    private String formatBinaryValue(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return BaseEncoding.base16().upperCase().encode(bytes);
    }

    /**
     * Formats an ordinary column, trimming trailing spaces only for CHAR and NCHAR.
     *
     * @param value raw column value, or null
     * @param columnName uppercase column label
     * @param sqlType JDBC type used to identify fixed-width character columns
     * @param dialectHandler DB-specific value formatter
     * @return formatted value, or null for SQL NULL
     * @throws SQLException on value formatting error
     */
    private String formatScalarValue(Object value, String columnName, int sqlType,
            DbDialectHandler dialectHandler) throws SQLException {
        if (value == null) {
            return null;
        }
        String formattedValue = dialectHandler.formatDbValueForCsv(columnName, value);
        if (sqlType == Types.CHAR || sqlType == Types.NCHAR) {
            return CsvUtils.trimTrailingSpaces(formattedValue);
        }
        return formattedValue;
    }

    /**
     * Returns whether the JDBC type should be handled as LOB output.
     *
     * @param sqlType JDBC SQL type
     * @return true if BLOB/CLOB/NCLOB
     */
    private boolean isLobSqlType(int sqlType) {
        return LOB_SQL_TYPES.contains(sqlType);
    }

    /**
     * Extracts placeholders like {@code {COLUMN}} from {@code rawPattern}, retrieves the values
     * from the {@link ResultSet} for the current row with matching column names, and returns a map
     * of placeholder names to values.
     *
     * @param rs JDBC result set (current row)
     * @param rawPattern file name pattern (e.g., {@code "tbl_{COL1}_{COL2}.bin"})
     * @return map of placeholder name → column value
     * @throws SQLException on column access error
     */
    Map<String, Object> buildKeyMap(ResultSet rs, String rawPattern) throws SQLException {
        Map<String, Object> keyMap = new HashMap<>();
        Matcher m = Pattern.compile("\\{(.+?)\\}").matcher(rawPattern);
        while (m.find()) {
            String col = m.group(1);
            keyMap.put(col, rs.getObject(col));
        }
        return keyMap;
    }

    /**
     * Replaces placeholders in the specified pattern with values from the provided key-value map.
     * Placeholders are written as {@code {COLUMN_NAME}}.
     *
     * @param pattern template string containing placeholders (e.g., {@code "tbl_{COL1}.bin"})
     * @param keyMap map keyed by placeholder names with replacement values
     * @return string with all placeholders replaced
     */
    String applyPlaceholders(String pattern, Map<String, Object> keyMap) {
        String result = pattern;
        for (Map.Entry<String, Object> e : keyMap.entrySet()) {
            String placeholder = "{" + e.getKey() + "}";
            if (result.contains(placeholder)) {
                result = result.replace(placeholder, Objects.toString(e.getValue()));
            }
        }
        return result;
    }
}
