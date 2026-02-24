package io.github.yok.flexdblink.core;

import com.google.common.io.BaseEncoding;
import io.github.yok.flexdblink.config.FilePatternConfig;
import io.github.yok.flexdblink.db.DbDialectHandler;
import io.github.yok.flexdblink.util.CsvUtils;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
import org.apache.commons.csv.QuoteMode;
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
        int fileCount = 0;

        // --- Read CSV ---
        File csvFile = new File(dbDir, table + ".csv");
        if (!csvFile.exists()) {
            log.warn("CSV file not found: {}", csvFile.getAbsolutePath());
            return new DumpResult(0, 0);
        }

        // 1) Parse header row
        CSVFormat headerFmt = CSVFormat.DEFAULT.builder().setSkipHeaderRecord(false).setTrim(false)
                .setQuote('"').setQuoteMode(QuoteMode.MINIMAL).get();
        List<String> headers = new ArrayList<>();
        try (CSVParser p = CSVParser.parse(csvFile, StandardCharsets.UTF_8, headerFmt)) {
            CSVRecord hr = p.iterator().next();
            for (String h : hr) {
                headers.add(StringUtils.strip(h.trim(), "\""));
            }
        }

        // 2) Parse data rows
        CSVFormat dataFmt = CSVFormat.DEFAULT.builder().setHeader(headers.toArray(new String[0]))
                .setSkipHeaderRecord(true).setTrim(false).setQuote('"')
                .setQuoteMode(QuoteMode.MINIMAL).setEscape('\\').get();
        List<CSVRecord> records;
        try (CSVParser p = CSVParser.parse(csvFile, StandardCharsets.UTF_8, dataFmt)) {
            records = p.getRecords();
        }

        // 3) Copy to memory
        List<List<String>> csvData = new ArrayList<>();
        for (CSVRecord rec : records) {
            List<String> row = new ArrayList<>();
            for (String h : headers) {
                row.add(rec.get(h));
            }
            csvData.add(row);
        }

        // 4) Log BLOB/CLOB patterns
        Map<String, String> tablePatterns = filePatternConfig.getPatternsForTable(table);
        String joined = tablePatterns.entrySet().stream()
                .map(e -> e.getKey() + " : " + e.getValue()).collect(Collectors.joining(", "));
        log.debug("BLOB/CLOB output filename patterns: [{}]", joined);

        // 5) Scan DB + write files + replace cells
        String quotedTable = dialectHandler.quoteIdentifier(table);
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + quotedTable)) {

            ResultSetMetaData md = rs.getMetaData();
            int colCount = md.getColumnCount();

            int rowIndex = 0;
            while (rs.next()) {
                List<String> row = csvData.get(rowIndex++);

                for (int i = 1; i <= colCount; i++) {
                    String col = md.getColumnLabel(i);
                    int type = md.getColumnType(i);
                    String typeName = md.getColumnTypeName(i);
                    int idx = headers.indexOf(col);
                    if (idx < 0) {
                        continue;
                    }

                    Object raw = rs.getObject(i);
                    String cell;
                    Optional<String> patternOpt = filePatternConfig.getPattern(table, col);

                    // DATE/TIMESTAMP and dialect-specific temporal types
                    if (dialectHandler.shouldUseRawTemporalValueForDump(col, type, typeName)) {
                        cell = dialectHandler.normalizeRawTemporalValueForDump(col,
                                rs.getString(i));

                    } else if (dialectHandler.isDateTimeTypeForDump(type, typeName)) {
                        Object temporalValue =
                                CsvUtils.resolveTemporalValue(rs, i, raw, type, typeName);
                        cell = (temporalValue == null) ? ""
                                : dialectHandler.formatDateTimeColumn(col, temporalValue, conn);

                        // BLOB/CLOB types with pattern
                    } else if (patternOpt.isPresent()) {
                        if (raw == null) {
                            cell = "";
                        } else {
                            String pattern = patternOpt.get();
                            Map<String, Object> keyMap = buildKeyMap(rs, pattern);
                            String fname = applyPlaceholders(pattern, keyMap);
                            Path outPath = filesDir.toPath().resolve(fname);
                            dialectHandler.writeLobFile(table, col, raw, outPath);
                            fileCount++;
                            cell = "file:" + fname;
                        }

                        // BLOB/CLOB types without pattern → error
                    } else if (isLobSqlType(type)) {
                        throw new IllegalStateException("No definition for \"" + table + "\" / \""
                                + col + "\" in file-patterns.");

                        // RAW/BINARY family
                    } else if (dialectHandler.isBinaryTypeForDump(type, typeName)) {
                        byte[] bytes = rs.getBytes(i);
                        cell = (bytes == null) ? ""
                                : BaseEncoding.base16().upperCase().encode(bytes);

                        // Others
                    } else {
                        cell = (raw == null) ? "" : dialectHandler.formatDbValueForCsv(col, raw);
                    }

                    row.set(idx, cell);
                }
            }
        }

        // 6) Compute sort-key indices
        List<String> pkColumns = CsvUtils.fetchPrimaryKeyColumns(conn, schema, table);
        List<Integer> sortIdx =
                CsvUtils.buildSortIndices(headers.toArray(new String[0]), pkColumns);

        // 7) Sort csvData ascending
        csvData.sort(CsvUtils.rowComparator(sortIdx));

        // 8) Overwrite CSV
        CsvUtils.writeCsvUtf8(csvFile, headers.toArray(new String[0]), csvData);

        return new DumpResult(csvData.size(), fileCount);
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
     * @throws java.sql.SQLException on column access error
     */
    Map<String, Object> buildKeyMap(ResultSet rs, String rawPattern) throws java.sql.SQLException {
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
