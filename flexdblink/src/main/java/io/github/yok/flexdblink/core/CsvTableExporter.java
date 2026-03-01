package io.github.yok.flexdblink.core;

import io.github.yok.flexdblink.db.DbDialectHandler;
import io.github.yok.flexdblink.util.CsvUtils;
import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Hex;

/**
 * Exports a single database table to a UTF-8 CSV file.
 *
 * <p>
 * Rows are sorted by primary key (ascending); when no primary key exists, the leftmost column is
 * used. Binary columns are encoded as uppercase hexadecimal strings. In-memory sorting is applied
 * after reading to guarantee deterministic output.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
@Slf4j
class CsvTableExporter {

    /**
     * Executes {@code SELECT *} for the specified table and writes the result to a UTF-8 CSV
     * file.<br>
     * If a primary key exists, rows are sorted in ascending order by the PK columns; otherwise,
     * rows are sorted by the leftmost column. Sorting is performed in-memory before writing
     * CSV.<br>
     * RAW/LONG RAW/VARBINARY-like types are emitted as hexadecimal strings.
     *
     * @param conn JDBC connection
     * @param table table name
     * @param csvFile destination CSV file
     * @param dialectHandler DB dialect handler used for identifier quoting and type formatting
     * @throws Exception on SQL or file I/O error
     */
    void export(Connection conn, String table, File csvFile, DbDialectHandler dialectHandler)
            throws Exception {

        // --- 1) Fetch primary key columns ---
        List<String> pkColumns = CsvUtils.fetchPrimaryKeyColumns(conn, conn.getSchema(), table);

        // --- 2) Single SELECT * query: headers and data from the same ResultSet ---
        String quotedTable = dialectHandler.quoteIdentifier(table);
        String sql = "SELECT * FROM " + quotedTable;
        log.debug("Table[{}] SQL: {}", table, sql);

        String[] headerArray;
        List<List<String>> rows = new ArrayList<>();

        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            int colCount = md.getColumnCount();

            // Extract headers from metadata
            headerArray = new String[colCount];
            for (int i = 1; i <= colCount; i++) {
                headerArray[i - 1] = md.getColumnLabel(i);
            }

            // Read all rows
            while (rs.next()) {
                List<String> row = new ArrayList<>(colCount);
                for (int i = 1; i <= colCount; i++) {
                    int sqlType = md.getColumnType(i);
                    String typeNm = md.getColumnTypeName(i);
                    String columnName = md.getColumnLabel(i);
                    Object val = rs.getObject(i);
                    String cell;
                    if (dialectHandler.isBinaryTypeForDump(sqlType, typeNm)) {
                        byte[] bytes = rs.getBytes(i);
                        cell = (bytes == null) ? "" : Hex.encodeHexString(bytes).toUpperCase();
                    } else if (dialectHandler.isDateTimeTypeForDump(sqlType, typeNm)) {
                        Object temporalValue =
                                CsvUtils.resolveTemporalValue(rs, i, val, sqlType, typeNm);
                        cell = (temporalValue == null) ? ""
                                : dialectHandler.formatDateTimeColumn(columnName, temporalValue,
                                        conn);
                    } else {
                        cell = (val == null) ? ""
                                : dialectHandler.formatDbValueForCsv(columnName, val);
                    }
                    row.add(cell);
                }
                rows.add(row);
            }
        }

        // --- 3) Sort in-memory ---
        List<Integer> sortIdx = CsvUtils.buildSortIndices(headerArray, pkColumns);
        rows.sort(CsvUtils.rowComparator(sortIdx));

        // --- 4) Write sorted data to CSV ---
        CsvUtils.writeCsvUtf8(csvFile, headerArray, rows);
    }
}
