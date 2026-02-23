package io.github.yok.flexdblink.core;

import io.github.yok.flexdblink.db.DbDialectHandler;
import io.github.yok.flexdblink.util.CsvUtils;
import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;

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

        // --- 1) Extract primary key column names from metadata ---
        List<String> pkColumns = fetchPrimaryKeyColumns(conn, conn.getSchema(), table);

        // --- 2) Read header first (for sorting) ---
        String[] headerArray;
        String quotedTable = dialectHandler.quoteIdentifier(table);
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + quotedTable + " WHERE 1=0")) {
            ResultSetMetaData md = rs.getMetaData();
            int colCount = md.getColumnCount();
            headerArray = new String[colCount];
            for (int i = 1; i <= colCount; i++) {
                headerArray[i - 1] = md.getColumnLabel(i);
            }
        }

        // --- 3) Build SELECT with ORDER BY (explicit column names) ---
        String orderBy;
        if (!pkColumns.isEmpty()) {
            String cols =
                    pkColumns.stream().map(col -> dialectHandler.quoteIdentifier(col) + " ASC")
                            .collect(Collectors.joining(", "));
            orderBy = " ORDER BY " + cols;
            log.debug("Table[{}] Sorting by primary key(s) (SQL): {}", table, cols);
        } else {
            String firstCol = headerArray[0];
            orderBy = " ORDER BY " + dialectHandler.quoteIdentifier(firstCol) + " ASC";
            log.debug("Table[{}] No primary key -> sort by first column '{}' (SQL)", table,
                    firstCol);
        }
        String sql = "SELECT * FROM " + quotedTable + orderBy;

        // --- 4) Read records into memory ---
        List<List<String>> rows = new ArrayList<>();
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {

            ResultSetMetaData md = rs.getMetaData();
            int colCount = md.getColumnCount();

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
                        Object temporalValue = val;
                        if ("DATE".equalsIgnoreCase(typeNm) || sqlType == Types.DATE) {
                            Object typed = rs.getDate(i);
                            temporalValue = (typed != null) ? typed : temporalValue;
                        } else if (sqlType == Types.TIME) {
                            Object typed = rs.getTime(i);
                            temporalValue = (typed != null) ? typed : temporalValue;
                        } else if (sqlType == Types.TIMESTAMP) {
                            Object typed = rs.getTimestamp(i);
                            temporalValue = (typed != null) ? typed : temporalValue;
                        }
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

        // --- 5) Sort in-memory ---
        List<Integer> sortIdx = CsvUtils.buildSortIndices(headerArray, pkColumns);

        rows.sort((a, b) -> {
            for (int idx : sortIdx) {
                String sa = StringUtils.trimToEmpty(a.get(idx));
                String sb = StringUtils.trimToEmpty(b.get(idx));
                int cmp;
                try {
                    cmp = Integer.compare(Integer.parseInt(sa), Integer.parseInt(sb));
                } catch (NumberFormatException ex) {
                    cmp = sa.compareTo(sb);
                }
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        });

        // --- 6) Write sorted data to CSV ---
        CsvUtils.writeCsvUtf8(csvFile, headerArray, rows);
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
    List<String> fetchPrimaryKeyColumns(Connection conn, String schema, String table)
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
}
