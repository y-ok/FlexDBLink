package io.github.yok.flexdblink.core;

import io.github.yok.flexdblink.db.DbDialectHandler;
import io.github.yok.flexdblink.util.CsvUtils;
import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import lombok.extern.slf4j.Slf4j;

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

        List<String> pkColumns = CsvUtils.fetchPrimaryKeyColumns(conn, conn.getSchema(), table);
        String quotedTable = dialectHandler.quoteIdentifier(table);
        String sql = "SELECT * FROM " + quotedTable;
        log.debug("Table[{}] SQL: {}", table, sql);

        String[] headers;
        List<List<String>> rows;

        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            ResultSetMetaData metadata = rs.getMetaData();
            headers = readHeaders(metadata);
            rows = readRows(rs, metadata, headers.length, dialectHandler, conn);
        }

        List<Integer> sortIdx = CsvUtils.buildSortIndices(headers, pkColumns);
        rows.sort(CsvUtils.rowComparator(sortIdx));
        CsvUtils.writeCsvUtf8(csvFile, headers, rows);
    }

    /**
     * Reads uppercase CSV headers in result-set column order.
     *
     * @param metadata column metadata from the query result
     * @return uppercase column labels, including when the result contains no rows
     * @throws SQLException on metadata access error
     */
    private String[] readHeaders(ResultSetMetaData metadata) throws SQLException {
        String[] headers = new String[metadata.getColumnCount()];
        for (int i = 1; i <= headers.length; i++) {
            headers[i - 1] = metadata.getColumnLabel(i).toUpperCase(Locale.ROOT);
        }
        return headers;
    }

    /**
     * Reads and formats all rows using the shared CSV conversion rules.
     *
     * @param rs result set positioned before the first row
     * @param metadata column metadata from the query result
     * @param columnCount number of columns to read per row
     * @param dialectHandler DB dialect handler used for value formatting
     * @param conn JDBC connection passed to datetime formatting
     * @return formatted rows in query order, preserving SQL NULL values
     * @throws Exception on SQL or formatting error
     */
    private List<List<String>> readRows(ResultSet rs, ResultSetMetaData metadata, int columnCount,
            DbDialectHandler dialectHandler, Connection conn) throws Exception {
        List<List<String>> rows = new ArrayList<>();
        while (rs.next()) {
            List<String> row = new ArrayList<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                row.add(CsvUtils.formatColumnValue(rs, i, metadata.getColumnLabel(i),
                        dialectHandler, conn));
            }
            rows.add(row);
        }
        return rows;
    }
}
