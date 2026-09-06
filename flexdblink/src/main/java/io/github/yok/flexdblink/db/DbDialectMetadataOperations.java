package io.github.yok.flexdblink.db;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;

/**
 * Metadata and table-definition operations for each database dialect.
 */
public interface DbDialectMetadataOperations {

    /**
     * Returns SQL type name for the specified schema/table/column.
     *
     * @param jdbc raw JDBC connection
     * @param schema schema name
     * @param table table name
     * @param column column name
     * @return SQL type name (never null)
     * @throws SQLException if metadata retrieval fails
     */
    String getColumnTypeName(Connection jdbc, String schema, String table, String column)
            throws SQLException;

    /**
     * Retrieves ordered primary-key column names.
     *
     * @param conn JDBC connection
     * @param schema schema name
     * @param table table name
     * @return ordered PK column list (empty if none)
     * @throws SQLException if metadata retrieval fails
     */
    List<String> getPrimaryKeyColumns(Connection conn, String schema, String table)
            throws SQLException;

    /**
     * Checks whether NOT NULL LOB column exists in the table.
     *
     * @param connection JDBC connection
     * @param schema schema name
     * @param table table name
     * @param columns table columns
     * @return true when NOT NULL LOB column exists
     * @throws SQLException if metadata retrieval fails
     */
    boolean hasNotNullLobColumn(Connection connection, String schema, String table,
            Column[] columns) throws SQLException;

    /**
     * Checks whether table has primary key.
     *
     * @param connection JDBC connection
     * @param schema schema name
     * @param table table name
     * @return true when primary key exists
     * @throws SQLException if metadata retrieval fails
     */
    boolean hasPrimaryKey(Connection connection, String schema, String table) throws SQLException;

    /**
     * Returns row count of table.
     *
     * @param connection JDBC connection
     * @param table table name
     * @return row count
     * @throws SQLException if SQL execution fails
     */
    int countRows(Connection connection, String table) throws SQLException;

    /**
     * Returns LOB column definitions from input dataset.
     *
     * @param dataDir dataset directory
     * @param table table name
     * @return LOB columns
     * @throws IOException if file I/O fails
     * @throws DataSetException if metadata resolution fails
     */
    Column[] getLobColumns(Path dataDir, String table) throws IOException, DataSetException;

    /**
     * Detects LOB columns using an already parsed CSV table.
     *
     * @param table parsed CSV table
     * @return LOB columns using the dialect's existing CSV rules
     * @throws DataSetException if table values cannot be accessed
     */
    default Column[] getLobColumns(ITable table) throws DataSetException {
        List<Column> result = new ArrayList<>();
        for (Column column : table.getTableMetaData().getColumns()) {
            for (int row = 0; row < table.getRowCount(); row++) {
                Object value = table.getValue(row, column.getColumnName());
                if (value instanceof String && ((String) value).startsWith("file:")) {
                    result.add(column);
                    break;
                }
            }
        }
        return result.toArray(new Column[0]);
    }

    /**
     * Logs table definition details.
     *
     * @param connection JDBC connection
     * @param schema schema name
     * @param table table name
     * @param loggerName logger context name
     * @throws SQLException if metadata retrieval fails
     */
    void logTableDefinition(Connection connection, String schema, String table, String loggerName)
            throws SQLException;

    /**
     * Quotes identifier in dialect style.
     *
     * @param identifier identifier
     * @return quoted identifier
     */
    String quoteIdentifier(String identifier);
}
