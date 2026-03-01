package io.github.yok.flexdblink.db;

import io.github.yok.flexdblink.config.ConnectionConfig;
import java.sql.Connection;
import java.sql.SQLException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.datatype.IDataTypeFactory;

/**
 * Connection/session related operations for each database dialect.
 */
public interface DbDialectConnectionOperations {

    /**
     * Applies dialect-specific initialization to JDBC connection.
     *
     * @param connection JDBC connection to initialize
     * @throws SQLException if session initialization fails
     */
    void prepareConnection(Connection connection) throws SQLException;

    /**
     * Resolves schema name for the provided connection entry.
     *
     * @param entry connection-config entry
     * @return schema name handled by the dialect
     */
    String resolveSchema(ConnectionConfig.Entry entry);

    /**
     * Creates a DBUnit connection configured for the dialect.
     *
     * @param connection JDBC connection
     * @param schema resolved schema name
     * @return initialized DBUnit connection
     * @throws Exception if initialization fails
     */
    DatabaseConnection createDbUnitConnection(Connection connection, String schema)
            throws Exception;

    /**
     * Returns DBUnit datatype factory for the dialect.
     *
     * @return DBUnit datatype factory
     */
    IDataTypeFactory getDataTypeFactory();
}
