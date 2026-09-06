package io.github.yok.flexdblink.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IMetadataHandler;

/**
 * Reuses DBUnit and JDBC metadata only for a selected-table load on the same transaction.
 * Standalone handlers retain their original connection and metadata lifecycle.
 */
public final class LoadMetadata {
    private final DatabaseConnection connection;

    /**
     * Enables load-scoped metadata reuse when the caller selects dataset tables.
     *
     * @param db metadata connection configured for the dialect
     * @param tables selected tables, or null for standalone initialization
     */
    public LoadMetadata(DatabaseConnection db, List<String> tables) {
        if (tables == null) {
            connection = null;
        } else {
            connection = db;
            DatabaseConfig config = db.getConfig();
            IMetadataHandler handler =
                    (IMetadataHandler) config.getProperty(DatabaseConfig.PROPERTY_METADATA_HANDLER);
            config.setProperty(DatabaseConfig.PROPERTY_METADATA_HANDLER,
                    new CachedColumnMetadataHandler(handler));
        }
    }

    /**
     * Shares columns already read by DBUnit while preserving standalone lookup patterns.
     *
     * @param meta JDBC metadata
     * @param schema schema name for standalone lookup
     * @param table canonical table name
     * @param columnPattern standalone JDBC column pattern
     * @return column metadata owned by the caller
     * @throws SQLException if metadata cannot be read
     */
    public ResultSet getColumns(DatabaseMetaData meta, String schema, String table,
            String columnPattern) throws SQLException {
        if (connection != null) {
            CachedColumnMetadataHandler handler = (CachedColumnMetadataHandler) connection
                    .getConfig().getProperty(DatabaseConfig.PROPERTY_METADATA_HANDLER);
            return handler.getConversionColumns(meta, connection.getSchema(), table);
        }
        return meta.getColumns(null, schema, table, columnPattern);
    }

    /**
     * Returns the existing DBUnit connection only for its original transaction and schema.
     *
     * @param jdbc requested JDBC connection
     * @param schema requested schema
     * @return reusable wrapper, or null when a new wrapper is required
     * @throws SQLException if the underlying connection cannot be retrieved
     */
    public DatabaseConnection getConnection(Connection jdbc, String schema) throws SQLException {
        if (connection != null && connection.getConnection() == jdbc
                && Objects.equals(connection.getSchema(), schema)) {
            return connection;
        }
        return null;
    }
}
