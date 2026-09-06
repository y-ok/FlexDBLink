package io.github.yok.flexdblink.db;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;
import org.dbunit.database.IMetadataHandler;

/**
 * Shares column metadata between DBUnit and dialect conversion within one load. Disconnected
 * snapshots contain metadata only and expire with the handler; each reader owns its cursor.
 */
public final class CachedColumnMetadataHandler implements IMetadataHandler {
    private final IMetadataHandler delegate;
    private final Map<List<String>, CachedRowSet> columns = new HashMap<>();

    /**
     * Decorates the configured DBUnit metadata rules for one connection and load.
     *
     * @param delegate original metadata handler
     */
    public CachedColumnMetadataHandler(IMetadataHandler delegate) {
        this.delegate = delegate;
    }

    /**
     * Returns an independent column cursor without repeating the database lookup.
     *
     * @param meta metadata for the load's connection
     * @param schema schema name, possibly null
     * @param table canonical table name
     * @return disconnected column metadata owned by the caller
     * @throws SQLException if reading or copying metadata fails
     */
    @Override
    public ResultSet getColumns(DatabaseMetaData meta, String schema, String table)
            throws SQLException {
        List<String> key = Arrays.asList(schema, table);
        CachedRowSet snapshot = columns.get(key);
        if (snapshot == null) {
            snapshot = RowSetProvider.newFactory().createCachedRowSet();
            try (ResultSet result = delegate.getColumns(meta, schema, table)) {
                snapshot.populate(result);
            }
            columns.put(key, snapshot);
        }
        return copyColumns(snapshot);
    }

    /**
     * Reuses columns already read by DBUnit, or streams the original metadata when a custom dataset
     * supplied its own column definitions without a JDBC lookup.
     *
     * @param meta metadata for the load's connection
     * @param schema schema name, possibly null
     * @param table canonical table name
     * @return column metadata owned by the caller
     * @throws SQLException if lookup or copying fails
     */
    public ResultSet getConversionColumns(DatabaseMetaData meta, String schema, String table)
            throws SQLException {
        CachedRowSet snapshot = columns.get(Arrays.asList(schema, table));
        if (snapshot != null) {
            return copyColumns(snapshot);
        }
        return delegate.getColumns(meta, schema, table);
    }

    /**
     * Copies metadata rows without CachedRowSet's serialization-based deep copy. Independent
     * row storage ensures closing one reader cannot invalidate another reader or the cache.
     *
     * @param snapshot load-scoped column metadata
     * @return independent metadata cursor
     * @throws SQLException if copying rows fails
     */
    private ResultSet copyColumns(CachedRowSet snapshot) throws SQLException {
        CachedRowSet copy = RowSetProvider.newFactory().createCachedRowSet();
        snapshot.beforeFirst();
        copy.populate(snapshot);
        return copy;
    }

    /** {@inheritDoc} */
    @Override
    public boolean matches(ResultSet result, String schema, String table, boolean caseSensitive)
            throws SQLException {
        return delegate.matches(result, schema, table, caseSensitive);
    }

    /** {@inheritDoc} */
    @Override
    public boolean matches(ResultSet result, String catalog, String schema, String table,
            String column, boolean caseSensitive) throws SQLException {
        return delegate.matches(result, catalog, schema, table, column, caseSensitive);
    }

    /** {@inheritDoc} */
    @Override
    public String getSchema(ResultSet result) throws SQLException {
        return delegate.getSchema(result);
    }

    /** {@inheritDoc} */
    @Override
    public boolean tableExists(DatabaseMetaData meta, String schema, String table)
            throws SQLException {
        return delegate.tableExists(meta, schema, table);
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet getTables(DatabaseMetaData meta, String schema, String[] tableTypes)
            throws SQLException {
        return delegate.getTables(meta, schema, tableTypes);
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet getPrimaryKeys(DatabaseMetaData meta, String schema, String table)
            throws SQLException {
        return delegate.getPrimaryKeys(meta, schema, table);
    }
}
