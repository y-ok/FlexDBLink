package io.github.yok.flexdblink.db;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;

/**
 * Caches JDBC column and key metadata for repeated data loads within one test class.
 *
 * <p>
 * Snapshots are disconnected from JDBC resources. Connections, statements, and live result sets are
 * never retained in the cache. Each lookup returns an independent cursor. Call {@link #clear()}
 * when the schema changes or the test class finishes.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
public final class JdbcMetadataCache {
    private static final Set<String> CACHED_METHODS =
            Set.of("getColumns", "getPrimaryKeys", "getImportedKeys");
    private final Map<List<Object>, CachedRowSet> snapshots = new HashMap<>();

    /**
     * Wraps a transaction connection, resolving metadata lazily on its first use.
     *
     * @param connection caller-owned connection; transaction and close operations are delegated
     * @return connection with independent metadata cursors backed by this class-scoped cache
     */
    public Connection wrap(Connection connection) {
        return (Connection) Proxy.newProxyInstance(JdbcMetadataCache.class.getClassLoader(),
                new Class<?>[] {Connection.class}, new InvocationHandler() {
                    private DatabaseMetaData metadata;

                    /**
                     * Returns cached metadata or delegates the requested connection operation.
                     *
                     * @param proxy connection proxy receiving the invocation
                     * @param method requested JDBC connection method
                     * @param args method arguments, or {@code null} for a no-argument method
                     * @return metadata proxy or the original connection result
                     * @throws Throwable if metadata initialization or the JDBC operation fails
                     */
                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args)
                            throws Throwable {
                        if ("getMetaData".equals(method.getName())) {
                            if (metadata == null) {
                                metadata = wrapMetadata(connection);
                            }
                            return metadata;
                        }
                        return invokeDelegate(connection, method, args);
                    }
                });
    }

    /**
     * Discards all snapshots so subsequent metadata calls observe the current schema.
     */
    public synchronized void clear() {
        snapshots.clear();
    }

    /**
     * Separates routing targets by their actual JDBC identity and session namespace.
     *
     * @param connection current transaction connection
     * @return metadata proxy that delegates uncached JDBC operations
     * @throws Exception if connection identity cannot be read
     */
    private DatabaseMetaData wrapMetadata(Connection connection) throws Exception {
        DatabaseMetaData metadata = connection.getMetaData();
        List<String> identity = Arrays.asList(metadata.getURL(), metadata.getUserName(),
                connection.getCatalog(), connection.getSchema());
        return (DatabaseMetaData) Proxy.newProxyInstance(JdbcMetadataCache.class.getClassLoader(),
                new Class<?>[] {DatabaseMetaData.class}, (proxy, method, args) -> {
                    if (CACHED_METHODS.contains(method.getName())) {
                        List<Object> key = Arrays.asList(identity, method.getName(),
                                Arrays.asList(args.clone()));
                        return readMetadata(metadata, method, args, key);
                    }
                    return invokeDelegate(metadata, method, args);
                });
    }

    /**
     * Reads each successful lookup once and returns independent, disconnected cursors.
     *
     * @param metadata live metadata for cache misses only
     * @param method requested column or key lookup
     * @param args exact JDBC lookup arguments, preserving nulls and identifier case
     * @param key connection identity and lookup parameters
     * @return caller-owned cursor that can be closed without invalidating other readers
     * @throws Throwable original JDBC failure; failed reads are not cached
     */
    private synchronized ResultSet readMetadata(DatabaseMetaData metadata, Method method,
            Object[] args, List<Object> key) throws Throwable {
        CachedRowSet snapshot = snapshots.get(key);
        if (snapshot == null) {
            snapshot = RowSetProvider.newFactory().createCachedRowSet();
            try (ResultSet result = (ResultSet) invokeDelegate(metadata, method, args)) {
                snapshot.populate(result);
            }
            snapshots.put(key, snapshot);
        }
        // Avoid serialization-based createCopy while keeping each cursor independently owned.
        CachedRowSet copy = RowSetProvider.newFactory().createCachedRowSet();
        snapshot.beforeFirst();
        copy.populate(snapshot);
        return copy;
    }

    /**
     * Delegates a JDBC call without wrapping its checked exceptions in reflection exceptions.
     *
     * @param target original JDBC object
     * @param method interface method
     * @param args invocation arguments
     * @return original result
     * @throws Throwable original invocation failure
     */
    private static Object invokeDelegate(Object target, Method method, Object[] args)
            throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
