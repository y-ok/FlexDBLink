package io.github.yok.flexdblink.db;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import lombok.extern.slf4j.Slf4j;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;

/**
 * Wrapper {@link ITable} implementation that resolves LOB references while delegating to the
 * original {@link ITable}.
 *
 * <p>
 * This class intercepts cell values and, if a value is a LOB file reference (e.g.,
 * {@code file:...}), loads the actual LOB content through a {@link DbDialectHandler}. Otherwise, it
 * converts CSV string values to DB-bindable objects using the dialect handler.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
@Slf4j
public class LobResolvingTableWrapper implements ITable {

    // The wrapped source table
    private final ITable delegate;
    // Base directory where LOB files are located
    private final File baseDir;
    // Dialect-specific LOB processing and CSV conversion utility
    private final DbDialectHandler dialectHandler;
    // Keep only the last reference per column, bounding retention to one row's LOB values.
    private final Map<String, Entry<String, Object>> lobValues = new HashMap<>();

    /**
     * Constructor.
     *
     * @param delegate the original {@link ITable} implementation to wrap
     * @param baseDir directory where LOB files are stored
     * @param dialectHandler handler for reading LOBs and converting CSV values
     */
    public LobResolvingTableWrapper(ITable delegate, File baseDir,
            DbDialectHandler dialectHandler) {
        this.delegate = delegate;
        this.baseDir = baseDir;
        this.dialectHandler = dialectHandler;
    }

    /**
     * Returns table metadata from the {@code delegate}.
     *
     * @return table metadata
     */
    @Override
    public ITableMetaData getTableMetaData() {
        return delegate.getTableMetaData();
    }

    /**
     * Returns the total row count from the {@code delegate}.
     *
     * @return number of rows
     */
    @Override
    public int getRowCount() {
        return delegate.getRowCount();
    }

    /**
     * Gets the value for the specified row/column. If the value is a LOB reference or requires CSV
     * string conversion, the appropriate processing is applied.
     *
     * @param row zero-based row index
     * @param columnName column name
     * @return converted value object
     * @throws DataSetException if conversion or file reading fails
     */
    @Override
    public Object getValue(int row, String columnName) throws DataSetException {
        Object raw = delegate.getValue(row, columnName);
        if (raw instanceof String) {
            String str = (String) raw;
            String table = getTableMetaData().getTableName();

            // LOB file reference ("file:...")
            if (str.startsWith("file:")) {
                String fileRef = str.substring("file:".length());
                try {
                    Entry<String, Object> cached = lobValues.get(columnName);
                    if (cached != null && cached.getKey().equals(fileRef)) {
                        return cached.getValue();
                    }
                    log.debug("Reading LOB file: table={}, column={}, file={}", table, columnName,
                            fileRef);
                    Object value = dialectHandler.readLobFile(fileRef, table, columnName, baseDir);
                    lobValues.put(columnName, new SimpleImmutableEntry<>(fileRef, value));
                    return value;
                } catch (IOException e) {
                    String msg =
                            String.format("Failed to read LOB file: table=%s, column=%s, file=%s",
                                    table, columnName, fileRef);
                    log.error(msg, e);
                    throw new DataSetException(msg, e);
                }
            }

            // CSV string → DB type conversion
            log.debug("Converting CSV string: table={}, column={}, value={}", table, columnName,
                    str);
            return dialectHandler.convertCsvValueToDbType(table, columnName, str);
        }

        // Non-string values are returned as-is
        return raw;
    }
}
