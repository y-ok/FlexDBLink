package io.github.yok.flexdblink.core;

import io.github.yok.flexdblink.db.DbDialectHandler;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;

/**
 * Handles scenario-mode duplicate detection and deletion logic extracted from {@code DataLoader}.
 *
 * <p>
 * In scenario mode, rows that already exist in the initial data are detected as duplicates and
 * deleted from the DB before the scenario INSERT is applied. This class encapsulates that logic in
 * isolation from the main orchestration flow.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
@Slf4j
class ScenarioDuplicateHandler {

    /**
     * An {@link ITable} decorator that hides a set of row indices from its delegate.
     *
     * <p>
     * Used to filter out duplicate rows before performing scenario INSERT.
     * </p>
     */
    static class FilteredTable implements ITable {

        private final ITable delegate;
        private final IntPredicate shouldSkip;
        private final int rowCount;

        /**
         * Constructs a FilteredTable that hides rows matching the given {@link IntPredicate}.
         *
         * @param delegate underlying table
         * @param shouldSkip predicate returning {@code true} for physical row indices to hide
         * @param rowCount number of logical rows (after filtering)
         */
        FilteredTable(ITable delegate, IntPredicate shouldSkip, int rowCount) {
            this.delegate = delegate;
            this.shouldSkip = shouldSkip;
            this.rowCount = rowCount;
        }

        /**
         * Constructs a FilteredTable that hides the specified set of physical row indices.
         *
         * @param delegate underlying table
         * @param skipRows physical row indices to hide
         */
        FilteredTable(ITable delegate, Set<Integer> skipRows) {
            this(delegate, skipRows::contains, delegate.getRowCount() - skipRows.size());
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public ITableMetaData getTableMetaData() {
            return delegate.getTableMetaData();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int getRowCount() {
            return rowCount;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Object getValue(int row, String column) throws DataSetException {
            int physical = row;
            for (int i = 0; i <= physical; i++) {
                if (shouldSkip.test(i)) {
                    physical++;
                }
            }
            return delegate.getValue(physical, column);
        }
    }

    /**
     * Detects which rows in {@code wrapped} are exact duplicates of rows already present in
     * {@code originalDbTable}.
     *
     * <p>
     * When primary key columns are provided, matching is done by PK values only using a HashMap for
     * O(n) lookup. When no PK columns exist, all columns are compared via {@link #rowsEqual}.
     * </p>
     *
     * @param wrapped the CSV-sourced dataset table
     * @param originalDbTable the current DB snapshot table
     * @param pkCols primary key column names (empty list means no PK)
     * @param jdbc raw JDBC connection (used when falling back to full-column comparison)
     * @param schema schema name
     * @param table table name
     * @param dialectHandler DB dialect handler
     * @return map of CSV row index → DB row index for each detected duplicate pair
     * @throws DataSetException on DBUnit error during comparison
     */
    Map<Integer, Integer> detectDuplicates(ITable wrapped, ITable originalDbTable,
            List<String> pkCols, Connection jdbc, String schema, String table,
            DbDialectHandler dialectHandler) throws DataSetException {

        Map<Integer, Integer> identicalMap = new LinkedHashMap<>();
        try {
            Column[] cols = wrapped.getTableMetaData().getColumns();

            if (!pkCols.isEmpty()) {
                // With PK: build HashMap from DB table first (O(n)), then lookup CSV rows (O(n))
                Map<List<Object>, Integer> dbIndex = new LinkedHashMap<>();
                for (int j = 0; j < originalDbTable.getRowCount(); j++) {
                    List<Object> key = new ArrayList<>();
                    for (String pk : pkCols) {
                        key.add(originalDbTable.getValue(j, pk));
                    }
                    dbIndex.putIfAbsent(key, j);
                }
                for (int i = 0; i < wrapped.getRowCount(); i++) {
                    List<Object> key = new ArrayList<>();
                    for (String pk : pkCols) {
                        key.add(wrapped.getValue(i, pk));
                    }
                    Integer dbRow = dbIndex.get(key);
                    if (dbRow != null) {
                        identicalMap.put(i, dbRow);
                        log.debug("[{}] Table[{}] Duplicate detected: csvRow={} matches dbRow={}",
                                schema, table, i, dbRow);
                    }
                }
            } else {
                // Without PK → compare all columns
                // Cache column type names to avoid repeated JDBC metadata queries
                Map<String, String> columnTypeNames = new LinkedHashMap<>();
                for (Column col : cols) {
                    String colName = col.getColumnName();
                    String typeName =
                            dialectHandler.getColumnTypeName(jdbc, schema, table, colName);
                    if (typeName == null) {
                        throw new SQLException(
                                "SQL type name must not be null: " + table + "." + colName);
                    }
                    columnTypeNames.put(colName, typeName.toUpperCase(Locale.ROOT));
                }
                for (int i = 0; i < wrapped.getRowCount(); i++) {
                    for (int j = 0; j < originalDbTable.getRowCount(); j++) {
                        boolean match = rowsEqual(wrapped, originalDbTable, columnTypeNames,
                                table, i, j, cols, dialectHandler);
                        if (match) {
                            identicalMap.put(i, j);
                            log.debug(
                                    "[{}] Table[{}] Duplicate detected: csvRow={} matches dbRow={}",
                                    schema, table, i, j);
                            break;
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new DataSetException("Failed to detect duplicates for table: " + table, e);
        }
        return identicalMap;
    }

    /**
     * Deletes rows from the DB that were identified as duplicates of initial data.
     *
     * <p>
     * When PK columns exist, a batch {@code DELETE WHERE pk = ?} is executed. When no PK exists,
     * individual {@code DELETE WHERE col1 = ? AND col2 IS NULL AND ...} statements are issued.
     * </p>
     *
     * @param jdbc raw JDBC connection
     * @param schema schema name
     * @param table table name
     * @param pkCols primary key column names
     * @param cols all columns of the table
     * @param originalDbTable DB snapshot table (used to read values for DELETE WHERE clauses)
     * @param identicalMap map of CSV row index → DB row index for duplicates
     * @param dialectHandler DB dialect handler (for identifier quoting)
     * @param dbId DB identifier for logging
     * @throws DataSetException on SQL error while deleting
     */
    void deleteDuplicates(Connection jdbc, String schema, String table, List<String> pkCols,
            Column[] cols, ITable originalDbTable, Map<Integer, Integer> identicalMap,
            DbDialectHandler dialectHandler, String dbId) throws DataSetException {

        if (identicalMap.isEmpty()) {
            return;
        }

        try {
            if (!pkCols.isEmpty()) {
                // DELETE by PK
                String where = pkCols.stream().map(c -> dialectHandler.quoteIdentifier(c) + " = ?")
                        .collect(Collectors.joining(" AND "));
                String deleteSql = buildQualifiedDeleteSql(schema, table, where, dialectHandler);

                try (PreparedStatement ps = jdbc.prepareStatement(deleteSql)) {
                    for (Map.Entry<Integer, Integer> e : identicalMap.entrySet()) {
                        int dbRow = e.getValue();
                        for (int k = 0; k < pkCols.size(); k++) {
                            String pk = pkCols.get(k);
                            Object val = originalDbTable.getValue(dbRow, pk);
                            ps.setObject(k + 1, val);
                        }
                        ps.addBatch();
                    }
                    int deleted = java.util.Arrays.stream(ps.executeBatch()).sum();
                    log.info("[{}] Table[{}] Deleted duplicates by primary key {}", dbId, table,
                            deleted);
                }
            } else {
                // DELETE by all columns (NULL-safe)
                int deleted = 0;
                for (Map.Entry<Integer, Integer> e : identicalMap.entrySet()) {
                    int dbRow = e.getValue();
                    List<Object> bindValues = new ArrayList<>();
                    List<String> predicates = new ArrayList<>();

                    for (Column col : cols) {
                        String colName = col.getColumnName();
                        String quotedColumn = dialectHandler.quoteIdentifier(colName);
                        Object val = originalDbTable.getValue(dbRow, colName);
                        if (val == null) {
                            predicates.add(quotedColumn + " IS NULL");
                        } else {
                            predicates.add(quotedColumn + " = ?");
                            bindValues.add(val);
                        }
                    }

                    String where = String.join(" AND ", predicates);
                    String deleteSql =
                            buildQualifiedDeleteSql(schema, table, where, dialectHandler);

                    try (PreparedStatement ps = jdbc.prepareStatement(deleteSql)) {
                        for (int i = 0; i < bindValues.size(); i++) {
                            ps.setObject(i + 1, bindValues.get(i));
                        }
                        deleted += ps.executeUpdate();
                    }
                }
                log.info("[{}] Table[{}] Deleted duplicates by all columns → {}", dbId, table,
                        deleted);
            }
        } catch (SQLException e) {
            throw new DataSetException("Failed to delete duplicates for table: " + table, e);
        }
    }

    /**
     * Compares a single row from the CSV table against a single row from the DB snapshot,
     * column-by-column, applying dialect-specific type normalization.
     *
     * @param csvTable the CSV-sourced dataset table
     * @param dbTable the DB snapshot table
     * @param columnTypeNames pre-cached map of column name to upper-case SQL type name
     * @param tableName table name
     * @param csvRow zero-based CSV row index
     * @param dbRow zero-based DB row index
     * @param cols columns to compare
     * @param dialectHandler DB dialect handler
     * @return {@code true} if all columns are equal after normalization
     * @throws DataSetException on DBUnit error during value access
     * @throws SQLException on JDBC metadata access failure during comparison
     */
    boolean rowsEqual(ITable csvTable, ITable dbTable, Map<String, String> columnTypeNames,
            String tableName, int csvRow, int dbRow, Column[] cols, DbDialectHandler dialectHandler)
            throws DataSetException, SQLException {

        for (Column col : cols) {
            String colName = col.getColumnName();
            String typeName = columnTypeNames.get(colName);

            log.debug("Table[{}] Column[{}] Type=[{}]", tableName, colName, typeName);

            String rawCsv = Optional.ofNullable(csvTable.getValue(csvRow, colName))
                    .map(Object::toString).orElse(null);
            String csvCell = StringUtils.trimToNull(rawCsv);

            Object rawDbObj = dbTable.getValue(dbRow, colName);
            String rawDb = rawDbObj == null ? null : rawDbObj.toString();
            String dbCell = StringUtils.trimToNull(rawDb);

            log.debug("Table[{}] Before normalize: csvRow={}, dbRow={}, col={}, csv=[{}], db=[{}]",
                    tableName, csvRow, dbRow, colName, csvCell, dbCell);

            boolean rawComparison = dialectHandler.shouldUseRawValueForComparison(typeName);
            if (!rawComparison) {
                String formatted = dialectHandler.formatDbValueForCsv(colName, rawDbObj);
                dbCell = StringUtils.trimToNull(formatted);
            }

            String normalizedCsv =
                    dialectHandler.normalizeValueForComparison(colName, typeName, csvCell);
            if (normalizedCsv != null || csvCell == null) {
                csvCell = normalizedCsv;
            }
            String normalizedDb =
                    dialectHandler.normalizeValueForComparison(colName, typeName, dbCell);
            if (normalizedDb != null || dbCell == null) {
                dbCell = normalizedDb;
            }

            log.debug("Table[{}] After normalize: csvRow={}, dbRow={}, col={}, csv=[{}], db=[{}]",
                    tableName, csvRow, dbRow, colName, csvCell, dbCell);

            if (StringUtils.isAllBlank(csvCell) && StringUtils.isAllBlank(dbCell)) {
                continue;
            }
            if (!Objects.equals(csvCell, dbCell)) {
                log.debug("Mismatch: Table[{}] csvRow={}, dbRow={}, col={}, csv=[{}], db=[{}]",
                        tableName, csvRow, dbRow, colName, csvCell, dbCell);
                return false;
            }
        }
        return true;
    }

    /**
     * Builds a DELETE SQL statement with an optional schema qualifier.
     *
     * <p>
     * When {@code schema} is {@code null}, the table name is used without a schema prefix.
     * </p>
     *
     * @param schema schema name (may be {@code null})
     * @param table table name
     * @param where WHERE clause
     * @param dialectHandler DB dialect handler (for identifier quoting)
     * @return DELETE SQL string
     */
    private String buildQualifiedDeleteSql(String schema, String table, String where,
            DbDialectHandler dialectHandler) {
        String qualifiedTable;
        if (schema == null) {
            qualifiedTable = dialectHandler.quoteIdentifier(table);
        } else {
            qualifiedTable = dialectHandler.quoteIdentifier(schema) + "."
                    + dialectHandler.quoteIdentifier(table);
        }
        return String.format("DELETE FROM %s WHERE %s", qualifiedTable, where);
    }
}
