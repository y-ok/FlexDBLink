package io.github.yok.flexdblink.core;

import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.db.DbDialectHandler;
import io.github.yok.flexdblink.db.LobResolvingTableWrapper;
import io.github.yok.flexdblink.parser.DataFormat;
import io.github.yok.flexdblink.parser.DataLoaderFactory;
import io.github.yok.flexdblink.util.ErrorHandler;
import io.github.yok.flexdblink.util.LogPathUtil;
import io.github.yok.flexdblink.util.TableDependencyResolver;
import io.github.yok.flexdblink.util.TableOrderingFile;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.filter.DefaultColumnFilter;
import org.dbunit.operation.DatabaseOperation;

/**
 * Utility class that loads CSV files and external LOB files via DBUnit and performs data loads
 * (CLEAN_INSERT / UPDATE / INSERT) for relational database products through dialect handlers.
 *
 * <p>
 * <strong>Operating modes:</strong>
 * </p>
 * <ul>
 * <li><strong>initial mode</strong>
 * <ul>
 * <li>Insert <em>non-LOB</em> columns using {@link DatabaseOperation#CLEAN_INSERT}.</li>
 * <li>Then apply all columns including LOBs using {@link DatabaseOperation#UPDATE}.</li>
 * </ul>
 * </li>
 * <li><strong>scenario mode</strong>
 * <ul>
 * <li>Delete rows from DB that are exact duplicates of those already inserted in
 * <em>initial</em>.</li>
 * <li>Execute {@link DatabaseOperation#INSERT} only for rows that do not exist in initial
 * data.</li>
 * </ul>
 * </li>
 * </ul>
 *
 * <p>
 * This class does not alter business logic; only configuration-driven behavior and logging.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
@Slf4j
public class DataLoader {

    /**
     * Abstraction for DBUnit write operations used by this loader.
     */
    interface OperationExecutor {

        /**
         * Executes DBUnit CLEAN_INSERT.
         *
         * @param connection DBUnit connection
         * @param dataSet dataset to write
         * @throws Exception execution failure
         */
        void cleanInsert(IDatabaseConnection connection, IDataSet dataSet) throws Exception;

        /**
         * Executes DBUnit DELETE_ALL.
         *
         * @param connection DBUnit connection
         * @param dataSet dataset to delete
         * @throws Exception execution failure
         */
        void deleteAll(IDatabaseConnection connection, IDataSet dataSet) throws Exception;

        /**
         * Executes DBUnit UPDATE.
         *
         * @param connection DBUnit connection
         * @param dataSet dataset to write
         * @throws Exception execution failure
         */
        void update(IDatabaseConnection connection, IDataSet dataSet) throws Exception;

        /**
         * Executes DBUnit INSERT.
         *
         * @param connection DBUnit connection
         * @param dataSet dataset to write
         * @throws Exception execution failure
         */
        void insert(IDatabaseConnection connection, IDataSet dataSet) throws Exception;
    }

    // Base directory settings for CSV/LOB files
    private final PathsConfig pathsConfig;

    // Holder of JDBC connection settings
    private final ConnectionConfig connectionConfig;

    // Factory function to create a DB dialect handler
    private final Function<ConnectionConfig.Entry, DbDialectHandler> dialectFactory;

    // Configuration class holding dbunit.* settings from application.yml
    private final DbUnitConfig dbUnitConfig;

    // Exclude-table settings for dump/load (dump.exclude-tables)
    private final DumpConfig dumpConfig;

    // DBUnit operation executor (replaceable in tests)
    private OperationExecutor operationExecutor;

    // Scenario-mode duplicate detection and deletion
    private ScenarioDuplicateHandler scenarioHandler;

    // Insert summary: dbId → (table → total inserted count)
    private final Map<String, Map<String, Integer>> insertSummary = new LinkedHashMap<>();

    /**
     * Creates a loader with default DBUnit operations.
     *
     * @param pathsConfig path settings
     * @param connectionConfig connection settings
     * @param dialectFactory dialect resolver
     * @param dbUnitConfig DBUnit settings
     * @param dumpConfig dump settings
     */
    public DataLoader(PathsConfig pathsConfig, ConnectionConfig connectionConfig,
            Function<ConnectionConfig.Entry, DbDialectHandler> dialectFactory,
            DbUnitConfig dbUnitConfig, DumpConfig dumpConfig) {
        this(pathsConfig, connectionConfig, dialectFactory, dbUnitConfig, dumpConfig,
                new OperationExecutor() {
                    @Override
                    public void cleanInsert(IDatabaseConnection connection, IDataSet dataSet)
                            throws Exception {
                        DatabaseOperation.CLEAN_INSERT.execute(connection, dataSet);
                    }

                    @Override
                    public void deleteAll(IDatabaseConnection connection, IDataSet dataSet)
                            throws Exception {
                        DatabaseOperation.DELETE_ALL.execute(connection, dataSet);
                    }

                    @Override
                    public void update(IDatabaseConnection connection, IDataSet dataSet)
                            throws Exception {
                        DatabaseOperation.UPDATE.execute(connection, dataSet);
                    }

                    @Override
                    public void insert(IDatabaseConnection connection, IDataSet dataSet)
                            throws Exception {
                        DatabaseOperation.INSERT.execute(connection, dataSet);
                    }
                });
    }

    /**
     * Creates a loader with a custom DBUnit operation executor.
     *
     * @param pathsConfig path settings
     * @param connectionConfig connection settings
     * @param dialectFactory dialect resolver
     * @param dbUnitConfig DBUnit settings
     * @param dumpConfig dump settings
     * @param operationExecutor executor for DBUnit write operations
     */
    DataLoader(PathsConfig pathsConfig, ConnectionConfig connectionConfig,
            Function<ConnectionConfig.Entry, DbDialectHandler> dialectFactory,
            DbUnitConfig dbUnitConfig, DumpConfig dumpConfig, OperationExecutor operationExecutor) {
        this.pathsConfig = pathsConfig;
        this.connectionConfig = connectionConfig;
        this.dialectFactory = dialectFactory;
        this.dbUnitConfig = dbUnitConfig;
        this.dumpConfig = dumpConfig;
        this.operationExecutor = operationExecutor;
        this.scenarioHandler = new ScenarioDuplicateHandler();
    }

    /**
     * Returns current operation executor (for tests).
     *
     * @return current operation executor
     */
    OperationExecutor getOperationExecutor() {
        return operationExecutor;
    }

    /**
     * Replaces operation executor (for tests).
     *
     * @param operationExecutor replacement executor
     */
    void setOperationExecutor(OperationExecutor operationExecutor) {
        this.operationExecutor = operationExecutor;
    }

    /**
     * Replaces the {@link ScenarioDuplicateHandler} used for scenario-mode duplicate detection.
     * Intended for testing.
     *
     * @param scenarioHandler replacement handler
     */
    void setScenarioDuplicateHandler(ScenarioDuplicateHandler scenarioHandler) {
        this.scenarioHandler = scenarioHandler;
    }

    /**
     * Entry point for data loading.
     *
     * @param scenario scenario name; if {@code null} or empty, only {@link DbUnitConfig#preDirName}
     *        is executed
     * @param targetDbIds list of target DB IDs; if {@code null} or empty, all DBs are targeted
     */
    public void execute(String scenario, List<String> targetDbIds) {
        String preMode = dbUnitConfig.getPreDirName();
        String mode = (scenario == null || scenario.isEmpty()) ? preMode : scenario;
        log.info("=== DataLoader started (mode={}, target DBs={}) ===", mode, targetDbIds);

        for (ConnectionConfig.Entry entry : connectionConfig.getConnections()) {
            String dbId = entry.getId();
            if (targetDbIds != null && !targetDbIds.isEmpty() && !targetDbIds.contains(dbId)) {
                log.info("[{}] Not targeted → skipping", dbId);
                continue;
            }

            DbDialectHandler dialectHandler = dialectFactory.apply(entry);

            // initial mode load
            File initialDir = new File(pathsConfig.getLoad(), preMode + File.separator + dbId);
            deploy(initialDir, dbId, true, entry, dialectHandler,
                    "Initial data load failed (DB=" + dbId + ")");

            // scenario mode load
            if (!preMode.equals(mode)) {
                File scenarioDir = new File(pathsConfig.getLoad(), mode + File.separator + dbId);
                deploy(scenarioDir, dbId, false, entry, dialectHandler,
                        "Scenario data load failed (DB=" + dbId + ")");
            }
        }

        log.info("=== DataLoader finished ===");
        logSummary();
    }

    /**
     * Deletes all rows for the specified tables in reverse order (child-first).
     *
     * <p>
     * DBUnit CLEAN_INSERT performs DELETE_ALL internally per table. If parent tables are deleted
     * while child tables still contain rows referencing them, FK constraints can fail.
     * </p>
     *
     * @param dbConn DBUnit connection
     * @param tables insert order (parent-first)
     * @param dbId DB identifier for logging
     * @throws Exception if DBUnit operation fails
     */
    private void deleteAllInReverseOrder(IDatabaseConnection dbConn, List<String> tables,
            String dbId) throws Exception {

        if (tables == null || tables.size() <= 1) {
            return;
        }

        List<String> deleteOrder = new ArrayList<>(tables);
        Collections.reverse(deleteOrder);

        for (String table : deleteOrder) {
            IDataSet ds = dbConn.createDataSet(new String[] {table});
            operationExecutor.deleteAll(dbConn, ds);
            log.info("[{}] Table[{}] Initial | deletedAll", dbId, table);
        }
    }

    /**
     * Reads dataset (CSV/JSON/YAML/XML + LOB) from the specified directory.
     *
     * <p>
     * In <em>initial</em> mode, performs CLEAN_INSERT + UPDATE.<br>
     * In <em>scenario</em> mode, deletes rows that are duplicates of initial and INSERTs the
     * remainder.
     * </p>
     *
     * @param dir directory where dataset files are located
     * @param dbId connections.id (for logging)
     * @param initial {@code true}=initial mode, {@code false}=scenario mode
     * @param entry JDBC connection info
     * @param dialectHandler DB dialect handler providing vendor-specific behavior
     * @param errorMessage log message to output on fatal error
     */
    private void deploy(File dir, String dbId, boolean initial, ConnectionConfig.Entry entry,
            DbDialectHandler dialectHandler, String errorMessage) {
        if (!dir.exists()) {
            log.warn("[{}] Directory does not exist → skipping", dbId);
            return;
        }

        try {
            // Ensure table-ordering.txt exists
            TableOrderingFile.ensure(dir);

            // Load table list from table-ordering.txt
            Path orderPath = new File(dir, "table-ordering.txt").toPath();
            if (!Files.exists(orderPath)) {
                log.info("[{}] No table-ordering.txt found → skipping", dbId);
                return;
            }
            List<String> tables = Files.readAllLines(orderPath, StandardCharsets.UTF_8).stream()
                    .map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList());

            if (tables.isEmpty()) {
                log.info("[{}] No tables → skipping", dbId);
                return;
            }

            // Exclude tables if configured
            if (dumpConfig != null && dumpConfig.getExcludeTables() != null
                    && !dumpConfig.getExcludeTables().isEmpty()) {
                final Set<String> excludeLower =
                        dumpConfig.getExcludeTables().stream().filter(Objects::nonNull)
                                .map(s -> s.toLowerCase(Locale.ROOT)).collect(Collectors.toSet());

                log.info("[{}] Excluded tables: {}", dbId, excludeLower);

                tables = tables.stream()
                        .filter(t -> !excludeLower.contains(t.toLowerCase(Locale.ROOT)))
                        .collect(Collectors.toList());

                if (tables.isEmpty()) {
                    log.info("[{}] No effective tables (all excluded) → skipping", dbId);
                    return;
                }
            }

            Class.forName(entry.getDriverClass());
            try (Connection jdbc = DriverManager.getConnection(entry.getUrl(), entry.getUser(),
                    entry.getPassword())) {
                jdbc.setAutoCommit(false);

                DatabaseConnection dbConn = null;
                try {
                    dbConn = createDbUnitConn(jdbc, entry, dialectHandler);
                    String schema = resolveSchema(entry, dialectHandler);

                    // FK依存関係に基づいてテーブルをトポロジカルソート（親テーブルが先）
                    tables = TableDependencyResolver.resolveLoadOrder(jdbc, jdbc.getCatalog(),
                            schema, tables);
                    log.info("[{}] Table load order resolved by FK dependencies: {}", dbId, tables);

                    if (initial) {
                        deleteAllInReverseOrder(dbConn, tables, dbId);
                    }

                    for (String table : tables) {
                        // ファイル形式を自動判別して読み込み
                        IDataSet dataSet = DataLoaderFactory.create(dir, table);

                        ITable base = dataSet.getTable(table);
                        int rowCount = base.getRowCount();
                        log.info("[{}] Table[{}] rows={}", dbId, table, rowCount);

                        ITable wrapped = new LobResolvingTableWrapper(base, dir, dialectHandler);
                        DefaultDataSet ds = new DefaultDataSet(wrapped);

                        if (initial) {
                            // --- Initial mode (CLEAN_INSERT + UPDATE) ---
                            Column[] lobCols = dialectHandler.getLobColumns(dir.toPath(), table);
                            if (lobCols.length > 0) {
                                boolean anyNotNullLob = dialectHandler.hasNotNullLobColumn(jdbc,
                                        schema, table, lobCols);
                                if (anyNotNullLob) {
                                    log.info(
                                            "[{}] {}: NOT NULL LOB detected; CLEAN_INSERT all cols",
                                            dbId, table);
                                    operationExecutor.cleanInsert(dbConn, ds);
                                } else {
                                    operationExecutor.cleanInsert(dbConn,
                                            new DefaultDataSet(DefaultColumnFilter
                                                    .excludedColumnsTable(base, lobCols)));
                                    operationExecutor.update(dbConn, ds);
                                }
                            } else {
                                operationExecutor.cleanInsert(dbConn, ds);
                            }
                            log.info("[{}] Table[{}] Initial | inserted={}", dbId, table, rowCount);

                        } else {
                            // --- Scenario mode (delete duplicates + INSERT new) ---
                            ITable originalDbTable = dbConn.createDataSet().getTable(table);
                            List<String> pkCols =
                                    dialectHandler.getPrimaryKeyColumns(jdbc, schema, table);

                            Map<Integer, Integer> identicalMap =
                                    scenarioHandler.detectDuplicates(wrapped, originalDbTable,
                                            pkCols, jdbc, schema, table, dialectHandler);

                            if (!identicalMap.isEmpty()) {
                                scenarioHandler.deleteDuplicates(jdbc, schema, table, pkCols,
                                        wrapped.getTableMetaData().getColumns(), originalDbTable,
                                        identicalMap, dialectHandler, dbId);
                            }

                            ScenarioDuplicateHandler.FilteredTable filtered =
                                    new ScenarioDuplicateHandler.FilteredTable(wrapped,
                                            identicalMap.keySet());
                            operationExecutor.insert(dbConn, new DefaultDataSet(filtered));
                            log.info("[{}] Table[{}] Scenario (INSERT only) | inserted={}", dbId,
                                    table, filtered.getRowCount());
                        }

                        // Summary
                        int currentCount = dialectHandler.countRows(jdbc, table);
                        insertSummary.computeIfAbsent(dbId, k -> new LinkedHashMap<>()).put(table,
                                currentCount);
                    }

                    jdbc.commit();
                    log.info("[{}] Transaction committed (tables={})", dbId, tables.size());
                } catch (Exception e) {
                    try {
                        jdbc.rollback();
                        log.warn("[{}] Transaction rolled back due to error.", dbId);
                    } catch (SQLException rollbackEx) {
                        log.warn("[{}] Rollback failed: {}", dbId, rollbackEx.getMessage(),
                                rollbackEx);
                    }
                    throw e;
                } finally {
                    if (dbConn != null) {
                        dbConn.close();
                    }
                }
            }

        } catch (Exception e) {
            log.error("[{}] Unexpected error occurred: {}", dbId, e.getMessage(), e);
            ErrorHandler.errorAndExit(errorMessage, e);
        } finally {
            TableOrderingFile.delete(dir);
        }
    }

    /**
     * Creates a DBUnit {@link DatabaseConnection} from a JDBC {@link Connection}, initializes the
     * session, and applies {@link org.dbunit.dataset.datatype.IDataTypeFactory}.
     *
     * @param jdbc JDBC connection to initialize
     * @param entry connection entry
     * @param dialectHandler DB dialect handler
     * @return configured {@link DatabaseConnection}
     * @throws Exception on errors during session initialization or configuration
     */
    private DatabaseConnection createDbUnitConn(Connection jdbc, ConnectionConfig.Entry entry,
            DbDialectHandler dialectHandler) throws Exception {

        dialectHandler.prepareConnection(jdbc);
        String schema = resolveSchema(entry, dialectHandler);
        return dialectHandler.createDbUnitConnection(jdbc, schema);
    }

    /**
     * Resolves schema by delegating to dialect.
     *
     * @param entry connection entry
     * @param dialectHandler DB dialect handler
     * @return resolved schema name
     */
    private String resolveSchema(ConnectionConfig.Entry entry, DbDialectHandler dialectHandler) {
        return dialectHandler.resolveSchema(entry);
    }

    /**
     * Outputs a consolidated log of data load results for all DBs.
     */
    private void logSummary() {
        log.info("===== Summary =====");
        insertSummary.forEach((dbId, tableMap) -> {
            log.info("DB[{}]:", dbId);
            int maxNameLen = tableMap.keySet().stream().mapToInt(String::length).max().orElse(0);
            int maxCountDigits = tableMap.values().stream().map(cnt -> String.valueOf(cnt).length())
                    .mapToInt(Integer::intValue).max().orElse(0);
            String fmt = "  Table[%-" + maxNameLen + "s] Total=%" + maxCountDigits + "d";
            tableMap.forEach((table, cnt) -> log.info(String.format(fmt, table, cnt)));
        });
        log.info("== Data loading to all DBs has completed ==");
    }

    /**
     * Loads all datasets (CSV / JSON / YAML/YML / XML) under the specified directory into a single
     * database. No "pre" or "scenario" modes are handled here.
     *
     * <p>
     * This method uses a caller-managed external JDBC {@link Connection}. The caller is responsible
     * for transaction control (commit/rollback/close).
     * </p>
     *
     * <p>
     * Load strategy is always equivalent to an "initial load": by default it performs
     * {@code CLEAN_INSERT}. If LOB columns exist and all of them are NULL-allowed, it first
     * performs {@code CLEAN_INSERT} for non-LOB columns, then applies {@code UPDATE} to reflect LOB
     * values. Excluded tables are taken from {@link DumpConfig#excludeTables}.
     * </p>
     *
     * @param dir target directory that contains table files (one file per table)
     * @param entry DB connection entry (used for schema/dialect resolution)
     * @param connection external JDBC connection managed by the caller (autoCommit=false
     *        recommended)
     * @throws SQLException if a database access error occurs
     * @throws IllegalArgumentException if any argument is null
     * @throws IllegalStateException if {@code dir} does not exist or is not a directory
     */
    public void executeWithConnection(File dir, ConnectionConfig.Entry entry, Connection connection)
            throws SQLException {

        // Fail fast on invalid arguments
        if (dir == null) {
            throw new IllegalArgumentException("Target directory must not be null.");
        }
        if (entry == null) {
            throw new IllegalArgumentException("Connection entry must not be null.");
        }
        if (connection == null) {
            throw new IllegalArgumentException("JDBC connection must not be null.");
        }

        final String dbId = entry.getId();
        log.info("=== DataLoader (external connection) START (db={}, dir={}) ===", dbId,
                LogPathUtil.renderDirForLog(dir));

        // Strict existence check (no silent skip)
        if (!dir.exists() || !dir.isDirectory()) {
            log.error("[{}] Target directory does not exist or is not a directory: {}", dbId,
                    dir.getAbsolutePath());
            throw new IllegalStateException(
                    "Target directory does not exist or is not a directory: "
                            + dir.getAbsolutePath());
        }

        // Resolve dialect handler
        DbDialectHandler dialectHandler = dialectFactory.apply(entry);

        // Execute actual loading with a single directory (no mode concept)
        deployWithConnection(dir, dbId, entry, connection, dialectHandler,
                "Data load failed (db=" + dbId + ")");

        log.info("=== DataLoader (external connection) END (db={}) ===", dbId);
    }

    /**
     * Uses the caller-managed JDBC {@link Connection} to load all tables under a single directory,
     * applying an "initial load" strategy (CLEAN_INSERT by default, with a LOB-specific exception).
     *
     * <p>
     * This method does not perform any "pre/scenario" branching. Only supported file formats within
     * the directory (CSV / JSON / YAML/YML / XML) are considered.
     * </p>
     *
     * @param dir dataset directory for this DB
     * @param dbId DB identifier used for logging (typically {@code entry.getId()})
     * @param entry DB connection entry (used for schema/dialect resolution)
     * @param jdbc caller-managed JDBC connection
     * @param dialectHandler DB dialect handler
     * @param errorMessage message passed to {@link ErrorHandler} on fatal errors
     * @throws IllegalStateException if {@code dir} does not exist or is not a directory
     */
    private void deployWithConnection(File dir, String dbId, ConnectionConfig.Entry entry,
            Connection jdbc, DbDialectHandler dialectHandler, String errorMessage) {

        // Defensive check (callers should have validated already)
        if (!dir.exists() || !dir.isDirectory()) {
            log.error("[{}] Target directory does not exist or is not a directory: {}", dbId,
                    dir.getAbsolutePath());
            throw new IllegalStateException(
                    "Target directory does not exist or is not a directory: "
                            + dir.getAbsolutePath());
        }

        IDatabaseConnection dbConn = null;
        try {
            // Generate table-ordering file for CSV (legacy behavior)
            TableOrderingFile.ensure(dir);

            // Collect candidate table names from files (only supported extensions)
            Set<String> tableSet = new HashSet<>();
            File[] files = dir.listFiles((d, name) -> {
                String ext = FilenameUtils.getExtension(name).toLowerCase(Locale.ROOT);
                return Arrays.stream(DataFormat.values()).anyMatch(fmt -> fmt.matches(ext));
            });
            if (files != null) {
                for (File f : files) {
                    // file base name = table name
                    String base = FilenameUtils.getBaseName(f.getName());
                    tableSet.add(base);
                }
            }

            List<String> tables = new ArrayList<>(tableSet);
            if (tables.isEmpty()) {
                log.info("[{}] No dataset files found under: {}", dbId, dir.getAbsolutePath());
                return;
            }
            // Stable alphabetical order
            tables.sort(String::compareTo);

            // Apply DumpConfig exclusions
            if (dumpConfig != null && dumpConfig.getExcludeTables() != null
                    && !dumpConfig.getExcludeTables().isEmpty()) {
                final Set<String> excludeLower =
                        dumpConfig.getExcludeTables().stream().filter(Objects::nonNull)
                                .map(s -> s.toLowerCase(Locale.ROOT)).collect(Collectors.toSet());

                log.info("[{}] Excluded tables: {}", dbId, excludeLower);

                tables = tables.stream()
                        .filter(t -> !excludeLower.contains(t.toLowerCase(Locale.ROOT)))
                        .collect(Collectors.toList());

                if (tables.isEmpty()) {
                    log.info("[{}] No effective tables remain after exclusions.", dbId);
                    return;
                }
            }

            // FK依存関係に基づいてテーブルをトポロジカルソート（親テーブルが先）
            String schema = resolveSchema(entry, dialectHandler);
            try {
                tables = TableDependencyResolver.resolveLoadOrder(jdbc, jdbc.getCatalog(), schema,
                        tables);
                log.info("[{}] Table load order resolved by FK dependencies: {}", dbId, tables);
            } catch (SQLException e) {
                log.warn(
                        "[{}] FK dependency resolution failed; using alphabetical order. reason={}",
                        dbId, e.getMessage());
            }

            // Create DBUnit connection
            dbConn = createDbUnitConn(jdbc, entry, dialectHandler);

            deleteAllInReverseOrder(dbConn, tables, dbId);

            // Load each table (always "initial load" strategy)
            for (String table : tables) {
                // Resolve dataset for the table (CSV / JSON / YAML / XML)
                IDataSet dataSet;
                try {
                    dataSet = DataLoaderFactory.create(dir, table);
                } catch (Exception e) {
                    // Skip only this table if resolution fails
                    log.warn("[{}] Failed to resolve dataset for table={} — skipping: {}", dbId,
                            table, e.getMessage());
                    continue;
                }

                // Log table definition details via dialect handler (DDL, PKs, etc.)
                dialectHandler.logTableDefinition(jdbc, schema, table, dbId);

                // Wrap with LOB resolver (expands file:... references)
                ITable base = dataSet.getTable(table);
                ITable wrapped = new LobResolvingTableWrapper(base, dir, dialectHandler);
                DefaultDataSet ds = new DefaultDataSet(wrapped);

                // Determine LOB handling strategy
                Column[] lobCols = dialectHandler.getLobColumns(dir.toPath(), table);
                if (lobCols.length > 0) {
                    boolean anyNotNullLob =
                            dialectHandler.hasNotNullLobColumn(jdbc, schema, table, lobCols);
                    if (anyNotNullLob) {
                        // LOB contains NOT NULL → single CLEAN_INSERT
                        operationExecutor.cleanInsert(dbConn, ds);
                    } else {
                        // All LOB columns are NULL-allowed:
                        // 1) CLEAN_INSERT for non-LOB columns
                        ITable nonLobOnly = DefaultColumnFilter.excludedColumnsTable(base, lobCols);
                        operationExecutor.cleanInsert(dbConn, new DefaultDataSet(nonLobOnly));
                        // 2) UPDATE to reflect LOB values (including file:... sources)
                        operationExecutor.update(dbConn, ds);
                    }
                } else {
                    // No LOB columns → simple CLEAN_INSERT
                    operationExecutor.cleanInsert(dbConn, ds);
                }

                // Update summary
                int currentCount = dialectHandler.countRows(jdbc, table);
                insertSummary.computeIfAbsent(dbId, k -> new LinkedHashMap<>()).put(table,
                        currentCount);

                log.info("[{}] Loaded table successfully: {} (current rows={})", dbId, table,
                        currentCount);
            }

        } catch (Exception e) {
            log.error("[{}] Unexpected error occurred: {}", dbId, e.getMessage(), e);
            ErrorHandler.errorAndExit(errorMessage, e);
        } finally {
            if (dbConn != null) {
                try {
                    dbConn.close();
                } catch (Exception closeEx) {
                    log.warn("[{}] Failed to close DBUnit connection: {}", dbId,
                            closeEx.getMessage(), closeEx);
                }
            }
            TableOrderingFile.delete(dir);
        }
    }
}
