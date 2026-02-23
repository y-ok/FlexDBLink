package io.github.yok.flexdblink.core;

import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.FilePatternConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.db.DbDialectHandler;
import io.github.yok.flexdblink.util.ErrorHandler;
import io.github.yok.flexdblink.util.LobPathConstants;
import io.github.yok.flexdblink.util.TableDependencyResolver;
import io.github.yok.flexdblink.util.TableOrderingFile;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.dbunit.database.DatabaseConnection;

/**
 * Core class that dumps table data (CSV) and BLOB/CLOB columns (files) from a database in one pass.
 *
 * <p>
 * <strong>Main responsibilities:</strong>
 * </p>
 * <ul>
 * <li>Process multiple DB connections in sequence (via {@link ConnectionConfig}).</li>
 * <li>Export all rows of each table as CSV (<code>dump/{scenario}/{dbId}/{table}.csv</code>).</li>
 * <li>Export all BLOB/CLOB columns under <code>dump/{scenario}/{dbId}/files/</code> and replace the
 * corresponding CSV cell with <code>file:&lt;fileName&gt;</code>.</li>
 * <li>Resolve file names using {@link io.github.yok.flexdblink.config.FileNameResolver} constructed
 * from {@link FilePatternConfig}.</li>
 * </ul>
 *
 * @see FilePatternConfig
 * @see PathsConfig
 * @author Yasuharu.Okawauchi
 */
@Slf4j
public class DataDumper {

    // Base output directory for dump (mapped from application.yml's dump path via PathsConfig)
    private final PathsConfig pathsConfig;

    // List of DB connection settings (application.yml's connections)
    private final ConnectionConfig connectionConfig;

    private final DumpConfig dumpConfig;

    // Factory function to obtain a DB dialect handler
    // Input : ConnectionConfig.Entry
    // Output: corresponding DbDialectHandler
    private final Function<ConnectionConfig.Entry, DbDialectHandler> dialectFactory;

    private final CsvTableExporter csvExporter;
    private final LobFileExporter lobExporter;

    /**
     * Creates dumper using dialect-based schema resolution.
     *
     * @param pathsConfig path settings
     * @param connectionConfig connection settings
     * @param filePatternConfig file pattern settings
     * @param dumpConfig dump settings
     * @param dialectFactory dialect handler factory
     */
    public DataDumper(PathsConfig pathsConfig, ConnectionConfig connectionConfig,
            FilePatternConfig filePatternConfig, DumpConfig dumpConfig,
            Function<ConnectionConfig.Entry, DbDialectHandler> dialectFactory) {
        this.pathsConfig = pathsConfig;
        this.connectionConfig = connectionConfig;
        this.dumpConfig = dumpConfig;
        this.dialectFactory = dialectFactory;
        this.csvExporter = new CsvTableExporter();
        this.lobExporter = new LobFileExporter(filePatternConfig);
    }

    /**
     * Dumps CSV and BLOB/CLOB files for each table according to the specified scenario and target
     * DB list.
     *
     * @param scenario dump scenario name (null/empty is an error)
     * @param targetDbIds list of target DB IDs (if null/empty, all DBs are processed)
     */
    public void execute(String scenario, List<String> targetDbIds) {
        // Validate scenario name
        if (scenario == null || scenario.isEmpty()) {
            ErrorHandler.errorAndExit("Scenario name is required in --dump mode.");
        }

        // Create or check the dump base directory
        File baseDir = new File(pathsConfig.getDump());
        ensureDirectoryExists(baseDir, "Failed to create base output directory");

        // Scenario folder (latest)
        File scenarioDir = new File(baseDir, scenario);

        // ── If the scenario folder already exists, back it up
        if (scenarioDir.exists()) {
            Path dataPath = Path.of(pathsConfig.getDataPath()).toAbsolutePath().normalize();
            backupScenarioDirectory(dataPath, scenarioDir);
        }

        // 3) Create the new scenario folder
        ensureDirectoryExists(scenarioDir, "Failed to create dump output directory");

        // Execute dump per DB
        for (ConnectionConfig.Entry entry : connectionConfig.getConnections()) {
            String dbId = entry.getId();
            // Target DB filter
            if (targetDbIds != null && !targetDbIds.isEmpty() && !targetDbIds.contains(dbId)) {
                log.info("[{}] Skipped: not in target DB list", dbId);
                continue;
            }
            log.info("[{}] === DB dump started ===", dbId);

            // Collect results in LinkedHashMap to keep table iteration order
            Map<String, Integer> summaryMap = new LinkedHashMap<>();

            try {
                // Load JDBC driver
                Class.forName(entry.getDriverClass());
                Connection conn = DriverManager.getConnection(entry.getUrl(), entry.getUser(),
                        entry.getPassword());
                try {

                    // Create dialect handler
                    DbDialectHandler dialectHandler = dialectFactory.apply(entry);

                    // Configure DBUnit connection
                    String schema = dialectHandler.resolveSchema(entry);
                    DatabaseConnection dbConn = dialectHandler.createDbUnitConnection(conn, schema);

                    // --- Get and filter table list ---
                    List<String> tables =
                            fetchTargetTables(conn, schema, dumpConfig.getExcludeTables());
                    if (tables.isEmpty()) {
                        log.warn("[{}] No tables to dump", dbId);
                        dbConn.close();
                        continue;
                    }

                    // FK依存関係に基づいてテーブルをトポロジカルソート（ロードと一貫した順序）
                    try {
                        tables = TableDependencyResolver.resolveLoadOrder(conn, conn.getCatalog(),
                                schema, tables);
                        log.info("[{}] Table dump order resolved by FK dependencies: {}", dbId,
                                tables);
                    } catch (SQLException e) {
                        log.warn("[{}] FK dependency resolution failed; using original order."
                                + " reason={}", dbId, e.getMessage());
                    }

                    // --- Prepare output directories (via common helper) ---
                    File[] dirs = prepareDbOutputDirs(scenarioDir, dbId);
                    File dbDir = dirs[0];
                    File filesDir = dirs[1];

                    // Write table-ordering.txt as a temporary working file
                    TableOrderingFile.write(dbDir, tables);
                    try {
                        for (String tbl : tables) {
                            // --- 1) CSV dump ---
                            File csvFile = new File(dbDir, tbl + ".csv");
                            csvExporter.export(conn, tbl, csvFile, dialectHandler);
                            log.info("[{}] Table[{}] CSV dump completed (UTF-8)", dbId, tbl);

                            // --- 2) BLOB/CLOB dump ---
                            DumpResult result = lobExporter.export(conn, tbl, dbDir, filesDir,
                                    schema, dialectHandler);
                            log.info("[{}] Table[{}] dumped-records={}, BLOB/CLOB file-outputs={}",
                                    dbId, tbl, result.getRowCount(), result.getFileCount());

                            // --- 3) Register to summary map ---
                            summaryMap.put(tbl, result.getRowCount());
                        }
                        // After the dump, output summary
                        logTableSummary(dbId, summaryMap);

                        log.info("[{}] === DB dump completed ===", dbId);
                    } finally {
                        TableOrderingFile.delete(dbDir);
                    }

                    dbConn.close();
                } finally {
                    conn.close();
                }
            } catch (Exception e) {
                ErrorHandler.errorAndExit("Dump failed (DB=" + dbId + ")", e);
            }
        }

        Path dataDir = Paths.get(pathsConfig.getDataPath()).toAbsolutePath().normalize();
        Path scenarioPath = scenarioDir.toPath().toAbsolutePath().normalize();
        String relPath =
                FilenameUtils.separatorsToUnix(dataDir.relativize(scenarioPath).toString());

        log.info("=== All DB dumps completed: Output [{}] ===", relPath);
    }

    /**
     * Creates the following under the specified scenario directory.
     * <ul>
     * <li>1. CSV dump directory ({@code scenarioDir/dbId})</li>
     * <li>2. Subdirectory for BLOB/CLOB files ({@code .../files})</li>
     * </ul>
     * Creates directories if they do not exist; terminates immediately on failure.
     *
     * @param scenarioDir scenario root directory
     * @param dbId DB identifier
     * @return File array: [0] = CSV output directory, [1] = BLOB/CLOB directory
     */
    private File[] prepareDbOutputDirs(File scenarioDir, String dbId) {
        File dbDir = new File(scenarioDir, dbId);
        ensureDirectoryExists(dbDir, "Failed to create DB output directory");
        File filesDir = new File(dbDir, LobPathConstants.DIRECTORY_NAME);
        ensureDirectoryExists(filesDir, "Failed to create 'files' directory");
        return new File[] {dbDir, filesDir};
    }

    /**
     * Ensures the given directory exists by creating it if missing; terminates via
     * {@link ErrorHandler} if creation fails.
     *
     * @param dir target directory to create
     * @param errorMsg message to display on error (the absolute path is appended)
     */
    private void ensureDirectoryExists(File dir, String errorMsg) {
        if (!dir.exists() && !dir.mkdirs()) {
            ErrorHandler.errorAndExit(errorMsg + ": " + dir.getAbsolutePath());
        }
    }

    /**
     * Renames the existing scenario output directory to a timestamped backup directory.<br>
     * Backup folder name format: {@code <originalName>_yyyyMMddHHmmssSSS}.<br>
     * Also logs the paths relative to {@code dataPath} using UNIX separators.
     *
     * @param dataPath the {@code dataPath} specified in application.yml
     * @param scenarioDir scenario directory to back up
     */
    private void backupScenarioDirectory(Path dataPath, File scenarioDir) {
        // Generate timestamped folder name
        String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS"));
        File backupDir =
                scenarioDir.toPath().getParent().resolve(scenarioDir.getName() + "_" + ts).toFile();

        // Perform rename
        if (!scenarioDir.renameTo(backupDir)) {
            ErrorHandler.errorAndExit("Failed to back up the existing output directory: "
                    + scenarioDir.getAbsolutePath());
        }

        // Compute relative paths and log
        String relOrig = FilenameUtils
                .separatorsToUnix(dataPath.relativize(scenarioDir.toPath()).toString());
        String relBackup =
                FilenameUtils.separatorsToUnix(dataPath.relativize(backupDir.toPath()).toString());
        log.info("Backed up existing dump output directory: {} → {}", relOrig, relBackup);
    }

    /**
     * Retrieves all table names in the specified schema and filters out excluded ones.
     *
     * @param conn JDBC connection
     * @param schema schema name
     * @param excludeTables list of table names to exclude (case-insensitive)
     * @return list of target table names after filtering
     * @throws SQLException on SQL error while retrieving tables
     */
    private List<String> fetchTargetTables(Connection conn, String schema,
            List<String> excludeTables) throws SQLException {
        List<String> tables = new ArrayList<>();
        List<String> effectiveExcludeTables = excludeTables;
        if (effectiveExcludeTables == null) {
            effectiveExcludeTables = new ArrayList<>();
        }
        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet rs = meta.getTables(null, schema, "%", new String[] {"TABLE"})) {
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                boolean excluded = effectiveExcludeTables.stream()
                        .anyMatch(ex -> ex.equalsIgnoreCase(tableName));
                if (excluded) {
                    log.info("Table [{}] is excluded; skipping", tableName);
                } else {
                    tables.add(tableName);
                }
            }
        }
        return tables;
    }

    /**
     * Logs a formatted summary of per-table dump results for the given DB.
     *
     * @param dbId DB identifier
     * @param tableCountMap map of table name → record count
     */
    private void logTableSummary(String dbId, Map<String, Integer> tableCountMap) {
        log.info("===== Summary =====");
        log.info("DB[{}]:", dbId);

        // Compute the maximum table-name length
        int maxNameLen = tableCountMap.keySet().stream().mapToInt(String::length).max().orElse(0);

        // Compute the maximum digit width for counts
        int maxCountDigits =
                tableCountMap.values().stream().map(count -> String.valueOf(count).length())
                        .mapToInt(Integer::intValue).max().orElse(0);

        // Example: " Table[%-20s] Total=%5d"
        String fmt = "  Table[%-" + maxNameLen + "s] Total=%" + maxCountDigits + "d";

        // Format and log each table
        tableCountMap.forEach((table, count) -> log.info(String.format(fmt, table, count)));
    }
}
