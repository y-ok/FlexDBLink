package io.github.yok.flexdblink.integration;

import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.CsvDateTimeFormatProperties;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.FilePatternConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.core.DataDumper;
import io.github.yok.flexdblink.core.DataLoader;
import io.github.yok.flexdblink.db.DbDialectHandlerFactory;
import io.github.yok.flexdblink.db.DbUnitConfigFactory;
import io.github.yok.flexdblink.util.DateTimeFormatUtil;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.flywaydb.core.Flyway;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.mssqlserver.MSSQLServerContainer;

/**
 * Utility class for SQL Server integration tests.
 *
 * <p>
 * Initializes SQL Server via Testcontainers + Flyway and runs FlexDBLink (DataLoader/DataDumper) in
 * a production-like configuration.
 * </p>
 *
 * <ul>
 * <li>DB setup: Flyway clean/migrate ({@code classpath:db/migration/sqlserver})</li>
 * <li>Runtime setup: assembles Paths/Connection/DbUnit/Dump/Pattern/DateTime/DialectFactory</li>
 * <li>Execution: invokes load/dump</li>
 * </ul>
 */
final class SqlServerIntegrationSupport {

    private SqlServerIntegrationSupport() {}

    /**
     * Starts the SQL Server container and initializes the test schema with Flyway (all migrations).
     *
     * @param container SQL Server container
     */
    static void prepareDatabase(MSSQLServerContainer container) {
        ensureContainerRunning(container);
        migrate(container);
    }

    /**
     * Initializes the SQL Server container without FK constraints.
     *
     * <p>
     * Applies Flyway migrations only up to V2, excluding the V3 FK constraint (FK_AUX_MAIN).
     * </p>
     *
     * @param container SQL Server container
     */
    static void prepareDatabaseWithoutFk(MSSQLServerContainer container) {
        ensureContainerRunning(container);
        migrateWithoutFk(container);
    }

    private static void migrateWithoutFk(MSSQLServerContainer container) {
        Flyway flyway = Flyway.configure().cleanDisabled(false)
                .dataSource(container.getJdbcUrl(), container.getUsername(),
                        container.getPassword())
                .locations("classpath:db/migration/sqlserver").target("2").load();
        flyway.clean();
        flyway.migrate();
    }

    /**
     * Assembles the runtime required to run an integration test.
     *
     * @param container SQL Server container
     * @param dataPath work directory for the test
     * @param copyLoadFixtures {@code true} to copy load fixtures into the work directory
     * @return assembled runtime
     * @throws IOException on file I/O failure
     */
    static Runtime prepareRuntime(MSSQLServerContainer container, Path dataPath,
            boolean copyLoadFixtures) throws IOException {
        Files.createDirectories(dataPath);
        if (copyLoadFixtures) {
            copyLoadFixtures(dataPath);
        }

        PathsConfig pathsConfig = pathsConfig(dataPath);
        ConnectionConfig connectionConfig = connectionConfig(container);
        DbUnitConfig dbUnitConfig = dbUnitConfig();
        DumpConfig dumpConfig = dumpConfig();

        // Flyway の履歴テーブルはダンプ対象から除外する
        dumpConfig.setExcludeTables(List.of("flyway_schema_history"));

        FilePatternConfig filePatternConfig = filePatternConfig();
        DateTimeFormatUtil dateTimeUtil = dateTimeUtil();
        DbDialectHandlerFactory factory =
                dialectFactory(dbUnitConfig, dumpConfig, pathsConfig, dateTimeUtil);

        return new Runtime(dataPath, connectionConfig, dbUnitConfig, dumpConfig, pathsConfig,
                filePatternConfig, factory);
    }

    /**
     * Executes DataLoader for the specified scenario.
     *
     * @param runtime runtime
     * @param scenario scenario name (e.g. {@code pre})
     */
    static void executeLoad(Runtime runtime, String scenario) {
        DataLoader loader = runtime.newLoader();
        loader.execute(scenario, List.of("db1"));
    }

    /**
     * Executes DataDumper for the specified scenario and returns the output directory.
     *
     * @param runtime runtime
     * @param scenario scenario name
     * @return {@code dump/{scenario}/db1} directory
     */
    static Path executeDump(Runtime runtime, String scenario) {
        DataDumper dumper = runtime.newDumper();
        dumper.execute(scenario, List.of("db1"));
        return runtime.dataPath().resolve("dump").resolve(scenario).resolve("db1");
    }

    /**
     * Opens a JDBC connection to the SQL Server container.
     *
     * @param container SQL Server container
     * @return JDBC connection
     * @throws SQLException on JDBC error
     */
    static Connection openConnection(MSSQLServerContainer container) throws SQLException {
        return DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(),
                container.getPassword());
    }

    /**
     * Initializes the SQL Server schema using Flyway (all migrations).
     *
     * @param container SQL Server container
     */
    static void migrate(MSSQLServerContainer container) {
        Flyway flyway = Flyway.configure().cleanDisabled(false)
                .dataSource(container.getJdbcUrl(), container.getUsername(),
                        container.getPassword())
                .locations("classpath:db/migration/sqlserver").load();
        flyway.clean();
        flyway.migrate();
    }

    /**
     * Copies the load fixtures for SQL Server tests into {@code dataPath/load}.
     *
     * @param dataPath work directory for the test
     * @throws IOException on copy failure
     */
    static void copyLoadFixtures(Path dataPath) throws IOException {
        Path src = Path.of("src", "test", "resources", "integration", "sqlserver", "load");
        Path commonSrc = Path.of("src", "test", "resources", "integration", "common", "load");
        Path dst = dataPath.resolve("load");
        Files.createDirectories(dst);
        copyTree(src, dst);
        copyTree(commonSrc, dst);
    }

    private static void copyTree(Path src, Path dst) throws IOException {
        try (var stream = Files.walk(src)) {
            stream.forEach(path -> {
                try {
                    Path rel = src.relativize(path);
                    Path target = dst.resolve(rel);
                    if (Files.isDirectory(path)) {
                        Files.createDirectories(target);
                    } else {
                        Files.createDirectories(target.getParent());
                        Files.copy(path, target, StandardCopyOption.REPLACE_EXISTING);
                    }
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            });
        }
    }

    static PathsConfig pathsConfig(Path dataPath) {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(dataPath.toAbsolutePath().toString());
        return pathsConfig;
    }

    static ConnectionConfig connectionConfig(MSSQLServerContainer container) {
        ConnectionConfig connectionConfig = new ConnectionConfig();
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        entry.setUrl(container.getJdbcUrl());
        entry.setUser(container.getUsername());
        entry.setPassword(container.getPassword());
        entry.setDriverClass("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        connectionConfig.setConnections(List.of(entry));
        return connectionConfig;
    }

    static DbUnitConfig dbUnitConfig() {
        DbUnitConfig cfg = new DbUnitConfig();
        cfg.setPreDirName("pre");
        return cfg;
    }

    static DumpConfig dumpConfig() {
        return new DumpConfig();
    }

    static DateTimeFormatUtil dateTimeUtil() {
        CsvDateTimeFormatProperties props = new CsvDateTimeFormatProperties();
        props.setDate("yyyy-MM-dd");
        props.setTime("HH:mm:ss");
        props.setDateTime("yyyy-MM-dd HH:mm:ss");
        props.setDateTimeWithMillis("yyyy-MM-dd HH:mm:ss.SSS");
        return new DateTimeFormatUtil(props);
    }

    static DbDialectHandlerFactory dialectFactory(DbUnitConfig dbUnitConfig, DumpConfig dumpConfig,
            PathsConfig pathsConfig, DateTimeFormatUtil dateTimeUtil) {
        return new DbDialectHandlerFactory(dbUnitConfig, dumpConfig, pathsConfig, dateTimeUtil,
                new DbUnitConfigFactory());
    }

    static FilePatternConfig filePatternConfig() {
        FilePatternConfig filePatternConfig = new FilePatternConfig();
        Map<String, Map<String, String>> map = new HashMap<>();

        Map<String, String> main = new LinkedHashMap<>();
        main.put("CLOB_COL", "main_{LOB_KIND}_{ID}.txt");
        main.put("NCLOB_COL", "main_n_{LOB_KIND}_{ID}.txt");
        main.put("BLOB_COL", "main_{LOB_KIND}_{ID}.bin");
        map.put("IT_TYPED_MAIN", main);

        Map<String, String> aux = new LinkedHashMap<>();
        aux.put("PAYLOAD_CLOB", "aux_{LOB_KIND}_{ID}.txt");
        aux.put("PAYLOAD_BLOB", "aux_{LOB_KIND}_{ID}.bin");
        map.put("IT_TYPED_AUX", aux);

        filePatternConfig.setFilePatterns(map);
        return filePatternConfig;
    }

    static void ensureContainerRunning(MSSQLServerContainer container) {
        if (container.isRunning()) {
            return;
        }
        try {
            container.start();
        } catch (ContainerLaunchException e) {
            throw new IllegalStateException(
                    "SQL Server container must be available for integration tests", e);
        }
    }

    static final class Runtime {

        private final Path dataPath;
        private final ConnectionConfig connectionConfig;
        private final DbUnitConfig dbUnitConfig;
        private final DumpConfig dumpConfig;
        private final PathsConfig pathsConfig;
        private final FilePatternConfig filePatternConfig;
        private final DbDialectHandlerFactory dialectFactory;

        Runtime(Path dataPath, ConnectionConfig connectionConfig, DbUnitConfig dbUnitConfig,
                DumpConfig dumpConfig, PathsConfig pathsConfig, FilePatternConfig filePatternConfig,
                DbDialectHandlerFactory dialectFactory) {
            this.dataPath = dataPath;
            this.connectionConfig = connectionConfig;
            this.dbUnitConfig = dbUnitConfig;
            this.dumpConfig = dumpConfig;
            this.pathsConfig = pathsConfig;
            this.filePatternConfig = filePatternConfig;
            this.dialectFactory = dialectFactory;
        }

        Path dataPath() {
            return dataPath;
        }

        DataLoader newLoader() {
            return new DataLoader(pathsConfig, connectionConfig, dialectFactory::create,
                    dbUnitConfig, dumpConfig);
        }

        DataDumper newDumper() {
            return new DataDumper(pathsConfig, connectionConfig, filePatternConfig, dumpConfig,
                    dialectFactory::create);
        }
    }
}
