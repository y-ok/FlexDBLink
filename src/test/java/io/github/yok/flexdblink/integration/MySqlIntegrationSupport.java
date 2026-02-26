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
import org.testcontainers.containers.MySQLContainer;

/**
 * Utility class for MySQL integration tests.
 *
 * <p>
 * Initializes MySQL via Testcontainers + Flyway and runs FlexDBLink (DataLoader/DataDumper) in a
 * production-like configuration.
 * </p>
 *
 * <ul>
 * <li>DB setup: Flyway clean/migrate ({@code classpath:db/migration/mysql})</li>
 * <li>Runtime setup: assembles Paths/Connection/DbUnit/Dump/Pattern/DateTime/DialectFactory</li>
 * <li>Execution: invokes load/dump</li>
 * </ul>
 */
final class MySqlIntegrationSupport {

    private MySqlIntegrationSupport() {}

    /**
     * Starts the MySQL container and initializes the test schema with Flyway (all migrations).
     *
     * <p>
     * Runs Flyway clean → migrate on every call to ensure the DB is in a clean state at the start
     * of each test.
     * </p>
     *
     * @param container MySQL container
     */
    static void prepareDatabase(MySQLContainer<?> container) {
        ensureContainerRunning(container);
        migrate(container);
    }

    /**
     * Initializes the MySQL container without FK constraints.
     *
     * <p>
     * Applies Flyway migrations only up to V2, excluding the V3 FK constraint (FK_AUX_MAIN).
     * </p>
     *
     * @param container MySQL container
     */
    static void prepareDatabaseWithoutFk(MySQLContainer<?> container) {
        ensureContainerRunning(container);
        migrateWithoutFk(container);
    }

    private static void migrateWithoutFk(MySQLContainer<?> container) {
        Flyway flyway = Flyway.configure().cleanDisabled(false)
                .dataSource(container.getJdbcUrl(), container.getUsername(),
                        container.getPassword())
                .locations("classpath:db/migration/mysql").target("2").load();
        flyway.clean();
        flyway.migrate();
    }

    /**
     * Assembles the runtime required to run an integration test.
     *
     * <p>
     * The following directory structure is created under {@code dataPath}:
     * </p>
     *
     * <ul>
     * <li>{@code load/{scenario}/{dbId}/...} — DataLoader input</li>
     * <li>{@code dump/{scenario}/{dbId}/...} — DataDumper output</li>
     * </ul>
     *
     * <p>
     * When {@code copyLoadFixtures} is {@code true}, the contents of
     * {@code src/test/resources/integration/mysql/load} and
     * {@code src/test/resources/integration/common/load} are copied to {@code dataPath/load}.
     * </p>
     *
     * @param container MySQL container
     * @param dataPath work directory for the test
     * @param copyLoadFixtures {@code true} to copy load fixtures into the work directory
     * @return assembled runtime
     * @throws IOException on file I/O failure
     */
    static Runtime prepareRuntime(MySQLContainer<?> container, Path dataPath,
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
     * Opens a JDBC connection to the MySQL container.
     *
     * @param container MySQL container
     * @return JDBC connection
     * @throws SQLException on JDBC error
     */
    static Connection openConnection(MySQLContainer<?> container) throws SQLException {
        return DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(),
                container.getPassword());
    }

    /**
     * Initializes the MySQL schema using Flyway (all migrations).
     *
     * <p>
     * The migration location is fixed to {@code classpath:db/migration/mysql}.
     * </p>
     *
     * @param container MySQL container
     */
    static void migrate(MySQLContainer<?> container) {
        Flyway flyway = Flyway
                .configure().cleanDisabled(false).dataSource(container.getJdbcUrl(),
                        container.getUsername(), container.getPassword())
                .locations("classpath:db/migration/mysql").load();
        flyway.clean();
        flyway.migrate();
    }

    /**
     * Copies the load fixtures for MySQL tests into {@code dataPath/load}.
     *
     * <p>
     * Source: {@code src/test/resources/integration/mysql/load}<br>
     * Source: {@code src/test/resources/integration/common/load}<br>
     * Destination: {@code dataPath/load}
     * </p>
     *
     * @param dataPath work directory for the test
     * @throws IOException on copy failure
     */
    static void copyLoadFixtures(Path dataPath) throws IOException {
        Path src = Path.of("src", "test", "resources", "integration", "mysql", "load");
        Path commonSrc = Path.of("src", "test", "resources", "integration", "common", "load");
        Path dst = dataPath.resolve("load");
        Files.createDirectories(dst);
        copyTree(src, dst);
        copyTree(commonSrc, dst);
    }

    /**
     * Copies all files from source tree into destination tree.
     *
     * @param src source root
     * @param dst destination root
     * @throws IOException when file copy fails
     */
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

    /**
     * Creates a {@link PathsConfig} for the given data path.
     *
     * @param dataPath work directory for the test
     * @return PathsConfig
     */
    static PathsConfig pathsConfig(Path dataPath) {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(dataPath.toAbsolutePath().toString());
        return pathsConfig;
    }

    /**
     * Creates a {@link ConnectionConfig} for the MySQL container.
     *
     * <p>
     * The connection ID is fixed to {@code db1}.
     * </p>
     *
     * @param container MySQL container
     * @return ConnectionConfig
     */
    static ConnectionConfig connectionConfig(MySQLContainer<?> container) {
        ConnectionConfig connectionConfig = new ConnectionConfig();
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        entry.setUrl(container.getJdbcUrl());
        entry.setUser(container.getUsername());
        entry.setPassword(container.getPassword());
        entry.setDriverClass("com.mysql.cj.jdbc.Driver");
        connectionConfig.setConnections(List.of(entry));
        return connectionConfig;
    }

    /**
     * Creates a {@link DbUnitConfig} configured for MySQL.
     *
     * @return DbUnitConfig
     */
    static DbUnitConfig dbUnitConfig() {
        DbUnitConfig cfg = new DbUnitConfig();
        cfg.setPreDirName("pre");
        return cfg;
    }

    /**
     * Creates a default {@link DumpConfig}.
     *
     * @return DumpConfig
     */
    static DumpConfig dumpConfig() {
        return new DumpConfig();
    }

    /**
     * Creates a {@link DateTimeFormatUtil} with the standard CSV date/time format.
     *
     * @return DateTimeFormatUtil
     */
    static DateTimeFormatUtil dateTimeUtil() {
        CsvDateTimeFormatProperties props = new CsvDateTimeFormatProperties();
        props.setDate("yyyy-MM-dd");
        props.setTime("HH:mm:ss");
        props.setDateTime("yyyy-MM-dd HH:mm:ss");
        props.setDateTimeWithMillis("yyyy-MM-dd HH:mm:ss.SSS");
        return new DateTimeFormatUtil(props);
    }

    /**
     * Creates a {@link DbDialectHandlerFactory}.
     *
     * @param dbUnitConfig DbUnitConfig
     * @param dumpConfig DumpConfig
     * @param pathsConfig PathsConfig
     * @param dateTimeUtil DateTimeFormatUtil
     * @return DbDialectHandlerFactory
     */
    static DbDialectHandlerFactory dialectFactory(DbUnitConfig dbUnitConfig, DumpConfig dumpConfig,
            PathsConfig pathsConfig, DateTimeFormatUtil dateTimeUtil) {
        return new DbDialectHandlerFactory(dbUnitConfig, dumpConfig, pathsConfig, dateTimeUtil,
                new DbUnitConfigFactory());
    }

    /**
     * Creates a {@link FilePatternConfig} with the default LOB file naming patterns.
     *
     * @return FilePatternConfig
     */
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

    /**
     * Ensures the MySQL container is started.
     *
     * @param container MySQL container
     */
    static void ensureContainerRunning(MySQLContainer<?> container) {
        if (container.isRunning()) {
            return;
        }
        try {
            container.start();
        } catch (ContainerLaunchException e) {
            throw new IllegalStateException(
                    "MySQL container must be available for integration tests", e);
        }
    }

    /**
     * Holds the full set of configuration objects needed to run an integration test.
     */
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

        /**
         * Returns the work directory for this runtime.
         *
         * @return work directory
         */
        Path dataPath() {
            return dataPath;
        }

        /**
         * Creates a {@link DataLoader}.
         *
         * @return DataLoader
         */
        DataLoader newLoader() {
            return new DataLoader(pathsConfig, connectionConfig, dialectFactory::create,
                    dbUnitConfig, dumpConfig);
        }

        /**
         * Creates a {@link DataDumper}.
         *
         * @return DataDumper
         */
        DataDumper newDumper() {
            return new DataDumper(pathsConfig, connectionConfig, filePatternConfig, dumpConfig,
                    dialectFactory::create);
        }
    }
}
