package io.github.yok.flexdblink.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.github.yok.flexdblink.Main;
import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.FilePatternConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.db.DbDialectHandlerFactory;
import io.github.yok.flexdblink.db.DbUnitConfigFactory;
import io.github.yok.flexdblink.util.DateTimeFormatUtil;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mssqlserver.MSSQLServerContainer;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Multi-DB integration tests for {@link Main}.
 *
 * <p>
 * Verifies that {@code --setup}, {@code --load}, and {@code --dump} work with a mixed
 * {@code connections[]} configuration and target filtering per DB ID.
 * </p>
 */
@SpringBootTest(classes = IntegrationTestConfig.class)
@ContextConfiguration(initializers = YamlPropertySourceFactory.class)
@Testcontainers
class MainMultiDbIntegrationTest {

    private static final String ORACLE_ID = "ora";
    private static final String POSTGRES_ID = "pg";
    private static final String MYSQL_ID = "my";
    private static final String SQLSERVER_ID = "ss";

    private static final Map<String, String> DRIVER_CLASSES = Map.of("oracle",
            "oracle.jdbc.OracleDriver", "postgresql", "org.postgresql.Driver", "mysql",
            "com.mysql.cj.jdbc.Driver", "sqlserver",
            "com.microsoft.sqlserver.jdbc.SQLServerDriver");

    @Container
    static final OracleContainer ORACLE = new OracleContainer("gvenzl/oracle-free:slim-faststart");

    @Container
    static final PostgreSQLContainer POSTGRES = createPostgres();

    @Container
    static final MySQLContainer MYSQL = createMySql();

    @Container
    static final MSSQLServerContainer SQLSERVER = createSqlServer();

    @TempDir
    Path tempDir;

    @Autowired
    DbUnitConfig dbUnitConfig;

    @Autowired
    DumpConfig dumpConfig;

    @Autowired
    FilePatternConfig filePatternConfig;

    @Autowired
    DateTimeFormatUtil dateTimeUtil;

    /**
     * Creates a PostgreSQL Testcontainer configured for integration tests.
     *
     * @return configured PostgreSQL container
     */
    private static PostgreSQLContainer createPostgres() {
        PostgreSQLContainer container = new PostgreSQLContainer("postgres:16-alpine");
        container.withDatabaseName("testdb").withUsername("test").withPassword("test");
        return container;
    }

    /**
     * Creates a MySQL Testcontainer configured for integration tests.
     *
     * @return configured MySQL container
     */
    private static MySQLContainer createMySql() {
        MySQLContainer container = new MySQLContainer("mysql:8.4");
        container.withDatabaseName("testdb").withUsername("test").withPassword("test");
        return container;
    }

    /**
     * Creates a SQL Server Testcontainer configured for integration tests.
     *
     * @return configured SQL Server container
     */
    private static MSSQLServerContainer createSqlServer() {
        MSSQLServerContainer container =
                new MSSQLServerContainer("mcr.microsoft.com/mssql/server:2019-latest");
        container.acceptLicense();
        return container;
    }

    static Stream<Arguments> run_正常ケース_マルチDB接続でsetupLoadDumpを実行する_対象DBのみ処理が完了すること_データ() {
        return Stream.of(Arguments.of("oracle_only", List.of(DbKind.ORACLE)),
                Arguments.of("postgresql_only", List.of(DbKind.POSTGRESQL)),
                Arguments.of("mysql_only", List.of(DbKind.MYSQL)),
                Arguments.of("sqlserver_only", List.of(DbKind.SQLSERVER)), Arguments.of("mixed_all",
                        List.of(DbKind.ORACLE, DbKind.POSTGRESQL, DbKind.MYSQL, DbKind.SQLSERVER)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("run_正常ケース_マルチDB接続でsetupLoadDumpを実行する_対象DBのみ処理が完了すること_データ")
    void run_正常ケース_マルチDB接続でsetupLoadDumpを実行する_対象DBのみ処理が完了すること(String caseName, List<DbKind> targets)
            throws Exception {
        prepareTargetDatabases(targets);

        Path dataPath = tempDir.resolve(caseName);
        Path configFile = prepareConfigFile();
        copyLoadFixturesForTargets(dataPath, targets);

        Main main = buildMain(dataPath);
        String targetCsv =
                targets.stream().map(DbKind::id).reduce((a, b) -> a + "," + b).orElseThrow();

        try {
            main.run("--setup", "--target", targetCsv);
            String yaml = Files.readString(configFile);
            assertTrue(yaml.contains("file-patterns"), "file-patterns was not generated");

            clearTargetTestTables(targets);

            main.run("--load", "pre", "--target", targetCsv);
            assertLoadedRowCounts(targets);

            String dumpScenario = "dump_" + caseName;
            main.run("--dump", dumpScenario, "--target", targetCsv);
            assertDumpOutputs(dataPath, dumpScenario, targets);
        } finally {
            System.clearProperty("spring.config.additional-location");
        }
    }

    /**
     * Builds a {@link Main} instance wired with multi-DB connection configuration.
     *
     * @param dataPath base directory for load/dump data
     * @return configured Main instance
     */
    private Main buildMain(Path dataPath) {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(dataPath.toAbsolutePath().toString());

        ConnectionConfig connectionConfig = buildMultiConnectionConfig();

        dumpConfig.setExcludeTables(List.of("FLYWAY_SCHEMA_HISTORY", "flyway_schema_history"));

        DbDialectHandlerFactory factory = new DbDialectHandlerFactory(dbUnitConfig, dumpConfig,
                pathsConfig, dateTimeUtil, new DbUnitConfigFactory());
        return new Main(pathsConfig, dbUnitConfig, connectionConfig, filePatternConfig, dumpConfig,
                factory);
    }

    /**
     * Assembles a {@link ConnectionConfig} containing entries for all four database types.
     *
     * @return connection config with entries for Oracle, PostgreSQL, MySQL, and SQL Server
     */
    private ConnectionConfig buildMultiConnectionConfig() {
        List<ConnectionConfig.Entry> entries = new ArrayList<>();
        entries.add(buildConnectionEntry(ORACLE, ORACLE_ID, "oracle"));
        entries.add(buildConnectionEntry(POSTGRES, POSTGRES_ID, "postgresql"));
        entries.add(buildConnectionEntry(MYSQL, MYSQL_ID, "mysql"));
        entries.add(buildConnectionEntry(SQLSERVER, SQLSERVER_ID, "sqlserver"));

        ConnectionConfig config = new ConnectionConfig();
        config.setConnections(entries);
        return config;
    }

    /**
     * Builds a connection entry for the specified container and database type.
     *
     * @param container JDBC container
     * @param id logical connection ID
     * @param dbName database name for driver class lookup
     * @return connection entry
     */
    private ConnectionConfig.Entry buildConnectionEntry(JdbcDatabaseContainer<?> container,
            String id, String dbName) {
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId(id);
        entry.setDriverClass(DRIVER_CLASSES.get(dbName));
        entry.setUrl(container.getJdbcUrl());
        entry.setUser(container.getUsername());
        entry.setPassword(container.getPassword());
        return entry;
    }

    /**
     * Creates a minimal application.yml in the temp directory and sets the Spring config location.
     *
     * @return path to the created application.yml
     * @throws Exception if file creation fails
     */
    private Path prepareConfigFile() throws Exception {
        Path configFile = tempDir.resolve("application.yml");
        Files.writeString(configFile, "data-path: /tmp\n");
        System.setProperty("spring.config.additional-location",
                "file:" + tempDir.toAbsolutePath() + "/");
        return configFile;
    }

    /**
     * Copies load fixture files for each target database into the data directory.
     *
     * @param dataPath base directory for test data
     * @param targets list of target database kinds
     * @throws Exception if file copy fails
     */
    private void copyLoadFixturesForTargets(Path dataPath, List<DbKind> targets) throws Exception {
        Files.createDirectories(dataPath);
        for (DbKind target : targets) {
            copySingleTargetFixtures(dataPath, target);
        }
    }

    /**
     * Copies load fixtures for a single database type, renaming the directory to the target ID.
     *
     * @param dataPath base directory for test data
     * @param target database kind to copy fixtures for
     * @throws Exception if file copy or move fails
     */
    private void copySingleTargetFixtures(Path dataPath, DbKind target) throws Exception {
        IntegrationTestSupport.copyLoadFixtures(dataPath, target.dbName());

        Path source = dataPath.resolve("load").resolve("pre").resolve("db1");
        Path targetDir = dataPath.resolve("load").resolve("pre").resolve(target.id());
        Files.createDirectories(targetDir.getParent());
        Files.move(source, targetDir, StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * Runs Flyway migration for each target database to prepare the schema.
     *
     * @param targets list of target database kinds to migrate
     */
    private void prepareTargetDatabases(List<DbKind> targets) {
        for (DbKind target : targets) {
            IntegrationTestSupport.prepareDatabase(containerFor(target),
                    target.migrationLocation());
        }
    }

    /**
     * Deletes all rows from the test tables in each target database.
     *
     * @param targets list of target database kinds to clear
     * @throws Exception if database operation fails
     */
    private void clearTargetTestTables(List<DbKind> targets) throws Exception {
        for (DbKind target : targets) {
            try (Connection conn = IntegrationTestSupport.openConnection(containerFor(target))) {
                clearTestTables(conn);
            }
        }
    }

    /**
     * Deletes all rows from IT_TYPED_AUX and IT_TYPED_MAIN via the given connection.
     *
     * @param conn JDBC connection to use
     * @throws Exception if SQL execution fails
     */
    private void clearTestTables(Connection conn) throws Exception {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("DELETE FROM IT_TYPED_AUX");
            st.executeUpdate("DELETE FROM IT_TYPED_MAIN");
        }
    }

    /**
     * Asserts that the expected number of rows were loaded into each target database.
     *
     * @param targets list of target database kinds to verify
     * @throws Exception if database query fails
     */
    private void assertLoadedRowCounts(List<DbKind> targets) throws Exception {
        for (DbKind target : targets) {
            try (Connection conn = IntegrationTestSupport.openConnection(containerFor(target))) {
                assertEquals(6, countRows(conn, "IT_TYPED_MAIN"));
                assertEquals(2, countRows(conn, "IT_TYPED_AUX"));
            }
        }
    }

    /**
     * Returns the row count of the specified table via {@code SELECT COUNT(*)}.
     *
     * @param conn JDBC connection to use
     * @param tableName name of the table to count
     * @return number of rows in the table
     * @throws Exception if SQL execution fails
     */
    private int countRows(Connection conn, String tableName) throws Exception {
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
            assertTrue(rs.next(), "COUNT(*) did not return a row: " + tableName);
            return rs.getInt(1);
        }
    }

    /**
     * Asserts that dump CSV files exist for target databases and do not exist for non-targets.
     *
     * @param dataPath base directory for test data
     * @param scenario dump scenario name
     * @param targets list of target database kinds
     */
    private void assertDumpOutputs(Path dataPath, String scenario, List<DbKind> targets) {
        List<String> targetIds = targets.stream().map(DbKind::id).collect(Collectors.toList());
        for (DbKind dbKind : DbKind.values()) {
            Path dbDir = dataPath.resolve("dump").resolve(scenario).resolve(dbKind.id());
            if (targetIds.contains(dbKind.id())) {
                assertTrue(containsCsvIgnoreCase(dbDir, "IT_TYPED_MAIN"),
                        "Dump output for IT_TYPED_MAIN was not created: " + dbKind.id());
                assertTrue(containsCsvIgnoreCase(dbDir, "IT_TYPED_AUX"),
                        "Dump output for IT_TYPED_AUX was not created: " + dbKind.id());
            } else {
                assertFalse(Files.exists(dbDir),
                        "Dump directory was created for a non-target DB: " + dbKind.id());
            }
        }
    }

    /**
     * Checks whether the directory contains a CSV file matching the table name (case-insensitive).
     *
     * @param dbDir directory to search
     * @param tableName table name to match against CSV file names
     * @return true if a matching CSV file exists
     */
    private boolean containsCsvIgnoreCase(Path dbDir, String tableName) {
        if (!Files.isDirectory(dbDir)) {
            return false;
        }
        String expected = (tableName + ".csv").toLowerCase(Locale.ROOT);
        try (Stream<Path> files = Files.list(dbDir)) {
            return files.filter(Files::isRegularFile).map(Path::getFileName).map(Path::toString)
                    .map(name -> name.toLowerCase(Locale.ROOT)).anyMatch(expected::equals);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to inspect dump directory: " + dbDir, e);
        }
    }

    /**
     * Returns the container instance for the given database kind.
     *
     * @param dbKind database kind to look up
     * @return JDBC container for the specified database kind
     */
    private JdbcDatabaseContainer<?> containerFor(DbKind dbKind) {
        switch (dbKind) {
            case ORACLE:
                return ORACLE;
            case POSTGRESQL:
                return POSTGRES;
            case MYSQL:
                return MYSQL;
            case SQLSERVER:
                return SQLSERVER;
            default:
                throw new IllegalArgumentException("Unknown DbKind: " + dbKind);
        }
    }

    private enum DbKind {
        ORACLE(ORACLE_ID, "oracle", "classpath:db/migration/oracle"), POSTGRESQL(POSTGRES_ID,
                "postgresql", "classpath:db/migration/postgresql"), MYSQL(MYSQL_ID, "mysql",
                        "classpath:db/migration/mysql"), SQLSERVER(SQLSERVER_ID, "sqlserver",
                                "classpath:db/migration/sqlserver");

        private final String id;
        private final String dbName;
        private final String migrationLocation;

        DbKind(String id, String dbName, String migrationLocation) {
            this.id = id;
            this.dbName = dbName;
            this.migrationLocation = migrationLocation;
        }

        /**
         * Returns the connection ID (e.g. {@code ora}, {@code pg}).
         *
         * @return connection ID
         */
        String id() {
            return id;
        }

        /**
         * Returns the database name used for fixture paths (e.g. {@code oracle}, {@code mysql}).
         *
         * @return database name
         */
        String dbName() {
            return dbName;
        }

        /**
         * Returns the Flyway migration location.
         *
         * @return Flyway migration classpath location
         */
        String migrationLocation() {
            return migrationLocation;
        }
    }
}
