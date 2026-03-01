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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.mssqlserver.MSSQLServerContainer;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
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
@Testcontainers
class MainMultiDbIntegrationTest {

    private static final String ORACLE_ID = "ora";
    private static final String POSTGRES_ID = "pg";
    private static final String MYSQL_ID = "my";
    private static final String SQLSERVER_ID = "ss";

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

    private static PostgreSQLContainer createPostgres() {
        PostgreSQLContainer container = new PostgreSQLContainer("postgres:16-alpine");
        container.withDatabaseName("testdb").withUsername("test").withPassword("test");
        return container;
    }

    private static MySQLContainer createMySql() {
        MySQLContainer container = new MySQLContainer("mysql:8.4");
        container.withDatabaseName("testdb").withUsername("test").withPassword("test");
        return container;
    }

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
            assertTrue(yaml.contains("file-patterns"), "file-patterns が生成されていません");

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

    private Main buildMain(Path dataPath) {
        PathsConfig pathsConfig = OracleIntegrationSupport.pathsConfig(dataPath);
        DbUnitConfig dbUnitConfig = OracleIntegrationSupport.dbUnitConfig();
        ConnectionConfig connectionConfig = buildMultiConnectionConfig();
        FilePatternConfig filePatternConfig = OracleIntegrationSupport.filePatternConfig();
        DumpConfig dumpConfig = OracleIntegrationSupport.dumpConfig();
        dumpConfig.setExcludeTables(List.of("FLYWAY_SCHEMA_HISTORY", "flyway_schema_history"));
        DateTimeFormatUtil dateTimeUtil = OracleIntegrationSupport.dateTimeUtil();
        DbDialectHandlerFactory factory = OracleIntegrationSupport.dialectFactory(dbUnitConfig,
                dumpConfig, pathsConfig, dateTimeUtil);
        return new Main(pathsConfig, dbUnitConfig, connectionConfig, filePatternConfig, dumpConfig,
                factory);
    }

    private ConnectionConfig buildMultiConnectionConfig() {
        List<ConnectionConfig.Entry> entries = new ArrayList<>();
        entries.add(
                copyEntry(OracleIntegrationSupport.connectionConfig(ORACLE).getConnections().get(0),
                        ORACLE_ID));
        entries.add(copyEntry(
                PostgresqlIntegrationSupport.connectionConfig(POSTGRES).getConnections().get(0),
                POSTGRES_ID));
        entries.add(copyEntry(
                MySqlIntegrationSupport.connectionConfig(MYSQL).getConnections().get(0), MYSQL_ID));
        entries.add(copyEntry(
                SqlServerIntegrationSupport.connectionConfig(SQLSERVER).getConnections().get(0),
                SQLSERVER_ID));

        ConnectionConfig config = new ConnectionConfig();
        config.setConnections(entries);
        return config;
    }

    private ConnectionConfig.Entry copyEntry(ConnectionConfig.Entry source, String id) {
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId(id);
        entry.setUrl(source.getUrl());
        entry.setUser(source.getUser());
        entry.setPassword(source.getPassword());
        entry.setDriverClass(source.getDriverClass());
        return entry;
    }

    private Path prepareConfigFile() throws Exception {
        Path configFile = tempDir.resolve("application.yml");
        Files.writeString(configFile, "data-path: /tmp\n");
        System.setProperty("spring.config.additional-location",
                "file:" + tempDir.toAbsolutePath() + "/");
        return configFile;
    }

    private void copyLoadFixturesForTargets(Path dataPath, List<DbKind> targets) throws Exception {
        Files.createDirectories(dataPath);
        for (DbKind target : targets) {
            copySingleTargetFixtures(dataPath, target);
        }
    }

    private void copySingleTargetFixtures(Path dataPath, DbKind target) throws Exception {
        if (target == DbKind.ORACLE) {
            OracleIntegrationSupport.copyLoadFixtures(dataPath);
        } else if (target == DbKind.POSTGRESQL) {
            PostgresqlIntegrationSupport.copyLoadFixtures(dataPath);
        } else if (target == DbKind.MYSQL) {
            MySqlIntegrationSupport.copyLoadFixtures(dataPath);
        } else {
            SqlServerIntegrationSupport.copyLoadFixtures(dataPath);
        }

        Path source = dataPath.resolve("load").resolve("pre").resolve("db1");
        Path targetDir = dataPath.resolve("load").resolve("pre").resolve(target.id());
        Files.createDirectories(targetDir.getParent());
        Files.move(source, targetDir, StandardCopyOption.REPLACE_EXISTING);
    }

    private void prepareTargetDatabases(List<DbKind> targets) {
        for (DbKind target : targets) {
            if (target == DbKind.ORACLE) {
                OracleIntegrationSupport.prepareDatabase(ORACLE);
                continue;
            }
            if (target == DbKind.POSTGRESQL) {
                PostgresqlIntegrationSupport.prepareDatabase(POSTGRES);
                continue;
            }
            if (target == DbKind.MYSQL) {
                MySqlIntegrationSupport.prepareDatabase(MYSQL);
                continue;
            }
            SqlServerIntegrationSupport.prepareDatabase(SQLSERVER);
        }
    }

    private void clearTargetTestTables(List<DbKind> targets) throws Exception {
        for (DbKind target : targets) {
            if (target == DbKind.ORACLE) {
                try (Connection conn = OracleIntegrationSupport.openConnection(ORACLE)) {
                    clearTestTables(conn);
                }
                continue;
            }
            if (target == DbKind.POSTGRESQL) {
                try (Connection conn = PostgresqlIntegrationSupport.openConnection(POSTGRES)) {
                    clearTestTables(conn);
                }
                continue;
            }
            if (target == DbKind.MYSQL) {
                try (Connection conn = MySqlIntegrationSupport.openConnection(MYSQL)) {
                    clearTestTables(conn);
                }
                continue;
            }
            try (Connection conn = SqlServerIntegrationSupport.openConnection(SQLSERVER)) {
                clearTestTables(conn);
            }
        }
    }

    private void clearTestTables(Connection conn) throws Exception {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("DELETE FROM IT_TYPED_AUX");
            st.executeUpdate("DELETE FROM IT_TYPED_MAIN");
        }
    }

    private void assertLoadedRowCounts(List<DbKind> targets) throws Exception {
        for (DbKind target : targets) {
            if (target == DbKind.ORACLE) {
                try (Connection conn = OracleIntegrationSupport.openConnection(ORACLE)) {
                    assertEquals(6, countRows(conn, "IT_TYPED_MAIN"));
                    assertEquals(2, countRows(conn, "IT_TYPED_AUX"));
                }
                continue;
            }
            if (target == DbKind.POSTGRESQL) {
                try (Connection conn = PostgresqlIntegrationSupport.openConnection(POSTGRES)) {
                    assertEquals(6, countRows(conn, "IT_TYPED_MAIN"));
                    assertEquals(2, countRows(conn, "IT_TYPED_AUX"));
                }
                continue;
            }
            if (target == DbKind.MYSQL) {
                try (Connection conn = MySqlIntegrationSupport.openConnection(MYSQL)) {
                    assertEquals(6, countRows(conn, "IT_TYPED_MAIN"));
                    assertEquals(2, countRows(conn, "IT_TYPED_AUX"));
                }
                continue;
            }
            try (Connection conn = SqlServerIntegrationSupport.openConnection(SQLSERVER)) {
                assertEquals(6, countRows(conn, "IT_TYPED_MAIN"));
                assertEquals(2, countRows(conn, "IT_TYPED_AUX"));
            }
        }
    }

    private int countRows(Connection conn, String tableName) throws Exception {
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
            assertTrue(rs.next(), "COUNT(*) の結果が取得できません: " + tableName);
            return rs.getInt(1);
        }
    }

    private void assertDumpOutputs(Path dataPath, String scenario, List<DbKind> targets) {
        List<String> targetIds = targets.stream().map(DbKind::id).collect(Collectors.toList());
        for (DbKind dbKind : DbKind.values()) {
            Path dbDir = dataPath.resolve("dump").resolve(scenario).resolve(dbKind.id());
            if (targetIds.contains(dbKind.id())) {
                assertTrue(containsCsvIgnoreCase(dbDir, "IT_TYPED_MAIN"),
                        "ダンプ結果が存在しません: " + dbKind.id());
                assertTrue(containsCsvIgnoreCase(dbDir, "IT_TYPED_AUX"),
                        "ダンプ結果が存在しません: " + dbKind.id());
            } else {
                assertFalse(Files.exists(dbDir), "対象外DBのダンプディレクトリが生成されています: " + dbKind.id());
            }
        }
    }

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

    private enum DbKind {
        ORACLE(ORACLE_ID), POSTGRESQL(POSTGRES_ID), MYSQL(MYSQL_ID), SQLSERVER(SQLSERVER_ID);

        private final String id;

        DbKind(String id) {
            this.id = id;
        }

        String id() {
            return id;
        }
    }
}
