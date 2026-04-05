package io.github.yok.flexdblink.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import io.github.yok.flexdblink.junit.DataSourceRegistry;
import io.github.yok.flexdblink.junit.FlexAssert;
import io.github.yok.flexdblink.junit.LoadData;
import io.github.yok.flexdblink.junit.LoadDataExtension;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mssqlserver.MSSQLServerContainer;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * FlexAssert integration tests for mixed multi-DB loading and assertion.
 */
@Testcontainers
@SpringBootTest(classes = FlexAssertTestConfig.class)
@ContextConfiguration(initializers = YamlPropertySourceFactory.class)
class FlexAssertMixedMultiDbIT {

    private static final String DB1 = "db1";
    private static final String DB2 = "db2";
    private static final String DB3 = "db3";
    private static final String DB4 = "db4";
    private static final String FLEXASSERT_MULTI_DB1_URL = "FLEXASSERT_MULTI_DB1_URL";
    private static final String FLEXASSERT_MULTI_DB1_USER = "FLEXASSERT_MULTI_DB1_USER";
    private static final String FLEXASSERT_MULTI_DB1_PASSWORD = "FLEXASSERT_MULTI_DB1_PASSWORD";
    private static final String FLEXASSERT_MULTI_DB1_DRIVER = "FLEXASSERT_MULTI_DB1_DRIVER";
    private static final String FLEXASSERT_MULTI_DB2_URL = "FLEXASSERT_MULTI_DB2_URL";
    private static final String FLEXASSERT_MULTI_DB2_USER = "FLEXASSERT_MULTI_DB2_USER";
    private static final String FLEXASSERT_MULTI_DB2_PASSWORD = "FLEXASSERT_MULTI_DB2_PASSWORD";
    private static final String FLEXASSERT_MULTI_DB2_DRIVER = "FLEXASSERT_MULTI_DB2_DRIVER";
    private static final String FLEXASSERT_MULTI_DB3_URL = "FLEXASSERT_MULTI_DB3_URL";
    private static final String FLEXASSERT_MULTI_DB3_USER = "FLEXASSERT_MULTI_DB3_USER";
    private static final String FLEXASSERT_MULTI_DB3_PASSWORD = "FLEXASSERT_MULTI_DB3_PASSWORD";
    private static final String FLEXASSERT_MULTI_DB3_DRIVER = "FLEXASSERT_MULTI_DB3_DRIVER";
    private static final String FLEXASSERT_MULTI_DB4_URL = "FLEXASSERT_MULTI_DB4_URL";
    private static final String FLEXASSERT_MULTI_DB4_USER = "FLEXASSERT_MULTI_DB4_USER";
    private static final String FLEXASSERT_MULTI_DB4_PASSWORD = "FLEXASSERT_MULTI_DB4_PASSWORD";
    private static final String FLEXASSERT_MULTI_DB4_DRIVER = "FLEXASSERT_MULTI_DB4_DRIVER";

    @Container
    private static final PostgreSQLContainer POSTGRES = createPostgres();

    @Container
    private static final MySQLContainer MYSQL = createMySql();

    @Container
    private static final OracleContainer ORACLE = createOracle();

    @Container
    private static final MSSQLServerContainer SQLSERVER = createSqlServer();

    private static PostgreSQLContainer createPostgres() {
        PostgreSQLContainer container = new PostgreSQLContainer("postgres:16-alpine");
        container.withDatabaseName("testdb").withUsername("test").withPassword("test");
        container.start();
        System.setProperty("FLEXASSERT_DB_URL", container.getJdbcUrl());
        System.setProperty("FLEXASSERT_DB_USER", container.getUsername());
        System.setProperty("FLEXASSERT_DB_PASSWORD", container.getPassword());
        System.setProperty("FLEXASSERT_DB_DRIVER", "org.postgresql.Driver");
        System.setProperty(FLEXASSERT_MULTI_DB1_URL, container.getJdbcUrl());
        System.setProperty(FLEXASSERT_MULTI_DB1_USER, container.getUsername());
        System.setProperty(FLEXASSERT_MULTI_DB1_PASSWORD, container.getPassword());
        System.setProperty(FLEXASSERT_MULTI_DB1_DRIVER, "org.postgresql.Driver");
        return container;
    }

    private static MySQLContainer createMySql() {
        MySQLContainer container = new MySQLContainer("mysql:8.4");
        container.withDatabaseName("testdb").withUsername("test").withPassword("test");
        container.start();
        System.setProperty(FLEXASSERT_MULTI_DB2_URL, container.getJdbcUrl());
        System.setProperty(FLEXASSERT_MULTI_DB2_USER, container.getUsername());
        System.setProperty(FLEXASSERT_MULTI_DB2_PASSWORD, container.getPassword());
        System.setProperty(FLEXASSERT_MULTI_DB2_DRIVER, "com.mysql.cj.jdbc.Driver");
        return container;
    }

    private static OracleContainer createOracle() {
        OracleContainer container = new OracleContainer("gvenzl/oracle-free:slim-faststart");
        container.start();
        System.setProperty(FLEXASSERT_MULTI_DB3_URL, container.getJdbcUrl());
        System.setProperty(FLEXASSERT_MULTI_DB3_USER, container.getUsername());
        System.setProperty(FLEXASSERT_MULTI_DB3_PASSWORD, container.getPassword());
        System.setProperty(FLEXASSERT_MULTI_DB3_DRIVER, "oracle.jdbc.OracleDriver");
        return container;
    }

    private static MSSQLServerContainer createSqlServer() {
        MSSQLServerContainer container =
                new MSSQLServerContainer("mcr.microsoft.com/mssql/server:2019-latest");
        container.acceptLicense();
        container.start();
        System.setProperty(FLEXASSERT_MULTI_DB4_URL, container.getJdbcUrl());
        System.setProperty(FLEXASSERT_MULTI_DB4_USER, container.getUsername());
        System.setProperty(FLEXASSERT_MULTI_DB4_PASSWORD, container.getPassword());
        System.setProperty(FLEXASSERT_MULTI_DB4_DRIVER,
                "com.microsoft.sqlserver.jdbc.SQLServerDriver");
        return container;
    }

    @DynamicPropertySource
    static void containerProps(DynamicPropertyRegistry registry) {
        registry.add("connections[0].id", () -> DB1);
        registry.add("connections[0].driver-class", () -> "org.postgresql.Driver");
        registry.add("connections[0].url", POSTGRES::getJdbcUrl);
        registry.add("connections[0].user", POSTGRES::getUsername);
        registry.add("connections[0].password", POSTGRES::getPassword);

        registry.add("connections[1].id", () -> DB2);
        registry.add("connections[1].driver-class", () -> "com.mysql.cj.jdbc.Driver");
        registry.add("connections[1].url", MYSQL::getJdbcUrl);
        registry.add("connections[1].user", MYSQL::getUsername);
        registry.add("connections[1].password", MYSQL::getPassword);

        registry.add("connections[2].id", () -> DB3);
        registry.add("connections[2].driver-class", () -> "oracle.jdbc.OracleDriver");
        registry.add("connections[2].url", ORACLE::getJdbcUrl);
        registry.add("connections[2].user", ORACLE::getUsername);
        registry.add("connections[2].password", ORACLE::getPassword);

        registry.add("connections[3].id", () -> DB4);
        registry.add("connections[3].driver-class",
                () -> "com.microsoft.sqlserver.jdbc.SQLServerDriver");
        registry.add("connections[3].url", SQLSERVER::getJdbcUrl);
        registry.add("connections[3].user", SQLSERVER::getUsername);
        registry.add("connections[3].password", SQLSERVER::getPassword);
    }

    @BeforeEach
    void setUp() {
        prepareDatabase(POSTGRES, "classpath:db/migration_flexassert/postgresql");
        prepareDatabase(MYSQL, "classpath:db/migration_flexassert/mysql");
        prepareDatabase(ORACLE, "classpath:db/migration_flexassert/oracle");
        prepareDatabase(SQLSERVER, "classpath:db/migration_flexassert/sqlserver");
        DataSourceRegistry.registerAll(createDataSources());
    }

    @AfterEach
    void tearDown() {
        DataSourceRegistry.clear();
    }

    /**
     * Verifies that one mixed multi-DB scenario loads all DBs and all assertions succeed.
     *
     * @throws Exception when validation fails
     */
    @Test
    @LoadData(scenario = "flexassert", dbNames = {"db1", "db2", "db3", "db4"})
    void assertTables_正常ケース_異種4DBを同時ロードして比較する_全DBで一括比較が一致すること() throws Exception {
        assertEquals(100, countRows(DB1, "FA_SIMPLE"));
        assertEquals(100, countRows(DB2, "FA_SIMPLE"));
        assertEquals(100, countRows(DB3, "FA_SIMPLE"));
        assertEquals(100, countRows(DB4, "FA_SIMPLE"));

        FlexAssert.withDefaults().assertTables(DB1);
        FlexAssert.withDefaults().assertTables(DB2);
        FlexAssert.withDefaults().assertTables(DB3);
        FlexAssert.withDefaults().assertTables(DB4);
    }

    /**
     * Verifies DB-specific expected data isolation in mixed multi-DB assertion.
     *
     * @throws Exception when validation fails
     */
    @Test
    @LoadData(scenario = "flexassert-db2-corrupt", dbNames = {"db1", "db2", "db3", "db4"})
    void assertTable_異常ケース_db2期待値のみ破損する_他DBは一致しdb2のみAssertionErrorが送出されること() throws Exception {
        FlexAssert.withDefaults().assertTable(DB1, "FA_SIMPLE");
        assertThrows(AssertionError.class,
                () -> FlexAssert.withDefaults().assertTable(DB2, "FA_SIMPLE"));
        FlexAssert.withDefaults().assertTable(DB3, "FA_SIMPLE");
        FlexAssert.withDefaults().assertTable(DB4, "FA_SIMPLE");
    }

    /**
     * Migrates the target container schema using the DB-specific migration scripts.
     *
     * @param container target database container
     * @param migrationLocation Flyway migration location
     */
    private void prepareDatabase(JdbcDatabaseContainer<?> container, String migrationLocation) {
        Flyway flyway = Flyway
                .configure().cleanDisabled(false).dataSource(container.getJdbcUrl(),
                        container.getUsername(), container.getPassword())
                .locations(migrationLocation).load();
        flyway.clean();
        flyway.migrate();
    }

    /**
     * Creates all test DataSource registrations used by FlexAssert.
     *
     * @return logical DB ID to DataSource map
     */
    private Map<String, DataSource> createDataSources() {
        Map<String, DataSource> map = new LinkedHashMap<>();
        map.put(DB1, createDataSource(POSTGRES));
        map.put(DB2, createDataSource(MYSQL));
        map.put(DB3, createDataSource(ORACLE));
        map.put(DB4, createDataSource(SQLSERVER));
        return map;
    }

    /**
     * Creates a DriverManager-backed DataSource for the target container.
     *
     * @param container target database container
     * @return DataSource
     */
    private DataSource createDataSource(JdbcDatabaseContainer<?> container) {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setUrl(container.getJdbcUrl());
        ds.setUsername(container.getUsername());
        ds.setPassword(container.getPassword());
        ds.setDriverClassName(container.getDriverClassName());
        return ds;
    }

    /**
     * Counts rows from the target table.
     *
     * @param container target database container
     * @param tableName target table name
     * @return row count
     * @throws Exception when query execution fails
     */
    private int countRows(String dbName, String tableName) throws Exception {
        DataSource ds = LoadDataExtension.getCurrentDataSource(dbName);
        Connection conn = DataSourceUtils.getConnection(ds);
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
            rs.next();
            return rs.getInt(1);
        }
    }
}
