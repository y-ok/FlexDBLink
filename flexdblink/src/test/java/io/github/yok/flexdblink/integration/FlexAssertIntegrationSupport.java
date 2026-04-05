package io.github.yok.flexdblink.integration;

import static org.junit.jupiter.api.Assertions.assertThrows;
import io.github.yok.flexdblink.junit.DataSourceRegistry;
import io.github.yok.flexdblink.junit.FlexAssert;
import io.github.yok.flexdblink.junit.LoadData;
import io.github.yok.flexdblink.junit.LoadDataExtension;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.JdbcDatabaseContainer;

/**
 * Shared test logic for FlexAssert integration tests across all database types.
 *
 * <p>
 * Subclasses provide the container and migration location. Data loading uses {@link LoadData} via
 * the fixture CSVs under {@code src/test/resources/...}.
 * </p>
 */
abstract class FlexAssertIntegrationSupport {

    private static final String DB_NAME = "db1";

    /**
     * Returns the JDBC container used by the concrete database-specific test.
     *
     * @return JDBC container for the target database
     */
    abstract JdbcDatabaseContainer<?> container();

    /**
     * Returns the Flyway migration location for the concrete database-specific schema.
     *
     * @return Flyway migration location
     */
    abstract String migrationLocation();

    @BeforeEach
    void setUp() {
        prepareDatabase();
        DataSourceRegistry.register(DB_NAME, createDataSource());
    }

    @AfterEach
    void tearDown() {
        DataSourceRegistry.clear();
    }

    /**
     * Recreates the test schema from scratch using the database-specific Flyway migrations.
     */
    void prepareDatabase() {
        JdbcDatabaseContainer<?> c = container();
        if (!c.isRunning()) {
            c.start();
        }
        Flyway flyway = Flyway.configure().cleanDisabled(false)
                .dataSource(c.getJdbcUrl(), c.getUsername(), c.getPassword())
                .locations(migrationLocation()).load();
        flyway.clean();
        flyway.migrate();
    }

    /**
     * Creates a {@link DataSource} backed by the current test container.
     *
     * @return container-backed {@link DataSource}
     */
    DataSource createDataSource() {
        JdbcDatabaseContainer<?> c = container();
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setUrl(c.getJdbcUrl());
        ds.setUsername(c.getUsername());
        ds.setPassword(c.getPassword());
        return ds;
    }

    /**
     * Opens a standalone JDBC connection to the current container.
     *
     * @return standalone JDBC connection
     * @throws SQLException if connection creation fails
     */
    Connection openConnection() throws SQLException {
        JdbcDatabaseContainer<?> c = container();
        return DriverManager.getConnection(c.getJdbcUrl(), c.getUsername(), c.getPassword());
    }

    /**
     * Returns the JDBC connection participating in the active {@link LoadData} transaction.
     *
     * @return transactional JDBC connection for {@value #DB_NAME}
     */
    Connection getTransactionalConnection() {
        DataSource ds = LoadDataExtension.getCurrentDataSource(DB_NAME);
        return DataSourceUtils.getConnection(ds);
    }

    /**
     * Deletes rows whose {@code ID} is greater than the specified threshold inside the active test
     * transaction.
     *
     * @param count maximum row ID to keep
     * @param tableName target table name
     * @throws SQLException if the delete statement fails
     */
    void truncateToCount(int count, String tableName) throws SQLException {
        Connection conn = getTransactionalConnection();
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("DELETE FROM " + tableName + " WHERE ID > " + count);
        }
    }

    /**
     * Deletes all rows from the specified table inside the active test transaction.
     *
     * @param tableName target table name
     * @throws SQLException if the delete statement fails
     */
    void truncateTable(String tableName) throws SQLException {
        Connection conn = getTransactionalConnection();
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("DELETE FROM " + tableName);
        }
    }

    @Test
    @LoadData(scenario = "flexassert-0", dbNames = {"db1"})
    void assertTable_正常ケース_0件テーブル_空テーブル同士が一致すること() throws Exception {
        truncateTable("FA_SIMPLE");
        FlexAssert.withDefaults().assertTable(DB_NAME, "FA_SIMPLE");
    }

    @Test
    @LoadData(scenario = "flexassert-1", dbNames = {"db1"})
    void assertTable_正常ケース_1件テーブル_1行が一致すること() throws Exception {
        truncateToCount(1, "FA_SIMPLE");
        FlexAssert.withDefaults().assertTable(DB_NAME, "FA_SIMPLE");
    }

    @Test
    @LoadData(scenario = "flexassert-10", dbNames = {"db1"})
    void assertTable_正常ケース_10件テーブル_全行が一致すること() throws Exception {
        truncateToCount(10, "FA_SIMPLE");
        FlexAssert.withDefaults().assertTable(DB_NAME, "FA_SIMPLE");
    }

    @Test
    void assertTable_正常ケース_100件テーブル_全行が一致すること() throws Exception {
        FlexAssert.withDefaults().assertTable(DB_NAME, "FA_SIMPLE");
    }

    @Test
    void assertTable_正常ケース_NULLカラムあり_NULLを含む行が一致すること() throws Exception {
        FlexAssert.withDefaults().assertTable(DB_NAME, "FA_NULLABLE");
    }

    @Test
    void assertTable_正常ケース_空文字カラムあり_空文字を含む行が一致すること() throws Exception {
        FlexAssert.withDefaults().assertTable(DB_NAME, "FA_NULLABLE");
    }

    @Test
    void assertTable_正常ケース_NULLと空文字混在_混在パターンが一致すること() throws Exception {
        FlexAssert.withDefaults().assertTable(DB_NAME, "FA_NULLABLE");
    }

    @Test
    void assertTable_正常ケース_BLOBカラム_バイナリLOBが一致すること() throws Exception {
        FlexAssert.withDefaults().assertTable(DB_NAME, "FA_LOB");
    }

    @Test
    void assertTable_正常ケース_CLOBカラム_テキストLOBが一致すること() throws Exception {
        FlexAssert.withDefaults().assertTable(DB_NAME, "FA_LOB");
    }

    @Test
    void assertTable_正常ケース_NULL_LOB_NULLのLOBカラムが一致すること() throws Exception {
        FlexAssert.withDefaults().assertTable(DB_NAME, "FA_LOB");
    }

    @Test
    void assertTable_正常ケース_空LOB_0バイトLOBが一致すること() throws Exception {
        FlexAssert.withDefaults().assertTable(DB_NAME, "FA_LOB");
    }

    @Test
    void assertTable_正常ケース_全LOBパターン混在_全行が一致すること() throws Exception {
        FlexAssert.withDefaults().assertTable(DB_NAME, "FA_LOB");
    }

    @Test
    void assertTables_正常ケース_親子テーブル_一括比較が一致すること() throws Exception {
        FlexAssert.withDefaults().assertTables(DB_NAME);
    }

    @Test
    void assertTables_正常ケース_3階層テーブル_一括比較が一致すること() throws Exception {
        FlexAssert.withDefaults().assertTables(DB_NAME);
    }

    @Test
    void assertTable_正常ケース_複合PK_複合PKテーブルが一致すること() throws Exception {
        FlexAssert.withDefaults().assertTable(DB_NAME, "FA_COMPOSITE_PK");
    }

    @Test
    void assertTable_正常ケース_globalExcludeColumns_除外カラムを無視して一致すること() throws Exception {
        try (Statement st = getTransactionalConnection().createStatement()) {
            st.executeUpdate("UPDATE FA_SIMPLE SET CREATED_AT = NULL WHERE ID = 1");
        }
        FlexAssert.globalExcludeColumns("CREATED_AT").assertTable(DB_NAME, "FA_SIMPLE");
    }

    @Test
    void assertTable_正常ケース_tableExcludeColumns_テーブル個別除外で一致すること() throws Exception {
        try (Statement st = getTransactionalConnection().createStatement()) {
            st.executeUpdate("UPDATE FA_PARENT SET STATUS = 'CHANGED' WHERE ID = 1");
        }
        FlexAssert.withDefaults().table("FA_PARENT").excludeColumns("STATUS").assertTable(DB_NAME,
                "FA_PARENT");
    }

    @Test
    void assertTable_正常ケース_globalとtable併用除外_マージされた除外で一致すること() throws Exception {
        try (Statement st = getTransactionalConnection().createStatement()) {
            st.executeUpdate("UPDATE FA_SIMPLE SET CREATED_AT = NULL WHERE ID = 1");
            st.executeUpdate("UPDATE FA_SIMPLE SET VALUE = 9999.99 WHERE ID = 1");
        }
        FlexAssert.globalExcludeColumns("CREATED_AT").table("FA_SIMPLE").excludeColumns("VALUE")
                .assertTable(DB_NAME, "FA_SIMPLE");
    }

    @Test
    @LoadData(scenario = "flexassert-value-mismatch", dbNames = {"db1"})
    void assertTable_異常ケース_値不一致_AssertionErrorが送出されること() throws Exception {
        FlexAssert fa = FlexAssert.withDefaults();
        assertThrows(AssertionError.class, () -> fa.assertTable(DB_NAME, "FA_SIMPLE"));
    }

    @Test
    @LoadData(scenario = "flexassert-actual-more", dbNames = {"db1"})
    void assertTable_異常ケース_行数不一致で実が多い_AssertionErrorが送出されること() throws Exception {
        FlexAssert fa = FlexAssert.withDefaults();
        assertThrows(AssertionError.class, () -> fa.assertTable(DB_NAME, "FA_SIMPLE"));
    }

    @Test
    @LoadData(scenario = "flexassert-actual-less", dbNames = {"db1"})
    void assertTable_異常ケース_行数不一致で実が少ない_AssertionErrorが送出されること() throws Exception {
        FlexAssert fa = FlexAssert.withDefaults();
        assertThrows(AssertionError.class, () -> fa.assertTable(DB_NAME, "FA_SIMPLE"));
    }

    @Test
    void assertTables_異常ケース_expectedディレクトリなし_IllegalStateExceptionが送出されること() throws Exception {
        FlexAssert fa = FlexAssert.withDefaults();
        assertThrows(IllegalStateException.class, () -> fa.assertTables("NONEXISTENT_DB"));
    }

    @Test
    void assertTable_異常ケース_expectedCSVなし_例外が送出されること() throws Exception {
        FlexAssert fa = FlexAssert.withDefaults();
        assertThrows(Exception.class, () -> fa.assertTable(DB_NAME, "NONEXISTENT_TABLE"));
    }

    @Test
    void assertTables_異常ケース_DataSource未登録_IllegalStateExceptionが送出されること() throws Exception {
        LoadDataExtension.clearCurrentDataSources();
        DataSourceRegistry.clear();
        FlexAssert fa = FlexAssert.withDefaults();
        assertThrows(IllegalStateException.class, () -> fa.assertTables(DB_NAME));
    }

    @Test
    @LoadData(scenario = "flexassert-lob-missing", dbNames = {"db1"})
    void assertTable_異常ケース_LOBファイル参照切れ_例外が送出されること() throws Exception {
        FlexAssert fa = FlexAssert.withDefaults();
        assertThrows(Exception.class, () -> fa.assertTable(DB_NAME, "FA_LOB"));
    }

    @Test
    void assertTable_異常ケース_テーブル名不一致_例外が送出されること() throws Exception {
        FlexAssert fa = FlexAssert.withDefaults();
        assertThrows(Exception.class, () -> fa.assertTable(DB_NAME, "NONEXISTENT_TABLE"));
    }

    @Test
    void assertTables_異常ケース_DB接続不可_例外が送出されること() throws Exception {
        LoadDataExtension.clearCurrentDataSources();
        DriverManagerDataSource brokenDs = new DriverManagerDataSource();
        brokenDs.setUrl("jdbc:postgresql://localhost:1/nonexistent");
        brokenDs.setUsername("bad");
        brokenDs.setPassword("bad");
        DataSourceRegistry.register(DB_NAME, brokenDs);
        FlexAssert fa = FlexAssert.withDefaults();
        assertThrows(Exception.class, () -> fa.assertTables(DB_NAME));
    }
}
