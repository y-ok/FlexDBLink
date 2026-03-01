package io.github.yok.flexdblink.maven.plugin.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.maven.plugin.config.CoreConfigBundle;
import io.github.yok.flexdblink.maven.plugin.config.PluginConfig;
import io.github.yok.flexdblink.maven.plugin.support.CoreConfigAssembler;
import io.github.yok.flexdblink.maven.plugin.support.FlexDbLinkCoreInvoker;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Multi-DB integration test for {@link FlexDbLinkCoreInvoker}.
 *
 * <p>
 * Verifies that load and dump operations work correctly across PostgreSQL and MySQL simultaneously.
 * </p>
 */
@Testcontainers
class FlexDbLinkCoreInvokerMultiDbIT {

    private static final String PG_DB_ID = "PG1";
    private static final String MY_DB_ID = "MY1";

    @Container
    private static final PostgreSQLContainer POSTGRES = createPostgres();

    @Container
    private static final MySQLContainer MYSQL = createMySql();

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

    private final CoreConfigAssembler assembler = new CoreConfigAssembler();
    private final FlexDbLinkCoreInvoker invoker = new FlexDbLinkCoreInvoker();

    private Path tempDataPath;

    @BeforeEach
    void setUp() throws Exception {
        tempDataPath = Files.createTempDirectory("flexdblink-plugin-multidb-it-");
        initTable(pgConnection());
        initTable(myConnection());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (tempDataPath == null) {
            return;
        }
        if (!Files.exists(tempDataPath)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(tempDataPath)) {
            for (Path path : walk.sorted(Comparator.reverseOrder()).collect(Collectors.toList())) {
                Files.deleteIfExists(path);
            }
        }
    }

    @Test
    void load_正常ケース_複数DB同時にpreシナリオを投入する_全DBにレコードが登録されること() throws Exception {
        prepareCsvForBothDbs();

        CoreConfigBundle bundle = buildMultiBundle(tempDataPath);
        invoker.load(bundle, null, List.of(PG_DB_ID, MY_DB_ID));

        assertEquals(2, countRows(pgConnection()));
        assertEquals(2, countRows(myConnection()));
    }

    @Test
    void dump_正常ケース_複数DB同時にダンプする_全DBのCSVが生成されること() throws Exception {
        insertRows(pgConnection());
        insertRows(myConnection());

        CoreConfigBundle bundle = buildMultiBundle(tempDataPath);
        invoker.dump(bundle, "scenarioA", List.of(PG_DB_ID, MY_DB_ID));

        assertDumpCsvExists(PG_DB_ID);
        assertDumpCsvExists(MY_DB_ID);
    }

    @Test
    void load_正常ケース_target指定で特定DBのみ投入する_指定DBのみレコードが登録されること() throws Exception {
        prepareCsvForBothDbs();

        CoreConfigBundle bundle = buildMultiBundle(tempDataPath);
        // target only PostgreSQL
        invoker.load(bundle, null, List.of(PG_DB_ID));

        assertEquals(2, countRows(pgConnection()));
        assertEquals(0, countRows(myConnection()));
    }

    /** Prepares CSV files for both PostgreSQL and MySQL DB ID directories. */
    private void prepareCsvForBothDbs() throws Exception {
        String csv = "ID,NAME\n1,Alice\n2,Bob\n";
        for (String dbId : List.of(PG_DB_ID, MY_DB_ID)) {
            Path preDir = tempDataPath.resolve("load").resolve("pre").resolve(dbId);
            Files.createDirectories(preDir);
            Files.writeString(preDir.resolve("employee.csv"), csv, StandardCharsets.UTF_8);
        }
    }

    /** Initializes the employee table (create + truncate). */
    private void initTable(Connection conn) throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(
                    "CREATE TABLE IF NOT EXISTS employee (id INT PRIMARY KEY, name VARCHAR(100))");
            stmt.execute("TRUNCATE TABLE employee");
        }
        conn.close();
    }

    /**
     * Inserts 2 rows into the employee table for the given connection.
     *
     * @param conn the database connection to use for inserting rows
     * @throws Exception if any SQL error occurs during insertion
     */
    private void insertRows(Connection conn) throws Exception {
        try (PreparedStatement ps =
                conn.prepareStatement("INSERT INTO employee(id, name) VALUES (?, ?)")) {
            ps.setInt(1, 11);
            ps.setString(2, "Alice");
            ps.executeUpdate();
            ps.setInt(1, 22);
            ps.setString(2, "Bob");
            ps.executeUpdate();
        }
        conn.close();
    }

    /**
     * Asserts that a CSV file exists in the dump directory for the specified DB ID and contains
     * expected data.
     *
     * @param dbId the database ID for which to check the dump CSV
     * @throws Exception if any error occurs while accessing the file system or reading the CSV
     *         content
     */
    private void assertDumpCsvExists(String dbId) throws Exception {
        Path dumpDbDir = tempDataPath.resolve("dump").resolve("scenarioA").resolve(dbId);
        assertTrue(Files.isDirectory(dumpDbDir), "Dump directory should exist for " + dbId);

        List<Path> csvFiles;
        try (Stream<Path> files = Files.list(dumpDbDir)) {
            csvFiles = files.filter(p -> p.getFileName().toString().toLowerCase().endsWith(".csv"))
                    .collect(Collectors.toList());
        }
        assertFalse(csvFiles.isEmpty(), "CSV files should exist for " + dbId);

        String content = Files.readString(csvFiles.get(0), StandardCharsets.UTF_8).toLowerCase();
        assertTrue(content.contains("alice"), "Dump for " + dbId + " should contain alice");
        assertTrue(content.contains("bob"), "Dump for " + dbId + " should contain bob");
    }

    /**
     * Builds a CoreConfigBundle for multi-DB testing with the given data path and default
     * configurations for both PostgreSQL and MySQL.
     *
     * @param dataPath the base path for data files used in the tests
     * @return a CoreConfigBundle configured for both PostgreSQL and MySQL with the specified data
     *         path
     */
    private CoreConfigBundle buildMultiBundle(Path dataPath) {
        PluginConfig pluginConfig = new PluginConfig();
        pluginConfig.setDataPath(dataPath.toString());
        pluginConfig.setDbunit(defaultDbunit());
        pluginConfig.setFilePatterns(List.of());

        ConnectionConfig connectionConfig = new ConnectionConfig();
        List<ConnectionConfig.Entry> entries = new ArrayList<>();

        ConnectionConfig.Entry pgEntry = new ConnectionConfig.Entry();
        pgEntry.setId(PG_DB_ID);
        pgEntry.setUrl(POSTGRES.getJdbcUrl());
        pgEntry.setUser(POSTGRES.getUsername());
        pgEntry.setPassword(POSTGRES.getPassword());
        pgEntry.setDriverClass("org.postgresql.Driver");
        entries.add(pgEntry);

        ConnectionConfig.Entry myEntry = new ConnectionConfig.Entry();
        myEntry.setId(MY_DB_ID);
        myEntry.setUrl(MYSQL.getJdbcUrl());
        myEntry.setUser(MYSQL.getUsername());
        myEntry.setPassword(MYSQL.getPassword());
        myEntry.setDriverClass("com.mysql.cj.jdbc.Driver");
        entries.add(myEntry);

        connectionConfig.setConnections(entries);

        return assembler.assemble(pluginConfig, connectionConfig);
    }

    /**
     * Creates a default DbUnit configuration with common settings for both PostgreSQL and MySQL,
     * including CSV format and runtime configurations.
     *
     * @return a PluginConfig.DbUnit instance with default settings for CSV format and runtime
     *         configurations suitable for both databases
     */
    private PluginConfig.DbUnit defaultDbunit() {
        PluginConfig.DbUnit dbunit = new PluginConfig.DbUnit();
        dbunit.setPreDirName("pre");

        PluginConfig.Csv csv = new PluginConfig.Csv();
        PluginConfig.Format format = new PluginConfig.Format();
        format.setDate("yyyy-MM-dd");
        format.setTime("HH:mm:ss");
        format.setDateTime("yyyy-MM-dd HH:mm:ss");
        format.setDateTimeWithMillis("yyyy-MM-dd HH:mm:ss.SSS");
        csv.setFormat(format);
        dbunit.setCsv(csv);

        PluginConfig.RuntimeConfig runtimeConfig = new PluginConfig.RuntimeConfig();
        runtimeConfig.setAllowEmptyFields(Boolean.TRUE);
        runtimeConfig.setBatchedStatements(Boolean.TRUE);
        runtimeConfig.setBatchSize(Integer.valueOf(100));
        dbunit.setConfig(runtimeConfig);

        return dbunit;
    }

    /**
     * Creates and returns a new database connection to the PostgreSQL test container using the
     * configured JDBC URL, username, and password.
     *
     * @return a Connection to the PostgreSQL test container
     * @throws Exception if the connection fails
     */
    private Connection pgConnection() throws Exception {
        return DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(),
                POSTGRES.getPassword());
    }

    /**
     * Creates and returns a new database connection to the MySQL test container using the
     * configured JDBC URL, username, and password.
     *
     * @return a Connection to the MySQL test container
     * @throws Exception if the connection fails
     */
    private Connection myConnection() throws Exception {
        return DriverManager.getConnection(MYSQL.getJdbcUrl(), MYSQL.getUsername(),
                MYSQL.getPassword());
    }

    /**
     * Counts the number of rows in the employee table for the given database connection by
     * executing a SELECT COUNT(*) query and returns the result.
     * 
     * @param conn the database connection to use for counting rows
     * @return the number of rows in the employee table
     * @throws Exception if any SQL error occurs during the count operation
     */
    private int countRows(Connection conn) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM employee");
                ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getInt(1);
        } finally {
            conn.close();
        }
    }
}
