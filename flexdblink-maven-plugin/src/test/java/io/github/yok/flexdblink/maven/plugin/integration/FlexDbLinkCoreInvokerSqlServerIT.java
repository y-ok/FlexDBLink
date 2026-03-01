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
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.mssqlserver.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Integration test for {@link FlexDbLinkCoreInvoker} with SQL Server.
 */
@Testcontainers
class FlexDbLinkCoreInvokerSqlServerIT {

    @Container
    private static final MSSQLServerContainer SQLSERVER = createSqlServer();

    @SuppressWarnings("resource")
    private static MSSQLServerContainer createSqlServer() {
        return new MSSQLServerContainer("mcr.microsoft.com/mssql/server:2019-latest")
                .acceptLicense();
    }

    private final CoreConfigAssembler assembler = new CoreConfigAssembler();
    private final FlexDbLinkCoreInvoker invoker = new FlexDbLinkCoreInvoker();

    private Path tempDataPath;

    @BeforeEach
    void setUp() throws Exception {
        tempDataPath = Files.createTempDirectory("flexdblink-plugin-sqlserver-it-");
        try (Connection conn = connection(); Statement stmt = conn.createStatement()) {
            stmt.execute("IF OBJECT_ID('employee', 'U') IS NOT NULL DROP TABLE employee");
            stmt.execute("CREATE TABLE employee (id INT PRIMARY KEY, name VARCHAR(100))");
        }
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
    void load_正常ケース_preシナリオを投入する_レコードが登録されること() throws Exception {
        Path preDir = tempDataPath.resolve("load").resolve("pre").resolve("DB1");
        Files.createDirectories(preDir);
        String csv = "ID,NAME\n1,Alice\n2,Bob\n";
        Files.writeString(preDir.resolve("employee.csv"), csv, StandardCharsets.UTF_8);

        CoreConfigBundle bundle = buildBundle(tempDataPath);
        invoker.load(bundle, null, List.of("DB1"));

        assertEquals(2, countEmployeeRows());
    }

    @Test
    void dump_正常ケース_シナリオ指定で実行する_ダンプCSVが生成されること() throws Exception {
        try (Connection conn = connection();
                PreparedStatement ps =
                        conn.prepareStatement("INSERT INTO employee(id, name) VALUES (?, ?)")) {
            ps.setInt(1, 11);
            ps.setString(2, "Alice");
            ps.executeUpdate();
            ps.setInt(1, 22);
            ps.setString(2, "Bob");
            ps.executeUpdate();
        }

        CoreConfigBundle bundle = buildBundle(tempDataPath);
        invoker.dump(bundle, "scenarioA", List.of("DB1"));

        Path dumpDbDir = tempDataPath.resolve("dump").resolve("scenarioA").resolve("DB1");
        assertTrue(Files.isDirectory(dumpDbDir));

        List<Path> csvFiles;
        try (Stream<Path> files = Files.list(dumpDbDir)) {
            csvFiles = files
                    .filter(path -> path.getFileName().toString().toLowerCase().endsWith(".csv"))
                    .collect(Collectors.toList());
        }
        assertFalse(csvFiles.isEmpty());

        String dumped = Files.readString(csvFiles.get(0), StandardCharsets.UTF_8).toLowerCase();
        assertTrue(dumped.contains("alice"));
        assertTrue(dumped.contains("bob"));
    }

    private CoreConfigBundle buildBundle(Path dataPath) {
        PluginConfig pluginConfig = new PluginConfig();
        pluginConfig.setDataPath(dataPath.toString());
        pluginConfig.setDbunit(defaultDbunit());
        pluginConfig.setFilePatterns(List.of());

        ConnectionConfig connectionConfig = new ConnectionConfig();
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("DB1");
        entry.setUrl(SQLSERVER.getJdbcUrl());
        entry.setUser(SQLSERVER.getUsername());
        entry.setPassword(SQLSERVER.getPassword());
        entry.setDriverClass("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        connectionConfig.setConnections(List.of(entry));

        return assembler.assemble(pluginConfig, connectionConfig);
    }

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

    private Connection connection() throws Exception {
        return DriverManager.getConnection(SQLSERVER.getJdbcUrl(), SQLSERVER.getUsername(),
                SQLSERVER.getPassword());
    }

    private int countEmployeeRows() throws Exception {
        try (Connection conn = connection();
                PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM employee");
                ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getInt(1);
        }
    }
}
