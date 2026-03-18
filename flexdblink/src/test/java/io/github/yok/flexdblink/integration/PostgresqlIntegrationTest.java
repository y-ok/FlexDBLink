package io.github.yok.flexdblink.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.FilePatternConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.db.DbDialectHandlerFactory;
import io.github.yok.flexdblink.util.ErrorHandler;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Integration tests for FlexDBLink against a PostgreSQL container.
 *
 * <p>
 * Covers: DataLoader / DataDumper (CSV + LOB {@code file:} references).
 * </p>
 */
@SpringBootTest(classes = IntegrationTestConfig.class)
@ContextConfiguration(initializers = YamlPropertySourceFactory.class)
@Testcontainers
public class PostgresqlIntegrationTest {

    private static final String MIGRATION = "classpath:db/migration/postgresql";
    private static final String MIGRATION_NULL_EMPTY = "classpath:db/migration_extra/postgresql";
    private static final String MIGRATION_FK_HIERARCHY =
            "classpath:db/migration_extra_fk/postgresql";
    private static final String DB_NAME = "postgresql";

    @TempDir
    public Path tempDir;

    @Container
    private static final PostgreSQLContainer postgres = createPostgres();

    @Autowired
    PathsConfig pathsConfig;

    @Autowired
    ConnectionConfig connectionConfig;

    @Autowired
    DbUnitConfig dbUnitConfig;

    @Autowired
    DumpConfig dumpConfig;

    @Autowired
    FilePatternConfig filePatternConfig;

    @Autowired
    DbDialectHandlerFactory dialectFactory;

    /**
     * Registers container connection properties into the Spring environment.
     *
     * @param registry dynamic property registry
     */
    @DynamicPropertySource
    static void containerProps(DynamicPropertyRegistry registry) {
        registry.add("connections[0].id", () -> "db1");
        registry.add("connections[0].driver-class", () -> "org.postgresql.Driver");
        registry.add("connections[0].url", postgres::getJdbcUrl);
        registry.add("connections[0].user", postgres::getUsername);
        registry.add("connections[0].password", postgres::getPassword);
    }

    /**
     * Creates and configures the PostgreSQL test container with database name, username, and
     * password.
     *
     * @return configured PostgreSQL container
     */
    private static PostgreSQLContainer createPostgres() {
        PostgreSQLContainer container = new PostgreSQLContainer("postgres:16-alpine");
        container.withDatabaseName("testdb").withUsername("test").withPassword("test");
        return container;
    }

    @BeforeEach
    public void setup_正常ケース_PostgreSQLコンテナに対してFlywayを実行する_マイグレーションが完了すること() {
        IntegrationTestSupport.prepareDatabase(postgres, MIGRATION);
    }

    @Test
    public void execute_正常ケース_PostgreSQL型をロードする_全列値が登録されること() throws Exception {
        Path dataPath = tempDir.resolve("load_data");
        IntegrationTestSupport.Runtime runtime = IntegrationTestSupport.prepareRuntime(dataPath,
                true, DB_NAME, pathsConfig, connectionConfig, dbUnitConfig, dumpConfig,
                filePatternConfig, dialectFactory);

        IntegrationTestSupport.executeLoad(runtime, "pre");

        Path csvPath = dataPath.resolve("load/pre/db1/IT_TYPED_MAIN.csv");
        Path filesDir = dataPath.resolve("load/pre/db1/files");

        try (Connection conn = IntegrationTestSupport.openConnection(postgres)) {
            IntegrationTestSupport.assertCsvMatchesDb(csvPath, "IT_TYPED_MAIN", "ID", conn,
                    filesDir, runtime.newDialectHandler());
        }
    }

    @Test
    public void execute_正常ケース_CRLF改行のCSVをロードする_全列値が登録されること() throws Exception {
        Path dataPath = tempDir.resolve("load_data_crlf");
        IntegrationTestSupport.Runtime runtime = IntegrationTestSupport.prepareRuntime(dataPath,
                true, DB_NAME, pathsConfig, connectionConfig, dbUnitConfig, dumpConfig,
                filePatternConfig, dialectFactory);
        IntegrationTestSupport.overlayLoadScenario(dataPath, "pre_crlf", "pre", "db1");

        IntegrationTestSupport.executeLoad(runtime, "pre");

        Path csvPath = dataPath.resolve("load/pre/db1/IT_TYPED_MAIN.csv");
        Path filesDir = dataPath.resolve("load/pre/db1/files");

        try (Connection conn = IntegrationTestSupport.openConnection(postgres)) {
            IntegrationTestSupport.assertCsvMatchesDb(csvPath, "IT_TYPED_MAIN", "ID", conn,
                    filesDir, runtime.newDialectHandler());
        }
    }

    @Test
    public void execute_異常ケース_日時形式が不正である_ロールバックされること() throws Exception {
        Path dataPath = tempDir.resolve("load_error_data");
        IntegrationTestSupport.Runtime runtime = IntegrationTestSupport.prepareRuntime(dataPath,
                true, DB_NAME, pathsConfig, connectionConfig, dbUnitConfig, dumpConfig,
                filePatternConfig, dialectFactory);

        Path mainCsv = dataPath.resolve("load/pre/db1/IT_TYPED_MAIN.csv");
        String csv = Files.readString(mainCsv, StandardCharsets.UTF_8);
        csv = csv.replaceFirst(",2026-02-10 01:02:03,0A0B0C21,", ",bad-timestamptz,0A0B0C21,");
        Files.writeString(mainCsv, csv, StandardCharsets.UTF_8);

        ErrorHandler.disableExitForCurrentThread();
        try {
            assertThrows(IllegalStateException.class,
                    () -> IntegrationTestSupport.executeLoad(runtime, "pre"));
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }

        try (Connection conn = IntegrationTestSupport.openConnection(postgres);
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM IT_TYPED_MAIN")) {
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
    }

    @Test
    public void execute_正常ケース_ロード後にダンプする_入力CSVと出力CSVが一致すること() throws Exception {
        Path dataPath = tempDir.resolve("roundtrip_data");
        IntegrationTestSupport.Runtime runtime = IntegrationTestSupport.prepareRuntime(dataPath,
                true, DB_NAME, pathsConfig, connectionConfig, dbUnitConfig, dumpConfig,
                filePatternConfig, dialectFactory);

        IntegrationTestSupport.executeLoad(runtime, "pre");
        Path outputDbDir = IntegrationTestSupport.executeDump(runtime, "roundtrip_case");

        Path inputMainCsv = dataPath.resolve("load/pre/db1/IT_TYPED_MAIN.csv");
        Path inputAuxCsv = dataPath.resolve("load/pre/db1/IT_TYPED_AUX.csv");
        Path inputFilesDir = dataPath.resolve("load/pre/db1/files");

        Path outputMainCsv =
                IntegrationTestSupport.resolveFileIgnoreCase(outputDbDir, "IT_TYPED_MAIN.csv");
        Path outputAuxCsv =
                IntegrationTestSupport.resolveFileIgnoreCase(outputDbDir, "IT_TYPED_AUX.csv");
        Path outputFilesDir = outputDbDir.resolve("files");

        IntegrationTestSupport.assertCsvEquals("IT_TYPED_MAIN", "ID", inputMainCsv, outputMainCsv,
                inputFilesDir, outputFilesDir);
        IntegrationTestSupport.assertCsvEquals("IT_TYPED_AUX", "ID", inputAuxCsv, outputAuxCsv,
                inputFilesDir, outputFilesDir);
    }

    @Test
    public void execute_正常ケース_nullと空LOBを含むデータをダンプする_CSV表現とLOBファイル内容が期待どおりであること() throws Exception {
        Path dataPath = tempDir.resolve("dump_null_empty_data");
        IntegrationTestSupport.prepareDatabase(postgres, MIGRATION, MIGRATION_NULL_EMPTY);
        IntegrationTestSupport.Runtime runtime = IntegrationTestSupport.prepareRuntime(dataPath,
                false, DB_NAME, pathsConfig, connectionConfig, dbUnitConfig, dumpConfig,
                filePatternConfig, dialectFactory);

        Path dbDir = IntegrationTestSupport.executeDump(runtime, "dump_null_empty_case");
        Path filesDir = dbDir.resolve("files");
        Path mainCsv = IntegrationTestSupport.resolveFileIgnoreCase(dbDir, "IT_TYPED_MAIN.csv");
        Path auxCsv = IntegrationTestSupport.resolveFileIgnoreCase(dbDir, "IT_TYPED_AUX.csv");

        Map<String, String> mainRow = IntegrationTestSupport.readCsvRowById(mainCsv, "ID", "99");
        assertEquals("", mainRow.get("VC_COL"));
        assertEquals("file:main_empty_99.txt", mainRow.get("CLOB_COL"));
        assertEquals("", mainRow.get("NCLOB_COL"));
        assertEquals("file:main_empty_99.bin", mainRow.get("BLOB_COL"));

        Path emptyClobFile = filesDir.resolve("main_empty_99.txt");
        Path emptyBlobFile = filesDir.resolve("main_empty_99.bin");
        assertTrue(Files.exists(emptyClobFile));
        assertTrue(Files.exists(emptyBlobFile));
        assertEquals(0L, Files.size(emptyClobFile));
        assertEquals(0L, Files.size(emptyBlobFile));
        assertTrue(Files.notExists(filesDir.resolve("main_n_empty_99.txt")));

        Map<String, String> auxRow = IntegrationTestSupport.readCsvRowById(auxCsv, "ID", "99");
        assertEquals("", auxRow.get("LABEL"));
        assertEquals("file:aux_empty_99.txt", auxRow.get("PAYLOAD_CLOB"));
        assertEquals("", auxRow.get("PAYLOAD_BLOB"));

        Path emptyAuxClobFile = filesDir.resolve("aux_empty_99.txt");
        assertTrue(Files.exists(emptyAuxClobFile));
        assertEquals(0L, Files.size(emptyAuxClobFile));
        assertTrue(Files.notExists(filesDir.resolve("aux_empty_99.bin")));
    }

    @Test
    public void execute_正常ケース_FK制約あり_tableOrderingが毎回再生成されてロードが成功すること() throws Exception {
        Path dataPath = tempDir.resolve("fk_load_no_ordering");
        IntegrationTestSupport.Runtime runtime = IntegrationTestSupport.prepareRuntime(dataPath,
                true, DB_NAME, pathsConfig, connectionConfig, dbUnitConfig, dumpConfig,
                filePatternConfig, dialectFactory);
        Path orderingPath = IntegrationTestSupport.resolveTableOrderingPath(dataPath, "pre", "db1");
        Files.deleteIfExists(orderingPath);
        assertTrue(Files.notExists(orderingPath));

        IntegrationTestSupport.executeLoad(runtime, "pre");
        assertTrue(Files.notExists(orderingPath));

        try (Connection conn = IntegrationTestSupport.openConnection(postgres);
                Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM IT_TYPED_MAIN")) {
                assertTrue(rs.next());
                assertEquals(6, rs.getInt(1));
            }
            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM IT_TYPED_AUX")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM IT_TYPED_AUX a "
                    + "LEFT JOIN IT_TYPED_MAIN m ON m.ID = a.MAIN_ID WHERE m.ID IS NULL")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM IT_TYPED_AUX a "
                    + "INNER JOIN IT_TYPED_MAIN m ON m.ID = a.MAIN_ID")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
        }
    }

    @Test
    public void execute_正常ケース_FK制約あり_FK解決でアルファベット順が修正されてロードが成功すること() throws Exception {
        Path dataPath = tempDir.resolve("fk_load_reversed_ordering");
        IntegrationTestSupport.Runtime runtime = IntegrationTestSupport.prepareRuntime(dataPath,
                true, DB_NAME, pathsConfig, connectionConfig, dbUnitConfig, dumpConfig,
                filePatternConfig, dialectFactory);

        IntegrationTestSupport.executeLoad(runtime, "pre");

        try (Connection conn = IntegrationTestSupport.openConnection(postgres);
                Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM IT_TYPED_MAIN")) {
                assertTrue(rs.next());
                assertEquals(6, rs.getInt(1));
            }
            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM IT_TYPED_AUX")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
        }
    }

    @Test
    public void execute_正常ケース_FK制約あり_4世代の親子テーブルをtableOrdering未配置でロードする_FK整合で全件登録されること()
            throws Exception {
        Path dataPath = tempDir.resolve("fk_hierarchy_load_no_ordering");
        IntegrationTestSupport.prepareDatabase(postgres, MIGRATION, MIGRATION_FK_HIERARCHY);
        IntegrationTestSupport.Runtime runtime = IntegrationTestSupport.prepareRuntime(dataPath,
                false, DB_NAME, pathsConfig, connectionConfig, dbUnitConfig, dumpConfig,
                filePatternConfig, dialectFactory);
        String fixtureScenario = "fk_hierarchy";
        String loadScenario = "pre";
        IntegrationTestSupport.copyLoadScenarioFixtures(dataPath, DB_NAME, fixtureScenario,
                loadScenario);

        Path orderingPath =
                IntegrationTestSupport.resolveTableOrderingPath(dataPath, loadScenario, "db1");
        Files.deleteIfExists(orderingPath);
        assertTrue(Files.notExists(orderingPath));

        IntegrationTestSupport.executeLoad(runtime, loadScenario);
        assertTrue(Files.notExists(orderingPath));

        try (Connection conn = IntegrationTestSupport.openConnection(postgres);
                Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM IT_FK_ROOT")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM IT_FK_CHILD")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM IT_FK_GRANDCHILD")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM IT_FK_GREATGRANDCHILD")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM IT_FK_CHILD c "
                    + "LEFT JOIN IT_FK_ROOT p ON p.ID = c.ROOT_ID WHERE p.ID IS NULL")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM IT_FK_GRANDCHILD c "
                    + "LEFT JOIN IT_FK_CHILD p ON p.ID = c.CHILD_ID WHERE p.ID IS NULL")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM IT_FK_GREATGRANDCHILD c "
                    + "LEFT JOIN IT_FK_GRANDCHILD p ON p.ID = c.GRANDCHILD_ID "
                    + "WHERE p.ID IS NULL")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    public void execute_正常ケース_FK制約あり_ダンプが完了してCSVと件数が出力されること() throws Exception {
        Path dataPath = tempDir.resolve("fk_dump_data");
        IntegrationTestSupport.Runtime runtime = IntegrationTestSupport.prepareRuntime(dataPath,
                false, DB_NAME, pathsConfig, connectionConfig, dbUnitConfig, dumpConfig,
                filePatternConfig, dialectFactory);

        Path dbDir = IntegrationTestSupport.executeDump(runtime, "fk_dump_case");

        Path mainCsv = IntegrationTestSupport.resolveFileIgnoreCase(dbDir, "IT_TYPED_MAIN.csv");
        Path auxCsv = IntegrationTestSupport.resolveFileIgnoreCase(dbDir, "IT_TYPED_AUX.csv");
        assertTrue(Files.exists(mainCsv));
        assertTrue(Files.exists(auxCsv));

        try (Connection conn = IntegrationTestSupport.openConnection(postgres);
                Statement st = conn.createStatement()) {
            int expectedMain, expectedAux;
            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM IT_TYPED_MAIN")) {
                rs.next();
                expectedMain = rs.getInt(1);
            }
            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM IT_TYPED_AUX")) {
                rs.next();
                expectedAux = rs.getInt(1);
            }
            long mainCsvRows = Files.lines(mainCsv).count() - 1;
            long auxCsvRows = Files.lines(auxCsv).count() - 1;
            assertEquals(expectedMain, mainCsvRows, "IT_TYPED_MAIN CSV row count mismatch");
            assertEquals(expectedAux, auxCsvRows, "IT_TYPED_AUX CSV row count mismatch");
        }
    }

    @Test
    public void execute_正常ケース_FK制約あり_ロード後にダンプする_件数が一致すること() throws Exception {
        Path dataPath = tempDir.resolve("fk_roundtrip_data");
        IntegrationTestSupport.Runtime runtime = IntegrationTestSupport.prepareRuntime(dataPath,
                true, DB_NAME, pathsConfig, connectionConfig, dbUnitConfig, dumpConfig,
                filePatternConfig, dialectFactory);

        IntegrationTestSupport.executeLoad(runtime, "pre");
        Path dbDir = IntegrationTestSupport.executeDump(runtime, "fk_roundtrip_case");

        Path mainCsv = IntegrationTestSupport.resolveFileIgnoreCase(dbDir, "IT_TYPED_MAIN.csv");
        Path auxCsv = IntegrationTestSupport.resolveFileIgnoreCase(dbDir, "IT_TYPED_AUX.csv");
        assertTrue(Files.exists(mainCsv));
        assertTrue(Files.exists(auxCsv));

        long mainCsvRows = Files.lines(mainCsv).count() - 1;
        long auxCsvRows = Files.lines(auxCsv).count() - 1;
        assertEquals(6L, mainCsvRows, "IT_TYPED_MAIN should have 6 rows after load");
        assertEquals(2L, auxCsvRows, "IT_TYPED_AUX should have 2 rows after load");
    }

    @Test
    public void execute_正常ケース_FK制約なし_tableOrderingが毎回再生成されてロードが成功すること() throws Exception {
        IntegrationTestSupport.prepareDatabaseWithoutFk(postgres, MIGRATION);
        Path dataPath = tempDir.resolve("nofk_load_no_ordering");
        IntegrationTestSupport.Runtime runtime = IntegrationTestSupport.prepareRuntime(dataPath,
                true, DB_NAME, pathsConfig, connectionConfig, dbUnitConfig, dumpConfig,
                filePatternConfig, dialectFactory);

        IntegrationTestSupport.executeLoad(runtime, "pre");

        try (Connection conn = IntegrationTestSupport.openConnection(postgres);
                Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM IT_TYPED_MAIN")) {
                assertTrue(rs.next());
                assertEquals(6, rs.getInt(1));
            }
            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM IT_TYPED_AUX")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
        }
    }

    @Test
    public void execute_正常ケース_FK制約なし_アルファベット順でロードが成功すること() throws Exception {
        IntegrationTestSupport.prepareDatabaseWithoutFk(postgres, MIGRATION);
        Path dataPath = tempDir.resolve("nofk_load_reversed_ordering");
        IntegrationTestSupport.Runtime runtime = IntegrationTestSupport.prepareRuntime(dataPath,
                true, DB_NAME, pathsConfig, connectionConfig, dbUnitConfig, dumpConfig,
                filePatternConfig, dialectFactory);

        IntegrationTestSupport.executeLoad(runtime, "pre");

        try (Connection conn = IntegrationTestSupport.openConnection(postgres);
                Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM IT_TYPED_MAIN")) {
                assertTrue(rs.next());
                assertEquals(6, rs.getInt(1));
            }
            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM IT_TYPED_AUX")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
        }
    }

    @Test
    public void execute_正常ケース_FK制約なし_ダンプが完了してCSVと件数が出力されること() throws Exception {
        IntegrationTestSupport.prepareDatabaseWithoutFk(postgres, MIGRATION);
        Path dataPath = tempDir.resolve("nofk_dump_data");
        IntegrationTestSupport.Runtime runtime = IntegrationTestSupport.prepareRuntime(dataPath,
                false, DB_NAME, pathsConfig, connectionConfig, dbUnitConfig, dumpConfig,
                filePatternConfig, dialectFactory);

        Path dbDir = IntegrationTestSupport.executeDump(runtime, "nofk_dump_case");

        Path mainCsv = IntegrationTestSupport.resolveFileIgnoreCase(dbDir, "IT_TYPED_MAIN.csv");
        Path auxCsv = IntegrationTestSupport.resolveFileIgnoreCase(dbDir, "IT_TYPED_AUX.csv");
        assertTrue(Files.exists(mainCsv));
        assertTrue(Files.exists(auxCsv));

        try (Connection conn = IntegrationTestSupport.openConnection(postgres);
                Statement st = conn.createStatement()) {
            int expectedMain, expectedAux;
            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM IT_TYPED_MAIN")) {
                rs.next();
                expectedMain = rs.getInt(1);
            }
            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM IT_TYPED_AUX")) {
                rs.next();
                expectedAux = rs.getInt(1);
            }
            long mainCsvRows = Files.lines(mainCsv).count() - 1;
            long auxCsvRows = Files.lines(auxCsv).count() - 1;
            assertEquals(expectedMain, mainCsvRows, "IT_TYPED_MAIN CSV row count mismatch");
            assertEquals(expectedAux, auxCsvRows, "IT_TYPED_AUX CSV row count mismatch");
        }
    }

    @Test
    public void execute_正常ケース_FK制約なし_ロード後にダンプする_件数が一致すること() throws Exception {
        IntegrationTestSupport.prepareDatabaseWithoutFk(postgres, MIGRATION);
        Path dataPath = tempDir.resolve("nofk_roundtrip_data");
        IntegrationTestSupport.Runtime runtime = IntegrationTestSupport.prepareRuntime(dataPath,
                true, DB_NAME, pathsConfig, connectionConfig, dbUnitConfig, dumpConfig,
                filePatternConfig, dialectFactory);

        IntegrationTestSupport.executeLoad(runtime, "pre");
        Path dbDir = IntegrationTestSupport.executeDump(runtime, "nofk_roundtrip_case");

        Path mainCsv = IntegrationTestSupport.resolveFileIgnoreCase(dbDir, "IT_TYPED_MAIN.csv");
        Path auxCsv = IntegrationTestSupport.resolveFileIgnoreCase(dbDir, "IT_TYPED_AUX.csv");
        assertTrue(Files.exists(mainCsv));
        assertTrue(Files.exists(auxCsv));

        long mainCsvRows = Files.lines(mainCsv).count() - 1;
        long auxCsvRows = Files.lines(auxCsv).count() - 1;
        assertEquals(6L, mainCsvRows, "IT_TYPED_MAIN should have 6 rows after load");
        assertEquals(2L, auxCsvRows, "IT_TYPED_AUX should have 2 rows after load");
    }

}
