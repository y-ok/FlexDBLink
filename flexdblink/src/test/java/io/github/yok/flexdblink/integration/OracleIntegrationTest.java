package io.github.yok.flexdblink.integration;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.oracle.OracleContainer;

/**
 * Integration tests for FlexDBLink against an Oracle container.
 *
 * <p>
 * Covers: DataLoader / DataDumper (CSV + LOB {@code file:} references), including Oracle-specific
 * types (INTERVAL YEAR TO MONTH, INTERVAL DAY TO SECOND, XMLType).
 * </p>
 */
@SpringBootTest(classes = IntegrationTestConfig.class)
@ContextConfiguration(initializers = YamlPropertySourceFactory.class)
@Testcontainers
class OracleIntegrationTest {

    private static final String MIGRATION = "classpath:db/migration/oracle";
    private static final String DB_NAME = "oracle";

    @Container
    static final OracleContainer ORACLE = new OracleContainer("gvenzl/oracle-free:slim-faststart");

    @TempDir
    Path tempDir;

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
        registry.add("connections[0].driver-class", () -> "oracle.jdbc.OracleDriver");
        registry.add("connections[0].url", ORACLE::getJdbcUrl);
        registry.add("connections[0].user", ORACLE::getUsername);
        registry.add("connections[0].password", ORACLE::getPassword);
    }

    @BeforeEach
    void setup_正常ケース_Oracleコンテナに対してFlywayを実行する_マイグレーションが完了すること() {
        IntegrationTestSupport.prepareDatabase(ORACLE, MIGRATION);
    }

    @Test
    void execute_正常ケース_拡張Oracle型をロードする_全列値が登録されること() throws Exception {
        Path dataPath = tempDir.resolve("load_data");
        IntegrationTestSupport.Runtime runtime = IntegrationTestSupport.prepareRuntime(dataPath,
                true, DB_NAME, pathsConfig, connectionConfig, dbUnitConfig, dumpConfig,
                filePatternConfig, dialectFactory);

        IntegrationTestSupport.executeLoad(runtime, "pre");

        Path csvPath = dataPath.resolve("load/pre/db1/IT_TYPED_MAIN.csv");
        Path filesDir = dataPath.resolve("load/pre/db1/files");

        try (Connection conn = IntegrationTestSupport.openConnection(ORACLE)) {
            IntegrationTestSupport.assertCsvMatchesDb(csvPath, "IT_TYPED_MAIN", "ID", conn,
                    filesDir, runtime.newDialectHandler());
        }
    }

    @Test
    void execute_異常ケース_interval形式が不正である_ロールバックされること() throws Exception {
        Path dataPath = tempDir.resolve("load_error_data");
        IntegrationTestSupport.Runtime runtime = IntegrationTestSupport.prepareRuntime(dataPath,
                true, DB_NAME, pathsConfig, connectionConfig, dbUnitConfig, dumpConfig,
                filePatternConfig, dialectFactory);

        Path mainCsv = dataPath.resolve("load/pre/db1/IT_TYPED_MAIN.csv");
        String csv = Files.readString(mainCsv);
        csv = csv.replaceFirst("1-2,1 2:3:4", "bad-interval,1 2:3:4");
        Files.writeString(mainCsv, csv);

        ErrorHandler.disableExitForCurrentThread();
        try {
            assertThrows(IllegalStateException.class,
                    () -> IntegrationTestSupport.executeLoad(runtime, "pre"));
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }

        try (Connection conn = IntegrationTestSupport.openConnection(ORACLE);
                Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM IT_TYPED_MAIN")) {
                assertTrue(rs.next());
                assertEquals(6, rs.getInt(1));
            }
            try (ResultSet rs = st.executeQuery("SELECT MIN(ID), MAX(ID) FROM IT_TYPED_MAIN")) {
                assertTrue(rs.next());
                assertEquals(1L, rs.getLong(1));
                assertEquals(6L, rs.getLong(2));
            }
        }
    }

    @Test
    void execute_正常ケース_拡張Oracle型をダンプする_CSVとLOBファイルが出力されること() throws Exception {
        Path dataPath = tempDir.resolve("dump_data");
        IntegrationTestSupport.Runtime runtime = IntegrationTestSupport.prepareRuntime(dataPath,
                false, DB_NAME, pathsConfig, connectionConfig, dbUnitConfig, dumpConfig,
                filePatternConfig, dialectFactory);

        Path dbDir = IntegrationTestSupport.executeDump(runtime, "it_dump_case");
        Path filesDir = dbDir.resolve("files");

        assertTrue(Files.exists(dbDir.resolve("IT_TYPED_MAIN.csv")));
        assertTrue(Files.exists(dbDir.resolve("IT_TYPED_AUX.csv")));

        String csvText =
                Files.readString(dbDir.resolve("IT_TYPED_MAIN.csv"), StandardCharsets.UTF_8);
        assertTrue(csvText.contains("NCHAR_COL"));
        assertTrue(csvText.contains("BF_COL"));
        assertTrue(csvText.contains("BD_COL"));
        assertTrue(csvText.contains("IV_YM_COL"));
        assertTrue(csvText.contains("IV_DS_COL"));
        assertTrue(csvText.contains("file:main_xml_1.txt"));
        assertTrue(csvText.contains("file:main_zip_5.bin"));
        assertTrue(csvText.contains("file:main_bin_6.bin"));

        assertTrue(Files.exists(filesDir.resolve("main_xml_1.txt")));
        assertTrue(Files.exists(filesDir.resolve("main_n_xml_1.txt")));
        assertTrue(Files.exists(filesDir.resolve("main_zip_5.bin")));
        assertTrue(Files.exists(filesDir.resolve("main_bin_6.bin")));
        assertTrue(Files.exists(filesDir.resolve("aux_xml_11.txt")));
        assertTrue(Files.exists(filesDir.resolve("aux_zip_12.bin")));

        try (Connection conn = IntegrationTestSupport.openConnection(ORACLE);
                Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT BLOB_COL FROM IT_TYPED_MAIN WHERE ID=6")) {
                rs.next();
                byte[] expected = rs.getBytes(1);
                byte[] actual = Files.readAllBytes(filesDir.resolve("main_bin_6.bin"));
                assertArrayEquals(expected, actual);
            }

            try (ResultSet rs = st.executeQuery("SELECT CLOB_COL FROM IT_TYPED_MAIN WHERE ID=1")) {
                rs.next();
                String expected = rs.getString(1);
                String actual = Files.readString(filesDir.resolve("main_xml_1.txt"),
                        StandardCharsets.UTF_8);
                assertEquals(expected, actual);
            }
        }
    }

    @Test
    void execute_正常ケース_interval列をダンプする_規定フォーマットで出力されること() throws Exception {
        Path dataPath = tempDir.resolve("dump_interval_data");
        IntegrationTestSupport.Runtime runtime = IntegrationTestSupport.prepareRuntime(dataPath,
                false, DB_NAME, pathsConfig, connectionConfig, dbUnitConfig, dumpConfig,
                filePatternConfig, dialectFactory);

        Path dbDir = IntegrationTestSupport.executeDump(runtime, "it_dump_interval_case");
        String csvText =
                Files.readString(dbDir.resolve("IT_TYPED_MAIN.csv"), StandardCharsets.UTF_8);
        assertTrue(csvText.contains("+01-02") || csvText.contains("1-2"));
        assertTrue(csvText.contains("+01 02:03:04") || csvText.contains("1 2:3:4"));
        assertTrue(csvText.contains("+06-07") || csvText.contains("6-7"));
        assertTrue(csvText.contains("+06 07:08:09") || csvText.contains("6 7:8:9"));
    }

    @Test
    void execute_正常ケース_nullと空LOBを含むデータをダンプする_CSV表現とLOBファイル内容が期待どおりであること() throws Exception {
        Path dataPath = tempDir.resolve("dump_null_empty_data");
        IntegrationTestSupport.Runtime runtime = IntegrationTestSupport.prepareRuntime(dataPath,
                false, DB_NAME, pathsConfig, connectionConfig, dbUnitConfig, dumpConfig,
                filePatternConfig, dialectFactory);

        try (Connection conn = IntegrationTestSupport.openConnection(ORACLE);
                Statement st = conn.createStatement()) {
            st.execute("INSERT INTO IT_TYPED_MAIN (ID, VC_COL, CHAR_COL, NVC_COL, NCHAR_COL, "
                    + "CLOB_COL, NCLOB_COL, BLOB_COL, LOB_KIND) "
                    + "VALUES (99, NULL, NULL, NULL, NULL, EMPTY_CLOB(), NULL, EMPTY_BLOB(), 'empty')");
            st.execute(
                    "INSERT INTO IT_TYPED_AUX (ID, MAIN_ID, LABEL, PAYLOAD_CLOB, PAYLOAD_BLOB, LOB_KIND) "
                            + "VALUES (99, 99, NULL, NULL, NULL, 'nullcase')");
        }

        Path dbDir = IntegrationTestSupport.executeDump(runtime, "it_dump_null_empty_case");
        Path filesDir = dbDir.resolve("files");

        Map<String, String> mainRow = IntegrationTestSupport
                .readCsvRowById(dbDir.resolve("IT_TYPED_MAIN.csv"), "ID", "99");
        assertEquals("", mainRow.get("VC_COL"));
        assertEquals("", mainRow.get("CHAR_COL"));
        assertEquals("", mainRow.get("NVC_COL"));
        assertEquals("", mainRow.get("NCHAR_COL"));
        assertEquals("file:main_empty_99.txt", mainRow.get("CLOB_COL"));
        assertEquals("", mainRow.get("NCLOB_COL"));
        assertEquals("file:main_empty_99.bin", mainRow.get("BLOB_COL"));

        Path emptyClobFile = filesDir.resolve("main_empty_99.txt");
        Path emptyBlobFile = filesDir.resolve("main_empty_99.bin");
        assertTrue(Files.exists(emptyClobFile));
        assertTrue(Files.exists(emptyBlobFile));
        assertEquals(0L, Files.size(emptyClobFile));
        assertEquals(0L, Files.size(emptyBlobFile));

        Map<String, String> auxRow = IntegrationTestSupport
                .readCsvRowById(dbDir.resolve("IT_TYPED_AUX.csv"), "ID", "99");
        assertEquals("", auxRow.get("LABEL"));
        assertEquals("", auxRow.get("PAYLOAD_CLOB"));
        assertEquals("", auxRow.get("PAYLOAD_BLOB"));
        assertTrue(Files.notExists(filesDir.resolve("aux_nullcase_99.txt")));
        assertTrue(Files.notExists(filesDir.resolve("aux_nullcase_99.bin")));
    }

    @Test
    void execute_正常ケース_ロード後にダンプする_入力CSVと出力CSVが一致すること() throws Exception {
        Path dataPath = tempDir.resolve("roundtrip_data");
        IntegrationTestSupport.Runtime runtime = IntegrationTestSupport.prepareRuntime(dataPath,
                true, DB_NAME, pathsConfig, connectionConfig, dbUnitConfig, dumpConfig,
                filePatternConfig, dialectFactory);

        IntegrationTestSupport.executeLoad(runtime, "pre");
        Path outputDbDir = IntegrationTestSupport.executeDump(runtime, "it_roundtrip_case");

        Path inputMainCsv = dataPath.resolve("load/pre/db1/IT_TYPED_MAIN.csv");
        Path inputAuxCsv = dataPath.resolve("load/pre/db1/IT_TYPED_AUX.csv");
        Path inputFilesDir = dataPath.resolve("load/pre/db1/files");

        Path outputMainCsv = outputDbDir.resolve("IT_TYPED_MAIN.csv");
        Path outputAuxCsv = outputDbDir.resolve("IT_TYPED_AUX.csv");
        Path outputFilesDir = outputDbDir.resolve("files");

        IntegrationTestSupport.assertCsvEquals("IT_TYPED_MAIN", "ID", inputMainCsv, outputMainCsv,
                inputFilesDir, outputFilesDir);
        IntegrationTestSupport.assertCsvEquals("IT_TYPED_AUX", "ID", inputAuxCsv, outputAuxCsv,
                inputFilesDir, outputFilesDir);
    }

    @Test
    void execute_正常ケース_FK制約あり_tableOrderingが毎回再生成されてロードが成功すること() throws Exception {
        Path dataPath = tempDir.resolve("fk_load_no_ordering");
        IntegrationTestSupport.Runtime runtime = IntegrationTestSupport.prepareRuntime(dataPath,
                true, DB_NAME, pathsConfig, connectionConfig, dbUnitConfig, dumpConfig,
                filePatternConfig, dialectFactory);

        IntegrationTestSupport.executeLoad(runtime, "pre");

        try (Connection conn = IntegrationTestSupport.openConnection(ORACLE);
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
    void execute_正常ケース_FK制約あり_FK解決でアルファベット順が修正されてロードが成功すること() throws Exception {
        Path dataPath = tempDir.resolve("fk_load_reversed_ordering");
        IntegrationTestSupport.Runtime runtime = IntegrationTestSupport.prepareRuntime(dataPath,
                true, DB_NAME, pathsConfig, connectionConfig, dbUnitConfig, dumpConfig,
                filePatternConfig, dialectFactory);

        IntegrationTestSupport.executeLoad(runtime, "pre");

        try (Connection conn = IntegrationTestSupport.openConnection(ORACLE);
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
    void execute_正常ケース_FK制約あり_ダンプが完了してCSVと件数が出力されること() throws Exception {
        Path dataPath = tempDir.resolve("fk_dump_data");
        IntegrationTestSupport.Runtime runtime = IntegrationTestSupport.prepareRuntime(dataPath,
                false, DB_NAME, pathsConfig, connectionConfig, dbUnitConfig, dumpConfig,
                filePatternConfig, dialectFactory);

        Path dbDir = IntegrationTestSupport.executeDump(runtime, "fk_dump_case");

        assertTrue(Files.exists(dbDir.resolve("IT_TYPED_MAIN.csv")));
        assertTrue(Files.exists(dbDir.resolve("IT_TYPED_AUX.csv")));

        try (Connection conn = IntegrationTestSupport.openConnection(ORACLE);
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
            long mainCsvRows = Files.lines(dbDir.resolve("IT_TYPED_MAIN.csv")).count() - 1;
            long auxCsvRows = Files.lines(dbDir.resolve("IT_TYPED_AUX.csv")).count() - 1;
            assertEquals(expectedMain, mainCsvRows, "IT_TYPED_MAIN CSV row count mismatch");
            assertEquals(expectedAux, auxCsvRows, "IT_TYPED_AUX CSV row count mismatch");
        }
    }

    @Test
    void execute_正常ケース_FK制約あり_ロード後にダンプする_件数が一致すること() throws Exception {
        Path dataPath = tempDir.resolve("fk_roundtrip_data");
        IntegrationTestSupport.Runtime runtime = IntegrationTestSupport.prepareRuntime(dataPath,
                true, DB_NAME, pathsConfig, connectionConfig, dbUnitConfig, dumpConfig,
                filePatternConfig, dialectFactory);

        IntegrationTestSupport.executeLoad(runtime, "pre");
        Path dbDir = IntegrationTestSupport.executeDump(runtime, "fk_roundtrip_case");

        assertTrue(Files.exists(dbDir.resolve("IT_TYPED_MAIN.csv")));
        assertTrue(Files.exists(dbDir.resolve("IT_TYPED_AUX.csv")));

        long mainCsvRows = Files.lines(dbDir.resolve("IT_TYPED_MAIN.csv")).count() - 1;
        long auxCsvRows = Files.lines(dbDir.resolve("IT_TYPED_AUX.csv")).count() - 1;
        assertEquals(6L, mainCsvRows, "IT_TYPED_MAIN should have 6 rows after load");
        assertEquals(2L, auxCsvRows, "IT_TYPED_AUX should have 2 rows after load");
    }

    @Test
    void execute_正常ケース_FK制約なし_tableOrderingが毎回再生成されてロードが成功すること() throws Exception {
        IntegrationTestSupport.prepareDatabaseWithoutFk(ORACLE, MIGRATION);
        Path dataPath = tempDir.resolve("nofk_load_no_ordering");
        IntegrationTestSupport.Runtime runtime = IntegrationTestSupport.prepareRuntime(dataPath,
                true, DB_NAME, pathsConfig, connectionConfig, dbUnitConfig, dumpConfig,
                filePatternConfig, dialectFactory);

        IntegrationTestSupport.executeLoad(runtime, "pre");

        try (Connection conn = IntegrationTestSupport.openConnection(ORACLE);
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
    void execute_正常ケース_FK制約なし_アルファベット順でロードが成功すること() throws Exception {
        IntegrationTestSupport.prepareDatabaseWithoutFk(ORACLE, MIGRATION);
        Path dataPath = tempDir.resolve("nofk_load_reversed_ordering");
        IntegrationTestSupport.Runtime runtime = IntegrationTestSupport.prepareRuntime(dataPath,
                true, DB_NAME, pathsConfig, connectionConfig, dbUnitConfig, dumpConfig,
                filePatternConfig, dialectFactory);

        IntegrationTestSupport.executeLoad(runtime, "pre");

        try (Connection conn = IntegrationTestSupport.openConnection(ORACLE);
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
    void execute_正常ケース_FK制約なし_ダンプが完了してCSVと件数が出力されること() throws Exception {
        IntegrationTestSupport.prepareDatabaseWithoutFk(ORACLE, MIGRATION);
        Path dataPath = tempDir.resolve("nofk_dump_data");
        IntegrationTestSupport.Runtime runtime = IntegrationTestSupport.prepareRuntime(dataPath,
                false, DB_NAME, pathsConfig, connectionConfig, dbUnitConfig, dumpConfig,
                filePatternConfig, dialectFactory);

        Path dbDir = IntegrationTestSupport.executeDump(runtime, "nofk_dump_case");

        assertTrue(Files.exists(dbDir.resolve("IT_TYPED_MAIN.csv")));
        assertTrue(Files.exists(dbDir.resolve("IT_TYPED_AUX.csv")));

        try (Connection conn = IntegrationTestSupport.openConnection(ORACLE);
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
            long mainCsvRows = Files.lines(dbDir.resolve("IT_TYPED_MAIN.csv")).count() - 1;
            long auxCsvRows = Files.lines(dbDir.resolve("IT_TYPED_AUX.csv")).count() - 1;
            assertEquals(expectedMain, mainCsvRows, "IT_TYPED_MAIN CSV row count mismatch");
            assertEquals(expectedAux, auxCsvRows, "IT_TYPED_AUX CSV row count mismatch");
        }
    }

    @Test
    void execute_正常ケース_FK制約なし_ロード後にダンプする_件数が一致すること() throws Exception {
        IntegrationTestSupport.prepareDatabaseWithoutFk(ORACLE, MIGRATION);
        Path dataPath = tempDir.resolve("nofk_roundtrip_data");
        IntegrationTestSupport.Runtime runtime = IntegrationTestSupport.prepareRuntime(dataPath,
                true, DB_NAME, pathsConfig, connectionConfig, dbUnitConfig, dumpConfig,
                filePatternConfig, dialectFactory);

        IntegrationTestSupport.executeLoad(runtime, "pre");
        Path dbDir = IntegrationTestSupport.executeDump(runtime, "nofk_roundtrip_case");

        assertTrue(Files.exists(dbDir.resolve("IT_TYPED_MAIN.csv")));
        assertTrue(Files.exists(dbDir.resolve("IT_TYPED_AUX.csv")));

        long mainCsvRows = Files.lines(dbDir.resolve("IT_TYPED_MAIN.csv")).count() - 1;
        long auxCsvRows = Files.lines(dbDir.resolve("IT_TYPED_AUX.csv")).count() - 1;
        assertEquals(6L, mainCsvRows, "IT_TYPED_MAIN should have 6 rows after load");
        assertEquals(2L, auxCsvRows, "IT_TYPED_AUX should have 2 rows after load");
    }

}
