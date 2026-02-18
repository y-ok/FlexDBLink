package io.github.yok.flexdblink.integration;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.github.yok.flexdblink.util.ErrorHandler;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.oracle.OracleContainer;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

@Testcontainers
class OracleIntegrationTest {

    private static final Set<String> NUMERIC_COLUMNS =
            Set.of("ID", "MAIN_ID", "NUM_COL", "BF_COL", "BD_COL");

    @Container
    static final OracleContainer ORACLE = new OracleContainer("gvenzl/oracle-free:slim-faststart");

    @TempDir
    Path tempDir;

    @BeforeEach
    void setup_正常ケース_Oracleコンテナに対してFlywayを実行する_マイグレーションが完了すること() {
        OracleIntegrationSupport.prepareDatabase(ORACLE);
    }

    @Test
    void execute_正常ケース_拡張Oracle型をロードする_全列値が登録されること() throws Exception {
        Path dataPath = tempDir.resolve("load_data");
        OracleIntegrationSupport.Runtime runtime =
                OracleIntegrationSupport.prepareRuntime(ORACLE, dataPath, true);

        OracleIntegrationSupport.executeLoad(runtime, "pre");

        try (Connection conn = OracleIntegrationSupport.openConnection(ORACLE);
                Statement st = conn.createStatement()) {

            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM IT_TYPED_MAIN")) {
                assertTrue(rs.next(), "COUNT(*) の結果が取得できません: IT_TYPED_MAIN");
                assertEquals(6, rs.getInt(1));
            }
            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM IT_TYPED_AUX")) {
                assertTrue(rs.next(), "COUNT(*) の結果が取得できません: IT_TYPED_AUX");
                assertEquals(2, rs.getInt(1));
            }

            try (ResultSet rs = st.executeQuery("SELECT MIN(ID), MAX(ID) FROM IT_TYPED_MAIN")) {
                assertTrue(rs.next(), "MIN/MAX の結果が取得できません: IT_TYPED_MAIN");
                assertEquals(101L, rs.getLong(1));
                assertEquals(106L, rs.getLong(2));
            }

            try (ResultSet rs = st.executeQuery(
                    "SELECT ID, VC_COL, NCHAR_COL, BF_COL, BD_COL, IV_YM_COL, IV_DS_COL, RAW_COL, CLOB_COL, NCLOB_COL, BLOB_COL, LOB_KIND "
                            + "FROM IT_TYPED_MAIN WHERE ID = 101")) {

                assertTrue(rs.next(),
                        () -> "IT_TYPED_MAIN に ID=101 が存在しません。\n" + dumpMainRows(conn));
                assertEquals("load-xml", rs.getString("VC_COL"));
                assertEquals("na-xml", rs.getString("NCHAR_COL").trim());
                assertEquals(0, BigDecimal.valueOf(rs.getFloat("BF_COL"))
                        .compareTo(new BigDecimal("1.25")));
                assertEquals(0, BigDecimal.valueOf(rs.getDouble("BD_COL"))
                        .compareTo(new BigDecimal("10.125")));
                assertEquals("1-2", rs.getString("IV_YM_COL"));
                assertEquals("1 2:3:4.0", rs.getString("IV_DS_COL"));

                byte[] rawCol = rs.getBytes("RAW_COL");
                assertNotNull(rawCol);
                assertTrue(Arrays.equals(new byte[] {0x0A, 0x0B, 0x0C, 0x21}, rawCol),
                        "RAW_COL が期待値と一致しません");

                String clobCol = rs.getString("CLOB_COL");
                assertXmlTextEquals(clobCol, "/root/kind/text()", "xml");
                assertXmlTextEquals(clobCol, "/root/source/text()", "load");
                assertEquals("nclob-load-xml", rs.getString("NCLOB_COL"));
                assertEquals(null, rs.getBytes("BLOB_COL"));
                assertEquals("xml", rs.getString("LOB_KIND"));
                assertFalse(rs.next(), "ID=101 が複数行です");
            }

            try (ResultSet rs =
                    st.executeQuery("SELECT BLOB_COL FROM IT_TYPED_MAIN WHERE LOB_KIND='zip'")) {
                assertTrue(rs.next(),
                        () -> "IT_TYPED_MAIN に LOB_KIND='zip' 行が存在しません。\n" + dumpMainRows(conn));
                byte[] actual = rs.getBytes(1);
                assertFalse(rs.next(), "IT_TYPED_MAIN の LOB_KIND='zip' が複数行です。");

                Path expectedPath = dataPath.resolve("load/pre/db1/files/main_zip_1.zip");
                byte[] expected = Files.readAllBytes(expectedPath);
                assertArrayEquals(expected, actual);
            }

            try (ResultSet rs =
                    st.executeQuery("SELECT BLOB_COL FROM IT_TYPED_MAIN WHERE LOB_KIND='bin'")) {
                assertTrue(rs.next(),
                        () -> "IT_TYPED_MAIN に LOB_KIND='bin' 行が存在しません。\n" + dumpMainRows(conn));
                byte[] actual = rs.getBytes(1);
                assertFalse(rs.next(), "IT_TYPED_MAIN の LOB_KIND='bin' が複数行です。");

                byte[] expected =
                        Files.readAllBytes(dataPath.resolve("load/pre/db1/files/main_bin_1.bin"));
                assertArrayEquals(expected, actual);
            }

            try (ResultSet rs = st.executeQuery(
                    "SELECT PAYLOAD_CLOB FROM IT_TYPED_AUX WHERE PAYLOAD_CLOB IS NOT NULL")) {
                String auxDump = dumpAuxRows(conn);
                assertTrue(rs.next(), () -> "IT_TYPED_AUX に PAYLOAD_CLOB が存在しません。\n" + auxDump);
                String payloadClob = rs.getString(1);
                assertFalse(rs.next(), () -> "IT_TYPED_AUX の PAYLOAD_CLOB が複数行です。\n" + auxDump);

                assertXmlTextEquals(payloadClob, "/aux/kind/text()", "xml");
                assertXmlTextEquals(payloadClob, "/aux/source/text()", "load");
            }

            try (ResultSet rs = st.executeQuery(
                    "SELECT PAYLOAD_BLOB FROM IT_TYPED_AUX WHERE PAYLOAD_BLOB IS NOT NULL")) {
                assertTrue(rs.next(), "IT_TYPED_AUX に PAYLOAD_BLOB が存在しません。\n" + dumpAuxRows(conn));

                byte[] actual = rs.getBytes(1);
                assertFalse(rs.next(), "IT_TYPED_AUX の PAYLOAD_BLOB が複数行です。");

                byte[] expected =
                        Files.readAllBytes(dataPath.resolve("load/pre/db1/files/aux_zip_1.zip"));
                assertArrayEquals(expected, actual);
            }
        }
    }

    @Test
    void execute_異常ケース_interval形式が不正である_ロールバックされること() throws Exception {
        Path dataPath = tempDir.resolve("load_error_data");
        OracleIntegrationSupport.Runtime runtime =
                OracleIntegrationSupport.prepareRuntime(ORACLE, dataPath, true);

        Path mainCsv = dataPath.resolve("load/pre/db1/IT_TYPED_MAIN.csv");
        String csv = Files.readString(mainCsv);
        csv = csv.replaceFirst("1-2,1 2:3:4", "bad-interval,1 2:3:4");
        Files.writeString(mainCsv, csv);

        ErrorHandler.disableExitForCurrentThread();
        try {
            assertThrows(IllegalStateException.class,
                    () -> OracleIntegrationSupport.executeLoad(runtime, "pre"));
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }

        try (Connection conn = OracleIntegrationSupport.openConnection(ORACLE);
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
        OracleIntegrationSupport.Runtime runtime =
                OracleIntegrationSupport.prepareRuntime(ORACLE, dataPath, false);

        Path dbDir = OracleIntegrationSupport.executeDump(runtime, "it_dump_case");
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

        try (Connection conn = OracleIntegrationSupport.openConnection(ORACLE);
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
        OracleIntegrationSupport.Runtime runtime =
                OracleIntegrationSupport.prepareRuntime(ORACLE, dataPath, false);

        Path dbDir = OracleIntegrationSupport.executeDump(runtime, "it_dump_interval_case");
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
        OracleIntegrationSupport.Runtime runtime =
                OracleIntegrationSupport.prepareRuntime(ORACLE, dataPath, false);

        try (Connection conn = OracleIntegrationSupport.openConnection(ORACLE);
                Statement st = conn.createStatement()) {
            st.execute("INSERT INTO IT_TYPED_MAIN (ID, VC_COL, CHAR_COL, NVC_COL, NCHAR_COL, "
                    + "CLOB_COL, NCLOB_COL, BLOB_COL, LOB_KIND) "
                    + "VALUES (99, NULL, NULL, NULL, NULL, EMPTY_CLOB(), NULL, EMPTY_BLOB(), 'empty')");
            st.execute(
                    "INSERT INTO IT_TYPED_AUX (ID, MAIN_ID, LABEL, PAYLOAD_CLOB, PAYLOAD_BLOB, LOB_KIND) "
                            + "VALUES (99, 99, NULL, NULL, NULL, 'nullcase')");
        }

        Path dbDir = OracleIntegrationSupport.executeDump(runtime, "it_dump_null_empty_case");
        Path filesDir = dbDir.resolve("files");

        Map<String, String> mainRow =
                OracleIntegrationSupport.readCsvRowById(dbDir.resolve("IT_TYPED_MAIN.csv"), "99");
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

        Map<String, String> auxRow =
                OracleIntegrationSupport.readCsvRowById(dbDir.resolve("IT_TYPED_AUX.csv"), "99");
        assertEquals("", auxRow.get("LABEL"));
        assertEquals("", auxRow.get("PAYLOAD_CLOB"));
        assertEquals("", auxRow.get("PAYLOAD_BLOB"));
        assertTrue(Files.notExists(filesDir.resolve("aux_nullcase_99.txt")));
        assertTrue(Files.notExists(filesDir.resolve("aux_nullcase_99.bin")));
    }

    @Test
    void execute_正常ケース_拡張Oracle型をロード後にダンプする_件数と全列値が一致すること() throws Exception {
        Path dataPath = tempDir.resolve("roundtrip_data");
        OracleIntegrationSupport.Runtime runtime =
                OracleIntegrationSupport.prepareRuntime(ORACLE, dataPath, true);

        OracleIntegrationSupport.executeLoad(runtime, "pre");
        Path outputDbDir = OracleIntegrationSupport.executeDump(runtime, "it_roundtrip_case");

        Path inputMainCsv = dataPath.resolve("load/pre/db1/IT_TYPED_MAIN.csv");
        Path inputAuxCsv = dataPath.resolve("load/pre/db1/IT_TYPED_AUX.csv");
        Path inputFilesDir = dataPath.resolve("load/pre/db1/files");

        Path outputMainCsv = outputDbDir.resolve("IT_TYPED_MAIN.csv");
        Path outputAuxCsv = outputDbDir.resolve("IT_TYPED_AUX.csv");
        Path outputFilesDir = outputDbDir.resolve("files");

        assertTableEquals("IT_TYPED_MAIN", "ID", inputMainCsv, outputMainCsv, inputFilesDir,
                outputFilesDir);
        assertTableEquals("IT_TYPED_AUX", "ID", inputAuxCsv, outputAuxCsv, inputFilesDir,
                outputFilesDir);
    }

    private static void assertTableEquals(String table, String idColumn, Path inputCsv,
            Path outputCsv, Path inputFilesDir, Path outputFilesDir) throws Exception {
        Map<String, Map<String, String>> inputRows =
                OracleIntegrationSupport.readCsvById(inputCsv, idColumn);
        Map<String, Map<String, String>> outputRows =
                OracleIntegrationSupport.readCsvById(outputCsv, idColumn);

        assertEquals(inputRows.size(), outputRows.size(),
                table + " 件数が一致しません: input=" + inputRows.size() + " output=" + outputRows.size());

        for (Map.Entry<String, Map<String, String>> entry : inputRows.entrySet()) {
            String id = entry.getKey();
            Map<String, String> inRow = entry.getValue();
            Map<String, String> outRow = outputRows.get(id);
            assertTrue(outRow != null, table + " に ID=" + id + " が存在しません");

            for (Map.Entry<String, String> colEntry : inRow.entrySet()) {
                String column = colEntry.getKey();
                String inVal = colEntry.getValue();
                String outVal = outRow.get(column);
                assertCellEquals(table, id, column, inVal, outVal, inputFilesDir, outputFilesDir);
            }
        }
    }

    private static void assertCellEquals(String table, String id, String column, String inputValue,
            String outputValue, Path inputFilesDir, Path outputFilesDir) throws Exception {
        String inVal = OracleIntegrationSupport.trimToNull(inputValue);
        String outVal = OracleIntegrationSupport.trimToNull(outputValue);

        if (inVal == null && outVal == null) {
            return;
        }

        if (isBlobColumn(column)) {
            byte[] inBlob = resolveBlobValue(inVal, inputFilesDir);
            byte[] outBlob = resolveBlobValue(outVal, outputFilesDir);
            assertArrayEquals(inBlob, outBlob, diffPrefix(table, id, column) + "BLOB値が一致しません");
            return;
        }

        if (isLobTextColumn(column)) {
            String inText = resolveTextValue(inVal, inputFilesDir);
            String outText = resolveTextValue(outVal, outputFilesDir);
            assertEquals(inText, outText, diffPrefix(table, id, column) + "CLOB/NCLOB値が一致しません");
            return;
        }

        if ("RAW_COL".equalsIgnoreCase(column)) {
            byte[] inRaw =
                    OracleIntegrationSupport.decodeHex(OracleIntegrationSupport.trimToEmpty(inVal));
            byte[] outRaw = OracleIntegrationSupport
                    .decodeHex(OracleIntegrationSupport.trimToEmpty(outVal));
            assertArrayEquals(inRaw, outRaw, diffPrefix(table, id, column) + "RAW値が一致しません");
            return;
        }

        if (NUMERIC_COLUMNS.contains(column.toUpperCase())) {
            BigDecimal inNum = new BigDecimal(OracleIntegrationSupport.trimToEmpty(inVal));
            BigDecimal outNum = new BigDecimal(OracleIntegrationSupport.trimToEmpty(outVal));
            assertEquals(0, inNum.compareTo(outNum), diffPrefix(table, id, column) + "数値が一致しません");
            return;
        }

        if ("IV_YM_COL".equalsIgnoreCase(column) || "IV_DS_COL".equalsIgnoreCase(column)
                || "TSTZ_COL".equalsIgnoreCase(column) || "TSLTZ_COL".equalsIgnoreCase(column)
                || "DATE_COL".equalsIgnoreCase(column)) {
            assertEquals(OracleIntegrationSupport.trimToEmpty(inVal),
                    OracleIntegrationSupport.trimToEmpty(outVal),
                    diffPrefix(table, id, column) + "日時/INTERVAL値が一致しません");
            return;
        }

        if ("CHAR_COL".equalsIgnoreCase(column) || "NCHAR_COL".equalsIgnoreCase(column)) {
            assertEquals(OracleIntegrationSupport.trimToEmpty(inVal),
                    OracleIntegrationSupport.trimToEmpty(outVal),
                    diffPrefix(table, id, column) + "固定長文字が一致しません");
            return;
        }

        assertEquals(OracleIntegrationSupport.trimToEmpty(inVal),
                OracleIntegrationSupport.trimToEmpty(outVal),
                diffPrefix(table, id, column) + "値が一致しません");
    }

    private static boolean isBlobColumn(String column) {
        String c = column.toUpperCase();
        return "BLOB_COL".equals(c) || "PAYLOAD_BLOB".equals(c);
    }

    private static boolean isLobTextColumn(String column) {
        String c = column.toUpperCase();
        return "CLOB_COL".equals(c) || "NCLOB_COL".equals(c) || "PAYLOAD_CLOB".equals(c);
    }

    private static byte[] resolveBlobValue(String value, Path filesDir) throws IOException {
        if (value == null || value.isEmpty()) {
            return new byte[0];
        }
        if (value.startsWith("file:")) {
            return Files.readAllBytes(filesDir.resolve(value.substring("file:".length())));
        }
        return OracleIntegrationSupport.decodeHex(value);
    }

    private static String resolveTextValue(String value, Path filesDir) throws IOException {
        if (value == null) {
            return "";
        }
        if (value.startsWith("file:")) {
            return Files.readString(filesDir.resolve(value.substring("file:".length())),
                    StandardCharsets.UTF_8);
        }
        return value;
    }

    private static String diffPrefix(String table, String id, String column) {
        return "Table=" + table + " ID=" + id + " Column=" + column + " ";
    }

    private static String dumpMainRows(Connection conn) {
        StringBuilder sb = new StringBuilder();
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT ID, VC_COL, LOB_KIND FROM IT_TYPED_MAIN ORDER BY ID")) {
            while (rs.next()) {
                sb.append("ID=").append(rs.getLong("ID")).append(" VC_COL=")
                        .append(rs.getString("VC_COL")).append(" LOB_KIND=")
                        .append(rs.getString("LOB_KIND")).append("\n");
            }
        } catch (Exception e) {
            sb.append("Error dumping main rows: ").append(e.getMessage());
        }
        return sb.toString();
    }

    private static String dumpAuxRows(Connection conn) {
        StringBuilder sb = new StringBuilder();
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT ID, "
                        + "CASE WHEN PAYLOAD_CLOB IS NULL THEN 'NULL' ELSE 'NOT_NULL' END AS HAS_CLOB, "
                        + "CASE WHEN PAYLOAD_BLOB IS NULL THEN 'NULL' ELSE 'NOT_NULL' END AS HAS_BLOB "
                        + "FROM IT_TYPED_AUX ORDER BY ID")) {
            while (rs.next()) {
                sb.append("ID=").append(rs.getLong("ID")).append(" PAYLOAD_CLOB=")
                        .append(rs.getString("HAS_CLOB")).append(" PAYLOAD_BLOB=")
                        .append(rs.getString("HAS_BLOB")).append("\n");
            }
        } catch (Exception e) {
            sb.append("Error dumping aux rows: ").append(e.getMessage());
        }
        return sb.toString();
    }

    private static void assertXmlTextEquals(String xml, String xPathExpr, String expected)
            throws Exception {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(false);
        dbf.setExpandEntityReferences(false);
        dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);

        Document doc = dbf.newDocumentBuilder().parse(new InputSource(new StringReader(xml)));

        String actual = (String) XPathFactory.newInstance().newXPath().evaluate(xPathExpr, doc,
                XPathConstants.STRING);

        assertEquals(expected, actual,
                () -> "XML の値が一致しません。\nXPath=" + xPathExpr + "\nxml=\n" + xml);
    }
}
