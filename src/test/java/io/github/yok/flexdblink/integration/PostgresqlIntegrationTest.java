package io.github.yok.flexdblink.integration;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.github.yok.flexdblink.util.ErrorHandler;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
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
@Testcontainers
public class PostgresqlIntegrationTest {

    private static final Set<String> NUMERIC_COLUMNS =
            Set.of("ID", "MAIN_ID", "NUM_COL", "BF_COL", "BD_COL");


    @TempDir
    public Path tempDir;

    @Container
    private static final PostgreSQLContainer postgres = createPostgres();

    private static PostgreSQLContainer createPostgres() {
        PostgreSQLContainer container = new PostgreSQLContainer("postgres:16-alpine");
        container.withDatabaseName("testdb").withUsername("test").withPassword("test");
        return container;
    }

    @BeforeEach
    public void setup_正常ケース_PostgreSQLコンテナに対してFlywayを実行する_マイグレーションが完了すること() {
        PostgresqlIntegrationSupport.prepareDatabase(postgres);
    }

    @Test
    public void execute_正常ケース_PostgreSQL型をロードする_全列値が登録されること() throws Exception {
        Path dataPath = tempDir.resolve("pg_load_data");
        PostgresqlIntegrationSupport.Runtime runtime =
                PostgresqlIntegrationSupport.prepareRuntime(postgres, dataPath, true);

        PostgresqlIntegrationSupport.executeLoad(runtime, "pre");

        try (Connection conn = PostgresqlIntegrationSupport.openConnection(postgres);
                Statement st = conn.createStatement()) {

            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM IT_TYPED_MAIN")) {
                assertTrue(rs.next(), "COUNT(*) の結果が取得できません: IT_TYPED_MAIN");
                assertEquals(6, rs.getInt(1));
            }
            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM IT_TYPED_AUX")) {
                assertTrue(rs.next(), "COUNT(*) の結果が取得できません: IT_TYPED_AUX");
                assertEquals(2, rs.getInt(1));
            }

            try (ResultSet rs = st.executeQuery(
                    "SELECT ID, VC_COL, CHAR_COL, NCHAR_COL, NUM_COL, BF_COL, BD_COL, "
                            + "DATE_COL, TIME_COL, TS_COL, TSTZ_COL, RAW_COL, XML_COL, CLOB_COL, NCLOB_COL, BLOB_COL, LOB_KIND "
                            + "FROM IT_TYPED_MAIN WHERE ID=101")) {

                assertTrue(rs.next(), "IT_TYPED_MAIN に ID=101 が存在しません。");
                assertEquals("load-xml", rs.getString("VC_COL"));
                assertEquals("na-xml", rs.getString("CHAR_COL").trim());
                assertEquals("na-nchar", rs.getString("NCHAR_COL").trim());
                assertEquals(0, new BigDecimal(rs.getString("NUM_COL"))
                        .compareTo(new BigDecimal("123.456")));
                assertEquals(0, BigDecimal.valueOf(rs.getFloat("BF_COL"))
                        .compareTo(new BigDecimal("1.25")));
                assertEquals(0, BigDecimal.valueOf(rs.getDouble("BD_COL"))
                        .compareTo(new BigDecimal("10.125")));

                byte[] rawCol = rs.getBytes("RAW_COL");
                assertTrue(Arrays.equals(new byte[] {0x0A, 0x0B, 0x0C, 0x21}, rawCol),
                        "RAW_COL が期待値と一致しません");

                assertTrue(rs.getString("XML_COL").contains("<root>"));
                assertTrue(rs.getString("CLOB_COL").contains("<root>"));
                assertEquals("nclob-load-xml", rs.getString("NCLOB_COL"));

                // bytea は file: 参照で投入しているため、実ファイルと一致することを確認
                byte[] actualBlob = rs.getBytes("BLOB_COL");
                byte[] expectedBlob =
                        Files.readAllBytes(dataPath.resolve("load/pre/db1/files/main_xml_1.xml"));
                assertArrayEquals(expectedBlob, actualBlob);

                assertEquals("xml", rs.getString("LOB_KIND"));
                assertFalse(rs.next(), "ID=101 が複数行です");
            }
        }
    }

    @Test
    public void execute_異常ケース_日時形式が不正である_ロールバックされること() throws Exception {
        Path dataPath = tempDir.resolve("pg_load_error_data");
        PostgresqlIntegrationSupport.Runtime runtime =
                PostgresqlIntegrationSupport.prepareRuntime(postgres, dataPath, true);

        // TSTZ_COL を不正値にする（パース失敗→例外→ロールバック想定）
        Path mainCsv = dataPath.resolve("load/pre/db1/IT_TYPED_MAIN.csv");
        String csv = Files.readString(mainCsv, StandardCharsets.UTF_8);
        csv = csv.replaceFirst(",2026-02-10 01:02:03,0A0B0C21,", ",bad-timestamptz,0A0B0C21,");
        Files.writeString(mainCsv, csv, StandardCharsets.UTF_8);

        ErrorHandler.disableExitForCurrentThread();
        try {
            assertThrows(IllegalStateException.class,
                    () -> PostgresqlIntegrationSupport.executeLoad(runtime, "pre"));
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }

        // 失敗後：件数が 0 である（ロード全体がロールバックされる想定）
        try (Connection conn = PostgresqlIntegrationSupport.openConnection(postgres);
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM IT_TYPED_MAIN")) {
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
    }

    @Test
    public void execute_正常ケース_PostgreSQL型をダンプする_CSVとLOBファイルが出力されること() throws Exception {
        Path dataPath = tempDir.resolve("pg_dump_data");
        PostgresqlIntegrationSupport.Runtime runtime =
                PostgresqlIntegrationSupport.prepareRuntime(postgres, dataPath, false);

        Path dbDir = PostgresqlIntegrationSupport.executeDump(runtime, "pg_dump_case");
        Path filesDir = dbDir.resolve("files");

        Path mainCsv = resolveFileIgnoreCase(dbDir, "IT_TYPED_MAIN.csv");
        Path auxCsv = resolveFileIgnoreCase(dbDir, "IT_TYPED_AUX.csv");

        assertTrue(Files.exists(mainCsv));
        assertTrue(Files.exists(auxCsv));

        String csvText = Files.readString(mainCsv, StandardCharsets.UTF_8);
        assertTrue(csvText.contains("file:"), "LOB列が file: 参照として出力されていません。");

        // 出力された BLOB ファイルのうち、少なくとも1つはDBの bytea と一致すること
        try (Connection conn = PostgresqlIntegrationSupport.openConnection(postgres);
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT ID, BLOB_COL FROM IT_TYPED_MAIN WHERE BLOB_COL IS NOT NULL ORDER BY ID LIMIT 1")) {
            assertTrue(rs.next(), "seed データに BLOB_COL が存在しません。");
            long id = rs.getLong("ID");
            byte[] expected = rs.getBytes("BLOB_COL");

            Map<String, String> row = readCsvRowById(mainCsv, "ID", String.valueOf(id));
            String ref = row.get("BLOB_COL");
            assertTrue(ref.startsWith("file:"), "BLOB_COL が file: 参照ではありません: " + ref);

            byte[] actual = Files.readAllBytes(filesDir.resolve(ref.substring("file:".length())));
            assertArrayEquals(expected, actual, "ダンプされた BLOB ファイル内容がDBと一致しません。");
        }
    }

    @Test
    public void execute_正常ケース_ロード後にダンプする_件数と全列値が一致すること() throws Exception {
        Path dataPath = tempDir.resolve("pg_roundtrip_data");
        PostgresqlIntegrationSupport.Runtime runtime =
                PostgresqlIntegrationSupport.prepareRuntime(postgres, dataPath, true);

        PostgresqlIntegrationSupport.executeLoad(runtime, "pre");
        Path outputDbDir = PostgresqlIntegrationSupport.executeDump(runtime, "pg_roundtrip_case");

        Path inputMainCsv = dataPath.resolve("load/pre/db1/IT_TYPED_MAIN.csv");
        Path inputAuxCsv = dataPath.resolve("load/pre/db1/IT_TYPED_AUX.csv");
        Path inputFilesDir = dataPath.resolve("load/pre/db1/files");

        Path outputMainCsv = resolveFileIgnoreCase(outputDbDir, "IT_TYPED_MAIN.csv");
        Path outputAuxCsv = resolveFileIgnoreCase(outputDbDir, "IT_TYPED_AUX.csv");
        Path outputFilesDir = outputDbDir.resolve("files");

        assertTableEquals("IT_TYPED_MAIN", "ID", inputMainCsv, outputMainCsv, inputFilesDir,
                outputFilesDir);
        assertTableEquals("IT_TYPED_AUX", "ID", inputAuxCsv, outputAuxCsv, inputFilesDir,
                outputFilesDir);
    }

    @Test
    public void execute_正常ケース_FK制約あり_tableOrderingが毎回再生成されてロードが成功すること() throws Exception {
        Path dataPath = tempDir.resolve("fk_load_no_ordering");
        PostgresqlIntegrationSupport.Runtime runtime =
                PostgresqlIntegrationSupport.prepareRuntime(postgres, dataPath, true);

        // table-ordering.txt is always regenerated at the start and deleted at the end;
        // the FK resolver corrects the alphabetical order (AUX before MAIN) to load order
        PostgresqlIntegrationSupport.executeLoad(runtime, "pre");

        try (Connection conn = PostgresqlIntegrationSupport.openConnection(postgres);
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
    public void execute_正常ケース_FK制約あり_FK解決でアルファベット順が修正されてロードが成功すること() throws Exception {
        Path dataPath = tempDir.resolve("fk_load_reversed_ordering");
        PostgresqlIntegrationSupport.Runtime runtime =
                PostgresqlIntegrationSupport.prepareRuntime(postgres, dataPath, true);

        // table-ordering.txt is always regenerated alphabetically (AUX before MAIN);
        // FK resolver must correct the order so the load completes without FK violation
        PostgresqlIntegrationSupport.executeLoad(runtime, "pre");

        try (Connection conn = PostgresqlIntegrationSupport.openConnection(postgres);
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
    public void execute_正常ケース_FK制約あり_ダンプが完了してCSVと件数が出力されること() throws Exception {
        Path dataPath = tempDir.resolve("fk_dump_data");
        PostgresqlIntegrationSupport.Runtime runtime =
                PostgresqlIntegrationSupport.prepareRuntime(postgres, dataPath, false);

        Path dbDir = PostgresqlIntegrationSupport.executeDump(runtime, "fk_dump_case");

        Path mainCsv = resolveFileIgnoreCase(dbDir, "IT_TYPED_MAIN.csv");
        Path auxCsv = resolveFileIgnoreCase(dbDir, "IT_TYPED_AUX.csv");
        assertTrue(Files.exists(mainCsv));
        assertTrue(Files.exists(auxCsv));

        // Row counts in the dumped CSVs must match what is in the DB (seed data)
        try (Connection conn = PostgresqlIntegrationSupport.openConnection(postgres);
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
            long mainCsvRows = Files.lines(mainCsv).count() - 1; // minus header
            long auxCsvRows = Files.lines(auxCsv).count() - 1;
            assertEquals(expectedMain, mainCsvRows, "IT_TYPED_MAIN CSV row count mismatch");
            assertEquals(expectedAux, auxCsvRows, "IT_TYPED_AUX CSV row count mismatch");
        }
    }

    @Test
    public void execute_正常ケース_FK制約あり_ロード後にダンプする_件数が一致すること() throws Exception {
        Path dataPath = tempDir.resolve("fk_roundtrip_data");
        PostgresqlIntegrationSupport.Runtime runtime =
                PostgresqlIntegrationSupport.prepareRuntime(postgres, dataPath, true);

        PostgresqlIntegrationSupport.executeLoad(runtime, "pre");
        Path dbDir = PostgresqlIntegrationSupport.executeDump(runtime, "fk_roundtrip_case");

        Path mainCsv = resolveFileIgnoreCase(dbDir, "IT_TYPED_MAIN.csv");
        Path auxCsv = resolveFileIgnoreCase(dbDir, "IT_TYPED_AUX.csv");
        assertTrue(Files.exists(mainCsv));
        assertTrue(Files.exists(auxCsv));

        long mainCsvRows = Files.lines(mainCsv).count() - 1;
        long auxCsvRows = Files.lines(auxCsv).count() - 1;
        assertEquals(6L, mainCsvRows, "IT_TYPED_MAIN should have 6 rows after load");
        assertEquals(2L, auxCsvRows, "IT_TYPED_AUX should have 2 rows after load");
    }

    @Test
    public void execute_正常ケース_FK制約なし_tableOrderingが毎回再生成されてロードが成功すること() throws Exception {
        PostgresqlIntegrationSupport.prepareDatabaseWithoutFk(postgres);
        Path dataPath = tempDir.resolve("nofk_load_no_ordering");
        PostgresqlIntegrationSupport.Runtime runtime =
                PostgresqlIntegrationSupport.prepareRuntime(postgres, dataPath, true);

        // table-ordering.txt is always regenerated alphabetically; without FK constraints
        // any load order is acceptable, so load completes successfully
        PostgresqlIntegrationSupport.executeLoad(runtime, "pre");

        try (Connection conn = PostgresqlIntegrationSupport.openConnection(postgres);
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
        PostgresqlIntegrationSupport.prepareDatabaseWithoutFk(postgres);
        Path dataPath = tempDir.resolve("nofk_load_reversed_ordering");
        PostgresqlIntegrationSupport.Runtime runtime =
                PostgresqlIntegrationSupport.prepareRuntime(postgres, dataPath, true);

        // table-ordering.txt is always regenerated alphabetically (AUX before MAIN);
        // without FK constraints, this order is acceptable for CLEAN_INSERT
        PostgresqlIntegrationSupport.executeLoad(runtime, "pre");

        try (Connection conn = PostgresqlIntegrationSupport.openConnection(postgres);
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
        PostgresqlIntegrationSupport.prepareDatabaseWithoutFk(postgres);
        Path dataPath = tempDir.resolve("nofk_dump_data");
        PostgresqlIntegrationSupport.Runtime runtime =
                PostgresqlIntegrationSupport.prepareRuntime(postgres, dataPath, false);

        Path dbDir = PostgresqlIntegrationSupport.executeDump(runtime, "nofk_dump_case");

        Path mainCsv = resolveFileIgnoreCase(dbDir, "IT_TYPED_MAIN.csv");
        Path auxCsv = resolveFileIgnoreCase(dbDir, "IT_TYPED_AUX.csv");
        assertTrue(Files.exists(mainCsv));
        assertTrue(Files.exists(auxCsv));

        try (Connection conn = PostgresqlIntegrationSupport.openConnection(postgres);
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
            long mainCsvRows = Files.lines(mainCsv).count() - 1; // minus header
            long auxCsvRows = Files.lines(auxCsv).count() - 1;
            assertEquals(expectedMain, mainCsvRows, "IT_TYPED_MAIN CSV row count mismatch");
            assertEquals(expectedAux, auxCsvRows, "IT_TYPED_AUX CSV row count mismatch");
        }
    }

    @Test
    public void execute_正常ケース_FK制約なし_ロード後にダンプする_件数が一致すること() throws Exception {
        PostgresqlIntegrationSupport.prepareDatabaseWithoutFk(postgres);
        Path dataPath = tempDir.resolve("nofk_roundtrip_data");
        PostgresqlIntegrationSupport.Runtime runtime =
                PostgresqlIntegrationSupport.prepareRuntime(postgres, dataPath, true);

        PostgresqlIntegrationSupport.executeLoad(runtime, "pre");
        Path dbDir = PostgresqlIntegrationSupport.executeDump(runtime, "nofk_roundtrip_case");

        Path mainCsv = resolveFileIgnoreCase(dbDir, "IT_TYPED_MAIN.csv");
        Path auxCsv = resolveFileIgnoreCase(dbDir, "IT_TYPED_AUX.csv");
        assertTrue(Files.exists(mainCsv));
        assertTrue(Files.exists(auxCsv));

        long mainCsvRows = Files.lines(mainCsv).count() - 1;
        long auxCsvRows = Files.lines(auxCsv).count() - 1;
        assertEquals(6L, mainCsvRows, "IT_TYPED_MAIN should have 6 rows after load");
        assertEquals(2L, auxCsvRows, "IT_TYPED_AUX should have 2 rows after load");
    }

    private static void assertTableEquals(String table, String idColumn, Path inputCsv,
            Path outputCsv, Path inputFilesDir, Path outputFilesDir) throws Exception {

        Map<String, Map<String, String>> inputRows = readCsvById(inputCsv, idColumn);
        Map<String, Map<String, String>> outputRows = readCsvById(outputCsv, idColumn);

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

        String inVal = trimToNull(inputValue);
        String outVal = trimToNull(outputValue);

        if (inVal == null && outVal == null) {
            return;
        }

        if (isByteaColumn(column)) {
            byte[] inBlob = resolveBlobValue(inVal, inputFilesDir);
            byte[] outBlob = resolveBlobValue(outVal, outputFilesDir);
            assertArrayEquals(inBlob, outBlob, diffPrefix(table, id, column) + "bytea値が一致しません");
            return;
        }

        if (isTextColumn(column)) {
            String inText = resolveTextValue(inVal, inputFilesDir);
            String outText = resolveTextValue(outVal, outputFilesDir);
            assertEquals(inText, outText, diffPrefix(table, id, column) + "TEXT値が一致しません");
            return;
        }

        if ("RAW_COL".equalsIgnoreCase(column)) {
            byte[] inRaw = decodeHex(trimToEmpty(inVal));
            byte[] outRaw = decodeHex(trimToEmpty(outVal));
            assertArrayEquals(inRaw, outRaw, diffPrefix(table, id, column) + "RAW値が一致しません");
            return;
        }

        if (NUMERIC_COLUMNS.contains(column.toUpperCase())) {
            BigDecimal inNum = new BigDecimal(trimToEmpty(inVal));
            BigDecimal outNum = new BigDecimal(trimToEmpty(outVal));
            assertEquals(0, inNum.compareTo(outNum), diffPrefix(table, id, column) + "数値が一致しません");
            return;
        }

        String normalizedInput = trimToEmpty(inVal);
        String normalizedOutput = trimToEmpty(outVal);
        assertEquals(normalizedInput, normalizedOutput, diffPrefix(table, id, column) + "値が一致しません");
    }

    private static boolean isByteaColumn(String column) {
        String c = column.toUpperCase();
        return "BLOB_COL".equals(c) || "PAYLOAD_BLOB".equals(c);
    }

    private static boolean isTextColumn(String column) {
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
        return decodeHex(value);
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

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static String trimToEmpty(String value) {
        return value == null ? "" : value.trim();
    }

    private static byte[] decodeHex(String value) {
        if (value == null || value.isEmpty()) {
            return new byte[0];
        }
        int len = value.length();
        if ((len % 2) != 0) {
            throw new IllegalArgumentException("Invalid hex length: " + value);
        }
        byte[] out = new byte[len / 2];
        for (int i = 0; i < out.length; i++) {
            int hi = Character.digit(value.charAt(i * 2), 16);
            int lo = Character.digit(value.charAt(i * 2 + 1), 16);
            if (hi < 0 || lo < 0) {
                throw new IllegalArgumentException("Invalid hex: " + value);
            }
            out[i] = (byte) ((hi << 4) + lo);
        }
        return out;
    }

    private static Map<String, String> readCsvRowById(Path csvPath, String idColumn, String id)
            throws IOException {
        Map<String, Map<String, String>> rows = readCsvById(csvPath, idColumn);
        Map<String, String> row = rows.get(id);
        if (row == null) {
            throw new IllegalStateException("Row not found: ID=" + id + " in " + csvPath);
        }
        return row;
    }

    private static Map<String, Map<String, String>> readCsvById(Path csvPath, String idColumn)
            throws IOException {
        Map<String, Map<String, String>> rows = new LinkedHashMap<>();
        String normalizedIdColumn = idColumn.toUpperCase(Locale.ROOT);
        CSVFormat format = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true)
                .setIgnoreSurroundingSpaces(false).setTrim(false).get();

        try (CSVParser parser = CSVParser.parse(csvPath, StandardCharsets.UTF_8, format)) {
            for (CSVRecord record : parser) {
                Map<String, String> row = new LinkedHashMap<>();
                for (String header : parser.getHeaderMap().keySet()) {
                    row.put(header.toUpperCase(Locale.ROOT), record.get(header));
                }
                rows.put(row.get(normalizedIdColumn), row);
            }
        }
        return rows;
    }

    private static Path resolveFileIgnoreCase(Path dir, String fileName) throws IOException {
        Path direct = dir.resolve(fileName);
        if (Files.exists(direct)) {
            return direct;
        }

        try (Stream<Path> stream = Files.list(dir)) {
            return stream.filter(p -> p.getFileName().toString().equalsIgnoreCase(fileName))
                    .findFirst().orElseThrow(
                            () -> new IOException("File not found (case-insensitive): expected="
                                    + fileName + " dir=" + dir.toAbsolutePath()));
        }
    }
}
