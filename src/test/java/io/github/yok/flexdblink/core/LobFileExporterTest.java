package io.github.yok.flexdblink.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.github.yok.flexdblink.config.FilePatternConfig;
import io.github.yok.flexdblink.db.DbDialectHandler;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LobFileExporterTest {

    @TempDir
    Path tempDir;

    // -------------------------------------------------------------------------
    // applyPlaceholders
    // -------------------------------------------------------------------------

    @Test
    void applyPlaceholders_正常ケース_プレースホルダと値Mapを指定する_置換済み文字列が返ること() {
        LobFileExporter exporter = new LobFileExporter(new FilePatternConfig());
        Map<String, Object> keyMap = new LinkedHashMap<>();
        keyMap.put("ID", 7);
        keyMap.put("SEQ", "A");
        assertEquals("lob_7_A.bin", exporter.applyPlaceholders("lob_{ID}_{SEQ}.bin", keyMap));
    }

    @Test
    void applyPlaceholders_正常ケース_未使用キーを含むMapを指定する_非該当キーは無視されること() {
        LobFileExporter exporter = new LobFileExporter(new FilePatternConfig());
        Map<String, Object> keyMap = new LinkedHashMap<>();
        keyMap.put("ID", 9);
        keyMap.put("UNUSED", "X");
        assertEquals("lob_9.bin", exporter.applyPlaceholders("lob_{ID}.bin", keyMap));
    }

    // -------------------------------------------------------------------------
    // export
    // -------------------------------------------------------------------------

    @Test
    void export_正常ケース_型別null処理とクォートSQLを適用すること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        DbDialectHandler dialectHandler = createDialectHandlerMock();
        when(dialectHandler.quoteIdentifier(any()))
                .thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");

        when(filePatternConfig.getPatternsForTable("1TABLE"))
                .thenReturn(Collections.singletonMap("BLOB_COL", "blob_{ID}.bin"));
        when(filePatternConfig.getPattern("1TABLE", "BLOB_COL"))
                .thenReturn(Optional.of("blob_{ID}.bin"));

        Connection conn = mock(Connection.class);
        when(conn.getCatalog()).thenReturn(null);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet pkRs = mock(ResultSet.class);
        when(meta.getPrimaryKeys(any(), eq("APP"), eq("1TABLE"))).thenReturn(pkRs);
        when(pkRs.next()).thenReturn(false);

        Statement stmt = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmt);
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery("SELECT * FROM \"1TABLE\"")).thenReturn(rs);
        ResultSetMetaData md = mock(ResultSetMetaData.class);
        when(rs.getMetaData()).thenReturn(md);
        when(md.getColumnCount()).thenReturn(3);
        when(md.getColumnLabel(1)).thenReturn("DATE_COL");
        when(md.getColumnLabel(2)).thenReturn("BLOB_COL");
        when(md.getColumnLabel(3)).thenReturn("BIN_COL");
        when(md.getColumnType(1)).thenReturn(Types.DATE);
        when(md.getColumnType(2)).thenReturn(Types.BLOB);
        when(md.getColumnType(3)).thenReturn(Types.BINARY);
        when(rs.next()).thenReturn(true, false);
        when(rs.getObject(1)).thenReturn(null);
        when(rs.getObject(2)).thenReturn(null);
        when(rs.getObject(3)).thenReturn(null);
        when(rs.getBytes(3)).thenReturn(null);

        Path dbDirPath = Files.createDirectories(tempDir.resolve("db"));
        Path filesDirPath = Files.createDirectories(dbDirPath.resolve("files"));
        Path csvPath = dbDirPath.resolve("1TABLE.csv");
        Files.writeString(csvPath, "DATE_COL,BLOB_COL,BIN_COL\nx,y,z\n", StandardCharsets.UTF_8);

        DumpResult result = new LobFileExporter(filePatternConfig).export(conn, "1TABLE",
                dbDirPath.toFile(), filesDirPath.toFile(), "APP", dialectHandler);

        assertEquals(1, result.getRowCount());
        assertEquals(0, result.getFileCount());
        verify(dialectHandler, never()).writeLobFile(any(), any(), any(), any());

        CSVFormat fmt = CSVFormat.DEFAULT.builder().setHeader("DATE_COL", "BLOB_COL", "BIN_COL")
                .setSkipHeaderRecord(true).get();
        try (CSVParser parser = CSVParser.parse(csvPath.toFile(), StandardCharsets.UTF_8, fmt)) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals(1, records.size());
            assertEquals("", records.get(0).get("DATE_COL"));
            assertEquals("", records.get(0).get("BLOB_COL"));
            assertEquals("", records.get(0).get("BIN_COL"));
        }
    }

    @Test
    void export_正常ケース_interval列を含む_日時整形値が設定されること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        DbDialectHandler dialectHandler = createDialectHandlerMock();
        when(dialectHandler.quoteIdentifier(any()))
                .thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");
        when(filePatternConfig.getPatternsForTable("TINTERVAL")).thenReturn(Collections.emptyMap());
        when(dialectHandler.shouldUseRawTemporalValueForDump("INTERVAL_DS_COL", Types.VARCHAR,
                "INTERVAL DAY TO SECOND")).thenReturn(true);
        when(dialectHandler.normalizeRawTemporalValueForDump("INTERVAL_DS_COL", "1 00:00:00.0"))
                .thenReturn("1 00:00:00");

        Connection conn = mock(Connection.class);
        when(conn.getCatalog()).thenReturn(null);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet pkRs = mock(ResultSet.class);
        when(meta.getPrimaryKeys(any(), eq("APP"), eq("TINTERVAL"))).thenReturn(pkRs);
        when(pkRs.next()).thenReturn(false);
        Statement stmt = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmt);
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery("SELECT * FROM \"TINTERVAL\"")).thenReturn(rs);
        ResultSetMetaData md = mock(ResultSetMetaData.class);
        when(rs.getMetaData()).thenReturn(md);
        when(md.getColumnCount()).thenReturn(1);
        when(md.getColumnLabel(1)).thenReturn("INTERVAL_DS_COL");
        when(md.getColumnType(1)).thenReturn(Types.VARCHAR);
        when(md.getColumnTypeName(1)).thenReturn("INTERVAL DAY TO SECOND");
        when(rs.next()).thenReturn(true, false);
        when(rs.getObject(1)).thenReturn("1 00:00:00.0");
        when(rs.getString(1)).thenReturn("1 00:00:00.0");

        Path dbDirPath = Files.createDirectories(tempDir.resolve("db_interval"));
        Path filesDirPath = Files.createDirectories(dbDirPath.resolve("files"));
        Path csvPath = dbDirPath.resolve("TINTERVAL.csv");
        Files.writeString(csvPath, "INTERVAL_DS_COL\nx\n", StandardCharsets.UTF_8);

        DumpResult result = new LobFileExporter(filePatternConfig).export(conn, "TINTERVAL",
                dbDirPath.toFile(), filesDirPath.toFile(), "APP", dialectHandler);
        assertEquals(1, result.getRowCount());

        CSVFormat fmt = CSVFormat.DEFAULT.builder().setHeader("INTERVAL_DS_COL")
                .setSkipHeaderRecord(true).get();
        try (CSVParser parser = CSVParser.parse(csvPath.toFile(), StandardCharsets.UTF_8, fmt)) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals("1 00:00:00", records.get(0).get("INTERVAL_DS_COL"));
        }
    }

    @Test
    void export_正常ケース_timestampwithtimezone列を含む_方言整形値が設定されること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        DbDialectHandler dialectHandler = createDialectHandlerMock();
        when(dialectHandler.quoteIdentifier(any()))
                .thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");
        when(filePatternConfig.getPatternsForTable("TTSTZ")).thenReturn(Collections.emptyMap());

        Connection conn = mock(Connection.class);
        when(dialectHandler.formatDateTimeColumn("TS_TZ", "2026-02-15T01:02:03+09:00", conn))
                .thenReturn("2026-02-15 01:02:03+09:00");
        when(conn.getCatalog()).thenReturn(null);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet pkRs = mock(ResultSet.class);
        when(meta.getPrimaryKeys(any(), eq("APP"), eq("TTSTZ"))).thenReturn(pkRs);
        when(pkRs.next()).thenReturn(false);
        Statement stmt = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmt);
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery("SELECT * FROM \"TTSTZ\"")).thenReturn(rs);
        ResultSetMetaData md = mock(ResultSetMetaData.class);
        when(rs.getMetaData()).thenReturn(md);
        when(md.getColumnCount()).thenReturn(1);
        when(md.getColumnLabel(1)).thenReturn("TS_TZ");
        when(md.getColumnType(1)).thenReturn(-101);
        when(rs.next()).thenReturn(true, false);
        when(rs.getObject(1)).thenReturn("2026-02-15T01:02:03+09:00");

        Path dbDirPath = Files.createDirectories(tempDir.resolve("db_tstz"));
        Path filesDirPath = Files.createDirectories(dbDirPath.resolve("files"));
        Path csvPath = dbDirPath.resolve("TTSTZ.csv");
        Files.writeString(csvPath, "TS_TZ\nx\n", StandardCharsets.UTF_8);

        DumpResult result = new LobFileExporter(filePatternConfig).export(conn, "TTSTZ",
                dbDirPath.toFile(), filesDirPath.toFile(), "APP", dialectHandler);
        assertEquals(1, result.getRowCount());

        CSVFormat fmt =
                CSVFormat.DEFAULT.builder().setHeader("TS_TZ").setSkipHeaderRecord(true).get();
        try (CSVParser parser = CSVParser.parse(csvPath.toFile(), StandardCharsets.UTF_8, fmt)) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals("2026-02-15 01:02:03+09:00", records.get(0).get("TS_TZ"));
        }
    }

    @Test
    void export_正常ケース_time列で型取得がnullを返す_生値が日時整形へ渡されること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        DbDialectHandler dialectHandler = createDialectHandlerMock();
        when(dialectHandler.quoteIdentifier(any()))
                .thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");
        when(filePatternConfig.getPatternsForTable("TTIME")).thenReturn(Collections.emptyMap());

        Connection conn = mock(Connection.class);
        when(conn.getCatalog()).thenReturn(null);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet pkRs = mock(ResultSet.class);
        when(meta.getPrimaryKeys(any(), eq("APP"), eq("TTIME"))).thenReturn(pkRs);
        when(pkRs.next()).thenReturn(false);
        Statement stmt = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmt);
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery("SELECT * FROM \"TTIME\"")).thenReturn(rs);
        ResultSetMetaData md = mock(ResultSetMetaData.class);
        when(rs.getMetaData()).thenReturn(md);
        when(md.getColumnCount()).thenReturn(1);
        when(md.getColumnLabel(1)).thenReturn("TIME_COL");
        when(md.getColumnType(1)).thenReturn(Types.TIME);
        when(md.getColumnTypeName(1)).thenReturn("TIME");
        when(rs.next()).thenReturn(true, false);

        Object rawTime = new Object();
        when(rs.getObject(1)).thenReturn(rawTime);
        when(rs.getTime(1)).thenReturn(null);
        when(dialectHandler.formatDateTimeColumn("TIME_COL", rawTime, conn)).thenReturn("12:34:56");

        Path dbDirPath = Files.createDirectories(tempDir.resolve("db_time"));
        Path filesDirPath = Files.createDirectories(dbDirPath.resolve("files"));
        Path csvPath = dbDirPath.resolve("TTIME.csv");
        Files.writeString(csvPath, "TIME_COL\nx\n", StandardCharsets.UTF_8);

        DumpResult result = new LobFileExporter(filePatternConfig).export(conn, "TTIME",
                dbDirPath.toFile(), filesDirPath.toFile(), "APP", dialectHandler);
        assertEquals(1, result.getRowCount());

        CSVFormat fmt =
                CSVFormat.DEFAULT.builder().setHeader("TIME_COL").setSkipHeaderRecord(true).get();
        try (CSVParser parser = CSVParser.parse(csvPath.toFile(), StandardCharsets.UTF_8, fmt)) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals("12:34:56", records.get(0).get("TIME_COL"));
        }
    }

    @Test
    void export_正常ケース_nclob列を含む_file参照が設定されること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        DbDialectHandler dialectHandler = createDialectHandlerMock();
        when(dialectHandler.quoteIdentifier(any()))
                .thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");
        when(filePatternConfig.getPatternsForTable("TNClob"))
                .thenReturn(Collections.singletonMap("NCLOB_COL", "nclob_{ID}.txt"));
        when(filePatternConfig.getPattern("TNClob", "NCLOB_COL"))
                .thenReturn(Optional.of("nclob_{ID}.txt"));

        Connection conn = mock(Connection.class);
        when(conn.getCatalog()).thenReturn(null);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet pkRs = mock(ResultSet.class);
        when(meta.getPrimaryKeys(any(), eq("APP"), eq("TNClob"))).thenReturn(pkRs);
        when(pkRs.next()).thenReturn(true, false);
        when(pkRs.getString("COLUMN_NAME")).thenReturn("ID");

        Statement stmt = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmt);
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery("SELECT * FROM \"TNClob\"")).thenReturn(rs);
        ResultSetMetaData md = mock(ResultSetMetaData.class);
        when(rs.getMetaData()).thenReturn(md);
        when(md.getColumnCount()).thenReturn(2);
        when(md.getColumnLabel(1)).thenReturn("ID");
        when(md.getColumnLabel(2)).thenReturn("NCLOB_COL");
        when(md.getColumnType(1)).thenReturn(Types.INTEGER);
        when(md.getColumnType(2)).thenReturn(Types.NCLOB);
        when(rs.next()).thenReturn(true, false);
        when(rs.getObject(1)).thenReturn(10);
        when(rs.getObject("ID")).thenReturn(10);
        when(rs.getObject(2)).thenReturn("nclob-body");

        Path dbDirPath = Files.createDirectories(tempDir.resolve("db_nclob"));
        Path filesDirPath = Files.createDirectories(dbDirPath.resolve("files"));
        Path csvPath = dbDirPath.resolve("TNClob.csv");
        Files.writeString(csvPath, "ID,NCLOB_COL\n10,x\n", StandardCharsets.UTF_8);

        DumpResult result = new LobFileExporter(filePatternConfig).export(conn, "TNClob",
                dbDirPath.toFile(), filesDirPath.toFile(), "APP", dialectHandler);

        assertEquals(1, result.getRowCount());
        assertEquals(1, result.getFileCount());

        CSVFormat fmt = CSVFormat.DEFAULT.builder().setHeader("ID", "NCLOB_COL")
                .setSkipHeaderRecord(true).get();
        try (CSVParser parser = CSVParser.parse(csvPath.toFile(), StandardCharsets.UTF_8, fmt)) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals("file:nclob_10.txt", records.get(0).get("NCLOB_COL"));
        }
    }

    @Test
    void export_正常ケース_ソートで数値変換失敗かつ同値である_比較結果0で完了すること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        DbDialectHandler dialectHandler = createDialectHandlerMock();
        when(dialectHandler.quoteIdentifier(any()))
                .thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");
        when(filePatternConfig.getPatternsForTable("TSORT_EQ")).thenReturn(Collections.emptyMap());

        Connection conn = mock(Connection.class);
        when(conn.getCatalog()).thenReturn(null);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet pkRs = mock(ResultSet.class);
        when(meta.getPrimaryKeys(any(), eq("APP"), eq("TSORT_EQ"))).thenReturn(pkRs);
        when(pkRs.next()).thenReturn(false);

        Statement stmt = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmt);
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery("SELECT * FROM \"TSORT_EQ\"")).thenReturn(rs);
        ResultSetMetaData md = mock(ResultSetMetaData.class);
        when(rs.getMetaData()).thenReturn(md);
        when(md.getColumnCount()).thenReturn(1);
        when(md.getColumnLabel(1)).thenReturn("CODE");
        when(md.getColumnType(1)).thenReturn(Types.VARCHAR);
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getString(1)).thenReturn("A", "A");
        when(rs.getObject(1)).thenReturn("A", "A");

        Path dbDirPath = Files.createDirectories(tempDir.resolve("db_sort_eq"));
        Path filesDirPath = Files.createDirectories(dbDirPath.resolve("files"));
        Path csvPath = dbDirPath.resolve("TSORT_EQ.csv");
        Files.writeString(csvPath, "CODE\nA\nA\n", StandardCharsets.UTF_8);

        DumpResult result = new LobFileExporter(filePatternConfig).export(conn, "TSORT_EQ",
                dbDirPath.toFile(), filesDirPath.toFile(), "APP", dialectHandler);
        assertEquals(2, result.getRowCount());

        CSVFormat fmt =
                CSVFormat.DEFAULT.builder().setHeader("CODE").setSkipHeaderRecord(true).get();
        try (CSVParser parser = CSVParser.parse(csvPath.toFile(), StandardCharsets.UTF_8, fmt)) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals("A", records.get(0).get("CODE"));
            assertEquals("A", records.get(1).get("CODE"));
        }
    }

    @Test
    void export_正常ケース_型分岐を複合指定する_各列が期待形式へ変換されること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        DbDialectHandler dialectHandler = createDialectHandlerMock();
        when(dialectHandler.quoteIdentifier(any()))
                .thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");
        when(filePatternConfig.getPatternsForTable("TMULTI"))
                .thenReturn(Collections.singletonMap("CLOB_COL", "clob_{ID}.txt"));
        when(filePatternConfig.getPattern("TMULTI", "CLOB_COL"))
                .thenReturn(Optional.of("clob_{ID}.txt"));

        Connection conn = mock(Connection.class);
        when(conn.getCatalog()).thenReturn(null);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet pkRs = mock(ResultSet.class);
        when(meta.getPrimaryKeys(any(), eq("APP"), eq("TMULTI"))).thenReturn(pkRs);
        when(pkRs.next()).thenReturn(false);

        Statement stmt = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmt);
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery("SELECT * FROM \"TMULTI\"")).thenReturn(rs);
        ResultSetMetaData md = mock(ResultSetMetaData.class);
        when(rs.getMetaData()).thenReturn(md);
        when(md.getColumnCount()).thenReturn(6);
        when(md.getColumnLabel(1)).thenReturn("ID");
        when(md.getColumnLabel(2)).thenReturn("TZ_COL");
        when(md.getColumnLabel(3)).thenReturn("TZ_COL2");
        when(md.getColumnLabel(4)).thenReturn("CLOB_COL");
        when(md.getColumnLabel(5)).thenReturn("LBIN_COL");
        when(md.getColumnLabel(6)).thenReturn("STR_COL");
        when(md.getColumnType(1)).thenReturn(Types.INTEGER);
        when(md.getColumnType(2)).thenReturn(-101);
        when(md.getColumnType(3)).thenReturn(-102);
        when(md.getColumnType(4)).thenReturn(Types.CLOB);
        when(md.getColumnType(5)).thenReturn(Types.LONGVARBINARY);
        when(md.getColumnType(6)).thenReturn(Types.VARCHAR);
        Object tz1 = new Object();
        Object tz2 = new Object();
        Object clobRaw = new Object();
        when(rs.next()).thenReturn(true, false);
        when(rs.getObject(1)).thenReturn(1);
        when(rs.getObject(2)).thenReturn(tz1);
        when(rs.getObject(3)).thenReturn(tz2);
        when(rs.getObject(4)).thenReturn(clobRaw);
        when(rs.getObject(5)).thenReturn(new byte[] {0x00});
        when(rs.getObject(6)).thenReturn(null);
        when(rs.getObject("ID")).thenReturn(1);
        when(rs.getBytes(5)).thenReturn(new byte[] {0x01, 0x2A});
        when(dialectHandler.formatDateTimeColumn("TZ_COL", tz1, conn)).thenReturn("T1");
        when(dialectHandler.formatDateTimeColumn("TZ_COL2", tz2, conn)).thenReturn("T2");

        Path dbDirPath = Files.createDirectories(tempDir.resolve("db_multi_type"));
        Path filesDirPath = Files.createDirectories(dbDirPath.resolve("files"));
        Path csvPath = dbDirPath.resolve("TMULTI.csv");
        Files.writeString(csvPath, "ID,TZ_COL,TZ_COL2,CLOB_COL,LBIN_COL,STR_COL\n1,a,b,c,d,e\n",
                StandardCharsets.UTF_8);

        DumpResult result = new LobFileExporter(filePatternConfig).export(conn, "TMULTI",
                dbDirPath.toFile(), filesDirPath.toFile(), "APP", dialectHandler);
        assertEquals(1, result.getRowCount());
        assertEquals(1, result.getFileCount());

        CSVFormat fmt = CSVFormat.DEFAULT.builder()
                .setHeader("ID", "TZ_COL", "TZ_COL2", "CLOB_COL", "LBIN_COL", "STR_COL")
                .setSkipHeaderRecord(true).get();
        try (CSVParser parser = CSVParser.parse(csvPath.toFile(), StandardCharsets.UTF_8, fmt)) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals("T1", records.get(0).get("TZ_COL"));
            assertEquals("T2", records.get(0).get("TZ_COL2"));
            assertEquals("file:clob_1.txt", records.get(0).get("CLOB_COL"));
            assertEquals("012A", records.get(0).get("LBIN_COL"));
            assertEquals("", records.get(0).get("STR_COL"));
        }
    }

    @Test
    void export_正常ケース_重複ヘッダを含むCSVを指定する_先勝ちインデックスで処理されること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        DbDialectHandler dialectHandler = createDialectHandlerMock();
        when(dialectHandler.quoteIdentifier(any()))
                .thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");
        when(filePatternConfig.getPatternsForTable("TDUP")).thenReturn(Collections.emptyMap());
        Connection conn = mock(Connection.class);
        when(conn.getCatalog()).thenReturn(null);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet pkRs = mock(ResultSet.class);
        when(meta.getPrimaryKeys(any(), eq("APP"), eq("TDUP"))).thenReturn(pkRs);
        when(pkRs.next()).thenReturn(false);
        Statement stmt = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmt);
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery("SELECT * FROM \"TDUP\"")).thenReturn(rs);
        ResultSetMetaData md = mock(ResultSetMetaData.class);
        when(rs.getMetaData()).thenReturn(md);
        when(md.getColumnCount()).thenReturn(2);
        when(md.getColumnLabel(1)).thenReturn("ID");
        when(md.getColumnLabel(2)).thenReturn("ID");
        when(md.getColumnType(1)).thenReturn(Types.VARCHAR);
        when(md.getColumnType(2)).thenReturn(Types.VARCHAR);
        when(rs.next()).thenReturn(true, false);
        when(rs.getString(1)).thenReturn("A");
        when(rs.getString(2)).thenReturn("B");
        Path dbDirPath = Files.createDirectories(tempDir.resolve("db_dup_header"));
        Path filesDirPath = Files.createDirectories(dbDirPath.resolve("files"));
        Path csvPath = dbDirPath.resolve("TDUP.csv");
        Files.writeString(csvPath, "ID,ID\nX,Y\n", StandardCharsets.UTF_8);

        new LobFileExporter(filePatternConfig).export(conn, "TDUP", dbDirPath.toFile(),
                filesDirPath.toFile(), "APP", dialectHandler);
        assertTrue(Files.exists(csvPath));
    }

    @Test
    void export_正常ケース_blob非nullでファイル参照へ置換すること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        DbDialectHandler dialectHandler = createDialectHandlerMock();
        when(dialectHandler.quoteIdentifier(any()))
                .thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");

        when(filePatternConfig.getPatternsForTable("1TABLE"))
                .thenReturn(Collections.singletonMap("BLOB_COL", "blob_{ID}.bin"));
        when(filePatternConfig.getPattern("1TABLE", "BLOB_COL"))
                .thenReturn(Optional.of("blob_{ID}.bin"));

        Connection conn = mock(Connection.class);
        when(conn.getCatalog()).thenReturn(null);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet pkRs = mock(ResultSet.class);
        when(meta.getPrimaryKeys(any(), eq("APP"), eq("1TABLE"))).thenReturn(pkRs);
        when(pkRs.next()).thenReturn(false);

        Statement stmt = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmt);
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery("SELECT * FROM \"1TABLE\"")).thenReturn(rs);
        ResultSetMetaData md = mock(ResultSetMetaData.class);
        when(rs.getMetaData()).thenReturn(md);
        when(md.getColumnCount()).thenReturn(2);
        when(md.getColumnLabel(1)).thenReturn("ID");
        when(md.getColumnLabel(2)).thenReturn("BLOB_COL");
        when(md.getColumnType(1)).thenReturn(Types.INTEGER);
        when(md.getColumnType(2)).thenReturn(Types.BLOB);

        Object blobRaw = new Object();
        when(rs.next()).thenReturn(true, false);
        when(rs.getObject(1)).thenReturn(1);
        when(rs.getObject(2)).thenReturn(blobRaw);
        when(rs.getObject("ID")).thenReturn(1);
        when(rs.getString(1)).thenReturn("1");

        Path dbDirPath = Files.createDirectories(tempDir.resolve("db_non_null"));
        Path filesDirPath = Files.createDirectories(dbDirPath.resolve("files"));
        Path csvPath = dbDirPath.resolve("1TABLE.csv");
        Files.writeString(csvPath, "ID,BLOB_COL\n1,dummy\n", StandardCharsets.UTF_8);

        DumpResult result = new LobFileExporter(filePatternConfig).export(conn, "1TABLE",
                dbDirPath.toFile(), filesDirPath.toFile(), "APP", dialectHandler);

        assertEquals(1, result.getRowCount());
        assertEquals(1, result.getFileCount());
        verify(dialectHandler).writeLobFile(eq("1TABLE"), eq("BLOB_COL"), eq(blobRaw), any());

        CSVFormat fmt = CSVFormat.DEFAULT.builder().setHeader("ID", "BLOB_COL")
                .setSkipHeaderRecord(true).get();
        try (CSVParser parser = CSVParser.parse(csvPath.toFile(), StandardCharsets.UTF_8, fmt)) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals(1, records.size());
            assertEquals("1", records.get(0).get("ID"));
            assertEquals("file:blob_1.bin", records.get(0).get("BLOB_COL"));
        }
    }

    @Test
    void export_正常ケース_timestampnclobvarbinaryを処理する_型別の期待値であること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        DbDialectHandler dialectHandler = createDialectHandlerMock();
        when(dialectHandler.quoteIdentifier(any()))
                .thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");
        when(filePatternConfig.getPatternsForTable("1TABLE"))
                .thenReturn(Collections.singletonMap("NCLOB_COL", "nclob_{ID}.txt"));
        when(filePatternConfig.getPattern("1TABLE", "NCLOB_COL"))
                .thenReturn(Optional.of("nclob_{ID}.txt"));

        Connection conn = mock(Connection.class);
        when(conn.getCatalog()).thenReturn(null);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet pkRs = mock(ResultSet.class);
        when(meta.getPrimaryKeys(any(), eq("APP"), eq("1TABLE"))).thenReturn(pkRs);
        when(pkRs.next()).thenReturn(false);

        Statement stmt = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmt);
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery("SELECT * FROM \"1TABLE\"")).thenReturn(rs);
        ResultSetMetaData md = mock(ResultSetMetaData.class);
        when(rs.getMetaData()).thenReturn(md);
        when(md.getColumnCount()).thenReturn(4);
        when(md.getColumnLabel(1)).thenReturn("ID");
        when(md.getColumnLabel(2)).thenReturn("TS_COL");
        when(md.getColumnLabel(3)).thenReturn("NCLOB_COL");
        when(md.getColumnLabel(4)).thenReturn("VBIN_COL");
        when(md.getColumnType(1)).thenReturn(Types.INTEGER);
        when(md.getColumnType(2)).thenReturn(Types.TIMESTAMP);
        when(md.getColumnType(3)).thenReturn(Types.NCLOB);
        when(md.getColumnType(4)).thenReturn(Types.VARBINARY);

        Object timestampRaw = new Object();
        Object nclobRaw = new Object();
        when(rs.next()).thenReturn(true, false);
        when(rs.getObject(1)).thenReturn(10);
        when(rs.getObject(2)).thenReturn(timestampRaw);
        when(rs.getObject(3)).thenReturn(nclobRaw);
        when(rs.getObject(4)).thenReturn(new byte[] {0x00});
        when(rs.getObject("ID")).thenReturn(10);
        when(rs.getBytes(4)).thenReturn(new byte[] {0x0A, 0x0B});
        when(rs.getString(1)).thenReturn("10");
        when(dialectHandler.formatDateTimeColumn("TS_COL", timestampRaw, conn))
                .thenReturn("2026-02-15 12:34:56");

        Path dbDirPath = Files.createDirectories(tempDir.resolve("db_timestamp_nclob"));
        Path filesDirPath = Files.createDirectories(dbDirPath.resolve("files"));
        Path csvPath = dbDirPath.resolve("1TABLE.csv");
        Files.writeString(csvPath, "ID,TS_COL,NCLOB_COL,VBIN_COL\n10,a,b,c\n",
                StandardCharsets.UTF_8);

        DumpResult result = new LobFileExporter(filePatternConfig).export(conn, "1TABLE",
                dbDirPath.toFile(), filesDirPath.toFile(), "APP", dialectHandler);

        assertEquals(1, result.getRowCount());
        assertEquals(1, result.getFileCount());
        verify(dialectHandler).writeLobFile(eq("1TABLE"), eq("NCLOB_COL"), eq(nclobRaw), any());

        CSVFormat fmt = CSVFormat.DEFAULT.builder()
                .setHeader("ID", "TS_COL", "NCLOB_COL", "VBIN_COL").setSkipHeaderRecord(true).get();
        try (CSVParser parser = CSVParser.parse(csvPath.toFile(), StandardCharsets.UTF_8, fmt)) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals(1, records.size());
            assertEquals("10", records.get(0).get("ID"));
            assertEquals("2026-02-15 12:34:56", records.get(0).get("TS_COL"));
            assertEquals("file:nclob_10.txt", records.get(0).get("NCLOB_COL"));
            assertEquals("0A0B", records.get(0).get("VBIN_COL"));
        }
    }

    @Test
    void export_異常ケース_blob非nullでパターン未定義_IllegalStateExceptionが送出されること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        DbDialectHandler dialectHandler = createDialectHandlerMock();
        when(dialectHandler.quoteIdentifier(any()))
                .thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");

        when(filePatternConfig.getPatternsForTable("TERR"))
                .thenReturn(Collections.singletonMap("BLOB_COL", "blob_{ID}.bin"));
        when(filePatternConfig.getPattern("TERR", "BLOB_COL")).thenReturn(Optional.empty());

        Connection conn = mock(Connection.class);
        when(conn.getCatalog()).thenReturn(null);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet pkRs = mock(ResultSet.class);
        when(meta.getPrimaryKeys(any(), eq("APP"), eq("TERR"))).thenReturn(pkRs);
        when(pkRs.next()).thenReturn(false);

        Statement stmt = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmt);
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery("SELECT * FROM \"TERR\"")).thenReturn(rs);
        ResultSetMetaData md = mock(ResultSetMetaData.class);
        when(rs.getMetaData()).thenReturn(md);
        when(md.getColumnCount()).thenReturn(2);
        when(md.getColumnLabel(1)).thenReturn("ID");
        when(md.getColumnLabel(2)).thenReturn("BLOB_COL");
        when(md.getColumnType(1)).thenReturn(Types.INTEGER);
        when(md.getColumnType(2)).thenReturn(Types.BLOB);
        when(rs.next()).thenReturn(true, false);
        when(rs.getObject(1)).thenReturn(1);
        when(rs.getObject(2)).thenReturn(new Object());

        Path dbDirPath = Files.createDirectories(tempDir.resolve("db_err"));
        Path filesDirPath = Files.createDirectories(dbDirPath.resolve("files"));
        Path csvPath = dbDirPath.resolve("TERR.csv");
        Files.writeString(csvPath, "ID,BLOB_COL\n1,x\n", StandardCharsets.UTF_8);

        assertThrows(IllegalStateException.class,
                () -> new LobFileExporter(filePatternConfig).export(conn, "TERR",
                        dbDirPath.toFile(), filesDirPath.toFile(), "APP", dialectHandler));
    }

    @Test
    void export_異常ケース_パターン取得が途中で空になる_IllegalStateExceptionが送出されること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        DbDialectHandler dialectHandler = createDialectHandlerMock();
        when(dialectHandler.quoteIdentifier(any()))
                .thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");

        when(filePatternConfig.getPatternsForTable("TPATTERN_EMPTY"))
                .thenReturn(Collections.singletonMap("BLOB_COL", "blob_{ID}.bin"));
        when(filePatternConfig.getPattern("TPATTERN_EMPTY", "BLOB_COL"))
                .thenReturn(Optional.of("blob_{ID}.bin")).thenReturn(Optional.empty());

        Connection conn = mock(Connection.class);
        when(conn.getCatalog()).thenReturn(null);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet pkRs = mock(ResultSet.class);
        when(meta.getPrimaryKeys(any(), eq("APP"), eq("TPATTERN_EMPTY"))).thenReturn(pkRs);
        when(pkRs.next()).thenReturn(false);

        Statement stmt = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmt);
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery("SELECT * FROM \"TPATTERN_EMPTY\"")).thenReturn(rs);
        ResultSetMetaData md = mock(ResultSetMetaData.class);
        when(rs.getMetaData()).thenReturn(md);
        when(md.getColumnCount()).thenReturn(2);
        when(md.getColumnLabel(1)).thenReturn("ID");
        when(md.getColumnLabel(2)).thenReturn("BLOB_COL");
        when(md.getColumnType(1)).thenReturn(Types.INTEGER);
        when(md.getColumnType(2)).thenReturn(Types.BLOB);
        when(rs.next()).thenReturn(true, false);
        when(rs.getObject(1)).thenReturn(1);
        when(rs.getObject(2)).thenReturn(new byte[] {0x01});
        when(rs.getObject("ID")).thenReturn(1);
        when(rs.getString(1)).thenReturn("1");

        Path dbDirPath = Files.createDirectories(tempDir.resolve("db_pattern_empty"));
        Path filesDirPath = Files.createDirectories(dbDirPath.resolve("files"));
        Path csvPath = dbDirPath.resolve("TPATTERN_EMPTY.csv");
        Files.writeString(csvPath, "ID,BLOB_COL\n1,x\n", StandardCharsets.UTF_8);

        assertThrows(IllegalStateException.class,
                () -> new LobFileExporter(filePatternConfig).export(conn, "TPATTERN_EMPTY",
                        dbDirPath.toFile(), filesDirPath.toFile(), "APP", dialectHandler));
    }

    @Test
    void export_正常ケース_主キーで数値ソートする_並び順が昇順になること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        DbDialectHandler dialectHandler = createDialectHandlerMock();
        when(dialectHandler.quoteIdentifier(any()))
                .thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");
        when(filePatternConfig.getPatternsForTable("TSORT")).thenReturn(Collections.emptyMap());

        Connection conn = mock(Connection.class);
        when(conn.getCatalog()).thenReturn(null);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet pkRs = mock(ResultSet.class);
        when(meta.getPrimaryKeys(any(), eq("APP"), eq("TSORT"))).thenReturn(pkRs);
        when(pkRs.next()).thenReturn(true, false);
        when(pkRs.getString("COLUMN_NAME")).thenReturn("ID");

        Statement stmt = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmt);
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery("SELECT * FROM \"TSORT\"")).thenReturn(rs);
        ResultSetMetaData md = mock(ResultSetMetaData.class);
        when(rs.getMetaData()).thenReturn(md);
        when(md.getColumnCount()).thenReturn(2);
        when(md.getColumnLabel(1)).thenReturn("ID");
        when(md.getColumnLabel(2)).thenReturn("NAME");
        when(md.getColumnType(1)).thenReturn(Types.INTEGER);
        when(md.getColumnType(2)).thenReturn(Types.VARCHAR);
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getString(1)).thenReturn("10", "2");
        when(rs.getString(2)).thenReturn("x", "y");
        when(rs.getObject(1)).thenReturn("10", "2");
        when(rs.getObject(2)).thenReturn("x", "y");

        Path dbDirPath = Files.createDirectories(tempDir.resolve("db_sort_num"));
        Path filesDirPath = Files.createDirectories(dbDirPath.resolve("files"));
        Path csvPath = dbDirPath.resolve("TSORT.csv");
        Files.writeString(csvPath, "ID,NAME\n10,a\n2,b\n", StandardCharsets.UTF_8);

        DumpResult result = new LobFileExporter(filePatternConfig).export(conn, "TSORT",
                dbDirPath.toFile(), filesDirPath.toFile(), "APP", dialectHandler);
        assertEquals(2, result.getRowCount());

        CSVFormat fmt =
                CSVFormat.DEFAULT.builder().setHeader("ID", "NAME").setSkipHeaderRecord(true).get();
        try (CSVParser parser = CSVParser.parse(csvPath.toFile(), StandardCharsets.UTF_8, fmt)) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals("2", records.get(0).get("ID"));
            assertEquals("10", records.get(1).get("ID"));
        }
    }

    @Test
    void export_正常ケース_csvファイルが存在しない_件数ゼロが返ること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        Path dbDirPath = Files.createDirectories(tempDir.resolve("db_missing_csv"));
        Path filesDirPath = Files.createDirectories(dbDirPath.resolve("files"));
        DumpResult result = new LobFileExporter(filePatternConfig).export(mock(Connection.class),
                "NOFILE", dbDirPath.toFile(), filesDirPath.toFile(), "APP",
                createDialectHandlerMock());
        assertEquals(0, result.getRowCount());
        assertEquals(0, result.getFileCount());
    }

    @Test
    void export_異常ケース_テーブルのパターン定義がnullである_IllegalStateExceptionが送出されること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        DbDialectHandler dialectHandler = createDialectHandlerMock();
        when(dialectHandler.quoteIdentifier(any()))
                .thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");
        when(filePatternConfig.getPatternsForTable("TNOPAT")).thenReturn(null);

        Connection conn = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmt);
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery("SELECT * FROM \"TNOPAT\"")).thenReturn(rs);
        ResultSetMetaData md = mock(ResultSetMetaData.class);
        when(rs.getMetaData()).thenReturn(md);
        when(md.getColumnCount()).thenReturn(1);
        when(md.getColumnLabel(1)).thenReturn("ID");
        when(md.getColumnType(1)).thenReturn(Types.INTEGER);
        when(rs.next()).thenReturn(false);

        Path dbDirPath = Files.createDirectories(tempDir.resolve("db_no_pattern"));
        Path filesDirPath = Files.createDirectories(dbDirPath.resolve("files"));
        Path csvPath = dbDirPath.resolve("TNOPAT.csv");
        Files.writeString(csvPath, "ID\n1\n", StandardCharsets.UTF_8);

        assertThrows(IllegalStateException.class,
                () -> new LobFileExporter(filePatternConfig).export(conn, "TNOPAT",
                        dbDirPath.toFile(), filesDirPath.toFile(), "APP", dialectHandler));
    }

    @Test
    void export_正常ケース_ヘッダにない列を含む_未定義列は無視されること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        DbDialectHandler dialectHandler = createDialectHandlerMock();
        when(dialectHandler.quoteIdentifier(any()))
                .thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");
        when(filePatternConfig.getPatternsForTable("THEADER_SKIP"))
                .thenReturn(Collections.emptyMap());

        Connection conn = mock(Connection.class);
        when(conn.getCatalog()).thenReturn(null);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet pkRs = mock(ResultSet.class);
        when(meta.getPrimaryKeys(any(), eq("APP"), eq("THEADER_SKIP"))).thenReturn(pkRs);
        when(pkRs.next()).thenReturn(false);
        Statement stmt = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmt);
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery("SELECT * FROM \"THEADER_SKIP\"")).thenReturn(rs);
        ResultSetMetaData md = mock(ResultSetMetaData.class);
        when(rs.getMetaData()).thenReturn(md);
        when(md.getColumnCount()).thenReturn(2);
        when(md.getColumnLabel(1)).thenReturn("ID");
        when(md.getColumnLabel(2)).thenReturn("EXTRA_COL");
        when(md.getColumnType(1)).thenReturn(Types.INTEGER);
        when(md.getColumnType(2)).thenReturn(Types.VARCHAR);
        when(rs.next()).thenReturn(true, false);
        when(rs.getString(1)).thenReturn("1");
        when(rs.getString(2)).thenReturn("ignored");
        when(rs.getObject(1)).thenReturn("1");
        when(rs.getObject(2)).thenReturn("ignored");

        Path dbDirPath = Files.createDirectories(tempDir.resolve("db_header_skip"));
        Path filesDirPath = Files.createDirectories(dbDirPath.resolve("files"));
        Path csvPath = dbDirPath.resolve("THEADER_SKIP.csv");
        Files.writeString(csvPath, "ID\n1\n", StandardCharsets.UTF_8);

        DumpResult result = new LobFileExporter(filePatternConfig).export(conn, "THEADER_SKIP",
                dbDirPath.toFile(), filesDirPath.toFile(), "APP", dialectHandler);
        assertEquals(1, result.getRowCount());

        CSVFormat fmt = CSVFormat.DEFAULT.builder().setHeader("ID").setSkipHeaderRecord(true).get();
        try (CSVParser parser = CSVParser.parse(csvPath.toFile(), StandardCharsets.UTF_8, fmt)) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals(1, records.size());
            assertEquals("1", records.get(0).get("ID"));
        }
    }

    // -------------------------------------------------------------------------
    // Helper
    // -------------------------------------------------------------------------

    private DbDialectHandler createDialectHandlerMock() {
        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);
        when(dialectHandler.quoteIdentifier(any()))
                .thenAnswer(invocation -> "\"" + invocation.getArgument(0) + "\"");
        when(dialectHandler.resolveSchema(any())).thenReturn("APP");
        try {
            when(dialectHandler.formatDbValueForCsv(nullable(String.class), any()))
                    .thenAnswer(invocation -> {
                        Object value = invocation.getArgument(1);
                        return (value == null) ? "" : value.toString();
                    });
            when(dialectHandler.formatDateTimeColumn(nullable(String.class), any(), any()))
                    .thenAnswer(invocation -> {
                        Object value = invocation.getArgument(1);
                        return (value == null) ? "" : value.toString();
                    });
            when(dialectHandler.isDateTimeTypeForDump(anyInt(), nullable(String.class)))
                    .thenAnswer(invocation -> {
                        int sqlType = invocation.getArgument(0);
                        String typeName = invocation.getArgument(1);
                        if (sqlType == Types.DATE || sqlType == Types.TIME
                                || sqlType == Types.TIMESTAMP || sqlType == -101 || sqlType == -102
                                || sqlType == Types.TIMESTAMP_WITH_TIMEZONE) {
                            return true;
                        }
                        if (typeName == null) {
                            return false;
                        }
                        String normalized = typeName.toUpperCase(Locale.ROOT);
                        return normalized.contains("TIMESTAMP") || normalized.contains("DATE")
                                || normalized.contains("TIME");
                    });
            when(dialectHandler.isBinaryTypeForDump(anyInt(), nullable(String.class)))
                    .thenAnswer(invocation -> {
                        int sqlType = invocation.getArgument(0);
                        String typeName = invocation.getArgument(1);
                        if (sqlType == Types.BINARY || sqlType == Types.VARBINARY
                                || sqlType == Types.LONGVARBINARY) {
                            return true;
                        }
                        if (typeName == null) {
                            return false;
                        }
                        String normalized = typeName.toUpperCase(Locale.ROOT);
                        return "RAW".equals(normalized) || "LONG RAW".equals(normalized);
                    });
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return dialectHandler;
    }
}
