package io.github.yok.flexdblink.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.FileNameResolver;
import io.github.yok.flexdblink.config.FilePatternConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.db.DbDialectHandler;
import io.github.yok.flexdblink.util.ErrorHandler;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
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
import org.mockito.MockedStatic;

class DataDumperTest {

    @TempDir
    Path tempDir;

    @Test
    void buildSortIndices_正常ケース_主キー列ありを指定する_主キー順インデックスが返ること() throws Exception {
        DataDumper dumper = createDumper();
        Method method =
                DataDumper.class.getDeclaredMethod("buildSortIndices", String[].class, List.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<Integer> result = (List<Integer>) method.invoke(dumper,
                new Object[] {new String[] {"A", "B", "C"}, List.of("C", "A")});
        assertEquals(List.of(2, 0), result);
    }

    @Test
    void buildSortIndices_正常ケース_主キー列なしを指定する_先頭インデックスが返ること() throws Exception {
        DataDumper dumper = createDumper();
        Method method =
                DataDumper.class.getDeclaredMethod("buildSortIndices", String[].class, List.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<Integer> result = (List<Integer>) method.invoke(dumper,
                new Object[] {new String[] {"A", "B"}, Collections.emptyList()});
        assertEquals(List.of(0), result);
    }

    @Test
    void buildSortIndices_正常ケース_ヘッダ重複を指定する_先勝ちインデックスが返ること() throws Exception {
        DataDumper dumper = createDumper();
        Method method =
                DataDumper.class.getDeclaredMethod("buildSortIndices", String[].class, List.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<Integer> result = (List<Integer>) method.invoke(dumper,
                new Object[] {new String[] {"ID", "NAME", "ID"}, List.of("ID")});
        assertEquals(List.of(0), result);
    }

    @Test
    void applyPlaceholders_正常ケース_プレースホルダと値Mapを指定する_置換済み文字列が返ること() throws Exception {
        DataDumper dumper = createDumper();
        Method method =
                DataDumper.class.getDeclaredMethod("applyPlaceholders", String.class, Map.class);
        method.setAccessible(true);
        Map<String, Object> keyMap = new LinkedHashMap<>();
        keyMap.put("ID", 7);
        keyMap.put("SEQ", "A");
        Object actual = method.invoke(dumper, "lob_{ID}_{SEQ}.bin", keyMap);
        assertEquals("lob_7_A.bin", actual);
    }

    @Test
    void applyPlaceholders_正常ケース_未使用キーを含むMapを指定する_非該当キーは無視されること() throws Exception {
        DataDumper dumper = createDumper();
        Method method =
                DataDumper.class.getDeclaredMethod("applyPlaceholders", String.class, Map.class);
        method.setAccessible(true);
        Map<String, Object> keyMap = new LinkedHashMap<>();
        keyMap.put("ID", 9);
        keyMap.put("UNUSED", "X");
        Object actual = method.invoke(dumper, "lob_{ID}.bin", keyMap);
        assertEquals("lob_9.bin", actual);
    }

    @Test
    void fetchTargetTables_正常ケース_excludeTablesがnullでテーブルが複数ある_全件が返ること() throws Exception {
        DataDumper dumper = createDumper();
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);

        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getTables(null, "APP", "%", new String[] {"TABLE"})).thenReturn(rs);
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getString("TABLE_NAME")).thenReturn("T1", "T2");

        Method method = DataDumper.class.getDeclaredMethod("fetchTargetTables", Connection.class,
                String.class, List.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<String> result = (List<String>) method.invoke(dumper, conn, "APP", null);
        assertEquals(List.of("T1", "T2"), result);
    }

    @Test
    void fetchTargetTables_正常ケース_excludeTablesに一致するテーブルがある_除外された一覧が返ること() throws Exception {
        DataDumper dumper = createDumper();
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);

        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getTables(null, "APP", "%", new String[] {"TABLE"})).thenReturn(rs);
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getString("TABLE_NAME")).thenReturn("T1", "T2");

        Method method = DataDumper.class.getDeclaredMethod("fetchTargetTables", Connection.class,
                String.class, List.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<String> result = (List<String>) method.invoke(dumper, conn, "APP", List.of("t1"));
        assertEquals(List.of("T2"), result);
    }

    @Test
    void exportTableAsCsvUtf8_識別子クォートとbinarynull空文字化を行うこと() throws Exception {
        DataDumper dumper = createDumper();
        DbDialectHandler dialectHandler = createDialectHandlerMock();
        when(dialectHandler.quoteIdentifier(any()))
                .thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");

        Connection conn = mock(Connection.class);
        when(conn.getSchema()).thenReturn("APP");
        when(conn.getCatalog()).thenReturn(null);

        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet pkRs = mock(ResultSet.class);
        when(meta.getPrimaryKeys(any(), eq("APP"), eq("1TABLE"))).thenReturn(pkRs);
        when(pkRs.next()).thenReturn(true, false);
        when(pkRs.getString("COLUMN_NAME")).thenReturn("1ID");

        Statement stmtHeader = mock(Statement.class);
        Statement stmtData = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmtHeader, stmtData);

        ResultSet rsHeader = mock(ResultSet.class);
        when(stmtHeader.executeQuery("SELECT * FROM \"1TABLE\" WHERE 1=0")).thenReturn(rsHeader);
        ResultSetMetaData mdHeader = mock(ResultSetMetaData.class);
        when(rsHeader.getMetaData()).thenReturn(mdHeader);
        when(mdHeader.getColumnCount()).thenReturn(1);
        when(mdHeader.getColumnLabel(1)).thenReturn("1ID");

        ResultSet rsData = mock(ResultSet.class);
        when(stmtData.executeQuery("SELECT * FROM \"1TABLE\" ORDER BY \"1ID\" ASC"))
                .thenReturn(rsData);
        ResultSetMetaData mdData = mock(ResultSetMetaData.class);
        when(rsData.getMetaData()).thenReturn(mdData);
        when(mdData.getColumnCount()).thenReturn(1);
        when(mdData.getColumnType(1)).thenReturn(Types.BINARY);
        when(mdData.getColumnTypeName(1)).thenReturn("BINARY");
        when(rsData.next()).thenReturn(true, false);
        when(rsData.getBytes(1)).thenReturn(null);

        File csvFile = tempDir.resolve("1TABLE.csv").toFile();
        Method method = DataDumper.class.getDeclaredMethod("exportTableAsCsvUtf8", Connection.class,
                String.class, File.class, DbDialectHandler.class);
        method.setAccessible(true);
        method.invoke(dumper, conn, "1TABLE", csvFile, dialectHandler);

        CSVFormat fmt =
                CSVFormat.DEFAULT.builder().setHeader("1ID").setSkipHeaderRecord(true).get();
        try (CSVParser parser = CSVParser.parse(csvFile, StandardCharsets.UTF_8, fmt)) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals(1, records.size());
            assertEquals("", records.get(0).get("1ID"));
        }
    }

    @Test
    void exportTableAsCsvUtf8_正常ケース_binary非nullを16進へ変換すること() throws Exception {
        DataDumper dumper = createDumper();
        DbDialectHandler dialectHandler = createDialectHandlerMock();
        when(dialectHandler.quoteIdentifier(any()))
                .thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");

        Connection conn = mock(Connection.class);
        when(conn.getSchema()).thenReturn("APP");
        when(conn.getCatalog()).thenReturn(null);

        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet pkRs = mock(ResultSet.class);
        when(meta.getPrimaryKeys(any(), eq("APP"), eq("1TABLE"))).thenReturn(pkRs);
        when(pkRs.next()).thenReturn(true, false);
        when(pkRs.getString("COLUMN_NAME")).thenReturn("1ID");

        Statement stmtHeader = mock(Statement.class);
        Statement stmtData = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmtHeader, stmtData);

        ResultSet rsHeader = mock(ResultSet.class);
        when(stmtHeader.executeQuery("SELECT * FROM \"1TABLE\" WHERE 1=0")).thenReturn(rsHeader);
        ResultSetMetaData mdHeader = mock(ResultSetMetaData.class);
        when(rsHeader.getMetaData()).thenReturn(mdHeader);
        when(mdHeader.getColumnCount()).thenReturn(1);
        when(mdHeader.getColumnLabel(1)).thenReturn("1ID");

        ResultSet rsData = mock(ResultSet.class);
        when(stmtData.executeQuery("SELECT * FROM \"1TABLE\" ORDER BY \"1ID\" ASC"))
                .thenReturn(rsData);
        ResultSetMetaData mdData = mock(ResultSetMetaData.class);
        when(rsData.getMetaData()).thenReturn(mdData);
        when(mdData.getColumnCount()).thenReturn(1);
        when(mdData.getColumnType(1)).thenReturn(Types.BINARY);
        when(mdData.getColumnTypeName(1)).thenReturn("BINARY");
        when(rsData.next()).thenReturn(true, false);
        when(rsData.getBytes(1)).thenReturn(new byte[] {0x01, 0x2A});

        File csvFile = tempDir.resolve("1TABLE_non_null.csv").toFile();
        Method method = DataDumper.class.getDeclaredMethod("exportTableAsCsvUtf8", Connection.class,
                String.class, File.class, DbDialectHandler.class);
        method.setAccessible(true);
        method.invoke(dumper, conn, "1TABLE", csvFile, dialectHandler);

        CSVFormat fmt =
                CSVFormat.DEFAULT.builder().setHeader("1ID").setSkipHeaderRecord(true).get();
        try (CSVParser parser = CSVParser.parse(csvFile, StandardCharsets.UTF_8, fmt)) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals(1, records.size());
            assertEquals("012A", records.get(0).get("1ID"));
        }
    }

    @Test
    void exportTableAsCsvUtf8_正常ケース_主キーなしで数値ソートすること() throws Exception {
        DataDumper dumper = createDumper();
        DbDialectHandler dialectHandler = createDialectHandlerMock();
        when(dialectHandler.quoteIdentifier(any()))
                .thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");

        Connection conn = mock(Connection.class);
        when(conn.getSchema()).thenReturn("APP");
        when(conn.getCatalog()).thenReturn(null);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet pkRs = mock(ResultSet.class);
        when(meta.getPrimaryKeys(any(), eq("APP"), eq("TNUM"))).thenReturn(pkRs);
        when(pkRs.next()).thenReturn(false);

        Statement stmtHeader = mock(Statement.class);
        Statement stmtData = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmtHeader, stmtData);

        ResultSet rsHeader = mock(ResultSet.class);
        when(stmtHeader.executeQuery("SELECT * FROM \"TNUM\" WHERE 1=0")).thenReturn(rsHeader);
        ResultSetMetaData mdHeader = mock(ResultSetMetaData.class);
        when(rsHeader.getMetaData()).thenReturn(mdHeader);
        when(mdHeader.getColumnCount()).thenReturn(2);
        when(mdHeader.getColumnLabel(1)).thenReturn("A");
        when(mdHeader.getColumnLabel(2)).thenReturn("B");

        ResultSet rsData = mock(ResultSet.class);
        when(stmtData.executeQuery("SELECT * FROM \"TNUM\" ORDER BY \"A\" ASC")).thenReturn(rsData);
        ResultSetMetaData mdData = mock(ResultSetMetaData.class);
        when(rsData.getMetaData()).thenReturn(mdData);
        when(mdData.getColumnCount()).thenReturn(2);
        when(mdData.getColumnType(1)).thenReturn(Types.VARCHAR);
        when(mdData.getColumnType(2)).thenReturn(Types.VARCHAR);
        when(mdData.getColumnTypeName(1)).thenReturn("VARCHAR2");
        when(mdData.getColumnTypeName(2)).thenReturn("VARCHAR2");
        when(rsData.next()).thenReturn(true, true, false);
        when(rsData.getObject(1)).thenReturn("10", "2");
        when(rsData.getObject(2)).thenReturn("x", "y");

        File csvFile = tempDir.resolve("TNUM.csv").toFile();
        Method method = DataDumper.class.getDeclaredMethod("exportTableAsCsvUtf8", Connection.class,
                String.class, File.class, DbDialectHandler.class);
        method.setAccessible(true);
        method.invoke(dumper, conn, "TNUM", csvFile, dialectHandler);

        CSVFormat fmt =
                CSVFormat.DEFAULT.builder().setHeader("A", "B").setSkipHeaderRecord(true).get();
        try (CSVParser parser = CSVParser.parse(csvFile, StandardCharsets.UTF_8, fmt)) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals(2, records.size());
            assertEquals("2", records.get(0).get("A"));
            assertEquals("10", records.get(1).get("A"));
        }
    }

    @Test
    void exportTableAsCsvUtf8_正常ケース_主キーありで文字列ソートすること() throws Exception {
        DataDumper dumper = createDumper();
        DbDialectHandler dialectHandler = createDialectHandlerMock();
        when(dialectHandler.quoteIdentifier(any()))
                .thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");

        Connection conn = mock(Connection.class);
        when(conn.getSchema()).thenReturn("APP");
        when(conn.getCatalog()).thenReturn(null);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet pkRs = mock(ResultSet.class);
        when(meta.getPrimaryKeys(any(), eq("APP"), eq("TSTR"))).thenReturn(pkRs);
        when(pkRs.next()).thenReturn(true, false);
        when(pkRs.getString("COLUMN_NAME")).thenReturn("CODE");

        Statement stmtHeader = mock(Statement.class);
        Statement stmtData = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmtHeader, stmtData);

        ResultSet rsHeader = mock(ResultSet.class);
        when(stmtHeader.executeQuery("SELECT * FROM \"TSTR\" WHERE 1=0")).thenReturn(rsHeader);
        ResultSetMetaData mdHeader = mock(ResultSetMetaData.class);
        when(rsHeader.getMetaData()).thenReturn(mdHeader);
        when(mdHeader.getColumnCount()).thenReturn(1);
        when(mdHeader.getColumnLabel(1)).thenReturn("CODE");

        ResultSet rsData = mock(ResultSet.class);
        when(stmtData.executeQuery("SELECT * FROM \"TSTR\" ORDER BY \"CODE\" ASC"))
                .thenReturn(rsData);
        ResultSetMetaData mdData = mock(ResultSetMetaData.class);
        when(rsData.getMetaData()).thenReturn(mdData);
        when(mdData.getColumnCount()).thenReturn(1);
        when(mdData.getColumnType(1)).thenReturn(Types.VARCHAR);
        when(mdData.getColumnTypeName(1)).thenReturn("VARCHAR2");
        when(rsData.next()).thenReturn(true, true, false);
        when(rsData.getObject(1)).thenReturn("b", "a");

        File csvFile = tempDir.resolve("TSTR.csv").toFile();
        Method method = DataDumper.class.getDeclaredMethod("exportTableAsCsvUtf8", Connection.class,
                String.class, File.class, DbDialectHandler.class);
        method.setAccessible(true);
        method.invoke(dumper, conn, "TSTR", csvFile, dialectHandler);

        CSVFormat fmt =
                CSVFormat.DEFAULT.builder().setHeader("CODE").setSkipHeaderRecord(true).get();
        try (CSVParser parser = CSVParser.parse(csvFile, StandardCharsets.UTF_8, fmt)) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals(2, records.size());
            assertEquals("a", records.get(0).get("CODE"));
            assertEquals("b", records.get(1).get("CODE"));
        }
    }

    @Test
    void exportTableAsCsvUtf8_正常ケース_sqlTypeOtherかつRAW型を指定する_16進文字列が出力されること() throws Exception {
        DataDumper dumper = createDumper();
        DbDialectHandler dialectHandler = createDialectHandlerMock();
        when(dialectHandler.quoteIdentifier(any()))
                .thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");

        Connection conn = mock(Connection.class);
        when(conn.getSchema()).thenReturn("APP");
        when(conn.getCatalog()).thenReturn(null);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet pkRs = mock(ResultSet.class);
        when(meta.getPrimaryKeys(any(), eq("APP"), eq("TRAW"))).thenReturn(pkRs);
        when(pkRs.next()).thenReturn(false);

        Statement stmtHeader = mock(Statement.class);
        Statement stmtData = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmtHeader, stmtData);

        ResultSet rsHeader = mock(ResultSet.class);
        when(stmtHeader.executeQuery("SELECT * FROM \"TRAW\" WHERE 1=0")).thenReturn(rsHeader);
        ResultSetMetaData mdHeader = mock(ResultSetMetaData.class);
        when(rsHeader.getMetaData()).thenReturn(mdHeader);
        when(mdHeader.getColumnCount()).thenReturn(1);
        when(mdHeader.getColumnLabel(1)).thenReturn("RAW_COL");

        ResultSet rsData = mock(ResultSet.class);
        when(stmtData.executeQuery("SELECT * FROM \"TRAW\" ORDER BY \"RAW_COL\" ASC"))
                .thenReturn(rsData);
        ResultSetMetaData mdData = mock(ResultSetMetaData.class);
        when(rsData.getMetaData()).thenReturn(mdData);
        when(mdData.getColumnCount()).thenReturn(1);
        when(mdData.getColumnType(1)).thenReturn(Types.OTHER);
        when(mdData.getColumnTypeName(1)).thenReturn("RAW");
        when(rsData.next()).thenReturn(true, false);
        when(rsData.getBytes(1)).thenReturn(new byte[] {0x01, 0x2A});

        File csvFile = tempDir.resolve("TRAW.csv").toFile();
        Method method = DataDumper.class.getDeclaredMethod("exportTableAsCsvUtf8", Connection.class,
                String.class, File.class, DbDialectHandler.class);
        method.setAccessible(true);
        method.invoke(dumper, conn, "TRAW", csvFile, dialectHandler);

        CSVFormat fmt =
                CSVFormat.DEFAULT.builder().setHeader("RAW_COL").setSkipHeaderRecord(true).get();
        try (CSVParser parser = CSVParser.parse(csvFile, StandardCharsets.UTF_8, fmt)) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals(1, records.size());
            assertEquals("012A", records.get(0).get("RAW_COL"));
        }
    }

    @Test
    void exportTableAsCsvUtf8_正常ケース_比較値が同一である_比較結果0で完了すること() throws Exception {
        DataDumper dumper = createDumper();
        DbDialectHandler dialectHandler = createDialectHandlerMock();
        when(dialectHandler.quoteIdentifier(any()))
                .thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");

        Connection conn = mock(Connection.class);
        when(conn.getSchema()).thenReturn("APP");
        when(conn.getCatalog()).thenReturn(null);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet pkRs = mock(ResultSet.class);
        when(meta.getPrimaryKeys(any(), eq("APP"), eq("TEQ"))).thenReturn(pkRs);
        when(pkRs.next()).thenReturn(false);

        Statement stmtHeader = mock(Statement.class);
        Statement stmtData = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmtHeader, stmtData);

        ResultSet rsHeader = mock(ResultSet.class);
        when(stmtHeader.executeQuery("SELECT * FROM \"TEQ\" WHERE 1=0")).thenReturn(rsHeader);
        ResultSetMetaData mdHeader = mock(ResultSetMetaData.class);
        when(rsHeader.getMetaData()).thenReturn(mdHeader);
        when(mdHeader.getColumnCount()).thenReturn(1);
        when(mdHeader.getColumnLabel(1)).thenReturn("ID");

        ResultSet rsData = mock(ResultSet.class);
        when(stmtData.executeQuery("SELECT * FROM \"TEQ\" ORDER BY \"ID\" ASC")).thenReturn(rsData);
        ResultSetMetaData mdData = mock(ResultSetMetaData.class);
        when(rsData.getMetaData()).thenReturn(mdData);
        when(mdData.getColumnCount()).thenReturn(1);
        when(mdData.getColumnType(1)).thenReturn(Types.VARCHAR);
        when(mdData.getColumnTypeName(1)).thenReturn("VARCHAR2");
        when(rsData.next()).thenReturn(true, true, false);
        when(rsData.getObject(1)).thenReturn("A", "A");

        File csvFile = tempDir.resolve("TEQ.csv").toFile();
        Method method = DataDumper.class.getDeclaredMethod("exportTableAsCsvUtf8", Connection.class,
                String.class, File.class, DbDialectHandler.class);
        method.setAccessible(true);
        method.invoke(dumper, conn, "TEQ", csvFile, dialectHandler);

        CSVFormat fmt = CSVFormat.DEFAULT.builder().setHeader("ID").setSkipHeaderRecord(true).get();
        try (CSVParser parser = CSVParser.parse(csvFile, StandardCharsets.UTF_8, fmt)) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals(2, records.size());
            assertEquals("A", records.get(0).get("ID"));
            assertEquals("A", records.get(1).get("ID"));
        }
    }

    @Test
    void exportTableAsCsvUtf8_正常ケース_longRawと通常nullを指定する_16進と空文字が出力されること() throws Exception {
        DataDumper dumper = createDumper();
        DbDialectHandler dialectHandler = createDialectHandlerMock();
        when(dialectHandler.quoteIdentifier(any()))
                .thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");

        Connection conn = mock(Connection.class);
        when(conn.getSchema()).thenReturn("APP");
        when(conn.getCatalog()).thenReturn(null);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet pkRs = mock(ResultSet.class);
        when(meta.getPrimaryKeys(any(), eq("APP"), eq("TRAW_NULL"))).thenReturn(pkRs);
        when(pkRs.next()).thenReturn(false);

        Statement stmtHeader = mock(Statement.class);
        Statement stmtData = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmtHeader, stmtData);
        ResultSet rsHeader = mock(ResultSet.class);
        when(stmtHeader.executeQuery("SELECT * FROM \"TRAW_NULL\" WHERE 1=0")).thenReturn(rsHeader);
        ResultSetMetaData mdHeader = mock(ResultSetMetaData.class);
        when(rsHeader.getMetaData()).thenReturn(mdHeader);
        when(mdHeader.getColumnCount()).thenReturn(2);
        when(mdHeader.getColumnLabel(1)).thenReturn("RAW_COL");
        when(mdHeader.getColumnLabel(2)).thenReturn("TXT_COL");

        ResultSet rsData = mock(ResultSet.class);
        when(stmtData.executeQuery("SELECT * FROM \"TRAW_NULL\" ORDER BY \"RAW_COL\" ASC"))
                .thenReturn(rsData);
        ResultSetMetaData mdData = mock(ResultSetMetaData.class);
        when(rsData.getMetaData()).thenReturn(mdData);
        when(mdData.getColumnCount()).thenReturn(2);
        when(mdData.getColumnType(1)).thenReturn(Types.OTHER);
        when(mdData.getColumnTypeName(1)).thenReturn("LONG RAW");
        when(mdData.getColumnType(2)).thenReturn(Types.VARCHAR);
        when(mdData.getColumnTypeName(2)).thenReturn("VARCHAR2");
        when(rsData.next()).thenReturn(true, false);
        when(rsData.getBytes(1)).thenReturn(new byte[] {0x0A});
        when(rsData.getObject(2)).thenReturn(null);

        File csvFile = tempDir.resolve("TRAW_NULL.csv").toFile();
        Method method = DataDumper.class.getDeclaredMethod("exportTableAsCsvUtf8", Connection.class,
                String.class, File.class, DbDialectHandler.class);
        method.setAccessible(true);
        method.invoke(dumper, conn, "TRAW_NULL", csvFile, dialectHandler);

        CSVFormat fmt = CSVFormat.DEFAULT.builder().setHeader("RAW_COL", "TXT_COL")
                .setSkipHeaderRecord(true).get();
        try (CSVParser parser = CSVParser.parse(csvFile, StandardCharsets.UTF_8, fmt)) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals(1, records.size());
            assertEquals("0A", records.get(0).get("RAW_COL"));
            assertEquals("", records.get(0).get("TXT_COL"));
        }
    }

    @Test
    void exportTableAsCsvUtf8_正常ケース_longVarBinaryを指定する_16進文字列が出力されること() throws Exception {
        DataDumper dumper = createDumper();
        DbDialectHandler dialectHandler = createDialectHandlerMock();
        when(dialectHandler.quoteIdentifier(any()))
                .thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");
        Connection conn = mock(Connection.class);
        when(conn.getSchema()).thenReturn("APP");
        when(conn.getCatalog()).thenReturn(null);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet pkRs = mock(ResultSet.class);
        when(meta.getPrimaryKeys(any(), eq("APP"), eq("TLVB"))).thenReturn(pkRs);
        when(pkRs.next()).thenReturn(false);
        Statement stmtHeader = mock(Statement.class);
        Statement stmtData = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmtHeader, stmtData);
        ResultSet rsHeader = mock(ResultSet.class);
        when(stmtHeader.executeQuery("SELECT * FROM \"TLVB\" WHERE 1=0")).thenReturn(rsHeader);
        ResultSetMetaData mdHeader = mock(ResultSetMetaData.class);
        when(rsHeader.getMetaData()).thenReturn(mdHeader);
        when(mdHeader.getColumnCount()).thenReturn(1);
        when(mdHeader.getColumnLabel(1)).thenReturn("B");
        ResultSet rsData = mock(ResultSet.class);
        when(stmtData.executeQuery("SELECT * FROM \"TLVB\" ORDER BY \"B\" ASC")).thenReturn(rsData);
        ResultSetMetaData mdData = mock(ResultSetMetaData.class);
        when(rsData.getMetaData()).thenReturn(mdData);
        when(mdData.getColumnCount()).thenReturn(1);
        when(mdData.getColumnType(1)).thenReturn(Types.LONGVARBINARY);
        when(mdData.getColumnTypeName(1)).thenReturn("LONGVARBINARY");
        when(rsData.next()).thenReturn(true, false);
        when(rsData.getBytes(1)).thenReturn(new byte[] {0x0F});
        File csvFile = tempDir.resolve("TLVB.csv").toFile();
        Method method = DataDumper.class.getDeclaredMethod("exportTableAsCsvUtf8", Connection.class,
                String.class, File.class, DbDialectHandler.class);
        method.setAccessible(true);
        method.invoke(dumper, conn, "TLVB", csvFile, dialectHandler);
        CSVFormat fmt = CSVFormat.DEFAULT.builder().setHeader("B").setSkipHeaderRecord(true).get();
        try (CSVParser parser = CSVParser.parse(csvFile, StandardCharsets.UTF_8, fmt)) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals("0F", records.get(0).get("B"));
        }
    }

    @Test
    void exportTableAsCsvUtf8_正常ケース_varBinaryを指定する_16進文字列が出力されること() throws Exception {
        DataDumper dumper = createDumper();
        DbDialectHandler dialectHandler = createDialectHandlerMock();
        when(dialectHandler.quoteIdentifier(any()))
                .thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");
        Connection conn = mock(Connection.class);
        when(conn.getSchema()).thenReturn("APP");
        when(conn.getCatalog()).thenReturn(null);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet pkRs = mock(ResultSet.class);
        when(meta.getPrimaryKeys(any(), eq("APP"), eq("TVB"))).thenReturn(pkRs);
        when(pkRs.next()).thenReturn(false);
        Statement stmtHeader = mock(Statement.class);
        Statement stmtData = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmtHeader, stmtData);
        ResultSet rsHeader = mock(ResultSet.class);
        when(stmtHeader.executeQuery("SELECT * FROM \"TVB\" WHERE 1=0")).thenReturn(rsHeader);
        ResultSetMetaData mdHeader = mock(ResultSetMetaData.class);
        when(rsHeader.getMetaData()).thenReturn(mdHeader);
        when(mdHeader.getColumnCount()).thenReturn(1);
        when(mdHeader.getColumnLabel(1)).thenReturn("B");
        ResultSet rsData = mock(ResultSet.class);
        when(stmtData.executeQuery("SELECT * FROM \"TVB\" ORDER BY \"B\" ASC")).thenReturn(rsData);
        ResultSetMetaData mdData = mock(ResultSetMetaData.class);
        when(rsData.getMetaData()).thenReturn(mdData);
        when(mdData.getColumnCount()).thenReturn(1);
        when(mdData.getColumnType(1)).thenReturn(Types.VARBINARY);
        when(mdData.getColumnTypeName(1)).thenReturn("VARBINARY");
        when(rsData.next()).thenReturn(true, false);
        when(rsData.getBytes(1)).thenReturn(new byte[] {0x0C});
        File csvFile = tempDir.resolve("TVB.csv").toFile();
        Method method = DataDumper.class.getDeclaredMethod("exportTableAsCsvUtf8", Connection.class,
                String.class, File.class, DbDialectHandler.class);
        method.setAccessible(true);
        method.invoke(dumper, conn, "TVB", csvFile, dialectHandler);
        CSVFormat fmt = CSVFormat.DEFAULT.builder().setHeader("B").setSkipHeaderRecord(true).get();
        try (CSVParser parser = CSVParser.parse(csvFile, StandardCharsets.UTF_8, fmt)) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals("0C", records.get(0).get("B"));
        }
    }

    @Test
    void exportTableAsCsvUtf8_正常ケース_sqlTypeDateとtime列で型取得がnullを返す_生値が日時整形へ渡されること()
            throws Exception {
        DataDumper dumper = createDumper();
        DbDialectHandler dialectHandler = createDialectHandlerMock();
        when(dialectHandler.quoteIdentifier(any()))
                .thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");

        Connection conn = mock(Connection.class);
        when(conn.getSchema()).thenReturn("APP");
        when(conn.getCatalog()).thenReturn(null);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet pkRs = mock(ResultSet.class);
        when(meta.getPrimaryKeys(any(), eq("APP"), eq("TDTIME"))).thenReturn(pkRs);
        when(pkRs.next()).thenReturn(false);

        Statement stmtHeader = mock(Statement.class);
        Statement stmtData = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmtHeader, stmtData);
        ResultSet rsHeader = mock(ResultSet.class);
        when(stmtHeader.executeQuery("SELECT * FROM \"TDTIME\" WHERE 1=0")).thenReturn(rsHeader);
        ResultSetMetaData mdHeader = mock(ResultSetMetaData.class);
        when(rsHeader.getMetaData()).thenReturn(mdHeader);
        when(mdHeader.getColumnCount()).thenReturn(2);
        when(mdHeader.getColumnLabel(1)).thenReturn("D_COL");
        when(mdHeader.getColumnLabel(2)).thenReturn("T_COL");

        ResultSet rsData = mock(ResultSet.class);
        when(stmtData.executeQuery("SELECT * FROM \"TDTIME\" ORDER BY \"D_COL\" ASC"))
                .thenReturn(rsData);
        ResultSetMetaData mdData = mock(ResultSetMetaData.class);
        when(rsData.getMetaData()).thenReturn(mdData);
        when(mdData.getColumnCount()).thenReturn(2);
        when(mdData.getColumnType(1)).thenReturn(Types.DATE);
        when(mdData.getColumnTypeName(1)).thenReturn("TIMESTAMP");
        when(mdData.getColumnLabel(1)).thenReturn("D_COL");
        when(mdData.getColumnType(2)).thenReturn(Types.TIME);
        when(mdData.getColumnTypeName(2)).thenReturn("TIME");
        when(mdData.getColumnLabel(2)).thenReturn("T_COL");
        when(rsData.next()).thenReturn(true, false);

        Object rawDate = new Object();
        Object rawTime = new Object();
        when(rsData.getObject(1)).thenReturn(rawDate);
        when(rsData.getDate(1)).thenReturn(null);
        when(rsData.getObject(2)).thenReturn(rawTime);
        when(rsData.getTime(2)).thenReturn(null);
        when(dialectHandler.formatDateTimeColumn("D_COL", rawDate, conn)).thenReturn("D_FMT");
        when(dialectHandler.formatDateTimeColumn("T_COL", rawTime, conn)).thenReturn("T_FMT");

        File csvFile = tempDir.resolve("TDTIME.csv").toFile();
        Method method = DataDumper.class.getDeclaredMethod("exportTableAsCsvUtf8", Connection.class,
                String.class, File.class, DbDialectHandler.class);
        method.setAccessible(true);
        method.invoke(dumper, conn, "TDTIME", csvFile, dialectHandler);

        CSVFormat fmt = CSVFormat.DEFAULT.builder().setHeader("D_COL", "T_COL")
                .setSkipHeaderRecord(true).get();
        try (CSVParser parser = CSVParser.parse(csvFile, StandardCharsets.UTF_8, fmt)) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals("D_FMT", records.get(0).get("D_COL"));
            assertEquals("T_FMT", records.get(0).get("T_COL"));
        }
    }

    @Test
    void dumpBlobClob_型別null処理とクォートSQLを適用すること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        DataDumper dumper = createDumper(filePatternConfig);
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

        Method method = DataDumper.class.getDeclaredMethod("dumpBlobClob", Connection.class,
                String.class, File.class, File.class, FileNameResolver.class, String.class,
                DbDialectHandler.class);
        method.setAccessible(true);
        DumpResult result = (DumpResult) method.invoke(dumper, conn, "1TABLE", dbDirPath.toFile(),
                filesDirPath.toFile(), new FileNameResolver(filePatternConfig), "APP",
                dialectHandler);

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
    void dumpBlobClob_正常ケース_interval列を含む_日時整形値が設定されること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        DataDumper dumper = createDumper(filePatternConfig);
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

        Method method = DataDumper.class.getDeclaredMethod("dumpBlobClob", Connection.class,
                String.class, File.class, File.class, FileNameResolver.class, String.class,
                DbDialectHandler.class);
        method.setAccessible(true);
        DumpResult result = (DumpResult) method.invoke(dumper, conn, "TINTERVAL",
                dbDirPath.toFile(), filesDirPath.toFile(), new FileNameResolver(filePatternConfig),
                "APP", dialectHandler);
        assertEquals(1, result.getRowCount());

        CSVFormat fmt = CSVFormat.DEFAULT.builder().setHeader("INTERVAL_DS_COL")
                .setSkipHeaderRecord(true).get();
        try (CSVParser parser = CSVParser.parse(csvPath.toFile(), StandardCharsets.UTF_8, fmt)) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals("1 00:00:00", records.get(0).get("INTERVAL_DS_COL"));
        }
    }

    @Test
    void dumpBlobClob_正常ケース_timestampwithtimezone列を含む_方言整形値が設定されること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        DataDumper dumper = createDumper(filePatternConfig);
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

        Method method = DataDumper.class.getDeclaredMethod("dumpBlobClob", Connection.class,
                String.class, File.class, File.class, FileNameResolver.class, String.class,
                DbDialectHandler.class);
        method.setAccessible(true);
        DumpResult result = (DumpResult) method.invoke(dumper, conn, "TTSTZ", dbDirPath.toFile(),
                filesDirPath.toFile(), new FileNameResolver(filePatternConfig), "APP",
                dialectHandler);
        assertEquals(1, result.getRowCount());

        CSVFormat fmt =
                CSVFormat.DEFAULT.builder().setHeader("TS_TZ").setSkipHeaderRecord(true).get();
        try (CSVParser parser = CSVParser.parse(csvPath.toFile(), StandardCharsets.UTF_8, fmt)) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals("2026-02-15 01:02:03+09:00", records.get(0).get("TS_TZ"));
        }
    }

    @Test
    void dumpBlobClob_正常ケース_time列で型取得がnullを返す_生値が日時整形へ渡されること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        DataDumper dumper = createDumper(filePatternConfig);
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

        Method method = DataDumper.class.getDeclaredMethod("dumpBlobClob", Connection.class,
                String.class, File.class, File.class, FileNameResolver.class, String.class,
                DbDialectHandler.class);
        method.setAccessible(true);
        DumpResult result = (DumpResult) method.invoke(dumper, conn, "TTIME", dbDirPath.toFile(),
                filesDirPath.toFile(), new FileNameResolver(filePatternConfig), "APP",
                dialectHandler);
        assertEquals(1, result.getRowCount());

        CSVFormat fmt =
                CSVFormat.DEFAULT.builder().setHeader("TIME_COL").setSkipHeaderRecord(true).get();
        try (CSVParser parser = CSVParser.parse(csvPath.toFile(), StandardCharsets.UTF_8, fmt)) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals("12:34:56", records.get(0).get("TIME_COL"));
        }
    }

    @Test
    void dumpBlobClob_正常ケース_nclob列を含む_file参照が設定されること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        DataDumper dumper = createDumper(filePatternConfig);
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

        Method method = DataDumper.class.getDeclaredMethod("dumpBlobClob", Connection.class,
                String.class, File.class, File.class, FileNameResolver.class, String.class,
                DbDialectHandler.class);
        method.setAccessible(true);
        DumpResult result = (DumpResult) method.invoke(dumper, conn, "TNClob", dbDirPath.toFile(),
                filesDirPath.toFile(), new FileNameResolver(filePatternConfig), "APP",
                dialectHandler);

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
    void dumpBlobClob_正常ケース_ソートで数値変換失敗かつ同値である_比較結果0で完了すること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        DataDumper dumper = createDumper(filePatternConfig);
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

        Method method = DataDumper.class.getDeclaredMethod("dumpBlobClob", Connection.class,
                String.class, File.class, File.class, FileNameResolver.class, String.class,
                DbDialectHandler.class);
        method.setAccessible(true);
        DumpResult result = (DumpResult) method.invoke(dumper, conn, "TSORT_EQ", dbDirPath.toFile(),
                filesDirPath.toFile(), new FileNameResolver(filePatternConfig), "APP",
                dialectHandler);
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
    void dumpBlobClob_正常ケース_型分岐を複合指定する_各列が期待形式へ変換されること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        DataDumper dumper = createDumper(filePatternConfig);
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

        Method method = DataDumper.class.getDeclaredMethod("dumpBlobClob", Connection.class,
                String.class, File.class, File.class, FileNameResolver.class, String.class,
                DbDialectHandler.class);
        method.setAccessible(true);
        DumpResult result = (DumpResult) method.invoke(dumper, conn, "TMULTI", dbDirPath.toFile(),
                filesDirPath.toFile(), new FileNameResolver(filePatternConfig), "APP",
                dialectHandler);
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
    void dumpBlobClob_正常ケース_重複ヘッダを含むCSVを指定する_先勝ちインデックスで処理されること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        DataDumper dumper = createDumper(filePatternConfig);
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
        Method method = DataDumper.class.getDeclaredMethod("dumpBlobClob", Connection.class,
                String.class, File.class, File.class, FileNameResolver.class, String.class,
                DbDialectHandler.class);
        method.setAccessible(true);
        method.invoke(dumper, conn, "TDUP", dbDirPath.toFile(), filesDirPath.toFile(),
                new FileNameResolver(filePatternConfig), "APP", dialectHandler);
        assertTrue(Files.exists(csvPath));
    }

    @Test
    void dumpBlobClob_正常ケース_blob非nullでファイル参照へ置換すること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        DataDumper dumper = createDumper(filePatternConfig);
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

        Method method = DataDumper.class.getDeclaredMethod("dumpBlobClob", Connection.class,
                String.class, File.class, File.class, FileNameResolver.class, String.class,
                DbDialectHandler.class);
        method.setAccessible(true);
        DumpResult result = (DumpResult) method.invoke(dumper, conn, "1TABLE", dbDirPath.toFile(),
                filesDirPath.toFile(), new FileNameResolver(filePatternConfig), "APP",
                dialectHandler);

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
    void dumpBlobClob_正常ケース_timestampnclobvarbinaryを処理する_型別の期待値であること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        DataDumper dumper = createDumper(filePatternConfig);
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

        Method method = DataDumper.class.getDeclaredMethod("dumpBlobClob", Connection.class,
                String.class, File.class, File.class, FileNameResolver.class, String.class,
                DbDialectHandler.class);
        method.setAccessible(true);
        DumpResult result = (DumpResult) method.invoke(dumper, conn, "1TABLE", dbDirPath.toFile(),
                filesDirPath.toFile(), new FileNameResolver(filePatternConfig), "APP",
                dialectHandler);

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
    void dumpBlobClob_異常ケース_blob非nullでパターン未定義_IllegalStateExceptionが送出されること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        DataDumper dumper = createDumper(filePatternConfig);
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

        Method method = DataDumper.class.getDeclaredMethod("dumpBlobClob", Connection.class,
                String.class, File.class, File.class, FileNameResolver.class, String.class,
                DbDialectHandler.class);
        method.setAccessible(true);

        InvocationTargetException ex = assertThrows(InvocationTargetException.class,
                () -> method.invoke(dumper, conn, "TERR", dbDirPath.toFile(), filesDirPath.toFile(),
                        new FileNameResolver(filePatternConfig), "APP", dialectHandler));
        assertTrue(ex.getCause() instanceof IllegalStateException);
    }

    @Test
    void dumpBlobClob_異常ケース_パターン取得が途中で空になる_IllegalStateExceptionが送出されること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        DataDumper dumper = createDumper(filePatternConfig);
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

        Method method = DataDumper.class.getDeclaredMethod("dumpBlobClob", Connection.class,
                String.class, File.class, File.class, FileNameResolver.class, String.class,
                DbDialectHandler.class);
        method.setAccessible(true);
        InvocationTargetException ex = assertThrows(InvocationTargetException.class,
                () -> method.invoke(dumper, conn, "TPATTERN_EMPTY", dbDirPath.toFile(),
                        filesDirPath.toFile(), new FileNameResolver(filePatternConfig), "APP",
                        dialectHandler));
        assertTrue(ex.getCause() instanceof IllegalStateException);
    }

    @Test
    void dumpBlobClob_正常ケース_主キーで数値ソートする_並び順が昇順になること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        DataDumper dumper = createDumper(filePatternConfig);
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

        Method method = DataDumper.class.getDeclaredMethod("dumpBlobClob", Connection.class,
                String.class, File.class, File.class, FileNameResolver.class, String.class,
                DbDialectHandler.class);
        method.setAccessible(true);
        DumpResult result = (DumpResult) method.invoke(dumper, conn, "TSORT", dbDirPath.toFile(),
                filesDirPath.toFile(), new FileNameResolver(filePatternConfig), "APP",
                dialectHandler);
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
    void dumpBlobClob_正常ケース_csvファイルが存在しない_件数ゼロが返ること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        DataDumper dumper = createDumper(filePatternConfig);
        Method method = DataDumper.class.getDeclaredMethod("dumpBlobClob", Connection.class,
                String.class, File.class, File.class, FileNameResolver.class, String.class,
                DbDialectHandler.class);
        method.setAccessible(true);
        Path dbDirPath = Files.createDirectories(tempDir.resolve("db_missing_csv"));
        Path filesDirPath = Files.createDirectories(dbDirPath.resolve("files"));
        DumpResult result = (DumpResult) method.invoke(dumper, mock(Connection.class), "NOFILE",
                dbDirPath.toFile(), filesDirPath.toFile(), new FileNameResolver(filePatternConfig),
                "APP", createDialectHandlerMock());
        assertEquals(0, result.getRowCount());
        assertEquals(0, result.getFileCount());
    }

    @Test
    void dumpBlobClob_異常ケース_テーブルのパターン定義がnullである_IllegalStateExceptionが送出されること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        DataDumper dumper = createDumper(filePatternConfig);
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

        Method method = DataDumper.class.getDeclaredMethod("dumpBlobClob", Connection.class,
                String.class, File.class, File.class, FileNameResolver.class, String.class,
                DbDialectHandler.class);
        method.setAccessible(true);
        InvocationTargetException ex = assertThrows(InvocationTargetException.class,
                () -> method.invoke(dumper, conn, "TNOPAT", dbDirPath.toFile(),
                        filesDirPath.toFile(), new FileNameResolver(filePatternConfig), "APP",
                        dialectHandler));
        assertTrue(ex.getCause() instanceof IllegalStateException);
    }

    @Test
    void dumpBlobClob_正常ケース_ヘッダにない列を含む_未定義列は無視されること() throws Exception {
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        DataDumper dumper = createDumper(filePatternConfig);
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

        Method method = DataDumper.class.getDeclaredMethod("dumpBlobClob", Connection.class,
                String.class, File.class, File.class, FileNameResolver.class, String.class,
                DbDialectHandler.class);
        method.setAccessible(true);
        DumpResult result = (DumpResult) method.invoke(dumper, conn, "THEADER_SKIP",
                dbDirPath.toFile(), filesDirPath.toFile(), new FileNameResolver(filePatternConfig),
                "APP", dialectHandler);
        assertEquals(1, result.getRowCount());

        CSVFormat fmt = CSVFormat.DEFAULT.builder().setHeader("ID").setSkipHeaderRecord(true).get();
        try (CSVParser parser = CSVParser.parse(csvPath.toFile(), StandardCharsets.UTF_8, fmt)) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals(1, records.size());
            assertEquals("1", records.get(0).get("ID"));
        }
    }

    @Test
    void execute_異常ケース_シナリオが未指定である_ErrorHandlerが呼ばれること() {
        DataDumper dumper = createDumper();
        try (MockedStatic<ErrorHandler> handler =
                org.mockito.Mockito.mockStatic(ErrorHandler.class)) {
            handler.when(() -> ErrorHandler.errorAndExit(any())).thenAnswer(inv -> {
                throw new IllegalStateException("exit");
            });
            assertThrows(IllegalStateException.class, () -> dumper.execute(null, null));
            handler.verify(
                    () -> ErrorHandler.errorAndExit("Scenario name is required in --dump mode."));
        }
    }

    @Test
    void ensureDirectoryExists_異常ケース_mkdirs失敗するパスを指定する_ErrorHandlerが呼ばれること() throws Exception {
        DataDumper dumper = createDumper();
        Path blockingFile = tempDir.resolve("blocking");
        Files.writeString(blockingFile, "x", StandardCharsets.UTF_8);
        File target = blockingFile.resolve("child").toFile();

        Method method = DataDumper.class.getDeclaredMethod("ensureDirectoryExists", File.class,
                String.class);
        method.setAccessible(true);

        try (MockedStatic<ErrorHandler> handler =
                org.mockito.Mockito.mockStatic(ErrorHandler.class)) {
            handler.when(() -> ErrorHandler.errorAndExit(any())).thenAnswer(inv -> {
                throw new IllegalStateException("exit");
            });
            assertThrows(Exception.class, () -> method.invoke(dumper, target, "msg"));
            handler.verify(() -> ErrorHandler.errorAndExit("msg: " + target.getAbsolutePath()));
        }
    }

    @Test
    void backupScenarioDirectory_正常ケース_既存シナリオを指定する_バックアップ先へリネームされること() throws Exception {
        DataDumper dumper = createDumper();
        Path dataPath = tempDir.resolve("data");
        Path scenario = dataPath.resolve("scn");
        Files.createDirectories(scenario);
        Files.writeString(scenario.resolve("a.txt"), "x", StandardCharsets.UTF_8);

        Method method = DataDumper.class.getDeclaredMethod("backupScenarioDirectory", Path.class,
                File.class);
        method.setAccessible(true);
        method.invoke(dumper, dataPath, scenario.toFile());

        assertTrue(Files.exists(dataPath));
        assertTrue(Files.notExists(scenario));
        boolean found;
        try (java.util.stream.Stream<Path> stream = Files.list(dataPath)) {
            found = stream.anyMatch(p -> p.getFileName().toString().startsWith("scn_"));
        }
        assertTrue(found);
    }

    @Test
    void backupScenarioDirectory_異常ケース_リネームが失敗する_ErrorHandlerが呼ばれること() throws Exception {
        DataDumper dumper = createDumper();
        Method method = DataDumper.class.getDeclaredMethod("backupScenarioDirectory", Path.class,
                File.class);
        method.setAccessible(true);

        class RenameFailFile extends File {
            private static final long serialVersionUID = 1L;

            RenameFailFile(String pathname) {
                super(pathname);
            }

            @Override
            public boolean renameTo(File dest) {
                return false;
            }
        }
        Path dataPath = Files.createDirectories(tempDir.resolve("data_rename_fail"));
        File scenario = new RenameFailFile(dataPath.resolve("scn").toString());
        try (MockedStatic<ErrorHandler> handler =
                org.mockito.Mockito.mockStatic(ErrorHandler.class)) {
            handler.when(() -> ErrorHandler.errorAndExit(any())).thenAnswer(inv -> null);
            method.invoke(dumper, dataPath, scenario);
            handler.verify(() -> ErrorHandler.errorAndExit(any()));
        }
    }

    @Test
    void prepareDbOutputDirs_正常ケース_ディレクトリ未作成で呼ぶ_DB用とfiles用が作成されること() throws Exception {
        DataDumper dumper = createDumper();
        Method method =
                DataDumper.class.getDeclaredMethod("prepareDbOutputDirs", File.class, String.class);
        method.setAccessible(true);

        File scenarioDir = tempDir.resolve("scenario").toFile();
        File[] dirs = (File[]) method.invoke(dumper, scenarioDir, "db1");

        assertEquals(2, dirs.length);
        assertTrue(dirs[0].exists());
        assertTrue(dirs[1].exists());
    }

    @Test
    void ensureDirectoryExists_正常ケース_既存ディレクトリを指定する_例外なく完了すること() throws Exception {
        DataDumper dumper = createDumper();
        Method method = DataDumper.class.getDeclaredMethod("ensureDirectoryExists", File.class,
                String.class);
        method.setAccessible(true);
        File existing = Files.createDirectories(tempDir.resolve("already_exists")).toFile();
        method.invoke(dumper, existing, "unused");
        assertTrue(existing.exists());
    }

    @Test
    void ensureDirectoryExists_異常ケース_mkdirs失敗時に終了処理を継続する_ErrorHandler呼び出し行が通過すること()
            throws Exception {
        DataDumper dumper = createDumper();
        Path blockingFile = tempDir.resolve("blocking2");
        Files.writeString(blockingFile, "x", StandardCharsets.UTF_8);
        File target = blockingFile.resolve("child").toFile();
        Method method = DataDumper.class.getDeclaredMethod("ensureDirectoryExists", File.class,
                String.class);
        method.setAccessible(true);
        try (MockedStatic<ErrorHandler> handler =
                org.mockito.Mockito.mockStatic(ErrorHandler.class)) {
            handler.when(() -> ErrorHandler.errorAndExit(any())).thenAnswer(inv -> null);
            method.invoke(dumper, target, "msg");
            handler.verify(() -> ErrorHandler.errorAndExit("msg: " + target.getAbsolutePath()));
        }
    }

    @Test
    void execute_正常ケース_対象DBに含まれない接続を指定する_DriverManager接続が呼ばれないこと() {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.resolve("data1").toString());

        ConnectionConfig config = new ConnectionConfig();
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:dummy");
        entry.setUser("u");
        entry.setPassword("p");
        config.setConnections(List.of(entry));

        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(List.of());

        DataDumper dumper = new DataDumper(pathsConfig, config, new FilePatternConfig(), dumpConfig,
                e -> createDialectHandlerMock());

        try (MockedStatic<DriverManager> driverManager =
                org.mockito.Mockito.mockStatic(DriverManager.class)) {
            dumper.execute("scenario", List.of("db2"));
            driverManager.verifyNoInteractions();
        }
    }

    @Test
    void execute_正常ケース_対象テーブルが0件である_DBUnit接続がクローズされること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.resolve("data2").toString());
        Files.createDirectories(tempDir.resolve("data2"));

        ConnectionConfig config = new ConnectionConfig();
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:dummy");
        entry.setUser("u");
        entry.setPassword("p");
        config.setConnections(List.of(entry));

        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(null);

        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        DbDialectHandler dialectHandler = createDialectHandlerMock();
        org.dbunit.database.DatabaseConnection dbConn =
                mock(org.dbunit.database.DatabaseConnection.class);

        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getTables(null, "APP", "%", new String[] {"TABLE"})).thenReturn(rs);
        when(rs.next()).thenReturn(false);
        when(dialectHandler.createDbUnitConnection(conn, "APP")).thenReturn(dbConn);

        DataDumper dumper = new DataDumper(pathsConfig, config, new FilePatternConfig(), dumpConfig,
                e -> dialectHandler);

        try (MockedStatic<DriverManager> driverManager =
                org.mockito.Mockito.mockStatic(DriverManager.class)) {
            driverManager.when(() -> DriverManager.getConnection("jdbc:dummy", "u", "p"))
                    .thenReturn(conn);
            dumper.execute("scenario", List.of("db1"));
        }

        verify(dialectHandler).createDbUnitConnection(conn, "APP");
        verify(dbConn).close();
    }

    @Test
    void execute_正常ケース_既存シナリオをバックアップして1テーブルを出力する_出力CSVが生成されること() throws Exception {
        Path dataRoot = Files.createDirectories(tempDir.resolve("data_exec_ok"));
        Path dumpRoot = Files.createDirectories(dataRoot.resolve("dump"));
        Path scenarioExisting = Files.createDirectories(dumpRoot.resolve("scn_ok"));
        Files.writeString(scenarioExisting.resolve("old.txt"), "x", StandardCharsets.UTF_8);

        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(dataRoot.toString());

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:execok");
        entry.setUser("u");
        entry.setPassword("p");
        ConnectionConfig config = new ConnectionConfig();
        config.setConnections(List.of(entry));

        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(List.of());

        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        when(filePatternConfig.getPatternsForTable("T1")).thenReturn(Collections.emptyMap());

        Connection conn = mock(Connection.class);
        when(conn.getSchema()).thenReturn("APP");
        when(conn.getCatalog()).thenReturn(null);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet tableRs = mock(ResultSet.class);
        when(meta.getTables(null, "APP", "%", new String[] {"TABLE"})).thenReturn(tableRs);
        when(tableRs.next()).thenReturn(true, false);
        when(tableRs.getString("TABLE_NAME")).thenReturn("T1");
        ResultSet pkRsForExport = mock(ResultSet.class);
        ResultSet pkRsForDump = mock(ResultSet.class);
        when(meta.getPrimaryKeys(any(), eq("APP"), eq("T1"))).thenReturn(pkRsForExport,
                pkRsForDump);
        when(pkRsForExport.next()).thenReturn(false);
        when(pkRsForDump.next()).thenReturn(false);

        Statement stmtHeader = mock(Statement.class);
        Statement stmtData = mock(Statement.class);
        Statement stmtDump = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmtHeader, stmtData, stmtDump);

        ResultSet rsHeader = mock(ResultSet.class);
        when(stmtHeader.executeQuery("SELECT * FROM \"T1\" WHERE 1=0")).thenReturn(rsHeader);
        ResultSetMetaData mdHeader = mock(ResultSetMetaData.class);
        when(rsHeader.getMetaData()).thenReturn(mdHeader);
        when(mdHeader.getColumnCount()).thenReturn(1);
        when(mdHeader.getColumnLabel(1)).thenReturn("ID");

        ResultSet rsData = mock(ResultSet.class);
        when(stmtData.executeQuery("SELECT * FROM \"T1\" ORDER BY \"ID\" ASC")).thenReturn(rsData);
        ResultSetMetaData mdData = mock(ResultSetMetaData.class);
        when(rsData.getMetaData()).thenReturn(mdData);
        when(mdData.getColumnCount()).thenReturn(1);
        when(mdData.getColumnType(1)).thenReturn(Types.VARCHAR);
        when(mdData.getColumnTypeName(1)).thenReturn("VARCHAR2");
        when(rsData.next()).thenReturn(true, false);
        when(rsData.getObject(1)).thenReturn("1");

        ResultSet rsDump = mock(ResultSet.class);
        when(stmtDump.executeQuery("SELECT * FROM \"T1\"")).thenReturn(rsDump);
        ResultSetMetaData mdDump = mock(ResultSetMetaData.class);
        when(rsDump.getMetaData()).thenReturn(mdDump);
        when(mdDump.getColumnCount()).thenReturn(1);
        when(mdDump.getColumnLabel(1)).thenReturn("ID");
        when(mdDump.getColumnType(1)).thenReturn(Types.VARCHAR);
        when(rsDump.next()).thenReturn(true, false);
        when(rsDump.getString(1)).thenReturn("1");

        DbDialectHandler dialect = createDialectHandlerMock();
        when(dialect.quoteIdentifier(any())).thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");
        when(dialect.createDbUnitConnection(eq(conn), eq("APP")))
                .thenReturn(mock(org.dbunit.database.DatabaseConnection.class));

        DataDumper dumper =
                new DataDumper(pathsConfig, config, filePatternConfig, dumpConfig, e -> dialect);
        try (MockedStatic<DriverManager> driverManager =
                org.mockito.Mockito.mockStatic(DriverManager.class)) {
            driverManager.when(() -> DriverManager.getConnection("jdbc:execok", "u", "p"))
                    .thenReturn(conn);
            dumper.execute("scn_ok", List.of("db1"));
        }

        Path outCsv = dumpRoot.resolve("scn_ok").resolve("db1").resolve("T1.csv");
        assertTrue(Files.exists(outCsv));
    }

    @Test
    void execute_異常ケース_ドライバ読込に失敗する_ErrorHandlerが呼ばれること() {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.resolve("data_exec_fail").toString());
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        entry.setDriverClass("no.such.Driver");
        entry.setUrl("jdbc:never");
        entry.setUser("u");
        entry.setPassword("p");
        ConnectionConfig config = new ConnectionConfig();
        config.setConnections(List.of(entry));
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(List.of());
        DataDumper dumper = new DataDumper(pathsConfig, config, new FilePatternConfig(), dumpConfig,
                e -> createDialectHandlerMock());
        try (MockedStatic<ErrorHandler> handler =
                org.mockito.Mockito.mockStatic(ErrorHandler.class)) {
            handler.when(() -> ErrorHandler.errorAndExit(any(), any())).thenAnswer(inv -> {
                throw new IllegalStateException("exit");
            });
            assertThrows(IllegalStateException.class,
                    () -> dumper.execute("scn_fail", List.of("db1")));
            handler.verify(() -> ErrorHandler.errorAndExit(eq("Dump failed (DB=db1)"), any()));
        }
    }

    @Test
    void execute_異常ケース_シナリオ未指定でも終了処理を継続する_必須チェック行が通過すること() {
        DataDumper dumper = createDumper();
        try (MockedStatic<ErrorHandler> handler =
                org.mockito.Mockito.mockStatic(ErrorHandler.class)) {
            handler.when(() -> ErrorHandler.errorAndExit(any())).thenAnswer(inv -> null);
            assertThrows(NullPointerException.class, () -> dumper.execute(null, null));
            handler.verify(
                    () -> ErrorHandler.errorAndExit("Scenario name is required in --dump mode."));
        }
    }

    @Test
    void execute_異常ケース_シナリオが空文字である_必須チェック行が通過すること() {
        DataDumper dumper = createDumper();
        try (MockedStatic<ErrorHandler> handler =
                org.mockito.Mockito.mockStatic(ErrorHandler.class)) {
            handler.when(() -> ErrorHandler.errorAndExit(any())).thenAnswer(inv -> null);
            assertThrows(NullPointerException.class, () -> dumper.execute("", null));
            handler.verify(
                    () -> ErrorHandler.errorAndExit("Scenario name is required in --dump mode."));
        }
    }

    @Test
    void execute_正常ケース_対象DBリストが空である_全DBが処理されること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        Path dataRoot = Files.createDirectories(tempDir.resolve("data_exec_empty_target"));
        pathsConfig.setDataPath(dataRoot.toString());
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:emptytarget");
        entry.setUser("u");
        entry.setPassword("p");
        ConnectionConfig config = new ConnectionConfig();
        config.setConnections(List.of(entry));
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(List.of());
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        DbDialectHandler dialect = createDialectHandlerMock();
        org.dbunit.database.DatabaseConnection dbConn =
                mock(org.dbunit.database.DatabaseConnection.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getTables(null, "APP", "%", new String[] {"TABLE"})).thenReturn(rs);
        when(rs.next()).thenReturn(false);
        when(dialect.createDbUnitConnection(conn, "APP")).thenReturn(dbConn);
        DataDumper dumper = new DataDumper(pathsConfig, config, new FilePatternConfig(), dumpConfig,
                e -> dialect);
        try (MockedStatic<DriverManager> driverManager =
                org.mockito.Mockito.mockStatic(DriverManager.class)) {
            driverManager.when(() -> DriverManager.getConnection("jdbc:emptytarget", "u", "p"))
                    .thenReturn(conn);
            dumper.execute("scenario_empty_target", List.of());
        }
        verify(dbConn).close();
    }

    @Test
    void execute_正常ケース_対象DBリストがnullである_全DBが処理されること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        Path dataRoot = Files.createDirectories(tempDir.resolve("data_exec_null_target"));
        pathsConfig.setDataPath(dataRoot.toString());
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:nulltarget");
        entry.setUser("u");
        entry.setPassword("p");
        ConnectionConfig config = new ConnectionConfig();
        config.setConnections(List.of(entry));
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(List.of());
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        DbDialectHandler dialect = createDialectHandlerMock();
        org.dbunit.database.DatabaseConnection dbConn =
                mock(org.dbunit.database.DatabaseConnection.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getTables(null, "APP", "%", new String[] {"TABLE"})).thenReturn(rs);
        when(rs.next()).thenReturn(false);
        when(dialect.createDbUnitConnection(conn, "APP")).thenReturn(dbConn);
        DataDumper dumper = new DataDumper(pathsConfig, config, new FilePatternConfig(), dumpConfig,
                e -> dialect);
        try (MockedStatic<DriverManager> driverManager =
                org.mockito.Mockito.mockStatic(DriverManager.class)) {
            driverManager.when(() -> DriverManager.getConnection("jdbc:nulltarget", "u", "p"))
                    .thenReturn(conn);
            dumper.execute("scenario_null_target", null);
        }
        verify(dbConn).close();
    }

    @Test
    void execute_異常ケース_接続クローズで例外が発生する_ErrorHandlerが呼ばれること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        Path dataRoot = Files.createDirectories(tempDir.resolve("data_exec_close_fail"));
        pathsConfig.setDataPath(dataRoot.toString());
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:closefail");
        entry.setUser("u");
        entry.setPassword("p");
        ConnectionConfig config = new ConnectionConfig();
        config.setConnections(List.of(entry));
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(List.of());
        Connection conn = mock(Connection.class);
        doThrow(new java.sql.SQLException("close failed")).when(conn).close();
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        DbDialectHandler dialect = createDialectHandlerMock();
        org.dbunit.database.DatabaseConnection dbConn =
                mock(org.dbunit.database.DatabaseConnection.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getTables(null, "APP", "%", new String[] {"TABLE"})).thenReturn(rs);
        when(rs.next()).thenReturn(false);
        when(dialect.createDbUnitConnection(conn, "APP")).thenReturn(dbConn);
        DataDumper dumper = new DataDumper(pathsConfig, config, new FilePatternConfig(), dumpConfig,
                e -> dialect);
        try (MockedStatic<DriverManager> driverManager =
                org.mockito.Mockito.mockStatic(DriverManager.class);
                MockedStatic<ErrorHandler> handler =
                        org.mockito.Mockito.mockStatic(ErrorHandler.class)) {
            driverManager.when(() -> DriverManager.getConnection("jdbc:closefail", "u", "p"))
                    .thenReturn(conn);
            handler.when(() -> ErrorHandler.errorAndExit(any(), any())).thenAnswer(inv -> null);
            dumper.execute("scenario_close_fail", List.of("db1"));
            handler.verify(() -> ErrorHandler.errorAndExit(eq("Dump failed (DB=db1)"), any()));
        }
    }

    @Test
    void execute_異常ケース_ダンプ後の接続クローズで例外が発生する_ErrorHandlerが呼ばれること() throws Exception {
        Path dataRoot = Files.createDirectories(tempDir.resolve("data_exec_close_fail_after_dump"));
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(dataRoot.toString());
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:closefail2");
        entry.setUser("u");
        entry.setPassword("p");
        ConnectionConfig config = new ConnectionConfig();
        config.setConnections(List.of(entry));
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(List.of());
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        when(filePatternConfig.getPatternsForTable("T1")).thenReturn(Collections.emptyMap());
        Connection conn = mock(Connection.class);
        doThrow(new java.sql.SQLException("close failed")).when(conn).close();
        when(conn.getSchema()).thenReturn("APP");
        when(conn.getCatalog()).thenReturn(null);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet tableRs = mock(ResultSet.class);
        when(meta.getTables(null, "APP", "%", new String[] {"TABLE"})).thenReturn(tableRs);
        when(tableRs.next()).thenReturn(true, false);
        when(tableRs.getString("TABLE_NAME")).thenReturn("T1");
        ResultSet pkRsForExport = mock(ResultSet.class);
        ResultSet pkRsForDump = mock(ResultSet.class);
        when(meta.getPrimaryKeys(any(), eq("APP"), eq("T1"))).thenReturn(pkRsForExport,
                pkRsForDump);
        when(pkRsForExport.next()).thenReturn(false);
        when(pkRsForDump.next()).thenReturn(false);
        Statement stmtHeader = mock(Statement.class);
        Statement stmtData = mock(Statement.class);
        Statement stmtDump = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmtHeader, stmtData, stmtDump);
        ResultSet rsHeader = mock(ResultSet.class);
        when(stmtHeader.executeQuery("SELECT * FROM \"T1\" WHERE 1=0")).thenReturn(rsHeader);
        ResultSetMetaData mdHeader = mock(ResultSetMetaData.class);
        when(rsHeader.getMetaData()).thenReturn(mdHeader);
        when(mdHeader.getColumnCount()).thenReturn(1);
        when(mdHeader.getColumnLabel(1)).thenReturn("ID");
        ResultSet rsData = mock(ResultSet.class);
        when(stmtData.executeQuery("SELECT * FROM \"T1\" ORDER BY \"ID\" ASC")).thenReturn(rsData);
        ResultSetMetaData mdData = mock(ResultSetMetaData.class);
        when(rsData.getMetaData()).thenReturn(mdData);
        when(mdData.getColumnCount()).thenReturn(1);
        when(mdData.getColumnType(1)).thenReturn(Types.VARCHAR);
        when(mdData.getColumnTypeName(1)).thenReturn("VARCHAR2");
        when(rsData.next()).thenReturn(true, false);
        when(rsData.getObject(1)).thenReturn("1");
        ResultSet rsDump = mock(ResultSet.class);
        when(stmtDump.executeQuery("SELECT * FROM \"T1\"")).thenReturn(rsDump);
        ResultSetMetaData mdDump = mock(ResultSetMetaData.class);
        when(rsDump.getMetaData()).thenReturn(mdDump);
        when(mdDump.getColumnCount()).thenReturn(1);
        when(mdDump.getColumnLabel(1)).thenReturn("ID");
        when(mdDump.getColumnType(1)).thenReturn(Types.VARCHAR);
        when(rsDump.next()).thenReturn(true, false);
        when(rsDump.getString(1)).thenReturn("1");
        DbDialectHandler dialect = createDialectHandlerMock();
        when(dialect.quoteIdentifier(any())).thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");
        when(dialect.createDbUnitConnection(eq(conn), eq("APP")))
                .thenReturn(mock(org.dbunit.database.DatabaseConnection.class));
        DataDumper dumper =
                new DataDumper(pathsConfig, config, filePatternConfig, dumpConfig, e -> dialect);
        try (MockedStatic<DriverManager> driverManager =
                org.mockito.Mockito.mockStatic(DriverManager.class);
                MockedStatic<ErrorHandler> handler =
                        org.mockito.Mockito.mockStatic(ErrorHandler.class)) {
            driverManager.when(() -> DriverManager.getConnection("jdbc:closefail2", "u", "p"))
                    .thenReturn(conn);
            handler.when(() -> ErrorHandler.errorAndExit(any(), any())).thenAnswer(inv -> null);
            dumper.execute("scenario_close_fail_after_dump", List.of("db1"));
            handler.verify(() -> ErrorHandler.errorAndExit(eq("Dump failed (DB=db1)"), any()));
        }
    }

    @Test
    void execute_異常ケース_ドライバ読込失敗時に終了処理を継続する_失敗通知行が通過すること() {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.resolve("data_exec_fail2").toString());
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        entry.setDriverClass("no.such.Driver");
        entry.setUrl("jdbc:never");
        entry.setUser("u");
        entry.setPassword("p");
        ConnectionConfig config = new ConnectionConfig();
        config.setConnections(List.of(entry));
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(List.of());
        DataDumper dumper = new DataDumper(pathsConfig, config, new FilePatternConfig(), dumpConfig,
                e -> createDialectHandlerMock());
        try (MockedStatic<ErrorHandler> handler =
                org.mockito.Mockito.mockStatic(ErrorHandler.class)) {
            handler.when(() -> ErrorHandler.errorAndExit(any(), any())).thenAnswer(inv -> null);
            dumper.execute("scn_fail2", List.of("db1"));
            handler.verify(() -> ErrorHandler.errorAndExit(eq("Dump failed (DB=db1)"), any()));
        }
    }

    @Test
    void logTableSummary_正常ケース_複数テーブル件数を指定する_例外なく完了すること() throws Exception {
        DataDumper dumper = createDumper();
        Method method =
                DataDumper.class.getDeclaredMethod("logTableSummary", String.class, Map.class);
        method.setAccessible(true);

        Map<String, Integer> counts = new LinkedHashMap<>();
        counts.put("T1", 1);
        counts.put("LONG_TABLE", 12);

        method.invoke(dumper, "DB1", counts);
        assertEquals(2, counts.size());
    }

    private DataDumper createDumper() {
        return createDumper(mock(FilePatternConfig.class));
    }

    private DbDialectHandler createDialectHandlerMock() {
        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);
        when(dialectHandler.quoteIdentifier(any()))
                .thenAnswer(invocation -> "\"" + invocation.getArgument(0) + "\"");
        when(dialectHandler.resolveSchema(any())).thenReturn("APP");
        try {
            when(dialectHandler.formatDbValueForCsv(nullable(String.class), any()))
                    .thenAnswer(invocation -> {
                        Object value = invocation.getArgument(1);
                        if (value == null) {
                            return "";
                        }
                        return value.toString();
                    });
            when(dialectHandler.formatDateTimeColumn(nullable(String.class), any(), any()))
                    .thenAnswer(invocation -> {
                        Object value = invocation.getArgument(1);
                        if (value == null) {
                            return "";
                        }
                        return value.toString();
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
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to stub formatDbValueForCsv", e);
        }
        return dialectHandler;
    }

    private DataDumper createDumper(FilePatternConfig filePatternConfig) {
        PathsConfig pathsConfig = mock(PathsConfig.class);
        ConnectionConfig connectionConfig = mock(ConnectionConfig.class);
        DumpConfig dumpConfig = mock(DumpConfig.class);
        when(dumpConfig.getExcludeTables()).thenReturn(Collections.emptyList());

        return new DataDumper(pathsConfig, connectionConfig, filePatternConfig, dumpConfig,
                e -> createDialectHandlerMock());
    }
}
