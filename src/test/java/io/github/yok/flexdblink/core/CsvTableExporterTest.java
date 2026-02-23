package io.github.yok.flexdblink.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.github.yok.flexdblink.db.DbDialectHandler;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Locale;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CsvTableExporterTest {

    @TempDir
    Path tempDir;

    // -------------------------------------------------------------------------
    // export
    // -------------------------------------------------------------------------

    @Test
    void export_正常ケース_binary列がnullである_空文字が出力されること() throws Exception {
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
        new CsvTableExporter().export(conn, "1TABLE", csvFile, dialectHandler);

        try (CSVParser parser = CSVParser.parse(csvFile, StandardCharsets.UTF_8,
                CSVFormat.DEFAULT.builder().setHeader("1ID").setSkipHeaderRecord(true).get())) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals(1, records.size());
            assertEquals("", records.get(0).get("1ID"));
        }
    }

    @Test
    void export_正常ケース_binary列が非nullである_16進文字列が出力されること() throws Exception {
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
        new CsvTableExporter().export(conn, "1TABLE", csvFile, dialectHandler);

        try (CSVParser parser = CSVParser.parse(csvFile, StandardCharsets.UTF_8,
                CSVFormat.DEFAULT.builder().setHeader("1ID").setSkipHeaderRecord(true).get())) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals(1, records.size());
            assertEquals("012A", records.get(0).get("1ID"));
        }
    }

    @Test
    void export_正常ケース_主キーなしで数値ソートすること() throws Exception {
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
        new CsvTableExporter().export(conn, "TNUM", csvFile, dialectHandler);

        try (CSVParser parser = CSVParser.parse(csvFile, StandardCharsets.UTF_8,
                CSVFormat.DEFAULT.builder().setHeader("A", "B").setSkipHeaderRecord(true).get())) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals(2, records.size());
            assertEquals("2", records.get(0).get("A"));
            assertEquals("10", records.get(1).get("A"));
        }
    }

    @Test
    void export_正常ケース_主キーありで文字列ソートすること() throws Exception {
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
        new CsvTableExporter().export(conn, "TSTR", csvFile, dialectHandler);

        try (CSVParser parser = CSVParser.parse(csvFile, StandardCharsets.UTF_8,
                CSVFormat.DEFAULT.builder().setHeader("CODE").setSkipHeaderRecord(true).get())) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals(2, records.size());
            assertEquals("a", records.get(0).get("CODE"));
            assertEquals("b", records.get(1).get("CODE"));
        }
    }

    @Test
    void export_正常ケース_RAW型を指定する_16進文字列が出力されること() throws Exception {
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
        new CsvTableExporter().export(conn, "TRAW", csvFile, dialectHandler);

        try (CSVParser parser = CSVParser.parse(csvFile, StandardCharsets.UTF_8,
                CSVFormat.DEFAULT.builder().setHeader("RAW_COL").setSkipHeaderRecord(true).get())) {
            assertEquals("012A", parser.getRecords().get(0).get("RAW_COL"));
        }
    }

    @Test
    void export_正常ケース_比較値が同一である_比較結果0で完了すること() throws Exception {
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
        new CsvTableExporter().export(conn, "TEQ", csvFile, dialectHandler);

        try (CSVParser parser = CSVParser.parse(csvFile, StandardCharsets.UTF_8,
                CSVFormat.DEFAULT.builder().setHeader("ID").setSkipHeaderRecord(true).get())) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals(2, records.size());
            assertEquals("A", records.get(0).get("ID"));
            assertEquals("A", records.get(1).get("ID"));
        }
    }

    @Test
    void export_正常ケース_LONG_RAW列とnull列が混在する_16進と空文字が出力されること() throws Exception {
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
        new CsvTableExporter().export(conn, "TRAW_NULL", csvFile, dialectHandler);

        try (CSVParser parser = CSVParser.parse(csvFile, StandardCharsets.UTF_8, CSVFormat.DEFAULT
                .builder().setHeader("RAW_COL", "TXT_COL").setSkipHeaderRecord(true).get())) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals("0A", records.get(0).get("RAW_COL"));
            assertEquals("", records.get(0).get("TXT_COL"));
        }
    }

    @Test
    void export_正常ケース_LONGVARBINARY列を指定する_16進文字列が出力されること() throws Exception {
        DbDialectHandler dialectHandler = createDialectHandlerMock();
        when(dialectHandler.quoteIdentifier(any()))
                .thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");

        Connection conn = buildSingleColConn("APP", "TLVB", "B", Types.LONGVARBINARY,
                "LONGVARBINARY", null, new byte[] {0x0F});
        File csvFile = tempDir.resolve("TLVB.csv").toFile();
        new CsvTableExporter().export(conn, "TLVB", csvFile, dialectHandler);

        try (CSVParser parser = CSVParser.parse(csvFile, StandardCharsets.UTF_8,
                CSVFormat.DEFAULT.builder().setHeader("B").setSkipHeaderRecord(true).get())) {
            assertEquals("0F", parser.getRecords().get(0).get("B"));
        }
    }

    @Test
    void export_正常ケース_VARBINARY列を指定する_16進文字列が出力されること() throws Exception {
        DbDialectHandler dialectHandler = createDialectHandlerMock();
        when(dialectHandler.quoteIdentifier(any()))
                .thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");

        Connection conn = buildSingleColConn("APP", "TVB", "B", Types.VARBINARY, "VARBINARY", null,
                new byte[] {0x0C});
        File csvFile = tempDir.resolve("TVB.csv").toFile();
        new CsvTableExporter().export(conn, "TVB", csvFile, dialectHandler);

        try (CSVParser parser = CSVParser.parse(csvFile, StandardCharsets.UTF_8,
                CSVFormat.DEFAULT.builder().setHeader("B").setSkipHeaderRecord(true).get())) {
            assertEquals("0C", parser.getRecords().get(0).get("B"));
        }
    }

    @Test
    void export_正常ケース_DATE列とTIME列でgetDategetTimeがnullを返す_生値で日時整形されること() throws Exception {
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
        new CsvTableExporter().export(conn, "TDTIME", csvFile, dialectHandler);

        try (CSVParser parser = CSVParser.parse(csvFile, StandardCharsets.UTF_8, CSVFormat.DEFAULT
                .builder().setHeader("D_COL", "T_COL").setSkipHeaderRecord(true).get())) {
            List<CSVRecord> records = parser.getRecords();
            assertEquals("D_FMT", records.get(0).get("D_COL"));
            assertEquals("T_FMT", records.get(0).get("T_COL"));
        }
    }

    // -------------------------------------------------------------------------
    // fetchPrimaryKeyColumns
    // -------------------------------------------------------------------------

    @Test
    void fetchPrimaryKeyColumns_正常ケース_主キーが1列である_列名が返ること() throws Exception {
        Connection conn = mock(Connection.class);
        when(conn.getCatalog()).thenReturn("cat");
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet pkRs = mock(ResultSet.class);
        when(meta.getPrimaryKeys("cat", "APP", "T1")).thenReturn(pkRs);
        when(pkRs.next()).thenReturn(true, false);
        when(pkRs.getString("COLUMN_NAME")).thenReturn("ID");

        List<String> result = new CsvTableExporter().fetchPrimaryKeyColumns(conn, "APP", "T1");
        assertEquals(List.of("ID"), result);
    }

    @Test
    void fetchPrimaryKeyColumns_正常ケース_主キーがない_空リストが返ること() throws Exception {
        Connection conn = mock(Connection.class);
        when(conn.getCatalog()).thenReturn(null);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet pkRs = mock(ResultSet.class);
        when(meta.getPrimaryKeys(null, "APP", "T1")).thenReturn(pkRs);
        when(pkRs.next()).thenReturn(false);

        List<String> result = new CsvTableExporter().fetchPrimaryKeyColumns(conn, "APP", "T1");
        assertEquals(List.of(), result);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Builds a mock Connection for a single-row, single-column table with no primary key, where
     * getBytes() returns the given bytes value.
     */
    private Connection buildSingleColConn(String schema, String table, String col, int sqlType,
            String typeName, Object objectValue, byte[] bytes) throws SQLException {
        Connection conn = mock(Connection.class);
        when(conn.getSchema()).thenReturn(schema);
        when(conn.getCatalog()).thenReturn(null);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet pkRs = mock(ResultSet.class);
        when(meta.getPrimaryKeys(any(), eq(schema), eq(table))).thenReturn(pkRs);
        when(pkRs.next()).thenReturn(false);
        Statement stmtH = mock(Statement.class);
        Statement stmtD = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmtH, stmtD);
        ResultSet rsH = mock(ResultSet.class);
        when(stmtH.executeQuery("SELECT * FROM \"" + table + "\" WHERE 1=0")).thenReturn(rsH);
        ResultSetMetaData mdH = mock(ResultSetMetaData.class);
        when(rsH.getMetaData()).thenReturn(mdH);
        when(mdH.getColumnCount()).thenReturn(1);
        when(mdH.getColumnLabel(1)).thenReturn(col);
        ResultSet rsD = mock(ResultSet.class);
        when(stmtD.executeQuery("SELECT * FROM \"" + table + "\" ORDER BY \"" + col + "\" ASC"))
                .thenReturn(rsD);
        ResultSetMetaData mdD = mock(ResultSetMetaData.class);
        when(rsD.getMetaData()).thenReturn(mdD);
        when(mdD.getColumnCount()).thenReturn(1);
        when(mdD.getColumnType(1)).thenReturn(sqlType);
        when(mdD.getColumnTypeName(1)).thenReturn(typeName);
        when(rsD.next()).thenReturn(true, false);
        if (objectValue != null) {
            when(rsD.getObject(1)).thenReturn(objectValue);
        }
        when(rsD.getBytes(1)).thenReturn(bytes);
        return conn;
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
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        return dialectHandler;
    }
}
