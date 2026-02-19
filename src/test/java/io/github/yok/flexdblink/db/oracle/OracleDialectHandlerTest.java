package io.github.yok.flexdblink.db.oracle;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.db.DbUnitConfigFactory;
import io.github.yok.flexdblink.util.OracleDateTimeFormatUtil;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.csv.CsvDataSet;
import org.dbunit.dataset.datatype.DataType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

public class OracleDialectHandlerTest {

    @TempDir
    Path tempDir;

    @Test
    void quoteIdentifier_正常ケース_識別子を引用符で囲む_ダブルクォート付き文字列であること() throws Exception {
        OracleDialectHandler handler = createHandler();
        assertEquals("\"A1\"", handler.quoteIdentifier("A1"));
    }

    @Test
    void applyPagination_正常ケース_offsetとlimitを指定する_Oracle形式SQLであること() throws Exception {
        OracleDialectHandler handler = createHandler();
        assertEquals("SELECT * FROM T OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY",
                handler.applyPagination("SELECT * FROM T", 5, 10));
    }

    @Test
    void formatDateLiteral_正常ケース_LocalDateTimeを指定する_TO_DATE式であること() throws Exception {
        OracleDialectHandler handler = createHandler();
        assertEquals("TO_DATE('2026-02-15 16:00:01','YYYY-MM-DD HH24:MI:SS')",
                handler.formatDateLiteral(LocalDateTime.of(2026, 2, 15, 16, 0, 1)));
    }

    @Test
    void buildUpsertSql_正常ケース_keyinsertupdateを指定する_MERGE文であること() throws Exception {
        OracleDialectHandler handler = createHandler();
        String sql = handler.buildUpsertSql("TBL", List.of("ID"), List.of("NAME"), List.of("NAME"));
        assertTrue(sql.contains("MERGE INTO \"TBL\""));
        assertTrue(sql.contains("ON (t.\"ID\" = s.\"ID\")"));
        assertTrue(sql.contains("UPDATE SET t.\"NAME\" = s.\"NAME\""));
        assertTrue(sql.contains("INSERT (\"ID\",\"NAME\") VALUES (s.\"ID\",s.\"NAME\")"));
    }

    @Test
    void buildUpsertSql_正常ケース_insert列が空である_key列のみのMERGE文であること() throws Exception {
        OracleDialectHandler handler = createHandler();
        String sql = handler.buildUpsertSql("TBL", List.of("ID"), List.of(), List.of("ID"));
        assertTrue(sql.contains("USING (SELECT ? AS \"ID\" FROM DUAL)"));
        assertTrue(sql.contains("INSERT (\"ID\") VALUES (s.\"ID\")"));
    }

    @Test
    void getCreateTempTableSql_正常ケース_列定義を指定する_GTT作成SQLであること() throws Exception {
        OracleDialectHandler handler = createHandler();
        String sql = handler.getCreateTempTableSql("TMP_TBL",
                Map.of("ID", "NUMBER", "NAME", "VARCHAR2(20)"));
        assertTrue(sql.contains("CREATE GLOBAL TEMPORARY TABLE \"TMP_TBL\""));
        assertTrue(sql.contains("\"ID\" NUMBER"));
        assertTrue(sql.contains("\"NAME\" VARCHAR2(20)"));
        assertTrue(sql.endsWith("ON COMMIT PRESERVE ROWS"));
    }

    @Test
    void formatDbValueForCsv_正常ケース_null値を指定する_空文字であること() throws Exception {
        OracleDialectHandler handler = createHandler();
        assertEquals("", handler.formatDbValueForCsv("COL", null));
    }

    @Test
    void formatDbValueForCsv_正常ケース_非null値を指定する_toString値であること() throws Exception {
        OracleDialectHandler handler = createHandler();
        assertEquals("123", handler.formatDbValueForCsv("COL", 123));
    }

    @Test
    void resolveSchema_正常ケース_ユーザー名を指定する_大文字スキーマ名であること() throws Exception {
        OracleDialectHandler handler = createHandler();
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setUser("app_user");
        assertEquals("APP_USER", handler.resolveSchema(entry));
    }

    @Test
    void prepareConnection_正常ケース_接続を初期化する_セッションSQLが実行されること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        when(conn.getSchema()).thenReturn("APP");
        when(conn.createStatement()).thenReturn(stmt);
        handler.prepareConnection(conn);
        verify(stmt).execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'");
        verify(stmt).execute("ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'");
        verify(stmt).execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'");
        verify(stmt).execute("ALTER SESSION SET CURRENT_SCHEMA = APP");
    }

    @Test
    void getPrimaryKeyColumns_正常ケース_KEYSEQ順が逆順で返る_昇順で返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.getMetaData()).thenReturn(metaData);
        when(metaData.getPrimaryKeys(null, "APP", "TBL")).thenReturn(rs);
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getString("COLUMN_NAME")).thenReturn("C2", "C1");
        when(rs.getShort("KEY_SEQ")).thenReturn((short) 2, (short) 1);
        List<String> actual = handler.getPrimaryKeyColumns(conn, "APP", "TBL");
        assertEquals(List.of("C1", "C2"), actual);
    }

    @Test
    void getColumnTypeName_正常ケース_列メタデータが存在する_型名が返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.getMetaData()).thenReturn(metaData);
        when(metaData.getColumns(null, "APP", "TBL", "COL")).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getString("TYPE_NAME")).thenReturn("VARCHAR2");
        assertEquals("VARCHAR2", handler.getColumnTypeName(conn, "APP", "TBL", "COL"));
    }

    @Test
    void getColumnTypeName_異常ケース_列メタデータが存在しない_SQLExceptionが送出されること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.getMetaData()).thenReturn(metaData);
        when(metaData.getColumns(null, "APP", "TBL", "COL")).thenReturn(rs);
        when(rs.next()).thenReturn(false);
        assertThrows(Exception.class, () -> handler.getColumnTypeName(conn, "APP", "TBL", "COL"));
    }

    @Test
    void parseDateTimeValue_正常ケース_空文字を指定する_nullが返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        assertNull(handler.parseDateTimeValue("COL", " "));
    }

    @Test
    void parseDateTimeValue_正常ケース_nullを指定する_nullが返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        assertNull(handler.parseDateTimeValue("COL", null));
    }

    @Test
    void parseDateTimeValue_正常ケース_年月インターバルを指定する_INTERVALYMが返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Object parsed = handler.parseDateTimeValue("COL", "1-2");
        assertEquals("oracle.sql.INTERVALYM", parsed.getClass().getName());
    }

    @Test
    void parseDateTimeValue_正常ケース_日秒インターバルを指定する_INTERVALDSが返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Object parsed = handler.parseDateTimeValue("COL", "1 10:20:30");
        assertEquals("oracle.sql.INTERVALDS", parsed.getClass().getName());
    }

    @Test
    void parseDateTimeValue_正常ケース_オフセット付き日時を指定する_Timestampが返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Object parsed = handler.parseDateTimeValue("COL", "20260215T010203+0900");
        assertInstanceOf(Timestamp.class, parsed);
    }

    @Test
    void parseDateTimeValue_正常ケース_日付のみを指定する_Dateが返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Object parsed = handler.parseDateTimeValue("COL", "2026-02-15");
        assertEquals(java.sql.Date.class, parsed.getClass());
    }

    @Test
    void parseDateTimeValue_正常ケース_時刻を指定する_Timeが返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Object parsed = handler.parseDateTimeValue("COL", "01:02:03");
        assertEquals(java.sql.Time.class, parsed.getClass());
    }

    @Test
    void parseDateTimeValue_正常ケース_ローカル日時を指定する_Timestampが返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Object parsed = handler.parseDateTimeValue("COL", "2026-02-15 01:02:03");
        assertInstanceOf(Timestamp.class, parsed);
    }

    @Test
    public void parseDateTimeValue_異常ケース_全ての形式に一致しない_DateTimeParseExceptionが送出されること()
            throws Exception {
        OracleDialectHandler handler = createHandler();

        assertThrows(DateTimeParseException.class,
                () -> handler.parseDateTimeValue("COL", "not-a-datetime"));
    }

    @Test
    void formatDateTimeColumn_正常ケース_値がnullである_空文字が返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        assertEquals("", handler.formatDateTimeColumn("COL", null, mock(Connection.class)));
    }

    @Test
    void formatDateTimeColumn_正常ケース_フォーマッタが成功する_フォーマット値が返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        OracleDateTimeFormatUtil formatter = extractFormatter(handler);
        when(formatter.formatJdbcDateTime(eq("COL"), eq("X"), any())).thenReturn("FORMATTED");
        assertEquals("FORMATTED", handler.formatDateTimeColumn("COL", "X", mock(Connection.class)));
    }

    @Test
    void formatDateTimeColumn_正常ケース_フォーマッタが失敗する_toString値が返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        OracleDateTimeFormatUtil formatter = extractFormatter(handler);
        doThrow(new RuntimeException("x")).when(formatter).formatJdbcDateTime(eq("COL"), eq("X"),
                any());
        assertEquals("X", handler.formatDateTimeColumn("COL", "X", mock(Connection.class)));
    }

    @Test
    void readLobFile_正常ケース_blob列を指定する_bytesが返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMeta(handler, "TBL", "BLOB_COL", DataType.BLOB);
        File base = tempDir.toFile();
        File filesDir = new File(base, "files");
        Files.createDirectories(filesDir.toPath());
        Path p = new File(filesDir, "a.bin").toPath();
        Files.write(p, new byte[] {0x01, 0x02});
        Object actual = handler.readLobFile("a.bin", "TBL", "BLOB_COL", base);
        assertArrayEquals(new byte[] {0x01, 0x02}, (byte[]) actual);
    }

    @Test
    void readLobFile_正常ケース_clob列を指定する_文字列が返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMeta(handler, "TBL", "CLOB_COL", DataType.CLOB);
        File base = tempDir.toFile();
        File filesDir = new File(base, "files");
        Files.createDirectories(filesDir.toPath());
        Path p = new File(filesDir, "a.txt").toPath();
        Files.writeString(p, "abc", StandardCharsets.UTF_8);
        Object actual = handler.readLobFile("a.txt", "TBL", "CLOB_COL", base);
        assertEquals("abc", actual);
    }

    @Test
    void readLobFile_異常ケース_lobファイルが存在しない_DataSetExceptionが送出されること() throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMeta(handler, "TBL", "CLOB_COL", DataType.CLOB);
        assertThrows(DataSetException.class,
                () -> handler.readLobFile("missing.txt", "TBL", "CLOB_COL", tempDir.toFile()));
    }

    @Test
    void convertCsvValueToDbType_正常ケース_hex文字列を指定する_byte配列が返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMeta(handler, "TBL", "BIN_COL", DataType.VARBINARY);
        Object actual = handler.convertCsvValueToDbType("TBL", "BIN_COL", "0A0B");
        assertArrayEquals(new byte[] {0x0A, 0x0B}, (byte[]) actual);
    }

    @Test
    void convertCsvValueToDbType_正常ケース_numeric値を指定する_BigDecimalが返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMeta(handler, "TBL", "NUM_COL", DataType.NUMERIC);
        Object actual = handler.convertCsvValueToDbType("TBL", "NUM_COL", "123.45");
        assertEquals("123.45", actual.toString());
    }

    @Test
    void convertCsvValueToDbType_正常ケース_bigint値を指定する_Longが返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMeta(handler, "TBL", "BIG_COL", DataType.BIGINT);
        Object actual = handler.convertCsvValueToDbType("TBL", "BIG_COL", "2147483648");
        assertEquals(Long.class, actual.getClass());
        assertEquals(2147483648L, actual);
    }

    @Test
    void convertCsvValueToDbType_異常ケース_bigintに非数値を指定する_DataSetExceptionが送出されること() throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMeta(handler, "TBL", "BIG_COL", DataType.BIGINT);
        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("TBL", "BIG_COL", "abc"));
    }

    @Test
    void convertCsvValueToDbType_正常ケース_file参照を指定する_lob内容が返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMeta(handler, "TBL", "CLOB_COL", DataType.CLOB);
        Path lob = tempDir.resolve("dump").resolve("files").resolve("x.txt");
        Files.createDirectories(lob.getParent());
        Files.writeString(lob, "lobdata", StandardCharsets.UTF_8);
        Object actual = handler.convertCsvValueToDbType("TBL", "CLOB_COL", "file:x.txt");
        assertEquals("lobdata", actual);
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_timeWithTimezone_非コロン形式で2段目パーサ成功する_OffsetTimeが返ること()
            throws Exception {

        OracleDialectHandler handler = createHandler();
        putTableMetaBySqlType(handler, "TBL", "OT_COL", java.sql.Types.TIME_WITH_TIMEZONE, "TIME");

        Object actual = handler.convertCsvValueToDbType("TBL", "OT_COL", "010203+0900");

        assertEquals("java.time.OffsetTime", actual.getClass().getName());
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_timestamp_日付のみを指定する_0時のTimestampが返ること()
            throws Exception {

        OracleDialectHandler handler = createHandler();
        putTableMeta(handler, "TBL", "TS_COL", DataType.TIMESTAMP);

        Object actual = handler.convertCsvValueToDbType("TBL", "TS_COL", "20260215");
        assertInstanceOf(Timestamp.class, actual);

        Timestamp ts = (Timestamp) actual;
        assertEquals(Timestamp.valueOf("2026-02-15 00:00:00"), ts);
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_otherでinterval以外を指定する_文字列がそのまま返ること()
            throws Exception {

        OracleDialectHandler handler = createHandler();
        putTableMetaBySqlType(handler, "TBL", "OTHER_COL", java.sql.Types.OTHER, "OTHER");

        Object actual = handler.convertCsvValueToDbType("TBL", "OTHER_COL", "x");
        assertEquals("x", actual);
    }

    @Test
    void hasPrimaryKey_正常ケース_主キーありの結果セットを返す_trueであること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getPrimaryKeys(null, "APP", "TBL")).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        assertTrue(handler.hasPrimaryKey(conn, "APP", "TBL"));
    }

    @Test
    void hasNotNullLobColumn_正常ケース_NOTNULLのLOB列が存在する_trueであること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        Column lob = new Column("CLOB_COL", DataType.CLOB);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getColumns(null, "APP", "TBL", null)).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getString("COLUMN_NAME")).thenReturn("CLOB_COL");
        when(rs.getInt("NULLABLE")).thenReturn(DatabaseMetaData.columnNoNulls);
        assertTrue(handler.hasNotNullLobColumn(conn, "APP", "TBL", new Column[] {lob}));
    }

    @Test
    void hasNotNullLobColumn_正常ケース_NULL許容のLOB列のみである_falseであること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        Column lob = new Column("CLOB_COL", DataType.CLOB);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getColumns(null, "APP", "TBL", null)).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getString("COLUMN_NAME")).thenReturn("CLOB_COL");
        when(rs.getInt("NULLABLE")).thenReturn(DatabaseMetaData.columnNullable);
        assertFalse(handler.hasNotNullLobColumn(conn, "APP", "TBL", new Column[] {lob}));
    }

    @Test
    void hasNotNullLobColumn_正常ケース_列名が一致しない_falseであること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        Column lob = new Column("CLOB_COL", DataType.CLOB);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getColumns(null, "APP", "TBL", null)).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getString("COLUMN_NAME")).thenReturn("OTHER_COL");
        when(rs.getInt("NULLABLE")).thenReturn(DatabaseMetaData.columnNoNulls);
        assertFalse(handler.hasNotNullLobColumn(conn, "APP", "TBL", new Column[] {lob}));
    }

    @Test
    void hasNotNullLobColumn_正常ケース_列メタデータが空である_falseであること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getColumns(null, "APP", "TBL", null)).thenReturn(rs);
        when(rs.next()).thenReturn(false);
        assertFalse(handler.hasNotNullLobColumn(conn, "APP", "TBL",
                new Column[] {new Column("CLOB_COL", DataType.CLOB)}));
    }

    @Test
    void hasNotNullLobColumn_異常ケース_列メタデータ走査でSQLExceptionが発生する_SQLExceptionが再スローされること()
            throws Exception {
        OracleDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getColumns(null, "APP", "TBL", null)).thenReturn(rs);
        when(rs.next()).thenThrow(new SQLException("next-error"));

        SQLException ex = assertThrows(SQLException.class, () -> handler.hasNotNullLobColumn(conn,
                "APP", "TBL", new Column[] {new Column("CLOB_COL", DataType.CLOB)}));
        assertEquals("next-error", ex.getMessage());
    }

    @Test
    void hasNotNullLobColumn_異常ケース_getColumnsでSQLExceptionが発生する_SQLExceptionが再スローされること()
            throws Exception {
        OracleDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getColumns(null, "APP", "TBL", null))
                .thenThrow(new SQLException("columns-error"));

        SQLException ex = assertThrows(SQLException.class, () -> handler.hasNotNullLobColumn(conn,
                "APP", "TBL", new Column[] {new Column("CLOB_COL", DataType.CLOB)}));
        assertEquals("columns-error", ex.getMessage());
    }

    @Test
    void countRows_正常ケース_件数1行を返す結果セットを返す_件数が返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery("SELECT COUNT(*) FROM \"TBL\"")).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getInt(1)).thenReturn(7);
        assertEquals(7, handler.countRows(conn, "TBL"));
    }

    @Test
    void countRows_正常ケース_結果セットが空である_0が返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery("SELECT COUNT(*) FROM \"TBL\"")).thenReturn(rs);
        when(rs.next()).thenReturn(false);
        assertEquals(0, handler.countRows(conn, "TBL"));
    }

    @Test
    void countRows_異常ケース_クローズ時にSQLExceptionが発生する_SQLExceptionが再スローされること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery("SELECT COUNT(*) FROM \"TBL\"")).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getInt(1)).thenReturn(7);
        doThrow(new SQLException("close-error")).when(rs).close();

        SQLException ex = assertThrows(SQLException.class, () -> handler.countRows(conn, "TBL"));
        assertEquals("close-error", ex.getMessage());
    }

    @Test
    void countRows_異常ケース_走査中とクローズ時にSQLExceptionが発生する_クローズ時例外が再スローされること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery("SELECT COUNT(*) FROM \"TBL\"")).thenReturn(rs);
        when(rs.next()).thenThrow(new SQLException("next-error"));
        doThrow(new SQLException("rs-close-error")).when(rs).close();
        doThrow(new SQLException("stmt-close-error")).when(stmt).close();

        SQLException ex = assertThrows(SQLException.class, () -> handler.countRows(conn, "TBL"));
        assertEquals("stmt-close-error", ex.getMessage());
    }

    @Test
    void countRows_異常ケース_createStatementでSQLExceptionが発生する_SQLExceptionが再スローされること()
            throws Exception {
        OracleDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        when(conn.createStatement()).thenThrow(new SQLException("create-error"));

        SQLException ex = assertThrows(SQLException.class, () -> handler.countRows(conn, "TBL"));
        assertEquals("create-error", ex.getMessage());
    }

    @Test
    void countRows_異常ケース_executeQueryでSQLExceptionが発生する_SQLExceptionが再スローされること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery("SELECT COUNT(*) FROM \"TBL\""))
                .thenThrow(new SQLException("query-error"));

        SQLException ex = assertThrows(SQLException.class, () -> handler.countRows(conn, "TBL"));
        assertEquals("query-error", ex.getMessage());
    }

    @Test
    void writeLobFile_正常ケース_blobclobbytesstringを指定する_ファイルへ出力されること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Path outBase = tempDir.resolve("out");

        Path bytesFile = outBase.resolve("a.bin");
        handler.writeLobFile("TBL", "C1", new byte[] {0x01, 0x02}, bytesFile);
        assertArrayEquals(new byte[] {0x01, 0x02}, Files.readAllBytes(bytesFile));

        Path strFile = outBase.resolve("a.txt");
        handler.writeLobFile("TBL", "C2", "abc", strFile);
        assertEquals("abc", Files.readString(strFile, StandardCharsets.UTF_8));

        Blob blob = mock(Blob.class);
        when(blob.getBinaryStream()).thenReturn(new ByteArrayInputStream(new byte[] {0x0A}));
        Path blobFile = outBase.resolve("b.bin");
        handler.writeLobFile("TBL", "C3", blob, blobFile);
        assertArrayEquals(new byte[] {0x0A}, Files.readAllBytes(blobFile));

        Clob clob = mock(Clob.class);
        when(clob.getCharacterStream()).thenReturn(new StringReader("clob-data"));
        Path clobFile = outBase.resolve("b.txt");
        handler.writeLobFile("TBL", "C4", clob, clobFile);
        assertEquals("clob-data", Files.readString(clobFile, StandardCharsets.UTF_8));
    }

    @Test
    void writeLobFile_異常ケース_未対応型を指定する_IllegalArgumentExceptionが送出されること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Path out = tempDir.resolve("out").resolve("x.dat");
        assertThrows(IllegalArgumentException.class,
                () -> handler.writeLobFile("TBL", "C1", new Object(), out));
    }

    @Test
    void getLobColumns_正常ケース_csvが存在しない_空配列が返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Column[] actual = handler.getLobColumns(tempDir, "MISSING");
        assertEquals(0, actual.length);
    }

    @Test
    void getLobColumns_正常ケース_file参照列がlob型である_対象列のみ返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Path csvDir = tempDir.resolve("csv");
        Files.createDirectories(csvDir);
        Files.writeString(csvDir.resolve("TBL.csv"),
                "ID,BLOB_COL,CLOB_COL,TXT\n1,file:a.bin,file:a.txt,x\n", StandardCharsets.UTF_8);

        ITable table = mock(ITable.class);
        ITableMetaData meta = mock(ITableMetaData.class);
        when(table.getTableMetaData()).thenReturn(meta);
        when(meta.getColumns()).thenReturn(new Column[] {new Column("ID", DataType.INTEGER),
                new Column("BLOB_COL", DataType.BLOB), new Column("CLOB_COL", DataType.CLOB),
                new Column("TXT", DataType.VARCHAR)});

        try (MockedConstruction<org.dbunit.dataset.csv.CsvDataSet> ignored =
                org.mockito.Mockito.mockConstruction(org.dbunit.dataset.csv.CsvDataSet.class,
                        (mock, context) -> when(mock.getTable("TBL")).thenReturn(table))) {
            Column[] actual = handler.getLobColumns(csvDir, "TBL");
            assertEquals(2, actual.length);
            assertEquals("BLOB_COL", actual[0].getColumnName());
            assertEquals("CLOB_COL", actual[1].getColumnName());
        }
    }

    @Test
    public void getLobColumns_正常ケース_file参照があるがlob型でない_空配列が返ること() throws Exception {
        OracleDialectHandler handler = createHandler();

        Path csvDir = tempDir.resolve("csv2");
        Files.createDirectories(csvDir);
        Files.writeString(csvDir.resolve("TBL.csv"), "ID,TXT\n1,file:x.txt\n",
                StandardCharsets.UTF_8);

        ITable table = mock(ITable.class);
        ITableMetaData meta = mock(ITableMetaData.class);
        when(table.getTableMetaData()).thenReturn(meta);
        when(meta.getColumns()).thenReturn(new Column[] {new Column("ID", DataType.INTEGER),
                new Column("TXT", DataType.VARCHAR)});

        try (MockedConstruction<CsvDataSet> ignored = Mockito.mockConstruction(CsvDataSet.class,
                (mock, context) -> when(mock.getTable("TBL")).thenReturn(table))) {

            Column[] actual = handler.getLobColumns(csvDir, "TBL");
            assertEquals(0, actual.length);
        }
    }

    @Test
    void getLobColumns_正常ケース_sqlTypeでblobclob判定する_対象列が返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Path csvDir = tempDir.resolve("csv3");
        Files.createDirectories(csvDir);
        Files.writeString(csvDir.resolve("TBL.csv"),
                "ID,LOB1,LOB2\n1,file:a.bin,file:b.txt\n2,file:a.bin,file:b.txt\n",
                StandardCharsets.UTF_8);

        ITable table = mock(ITable.class);
        ITableMetaData meta = mock(ITableMetaData.class);
        Column idCol = new Column("ID", DataType.INTEGER);
        Column lob1 = mock(Column.class);
        Column lob2 = mock(Column.class);
        Column extra = new Column("EXTRA", DataType.VARCHAR);
        org.dbunit.dataset.datatype.DataType blobLike =
                mock(org.dbunit.dataset.datatype.DataType.class);
        org.dbunit.dataset.datatype.DataType clobLike =
                mock(org.dbunit.dataset.datatype.DataType.class);
        when(blobLike.getSqlType()).thenReturn(Types.BLOB);
        when(clobLike.getSqlType()).thenReturn(Types.CLOB);
        when(lob1.getColumnName()).thenReturn("LOB1");
        when(lob2.getColumnName()).thenReturn("LOB2");
        when(lob1.getDataType()).thenReturn(blobLike);
        when(lob2.getDataType()).thenReturn(clobLike);
        when(table.getTableMetaData()).thenReturn(meta);
        when(meta.getColumns()).thenReturn(new Column[] {idCol, lob1, lob2, extra});

        try (MockedConstruction<CsvDataSet> ignored = Mockito.mockConstruction(CsvDataSet.class,
                (mock, context) -> when(mock.getTable("TBL")).thenReturn(table))) {
            Column[] actual = handler.getLobColumns(csvDir, "TBL");
            assertEquals(2, actual.length);
            assertEquals("LOB1", actual[0].getColumnName());
            assertEquals("LOB2", actual[1].getColumnName());
        }
    }

    @Test
    void convertCsvValueToDbType_正常ケース_日付時刻系を指定する_各型へ変換されること() throws Exception {
        OracleDialectHandler handler = createHandler();

        putTableMeta(handler, "TBL", "D_COL", DataType.DATE);
        Object d = handler.convertCsvValueToDbType("TBL", "D_COL", "2026/02/15");
        assertEquals(java.sql.Date.class, d.getClass());

        putTableMeta(handler, "TBL", "T_COL", DataType.TIME);
        Object t = handler.convertCsvValueToDbType("TBL", "T_COL", "010203");
        assertEquals(java.sql.Time.class, t.getClass());

        putTableMeta(handler, "TBL", "TS_COL", DataType.TIMESTAMP);
        Object ts = handler.convertCsvValueToDbType("TBL", "TS_COL", "2026-02-15T01:02:03");
        assertInstanceOf(Timestamp.class, ts);
    }

    @Test
    void convertCsvValueToDbType_異常ケース_不正な日付時刻文字列を指定する_DataSetExceptionが送出されること() throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMeta(handler, "TBL", "D_COL", DataType.DATE);
        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("TBL", "D_COL", "not-a-date"));

        putTableMeta(handler, "TBL", "TS_COL", DataType.TIMESTAMP);
        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("TBL", "TS_COL", "bad-ts"));
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_varcharを指定する_文字列がそのまま返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMeta(handler, "TBL", "TXT_COL", DataType.VARCHAR);

        Object actual = handler.convertCsvValueToDbType("TBL", "TXT_COL", "abc");
        assertEquals("abc", actual);
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_blob_file参照を指定する_bytesが返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMeta(handler, "TBL", "BLOB_COL", DataType.BLOB);

        Path lob = tempDir.resolve("dump").resolve("files").resolve("b.bin");
        Files.createDirectories(lob.getParent());
        Files.write(lob, new byte[] {0x01, 0x02, 0x03});

        Object actual = handler.convertCsvValueToDbType("TBL", "BLOB_COL", "file:b.bin");
        assertArrayEquals(new byte[] {0x01, 0x02, 0x03}, (byte[]) actual);
    }

    @Test
    public void convertCsvValueToDbType_異常ケース_blob_file参照でファイルが存在しない_DataSetExceptionが送出されること()
            throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMeta(handler, "TBL", "BLOB_COL", DataType.BLOB);

        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("TBL", "BLOB_COL", "file:missing.bin"));
    }

    @Test
    public void convertCsvValueToDbType_異常ケース_file参照をlob以外列に指定する_DataSetExceptionが送出されること()
            throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMeta(handler, "TBL", "TXT_COL", DataType.VARCHAR);

        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("TBL", "TXT_COL", "file:x.txt"));
    }

    @Test
    public void convertCsvValueToDbType_異常ケース_テーブル未登録を指定する_DataSetExceptionが送出されること()
            throws Exception {
        OracleDialectHandler handler = createHandler();

        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("NO_TBL", "COL", "x"));
    }

    @Test
    public void convertCsvValueToDbType_異常ケース_列未登録を指定する_DataSetExceptionが送出されること()
            throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMeta(handler, "TBL", "C1", DataType.VARCHAR);

        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("TBL", "NO_COL", "x"));
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_DBUnit列未登録かつJDBC列登録でTSTZを指定する_Timestampが返ること()
            throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMeta(handler, "TBL", "C1", DataType.VARCHAR);
        putJdbcColumnSpec(handler, "TBL", "TSTZ_COL", Types.TIMESTAMP_WITH_TIMEZONE,
                "TIMESTAMP WITH TIME ZONE");

        Object actual =
                handler.convertCsvValueToDbType("TBL", "TSTZ_COL", "2026-02-15 01:02:03+09:00");

        assertInstanceOf(Timestamp.class, actual);
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_DBUnit列未登録かつJDBC列登録でNCLOBファイル参照を指定する_文字列が返ること()
            throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMeta(handler, "TBL", "C1", DataType.VARCHAR);
        putJdbcColumnSpec(handler, "TBL", "NCLOB_COL", Types.NCLOB, "NCLOB");

        Path lob = tempDir.resolve("dump").resolve("files").resolve("nclob.txt");
        Files.createDirectories(lob.getParent());
        Files.writeString(lob, "nclob-data", StandardCharsets.UTF_8);

        Object actual = handler.convertCsvValueToDbType("TBL", "NCLOB_COL", "file:nclob.txt");

        assertEquals("nclob-data", actual);
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_nullを指定する_nullが返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMeta(handler, "TBL", "C1", DataType.VARCHAR);

        Object actual = handler.convertCsvValueToDbType("TBL", "C1", null);
        assertNull(actual);
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_空白を指定する_nullが返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMeta(handler, "TBL", "C1", DataType.VARCHAR);

        Object actual = handler.convertCsvValueToDbType("TBL", "C1", "   ");
        assertNull(actual);
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_integerを指定する_Integerが返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMeta(handler, "TBL", "I_COL", DataType.INTEGER);

        Object actual = handler.convertCsvValueToDbType("TBL", "I_COL", "123");
        assertEquals(Integer.class, actual.getClass());
        assertEquals(123, actual);
    }

    @Test
    public void convertCsvValueToDbType_異常ケース_integerに非数値を指定する_DataSetExceptionが送出されること()
            throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMeta(handler, "TBL", "I_COL", DataType.INTEGER);

        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("TBL", "I_COL", "x"));
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_doubleを指定する_Doubleが返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMeta(handler, "TBL", "D_COL", DataType.DOUBLE);

        Object actual = handler.convertCsvValueToDbType("TBL", "D_COL", "10.5");
        assertEquals(Double.class, actual.getClass());
        assertEquals(10.5d, (Double) actual, 0.0d);
    }

    @Test
    public void convertCsvValueToDbType_異常ケース_doubleに非数値を指定する_DataSetExceptionが送出されること()
            throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMeta(handler, "TBL", "D_COL", DataType.DOUBLE);

        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("TBL", "D_COL", "bad"));
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_timeWithTimezoneを指定する_OffsetTimeが返ること()
            throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMetaBySqlType(handler, "TBL", "OT_COL", java.sql.Types.TIME_WITH_TIMEZONE, "TIME");

        Object actual = handler.convertCsvValueToDbType("TBL", "OT_COL", "01:02:03+09:00");
        assertEquals("java.time.OffsetTime", actual.getClass().getName());
    }

    @Test
    public void convertCsvValueToDbType_異常ケース_timeWithTimezoneに不正値を指定する_DataSetExceptionが送出されること()
            throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMetaBySqlType(handler, "TBL", "OT_COL", java.sql.Types.TIME_WITH_TIMEZONE, "TIME");

        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("TBL", "OT_COL", "bad"));
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_timestampWithTimezoneを指定する_Timestampが返ること()
            throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMetaBySqlType(handler, "TBL", "TZTS_COL", java.sql.Types.TIMESTAMP_WITH_TIMEZONE,
                "TIMESTAMP");

        Object actual =
                handler.convertCsvValueToDbType("TBL", "TZTS_COL", "2026-02-15 01:02:03 +0900");

        assertInstanceOf(Timestamp.class, actual);
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_timestampWithTimezone_コロン付きオフセットで2段目パーサを通る_Timestampが返ること()
            throws Exception {

        OracleDialectHandler handler = createHandler();

        // TIMESTAMP_WITH_TIMEZONE → parseTimestamp() 経由
        putTableMetaBySqlType(handler, "TBL", "TZTS_COL", Types.TIMESTAMP_WITH_TIMEZONE,
                "TIMESTAMP");

        // 1段目("... Z")を落として、2段目(FLEXIBLE_OFFSET_DATETIME_PARSER_COLON)を通す入力
        String input = "2026-02-15T01:02:03+09:00";

        Object actual = handler.convertCsvValueToDbType("TBL", "TZTS_COL", input);
        assertInstanceOf(Timestamp.class, actual);

        Timestamp ts = (Timestamp) actual;

        // Timestamp は Instant ベースで入るので、同じ Instant で比較する
        Timestamp expected = Timestamp.from(java.time.OffsetDateTime.parse(input).toInstant());
        assertEquals(expected.getTime(), ts.getTime());
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_intervalYearToMonthを指定する_INTERVALYMが返ること()
            throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMetaBySqlType(handler, "TBL", "IVYM_COL", Types.OTHER, "INTERVAL YEAR TO MONTH");

        Object actual = handler.convertCsvValueToDbType("TBL", "IVYM_COL", "1-2");
        assertEquals("oracle.sql.INTERVALYM", actual.getClass().getName());
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_intervalDayToSecondを指定する_INTERVALDSが返ること()
            throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMetaBySqlType(handler, "TBL", "IVDS_COL", Types.OTHER, "INTERVAL DAY TO SECOND");

        Object actual = handler.convertCsvValueToDbType("TBL", "IVDS_COL", "1 01:02:03");
        assertEquals("oracle.sql.INTERVALDS", actual.getClass().getName());
    }

    @Test
    public void convertCsvValueToDbType_異常ケース_varbinaryでhexデコード不能を指定する_DataSetExceptionが送出されること()
            throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMeta(handler, "TBL", "BIN_COL", DataType.VARBINARY);

        // isHexString は true だが、奇数桁で decodeHex が失敗するケース
        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("TBL", "BIN_COL", "ABC"));
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_binaryとlongvarbinaryのhexを指定する_byte配列が返ること()
            throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMeta(handler, "TBL", "BIN_COL", DataType.BINARY);
        Object bin = handler.convertCsvValueToDbType("TBL", "BIN_COL", "0A0B");
        assertArrayEquals(new byte[] {0x0A, 0x0B}, (byte[]) bin);

        putTableMeta(handler, "TBL", "LBIN_COL", DataType.LONGVARBINARY);
        Object lbin = handler.convertCsvValueToDbType("TBL", "LBIN_COL", "0C0D");
        assertArrayEquals(new byte[] {0x0C, 0x0D}, (byte[]) lbin);
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_varbinaryでhexでもfile参照でもない値を指定する_文字列が返ること()
            throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMeta(handler, "TBL", "BIN_COL", DataType.VARBINARY);

        Object actual = handler.convertCsvValueToDbType("TBL", "BIN_COL", "not-hex");
        assertEquals("not-hex", actual);
    }

    @Test
    public void convertCsvValueToDbType_異常ケース_clobのfile参照でファイル不存在を指定する_DataSetExceptionが送出されること()
            throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMeta(handler, "TBL", "CLOB_COL", DataType.CLOB);

        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("TBL", "CLOB_COL", "file:missing.txt"));
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_DBUnitがunknownかつJDBCがNCLOBを指定する_JDBCメタ優先で文字列が返ること()
            throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMetaBySqlType(handler, "TBL", "CLOB_COL", Types.OTHER, "UNKNOWN");
        putJdbcColumnSpec(handler, "TBL", "CLOB_COL", Types.NCLOB, "NCLOB");

        Path lob = tempDir.resolve("dump").resolve("files").resolve("unknown-nclob.txt");
        Files.createDirectories(lob.getParent());
        Files.writeString(lob, "unknown-nclob-data", StandardCharsets.UTF_8);

        Object actual =
                handler.convertCsvValueToDbType("TBL", "CLOB_COL", "file:unknown-nclob.txt");
        assertEquals("unknown-nclob-data", actual);
    }

    @Test
    void supports系_正常ケース_Oracle方言の固定値を返す_期待値であること() throws Exception {
        OracleDialectHandler handler = createHandler();
        assertTrue(handler.supportsLobStreamByStream());
        assertTrue(handler.supportsGetGeneratedKeys());
        assertTrue(handler.supportsSequences());
        assertFalse(handler.supportsIdentityColumns());
        assertTrue(handler.supportsBatchUpdates());
        assertEquals("1", handler.getBooleanTrueLiteral());
        assertEquals("0", handler.getBooleanFalseLiteral());
        assertEquals("CURRENT_TIMESTAMP", handler.getCurrentTimestampFunction());
        assertEquals(" RETURNING %s INTO ?", handler.getGeneratedKeyRetrievalSql());
        assertEquals("SELECT SEQ1.NEXTVAL FROM DUAL", handler.getNextSequenceSql("SEQ1"));
        assertNotNull(handler.getDataTypeFactory());
    }

    @Test
    void コンストラクタ_正常ケース_excludeTablesがnullでテーブルがある_例外なく初期化されること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        dbUnitConfig.setLobDirName("files");
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(null);

        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        Connection jdbc = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rsTables = mock(ResultSet.class);
        ResultSet rsColumns = mock(ResultSet.class);
        IDataSet dataSet = mock(IDataSet.class);
        ITableMetaData tableMetaData = mock(ITableMetaData.class);

        when(dbConn.getConnection()).thenReturn(jdbc);
        when(jdbc.getSchema()).thenReturn("APP");
        when(jdbc.getMetaData()).thenReturn(meta);
        when(meta.getTables(eq(null), eq("APP"), eq("%"), any(String[].class)))
                .thenReturn(rsTables);
        when(meta.getColumns(eq(null), eq("APP"), any(String.class), eq("%")))
                .thenReturn(rsColumns);
        when(rsTables.next()).thenReturn(true, false);
        when(rsColumns.next()).thenReturn(false);
        when(rsTables.getString("TABLE_NAME")).thenReturn("TBL1");
        when(dbConn.createDataSet()).thenReturn(dataSet);
        when(dataSet.getTableMetaData("TBL1")).thenReturn(tableMetaData);

        OracleDialectHandler handler = new OracleDialectHandler(dbConn, dumpConfig, dbUnitConfig,
                mock(DbUnitConfigFactory.class), mock(OracleDateTimeFormatUtil.class), pathsConfig);
        assertNotNull(handler);
    }

    @Test
    void コンストラクタ_正常ケース_excludeTablesに対象を含める_除外テーブルのメタデータ取得が行われないこと() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        dbUnitConfig.setLobDirName("files");
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(List.of("tbl1"));

        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        Connection jdbc = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rsTables = mock(ResultSet.class);
        ResultSet rsColumns = mock(ResultSet.class);
        IDataSet dataSet = mock(IDataSet.class);
        ITableMetaData tbl2Meta = mock(ITableMetaData.class);

        when(dbConn.getConnection()).thenReturn(jdbc);
        when(jdbc.getSchema()).thenReturn("APP");
        when(jdbc.getMetaData()).thenReturn(meta);
        when(meta.getTables(eq(null), eq("APP"), eq("%"), any(String[].class)))
                .thenReturn(rsTables);
        when(meta.getColumns(eq(null), eq("APP"), any(String.class), eq("%")))
                .thenReturn(rsColumns);
        when(rsTables.next()).thenReturn(true, true, false);
        when(rsColumns.next()).thenReturn(false);
        when(rsTables.getString("TABLE_NAME")).thenReturn("TBL1", "TBL2");
        when(dbConn.createDataSet()).thenReturn(dataSet);
        when(dataSet.getTableMetaData("TBL2")).thenReturn(tbl2Meta);

        OracleDialectHandler handler = new OracleDialectHandler(dbConn, dumpConfig, dbUnitConfig,
                mock(DbUnitConfigFactory.class), mock(OracleDateTimeFormatUtil.class), pathsConfig);

        assertNotNull(handler);
        verify(dataSet, never()).getTableMetaData("TBL1");
        verify(dataSet).getTableMetaData("TBL2");
    }

    @Test
    public void コンストラクタ_正常ケース_getSchemaがSQLException_userNameへフォールバックする_例外なく初期化されること()
            throws Exception {

        // PathsConfig は dump setter が無いので mock で getDump/getDataPath を返す
        PathsConfig pathsConfig = mock(PathsConfig.class);
        when(pathsConfig.getDataPath()).thenReturn(tempDir.toString());
        when(pathsConfig.getDump()).thenReturn(tempDir.resolve("dump").toString());

        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        dbUnitConfig.setLobDirName("files");

        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(new ArrayList<>());

        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        Connection jdbc = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rsTables = mock(ResultSet.class);
        ResultSet rsColumns = mock(ResultSet.class);
        IDataSet dataSet = mock(IDataSet.class);

        when(dbConn.getConnection()).thenReturn(jdbc);

        // ★ getSchema() が落ちるケース
        when(jdbc.getSchema()).thenThrow(new java.sql.SQLException("schema not supported"));
        when(jdbc.getMetaData()).thenReturn(meta);
        when(meta.getUserName()).thenReturn("app_user");

        // ★ schema には userName が使われる
        when(meta.getTables(eq(null), eq("app_user"), eq("%"), any(String[].class)))
                .thenReturn(rsTables);
        when(meta.getColumns(eq(null), eq("app_user"), any(String.class), eq("%")))
                .thenReturn(rsColumns);
        when(rsTables.next()).thenReturn(false);
        when(rsColumns.next()).thenReturn(false);

        when(dbConn.createDataSet()).thenReturn(dataSet);

        OracleDialectHandler handler = new OracleDialectHandler(dbConn, dumpConfig, dbUnitConfig,
                mock(DbUnitConfigFactory.class), mock(OracleDateTimeFormatUtil.class), pathsConfig);

        assertNotNull(handler);

        verify(meta).getTables(eq(null), eq("app_user"), eq("%"), any(String[].class));
    }

    @Test
    void createDbUnitConnection_正常ケース_設定ファクトリを適用する_生成接続が返ること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        dbUnitConfig.setLobDirName("files");
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(new ArrayList<>());

        DbUnitConfigFactory configFactory = mock(DbUnitConfigFactory.class);
        OracleDateTimeFormatUtil formatter = mock(OracleDateTimeFormatUtil.class);

        DatabaseConnection initialDbConn = mock(DatabaseConnection.class);
        Connection initJdbc = mock(Connection.class);
        DatabaseMetaData initMeta = mock(DatabaseMetaData.class);
        ResultSet initRs = mock(ResultSet.class);
        IDataSet initDataSet = mock(IDataSet.class);
        when(initialDbConn.getConnection()).thenReturn(initJdbc);
        when(initJdbc.getSchema()).thenReturn("APP");
        when(initJdbc.getMetaData()).thenReturn(initMeta);
        when(initMeta.getTables(eq(null), eq("APP"), eq("%"), any(String[].class)))
                .thenReturn(initRs);
        when(initRs.next()).thenReturn(false);
        when(initialDbConn.createDataSet()).thenReturn(initDataSet);

        OracleDialectHandler handler = new OracleDialectHandler(initialDbConn, dumpConfig,
                dbUnitConfig, configFactory, formatter, pathsConfig);

        Connection jdbc = mock(Connection.class);
        try (MockedConstruction<DatabaseConnection> mocked =
                Mockito.mockConstruction(DatabaseConnection.class, (dbConn, context) -> {
                    DatabaseConfig cfg = mock(DatabaseConfig.class);
                    when(dbConn.getConfig()).thenReturn(cfg);
                })) {
            DatabaseConnection actual = handler.createDbUnitConnection(jdbc, "APP");
            assertEquals(1, mocked.constructed().size());
            assertSame(mocked.constructed().get(0), actual);
            verify(configFactory).configure(any(DatabaseConfig.class), any());
        }
    }

    @Test
    void logTableDefinition_正常ケース_列メタデータが存在する_例外なく処理されること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getColumns(null, "APP", "TBL", null)).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getString("COLUMN_NAME")).thenReturn("C1");
        when(rs.getString("TYPE_NAME")).thenReturn("VARCHAR2");
        when(rs.getInt("COLUMN_SIZE")).thenReturn(20);
        when(rs.getInt("CHAR_OCTET_LENGTH")).thenReturn(20);
        handler.logTableDefinition(conn, "APP", "TBL", "db1");
        verify(meta).getColumns(null, "APP", "TBL", null);
    }

    @Test
    void parseTime_正常ケース_コロン形式と非コロン形式を指定する_Timeへ変換されること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Method method = OracleDialectHandler.class.getDeclaredMethod("parseTime", String.class,
                String.class);
        method.setAccessible(true);

        Object colon = method.invoke(handler, "01:02:03", "COL");
        Object noColon = method.invoke(handler, "010203", "COL");

        assertInstanceOf(Time.class, colon);
        assertInstanceOf(Time.class, noColon);
    }

    @Test
    void parseTime_異常ケース_不正時刻を指定する_DataSetExceptionが送出されること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Method method = OracleDialectHandler.class.getDeclaredMethod("parseTime", String.class,
                String.class);
        method.setAccessible(true);

        InvocationTargetException ex = assertThrows(InvocationTargetException.class,
                () -> method.invoke(handler, "xx", "COL"));
        assertTrue(ex.getCause() instanceof DataSetException);
    }

    @Test
    void parseOffsetTime_正常ケース_コロン形式と非コロン形式を指定する_OffsetTimeへ変換されること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Method method = OracleDialectHandler.class.getDeclaredMethod("parseOffsetTime",
                String.class, String.class);
        method.setAccessible(true);

        Object colon = method.invoke(handler, "01:02:03+09:00", "COL");
        Object noColon = method.invoke(handler, "010203+0900", "COL");

        assertEquals("java.time.OffsetTime", colon.getClass().getName());
        assertEquals("java.time.OffsetTime", noColon.getClass().getName());
    }

    @Test
    void parseOffsetTime_異常ケース_不正なオフセット時刻を指定する_DataSetExceptionが送出されること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Method method = OracleDialectHandler.class.getDeclaredMethod("parseOffsetTime",
                String.class, String.class);
        method.setAccessible(true);

        InvocationTargetException ex = assertThrows(InvocationTargetException.class,
                () -> method.invoke(handler, "bad", "COL"));
        assertTrue(ex.getCause() instanceof DataSetException);
    }

    @Test
    void parseInterval_正常ケース_年月日秒その他を指定する_型ごとに変換されること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Method method = OracleDialectHandler.class.getDeclaredMethod("parseInterval", int.class,
                String.class, String.class);
        method.setAccessible(true);

        Object ym = method.invoke(handler, -103, "OTHER", "1-2");
        Object ds = method.invoke(handler, -104, "OTHER", "1 01:02:03");
        Object other = method.invoke(handler, java.sql.Types.OTHER, "OTHER", "x");

        assertEquals("oracle.sql.INTERVALYM", ym.getClass().getName());
        assertEquals("oracle.sql.INTERVALDS", ds.getClass().getName());
        assertEquals("x", other);
    }

    @Test
    void parseInterval_正常ケース_sqlTypeNameにnullを指定する_sqlTypeに応じた型または文字列が返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Method method = OracleDialectHandler.class.getDeclaredMethod("parseInterval", int.class,
                String.class, String.class);
        method.setAccessible(true);

        Object ym = method.invoke(handler, -103, null, "1-2");
        Object ds = method.invoke(handler, -104, null, "1 01:02:03");
        Object other = method.invoke(handler, java.sql.Types.OTHER, null, "x");

        assertEquals("oracle.sql.INTERVALYM", ym.getClass().getName());
        assertEquals("oracle.sql.INTERVALDS", ds.getClass().getName());
        assertEquals("x", other);
    }

    @Test
    void loadLobFromFile_異常ケース_blobclob未対応を指定する_型に応じて値または例外となること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Method method = OracleDialectHandler.class.getDeclaredMethod("loadLobFromFile",
                String.class, String.class, String.class, int.class, DataType.class);
        method.setAccessible(true);

        Path baseLobDir = tempDir.resolve("dump").resolve("files");
        Files.createDirectories(baseLobDir);
        Files.write(baseLobDir.resolve("b.bin"), new byte[] {0x01});
        Files.writeString(baseLobDir.resolve("c.txt"), "clob", StandardCharsets.UTF_8);

        Object blob = method.invoke(handler, "b.bin", "TBL", "BLOB_COL", Types.BLOB, DataType.BLOB);
        Object clob = method.invoke(handler, "c.txt", "TBL", "CLOB_COL", Types.CLOB, DataType.CLOB);
        assertArrayEquals(new byte[] {0x01}, (byte[]) blob);
        assertEquals("clob", clob);

        InvocationTargetException unsupported =
                assertThrows(InvocationTargetException.class, () -> method.invoke(handler, "c.txt",
                        "TBL", "TXT_COL", Types.VARCHAR, DataType.VARCHAR));
        assertTrue(unsupported.getCause() instanceof DataSetException);

        InvocationTargetException missing =
                assertThrows(InvocationTargetException.class, () -> method.invoke(handler,
                        "missing.bin", "TBL", "BLOB_COL", Types.BLOB, DataType.BLOB));
        assertTrue(missing.getCause() instanceof DataSetException);
    }

    @Test
    void loadLobFromFile_正常ケース_sqlTypeのみblobclobを指定する_型に応じた値が返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Method method = OracleDialectHandler.class.getDeclaredMethod("loadLobFromFile",
                String.class, String.class, String.class, int.class, DataType.class);
        method.setAccessible(true);

        Path baseLobDir = tempDir.resolve("dump").resolve("files");
        Files.createDirectories(baseLobDir);
        Files.write(baseLobDir.resolve("blob.bin"), new byte[] {0x01, 0x02});
        Files.writeString(baseLobDir.resolve("clob.txt"), "abc", StandardCharsets.UTF_8);

        DataType blobLike = mock(DataType.class);
        DataType clobLike = mock(DataType.class);
        when(blobLike.getSqlType()).thenReturn(Types.BLOB);
        when(clobLike.getSqlType()).thenReturn(Types.CLOB);

        Object blob = method.invoke(handler, "blob.bin", "TBL", "BLOB_COL", Types.BLOB, blobLike);
        Object clob = method.invoke(handler, "clob.txt", "TBL", "CLOB_COL", Types.CLOB, clobLike);

        assertArrayEquals(new byte[] {0x01, 0x02}, (byte[]) blob);
        assertEquals("abc", clob);
    }

    @Test
    public void loadLobFromFile_異常ケース_読み取り時IOExceptionが発生する_DataSetExceptionが送出されること()
            throws Exception {

        OracleDialectHandler handler = createHandler();

        // handler の baseLobDir 想定と同じ位置に「ディレクトリ」を作る
        // （既存テストが tempDir/dump/files を使って成功している前提に合わせる）
        Path baseLobDir = tempDir.resolve("dump").resolve("files");
        Files.createDirectories(baseLobDir);

        // ★「ファイル名」のはずの場所にディレクトリを作る（これが readAllBytes で IOException になる）
        Path dirAsFile = baseLobDir.resolve("dir_lob");
        Files.createDirectories(dirAsFile);

        Method method = OracleDialectHandler.class.getDeclaredMethod("loadLobFromFile",
                String.class, String.class, String.class, int.class, DataType.class);
        method.setAccessible(true);

        InvocationTargetException ex = assertThrows(InvocationTargetException.class, () -> method
                .invoke(handler, "dir_lob", "TBL", "BLOB_COL", Types.BLOB, DataType.BLOB));

        assertTrue(ex.getCause() instanceof DataSetException);
        assertTrue(ex.getCause().getMessage().startsWith("Failed to read LOB file: "));
    }

    @Test
    void readLobFile_異常ケース_lob以外列にfile参照を指定する_DataSetExceptionが送出されること() throws Exception {
        OracleDialectHandler handler = createHandler();
        putTableMeta(handler, "TBL", "TXT", DataType.VARCHAR);
        File base = tempDir.toFile();
        File filesDir = new File(base, "files");
        Files.createDirectories(filesDir.toPath());
        Files.writeString(new File(filesDir, "a.txt").toPath(), "x", StandardCharsets.UTF_8);

        assertThrows(DataSetException.class,
                () -> handler.readLobFile("a.txt", "TBL", "TXT", base));
    }

    @Test
    void readLobFile_正常ケース_sqlType判定でblobclobを指定する_値が返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        File base = tempDir.toFile();
        File filesDir = new File(base, "files");
        Files.createDirectories(filesDir.toPath());
        Files.write(new File(filesDir, "s.bin").toPath(), new byte[] {0x01});
        Files.writeString(new File(filesDir, "s.txt").toPath(), "abc", StandardCharsets.UTF_8);

        ITableMetaData tableMetaData = mock(ITableMetaData.class);
        Column blobCol = mock(Column.class);
        Column clobCol = mock(Column.class);
        org.dbunit.dataset.datatype.DataType blobLike =
                mock(org.dbunit.dataset.datatype.DataType.class);
        org.dbunit.dataset.datatype.DataType clobLike =
                mock(org.dbunit.dataset.datatype.DataType.class);
        when(blobLike.getSqlType()).thenReturn(Types.BLOB);
        when(clobLike.getSqlType()).thenReturn(Types.CLOB);
        when(blobCol.getColumnName()).thenReturn("BLOB_COL");
        when(clobCol.getColumnName()).thenReturn("CLOB_COL");
        when(blobCol.getDataType()).thenReturn(blobLike);
        when(clobCol.getDataType()).thenReturn(clobLike);
        when(tableMetaData.getColumns()).thenReturn(new Column[] {blobCol, clobCol});

        Field f = OracleDialectHandler.class.getDeclaredField("tableMetaMap");
        f.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.Map<String, ITableMetaData> map =
                (java.util.Map<String, ITableMetaData>) f.get(handler);
        map.put("TBL", tableMetaData);

        Object blob = handler.readLobFile("s.bin", "TBL", "BLOB_COL", base);
        Object clob = handler.readLobFile("s.txt", "TBL", "CLOB_COL", base);
        assertArrayEquals(new byte[] {0x01}, (byte[]) blob);
        assertEquals("abc", clob);
    }

    @Test
    void isHexString_正常ケース_null空文字非hexhexを指定する_判定結果が返ること() throws Exception {
        Method method = OracleDialectHandler.class.getDeclaredMethod("isHexString", String.class);
        method.setAccessible(true);
        assertFalse((boolean) method.invoke(null, (String) null));
        assertFalse((boolean) method.invoke(null, ""));
        assertFalse((boolean) method.invoke(null, "GG"));
        assertTrue((boolean) method.invoke(null, "0A0B"));
    }

    @Test
    void isBlobSqlType_正常ケース_sqlTypeとdataTypeの組合せを指定する_判定結果が返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Method method = OracleDialectHandler.class.getDeclaredMethod("isBlobSqlType", int.class,
                DataType.class);
        method.setAccessible(true);

        DataType blobLike = mock(DataType.class);
        when(blobLike.getSqlType()).thenReturn(Types.BLOB);
        DataType notBlob = mock(DataType.class);
        when(notBlob.getSqlType()).thenReturn(Types.VARCHAR);

        assertTrue((boolean) method.invoke(handler, Types.BLOB, DataType.VARCHAR));
        assertFalse((boolean) method.invoke(handler, Types.VARCHAR, null));
        assertTrue((boolean) method.invoke(handler, Types.VARCHAR, DataType.BLOB));
        assertTrue((boolean) method.invoke(handler, Types.VARCHAR, blobLike));
        assertFalse((boolean) method.invoke(handler, Types.VARCHAR, notBlob));
    }

    @Test
    void isClobSqlType_正常ケース_sqlTypeとdataTypeの組合せを指定する_判定結果が返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Method method = OracleDialectHandler.class.getDeclaredMethod("isClobSqlType", int.class,
                DataType.class);
        method.setAccessible(true);

        DataType clobLike = mock(DataType.class);
        when(clobLike.getSqlType()).thenReturn(Types.CLOB);
        DataType nclobLike = mock(DataType.class);
        when(nclobLike.getSqlType()).thenReturn(Types.NCLOB);
        DataType notClob = mock(DataType.class);
        when(notClob.getSqlType()).thenReturn(Types.VARCHAR);

        assertTrue((boolean) method.invoke(handler, Types.CLOB, DataType.VARCHAR));
        assertTrue((boolean) method.invoke(handler, Types.NCLOB, DataType.VARCHAR));
        assertFalse((boolean) method.invoke(handler, Types.VARCHAR, null));
        assertTrue((boolean) method.invoke(handler, Types.VARCHAR, DataType.CLOB));
        assertTrue((boolean) method.invoke(handler, Types.VARCHAR, clobLike));
        assertTrue((boolean) method.invoke(handler, Types.VARCHAR, nclobLike));
        assertFalse((boolean) method.invoke(handler, Types.VARCHAR, notClob));
    }

    @Test
    void isUnknownDbUnitType_正常ケース_dataTypeの組合せを指定する_判定結果が返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Method method =
                OracleDialectHandler.class.getDeclaredMethod("isUnknownDbUnitType", DataType.class);
        method.setAccessible(true);

        DataType otherType = mock(DataType.class);
        when(otherType.getSqlType()).thenReturn(Types.OTHER);
        when(otherType.getSqlTypeName()).thenReturn("ANY");

        DataType nullNameType = mock(DataType.class);
        when(nullNameType.getSqlType()).thenReturn(Types.VARCHAR);
        when(nullNameType.getSqlTypeName()).thenReturn(null);

        DataType unknownNameType = mock(DataType.class);
        when(unknownNameType.getSqlType()).thenReturn(Types.VARCHAR);
        when(unknownNameType.getSqlTypeName()).thenReturn("UNKNOWN");

        DataType knownType = mock(DataType.class);
        when(knownType.getSqlType()).thenReturn(Types.VARCHAR);
        when(knownType.getSqlTypeName()).thenReturn("VARCHAR2");

        assertTrue((boolean) method.invoke(handler, (DataType) null));
        assertTrue((boolean) method.invoke(handler, DataType.UNKNOWN));
        assertTrue((boolean) method.invoke(handler, otherType));
        assertFalse((boolean) method.invoke(handler, nullNameType));
        assertTrue((boolean) method.invoke(handler, unknownNameType));
        assertFalse((boolean) method.invoke(handler, knownType));
    }

    @Test
    void applyForUpdate_正常ケース_SELECT文を指定する_FORUPDATE句が付与されること() throws Exception {
        OracleDialectHandler handler = createHandler();
        assertEquals("SELECT 1 FROM DUAL FOR UPDATE", handler.applyForUpdate("SELECT 1 FROM DUAL"));
    }

    private OracleDialectHandler createHandler() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        dbUnitConfig.setLobDirName("files");
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(new ArrayList<>());

        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        Connection jdbc = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rsTables = mock(ResultSet.class);
        IDataSet dataSet = mock(IDataSet.class);

        when(dbConn.getConnection()).thenReturn(jdbc);
        when(jdbc.getSchema()).thenReturn("APP");
        when(jdbc.getMetaData()).thenReturn(meta);
        when(meta.getTables(eq(null), eq("APP"), eq("%"), any(String[].class)))
                .thenReturn(rsTables);
        when(rsTables.next()).thenReturn(false);
        when(dbConn.createDataSet()).thenReturn(dataSet);

        return new OracleDialectHandler(dbConn, dumpConfig, dbUnitConfig,
                mock(DbUnitConfigFactory.class), mock(OracleDateTimeFormatUtil.class), pathsConfig);
    }

    private void putTableMeta(OracleDialectHandler handler, String table, String column,
            DataType dataType) throws Exception {
        ITableMetaData tableMetaData = mock(ITableMetaData.class);
        when(tableMetaData.getColumns()).thenReturn(new Column[] {new Column(column, dataType)});

        Field f = OracleDialectHandler.class.getDeclaredField("tableMetaMap");
        f.setAccessible(true);

        Object mapObj = f.get(handler);
        if (mapObj == null) {
            throw new IllegalStateException("tableMetaMap is null");
        }

        Method putMethod = mapObj.getClass().getMethod("put", Object.class, Object.class);
        putMethod.invoke(mapObj, table.toUpperCase(), tableMetaData);
    }

    private OracleDateTimeFormatUtil extractFormatter(OracleDialectHandler handler)
            throws Exception {
        Field f = OracleDialectHandler.class.getDeclaredField("dateTimeFormatter");
        f.setAccessible(true);
        return (OracleDateTimeFormatUtil) f.get(handler);
    }

    private void putTableMetaBySqlType(OracleDialectHandler handler, String table, String column,
            int sqlType, String sqlTypeName) throws Exception {

        ITableMetaData tableMetaData = mock(ITableMetaData.class);

        Column col = mock(Column.class);
        when(col.getColumnName()).thenReturn(column);

        org.dbunit.dataset.datatype.DataType dt = mock(org.dbunit.dataset.datatype.DataType.class);
        when(dt.getSqlType()).thenReturn(sqlType);
        when(dt.getSqlTypeName()).thenReturn(sqlTypeName);

        when(col.getDataType()).thenReturn(dt);
        when(tableMetaData.getColumns()).thenReturn(new Column[] {col});

        Field f = OracleDialectHandler.class.getDeclaredField("tableMetaMap");
        f.setAccessible(true);

        Object mapObj = f.get(handler);
        Method putMethod = mapObj.getClass().getMethod("put", Object.class, Object.class);
        putMethod.invoke(mapObj, table.toUpperCase(), tableMetaData);
    }

    private void putJdbcColumnSpec(OracleDialectHandler handler, String table, String column,
            int sqlType, String sqlTypeName) throws Exception {
        Field field = OracleDialectHandler.class.getDeclaredField("jdbcColumnSpecMap");
        field.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> jdbcMap =
                (Map<String, Map<String, Object>>) field.get(handler);

        Map<String, Object> perTable = jdbcMap.get(table.toUpperCase());
        if (perTable == null) {
            perTable = new java.util.HashMap<>();
            jdbcMap.put(table.toUpperCase(), perTable);
        }

        Class<?> specClass =
                Class.forName("io.github.yok.flexdblink.db.oracle.OracleDialectHandler$JdbcColumnSpec");
        Constructor<?> constructor = specClass.getDeclaredConstructor(int.class, String.class);
        constructor.setAccessible(true);

        Object spec = constructor.newInstance(sqlType, sqlTypeName);
        perTable.put(column.toUpperCase(), spec);
    }
}
