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
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.db.DbUnitConfigFactory;
import io.github.yok.flexdblink.util.DateTimeFormatUtil;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.FieldAccessor;
import net.bytebuddy.implementation.MethodCall;
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

    private static final Map<String, Class<?>> ORACLE_TEMPORAL_CLASS_CACHE =
            new ConcurrentHashMap<>();

    @Test
    void quoteIdentifier_正常ケース_識別子を引用符で囲む_ダブルクォート付き文字列であること() throws Exception {
        OracleDialectHandler handler = createHandler();
        assertEquals("\"A1\"", handler.quoteIdentifier("A1"));
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
        assertEquals(Date.class, parsed.getClass());
    }

    @Test
    void parseDateTimeValue_正常ケース_時刻を指定する_Timeが返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Object parsed = handler.parseDateTimeValue("COL", "01:02:03");
        assertEquals(Time.class, parsed.getClass());
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
        DateTimeFormatUtil formatter = mock(DateTimeFormatUtil.class);
        OracleDialectHandler handler = createHandler(formatter, mock(DbUnitConfigFactory.class));
        when(formatter.formatJdbcDateTime(eq("COL"), eq("X"), any())).thenReturn("FORMATTED");
        assertEquals("FORMATTED", handler.formatDateTimeColumn("COL", "X", mock(Connection.class)));
    }

    @Test
    void formatDateTimeColumn_正常ケース_フォーマッタが失敗する_toString値が返ること() throws Exception {
        DateTimeFormatUtil formatter = mock(DateTimeFormatUtil.class);
        OracleDialectHandler handler = createHandler(formatter, mock(DbUnitConfigFactory.class));
        doThrow(new RuntimeException("x")).when(formatter).formatJdbcDateTime(eq("COL"), eq("X"),
                any());
        assertEquals("X", handler.formatDateTimeColumn("COL", "X", mock(Connection.class)));
    }

    @Test
    void formatDateTimeColumn_正常ケース_OracleTIMESTAMPTZ形式を指定する_数値オフセット付き文字列が返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Object oracleValue = newDynamicOracleTemporalObject("oracle.jdbc.OracleTIMESTAMPTZ",
                "2020-01-01 12:34:56 +09:00");
        String actual = handler.formatDateTimeColumn("COL", oracleValue, mock(Connection.class));
        assertEquals("2020-01-01 12:34:56 +0900", actual);
    }

    @Test
    public void formatDateTimeColumn_正常ケース_OracleTimestampltz形式を指定する_数値オフセット付き文字列が返ること()
            throws Exception {
        OracleDialectHandler handler = createHandler();

        Object oracleValue = newDynamicOracleTemporalObject("oracle.jdbc.OracleTimestampltz",
                "2020-01-01 12:34:56 +09:00");

        String actual = handler.formatDateTimeColumn("COL", oracleValue, mock(Connection.class));

        assertEquals("2020-01-01 12:34:56 +0900", actual);
    }

    @Test
    void readLobFile_正常ケース_blob列を指定する_bytesが返ること() throws Exception {
        OracleDialectHandler handler =
                createHandlerWithMeta("TBL", new ColumnDef("BLOB_COL", DataType.BLOB));
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
        OracleDialectHandler handler =
                createHandlerWithMeta("TBL", new ColumnDef("CLOB_COL", DataType.CLOB));
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
        OracleDialectHandler handler =
                createHandlerWithMeta("TBL", new ColumnDef("CLOB_COL", DataType.CLOB));
        assertThrows(DataSetException.class,
                () -> handler.readLobFile("missing.txt", "TBL", "CLOB_COL", tempDir.toFile()));
    }

    @Test
    void convertCsvValueToDbType_正常ケース_hex文字列を指定する_byte配列が返ること() throws Exception {
        OracleDialectHandler handler =
                createHandlerWithMeta("TBL", new ColumnDef("BIN_COL", DataType.VARBINARY));
        Object actual = handler.convertCsvValueToDbType("TBL", "BIN_COL", "0A0B");
        assertArrayEquals(new byte[] {0x0A, 0x0B}, (byte[]) actual);
    }

    @Test
    void convertCsvValueToDbType_正常ケース_numeric値を指定する_BigDecimalが返ること() throws Exception {
        OracleDialectHandler handler =
                createHandlerWithMeta("TBL", new ColumnDef("NUM_COL", DataType.NUMERIC));
        Object actual = handler.convertCsvValueToDbType("TBL", "NUM_COL", "123.45");
        assertEquals("123.45", actual.toString());
    }

    @Test
    void convertCsvValueToDbType_正常ケース_bigint値を指定する_Longが返ること() throws Exception {
        OracleDialectHandler handler =
                createHandlerWithMeta("TBL", new ColumnDef("BIG_COL", DataType.BIGINT));
        Object actual = handler.convertCsvValueToDbType("TBL", "BIG_COL", "2147483648");
        assertEquals(Long.class, actual.getClass());
        assertEquals(2147483648L, actual);
    }

    @Test
    void convertCsvValueToDbType_異常ケース_bigintに非数値を指定する_DataSetExceptionが送出されること() throws Exception {
        OracleDialectHandler handler =
                createHandlerWithMeta("TBL", new ColumnDef("BIG_COL", DataType.BIGINT));
        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("TBL", "BIG_COL", "abc"));
    }

    @Test
    void convertCsvValueToDbType_正常ケース_file参照を指定する_lob内容が返ること() throws Exception {
        OracleDialectHandler handler =
                createHandlerWithMeta("TBL", new ColumnDef("CLOB_COL", DataType.CLOB));
        Path lob = tempDir.resolve("dump").resolve("files").resolve("x.txt");
        Files.createDirectories(lob.getParent());
        Files.writeString(lob, "lobdata", StandardCharsets.UTF_8);
        Object actual = handler.convertCsvValueToDbType("TBL", "CLOB_COL", "file:x.txt");
        assertEquals("lobdata", actual);
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_timeWithTimezone_非コロン形式で2段目パーサ成功する_OffsetTimeが返ること()
            throws Exception {
        OracleDialectHandler handler = createHandlerWithMeta("TBL",
                new ColumnDef("OT_COL", java.sql.Types.TIME_WITH_TIMEZONE, "TIME"));

        Object actual = handler.convertCsvValueToDbType("TBL", "OT_COL", "010203+0900");

        assertEquals("java.time.OffsetTime", actual.getClass().getName());
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_timestamp_日付のみを指定する_0時のTimestampが返ること()
            throws Exception {
        OracleDialectHandler handler =
                createHandlerWithMeta("TBL", new ColumnDef("TS_COL", DataType.TIMESTAMP));

        Object actual = handler.convertCsvValueToDbType("TBL", "TS_COL", "20260215");
        assertInstanceOf(Timestamp.class, actual);

        Timestamp ts = (Timestamp) actual;
        assertEquals(Timestamp.valueOf("2026-02-15 00:00:00"), ts);
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_otherでinterval以外を指定する_文字列がそのまま返ること()
            throws Exception {
        OracleDialectHandler handler = createHandlerWithMeta("TBL",
                new ColumnDef("OTHER_COL", java.sql.Types.OTHER, "OTHER"));

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

        try (MockedConstruction<CsvDataSet> ignored = mockConstruction(CsvDataSet.class,
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
        DataType blobLike = mock(DataType.class);
        DataType clobLike = mock(DataType.class);
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
        OracleDialectHandler handler = createHandlerWithMeta("TBL",
                new ColumnDef("D_COL", DataType.DATE), new ColumnDef("T_COL", DataType.TIME),
                new ColumnDef("TS_COL", DataType.TIMESTAMP));
        Object d = handler.convertCsvValueToDbType("TBL", "D_COL", "2026/02/15");
        assertEquals(Date.class, d.getClass());

        Object t = handler.convertCsvValueToDbType("TBL", "T_COL", "010203");
        assertEquals(Time.class, t.getClass());

        Object ts = handler.convertCsvValueToDbType("TBL", "TS_COL", "2026-02-15T01:02:03");
        assertInstanceOf(Timestamp.class, ts);
    }

    @Test
    void convertCsvValueToDbType_異常ケース_不正な日付時刻文字列を指定する_DataSetExceptionが送出されること() throws Exception {
        OracleDialectHandler handler = createHandlerWithMeta("TBL",
                new ColumnDef("D_COL", DataType.DATE), new ColumnDef("TS_COL", DataType.TIMESTAMP));
        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("TBL", "D_COL", "not-a-date"));

        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("TBL", "TS_COL", "bad-ts"));
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_varcharを指定する_文字列がそのまま返ること() throws Exception {
        OracleDialectHandler handler =
                createHandlerWithMeta("TBL", new ColumnDef("TXT_COL", DataType.VARCHAR));

        Object actual = handler.convertCsvValueToDbType("TBL", "TXT_COL", "abc");
        assertEquals("abc", actual);
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_blob_file参照を指定する_bytesが返ること() throws Exception {
        OracleDialectHandler handler =
                createHandlerWithMeta("TBL", new ColumnDef("BLOB_COL", DataType.BLOB));

        Path lob = tempDir.resolve("dump").resolve("files").resolve("b.bin");
        Files.createDirectories(lob.getParent());
        Files.write(lob, new byte[] {0x01, 0x02, 0x03});

        Object actual = handler.convertCsvValueToDbType("TBL", "BLOB_COL", "file:b.bin");
        assertArrayEquals(new byte[] {0x01, 0x02, 0x03}, (byte[]) actual);
    }

    @Test
    public void convertCsvValueToDbType_異常ケース_blob_file参照でファイルが存在しない_DataSetExceptionが送出されること()
            throws Exception {
        OracleDialectHandler handler =
                createHandlerWithMeta("TBL", new ColumnDef("BLOB_COL", DataType.BLOB));

        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("TBL", "BLOB_COL", "file:missing.bin"));
    }

    @Test
    public void convertCsvValueToDbType_異常ケース_blob_file参照で読み取り時IOExceptionが発生する_DataSetExceptionが送出されること()
            throws Exception {
        OracleDialectHandler handler =
                createHandlerWithMeta("TBL", new ColumnDef("BLOB_COL", DataType.BLOB));
        Path lob = tempDir.resolve("dump").resolve("files").resolve("dir-lob");
        Files.createDirectories(lob);

        DataSetException ex = assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("TBL", "BLOB_COL", "file:dir-lob"));

        assertTrue(ex.getMessage().startsWith("Failed to read LOB file: "));
    }

    @Test
    public void convertCsvValueToDbType_異常ケース_file参照をlob以外列に指定する_DataSetExceptionが送出されること()
            throws Exception {
        OracleDialectHandler handler = createHandlerWithMetaAndJdbc("TBL",
                new ColumnDef[] {new ColumnDef("C1", DataType.VARCHAR)},
                new JdbcColumnDef[] {new JdbcColumnDef("TXT_COL", Types.VARCHAR, "VARCHAR2")});
        Path lob = tempDir.resolve("dump").resolve("files").resolve("x.txt");
        Files.createDirectories(lob.getParent());
        Files.writeString(lob, "x", StandardCharsets.UTF_8);

        DataSetException ex = assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("TBL", "TXT_COL", "file:x.txt"));

        assertTrue(ex.getMessage().startsWith("file: reference is supported only for BLOB/CLOB:"));
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
        OracleDialectHandler handler =
                createHandlerWithMeta("TBL", new ColumnDef("C1", DataType.VARCHAR));

        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("TBL", "NO_COL", "x"));
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_DBUnit列未登録かつJDBC列登録でTSTZを指定する_Timestampが返ること()
            throws Exception {
        OracleDialectHandler handler = createHandlerWithMetaAndJdbc("TBL",
                new ColumnDef[] {new ColumnDef("C1", DataType.VARCHAR)},
                new JdbcColumnDef[] {new JdbcColumnDef("TSTZ_COL", Types.TIMESTAMP_WITH_TIMEZONE,
                        "TIMESTAMP WITH TIME ZONE")});

        Object actual =
                handler.convertCsvValueToDbType("TBL", "TSTZ_COL", "2026-02-15 01:02:03+09:00");

        assertInstanceOf(Timestamp.class, actual);
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_DBUnit列未登録かつJDBC列登録でNCLOBファイル参照を指定する_文字列が返ること()
            throws Exception {
        OracleDialectHandler handler = createHandlerWithMetaAndJdbc("TBL",
                new ColumnDef[] {new ColumnDef("C1", DataType.VARCHAR)},
                new JdbcColumnDef[] {new JdbcColumnDef("NCLOB_COL", Types.NCLOB, "NCLOB")});

        Path lob = tempDir.resolve("dump").resolve("files").resolve("nclob.txt");
        Files.createDirectories(lob.getParent());
        Files.writeString(lob, "nclob-data", StandardCharsets.UTF_8);

        Object actual = handler.convertCsvValueToDbType("TBL", "NCLOB_COL", "file:nclob.txt");

        assertEquals("nclob-data", actual);
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_nullを指定する_nullが返ること() throws Exception {
        OracleDialectHandler handler =
                createHandlerWithMeta("TBL", new ColumnDef("C1", DataType.VARCHAR));

        Object actual = handler.convertCsvValueToDbType("TBL", "C1", null);
        assertNull(actual);
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_空白を指定する_nullが返ること() throws Exception {
        OracleDialectHandler handler =
                createHandlerWithMeta("TBL", new ColumnDef("C1", DataType.VARCHAR));

        Object actual = handler.convertCsvValueToDbType("TBL", "C1", "   ");
        assertNull(actual);
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_integerを指定する_Integerが返ること() throws Exception {
        OracleDialectHandler handler =
                createHandlerWithMeta("TBL", new ColumnDef("I_COL", DataType.INTEGER));

        Object actual = handler.convertCsvValueToDbType("TBL", "I_COL", "123");
        assertEquals(Integer.class, actual.getClass());
        assertEquals(123, actual);
    }

    @Test
    public void convertCsvValueToDbType_異常ケース_integerに非数値を指定する_DataSetExceptionが送出されること()
            throws Exception {
        OracleDialectHandler handler =
                createHandlerWithMeta("TBL", new ColumnDef("I_COL", DataType.INTEGER));

        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("TBL", "I_COL", "x"));
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_doubleを指定する_Doubleが返ること() throws Exception {
        OracleDialectHandler handler =
                createHandlerWithMeta("TBL", new ColumnDef("D_COL", DataType.DOUBLE));

        Object actual = handler.convertCsvValueToDbType("TBL", "D_COL", "10.5");
        assertEquals(Double.class, actual.getClass());
        assertEquals(10.5d, (Double) actual, 0.0d);
    }

    @Test
    public void convertCsvValueToDbType_異常ケース_doubleに非数値を指定する_DataSetExceptionが送出されること()
            throws Exception {
        OracleDialectHandler handler =
                createHandlerWithMeta("TBL", new ColumnDef("D_COL", DataType.DOUBLE));

        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("TBL", "D_COL", "bad"));
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_timeWithTimezoneを指定する_OffsetTimeが返ること()
            throws Exception {
        OracleDialectHandler handler = createHandlerWithMeta("TBL",
                new ColumnDef("OT_COL", Types.TIME_WITH_TIMEZONE, "TIME"));

        Object actual = handler.convertCsvValueToDbType("TBL", "OT_COL", "01:02:03+09:00");
        assertEquals("java.time.OffsetTime", actual.getClass().getName());
    }

    @Test
    public void convertCsvValueToDbType_異常ケース_timeWithTimezoneに不正値を指定する_DataSetExceptionが送出されること()
            throws Exception {
        OracleDialectHandler handler = createHandlerWithMeta("TBL",
                new ColumnDef("OT_COL", Types.TIME_WITH_TIMEZONE, "TIME"));

        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("TBL", "OT_COL", "bad"));
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_timestampWithTimezoneを指定する_Timestampが返ること()
            throws Exception {
        OracleDialectHandler handler = createHandlerWithMeta("TBL",
                new ColumnDef("TZTS_COL", Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP"));

        Object actual =
                handler.convertCsvValueToDbType("TBL", "TZTS_COL", "2026-02-15 01:02:03 +0900");

        assertInstanceOf(Timestamp.class, actual);
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_timestampWithTimezone_コロン付きオフセットで2段目パーサを通る_Timestampが返ること()
            throws Exception {
        OracleDialectHandler handler = createHandlerWithMeta("TBL",
                new ColumnDef("TZTS_COL", Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP"));

        // 1段目("... Z")を落として、2段目(FLEXIBLE_OFFSET_DATETIME_PARSER_COLON)を通す入力
        String input = "2026-02-15T01:02:03+09:00";

        Object actual = handler.convertCsvValueToDbType("TBL", "TZTS_COL", input);
        assertInstanceOf(Timestamp.class, actual);

        Timestamp ts = (Timestamp) actual;

        // Timestamp は Instant ベースで入るので、同じ Instant で比較する
        Timestamp expected = Timestamp.from(OffsetDateTime.parse(input).toInstant());
        assertEquals(expected.getTime(), ts.getTime());
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_intervalYearToMonthを指定する_INTERVALYMが返ること()
            throws Exception {
        OracleDialectHandler handler = createHandlerWithMeta("TBL",
                new ColumnDef("IVYM_COL", Types.OTHER, "INTERVAL YEAR TO MONTH"));

        Object actual = handler.convertCsvValueToDbType("TBL", "IVYM_COL", "1-2");
        assertEquals("oracle.sql.INTERVALYM", actual.getClass().getName());
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_intervalDayToSecondを指定する_INTERVALDSが返ること()
            throws Exception {
        OracleDialectHandler handler = createHandlerWithMeta("TBL",
                new ColumnDef("IVDS_COL", Types.OTHER, "INTERVAL DAY TO SECOND"));

        Object actual = handler.convertCsvValueToDbType("TBL", "IVDS_COL", "1 01:02:03");
        assertEquals("oracle.sql.INTERVALDS", actual.getClass().getName());
    }

    @Test
    public void convertCsvValueToDbType_異常ケース_varbinaryでhexデコード不能を指定する_DataSetExceptionが送出されること()
            throws Exception {
        OracleDialectHandler handler =
                createHandlerWithMeta("TBL", new ColumnDef("BIN_COL", DataType.VARBINARY));

        // isHexString は true だが、奇数桁で decodeHex が失敗するケース
        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("TBL", "BIN_COL", "ABC"));
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_binaryとlongvarbinaryのhexを指定する_byte配列が返ること()
            throws Exception {
        OracleDialectHandler handler =
                createHandlerWithMeta("TBL", new ColumnDef("BIN_COL", DataType.BINARY),
                        new ColumnDef("LBIN_COL", DataType.LONGVARBINARY));
        Object bin = handler.convertCsvValueToDbType("TBL", "BIN_COL", "0A0B");
        assertArrayEquals(new byte[] {0x0A, 0x0B}, (byte[]) bin);

        Object lbin = handler.convertCsvValueToDbType("TBL", "LBIN_COL", "0C0D");
        assertArrayEquals(new byte[] {0x0C, 0x0D}, (byte[]) lbin);
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_varbinaryでhexでもfile参照でもない値を指定する_文字列が返ること()
            throws Exception {
        OracleDialectHandler handler =
                createHandlerWithMeta("TBL", new ColumnDef("BIN_COL", DataType.VARBINARY));

        Object actual = handler.convertCsvValueToDbType("TBL", "BIN_COL", "not-hex");
        assertEquals("not-hex", actual);
    }

    @Test
    public void convertCsvValueToDbType_異常ケース_clobのfile参照でファイル不存在を指定する_DataSetExceptionが送出されること()
            throws Exception {
        OracleDialectHandler handler =
                createHandlerWithMeta("TBL", new ColumnDef("CLOB_COL", DataType.CLOB));

        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("TBL", "CLOB_COL", "file:missing.txt"));
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_DBUnitがunknownかつJDBCがNCLOBを指定する_JDBCメタ優先で文字列が返ること()
            throws Exception {
        OracleDialectHandler handler = createHandlerWithMetaAndJdbc("TBL",
                new ColumnDef[] {new ColumnDef("CLOB_COL", Types.OTHER, "UNKNOWN")},
                new JdbcColumnDef[] {new JdbcColumnDef("CLOB_COL", Types.NCLOB, "NCLOB")});

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
        assertNotNull(handler.getDataTypeFactory());
    }

    @Test
    void コンストラクタ_正常ケース_excludeTablesがnullでテーブルがある_例外なく初期化されること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DbUnitConfig dbUnitConfig = new DbUnitConfig();
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
                mock(DbUnitConfigFactory.class), mock(DateTimeFormatUtil.class), pathsConfig);
        assertNotNull(handler);
    }

    @Test
    void コンストラクタ_正常ケース_excludeTablesに対象を含める_除外テーブルのメタデータ取得が行われないこと() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DbUnitConfig dbUnitConfig = new DbUnitConfig();
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
                mock(DbUnitConfigFactory.class), mock(DateTimeFormatUtil.class), pathsConfig);

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
        when(jdbc.getSchema()).thenThrow(new SQLException("schema not supported"));
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
                mock(DbUnitConfigFactory.class), mock(DateTimeFormatUtil.class), pathsConfig);

        assertNotNull(handler);

        verify(meta).getTables(eq(null), eq("app_user"), eq("%"), any(String[].class));
    }

    @Test
    void createDbUnitConnection_正常ケース_設定ファクトリを適用する_生成接続が返ること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(new ArrayList<>());

        DbUnitConfigFactory configFactory = mock(DbUnitConfigFactory.class);
        DateTimeFormatUtil formatter = mock(DateTimeFormatUtil.class);

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
    void convertCsvValueToDbType_正常ケース_TIME型で設定済みフォーマッタ成功_設定値のTimeが返ること() throws Exception {
        DateTimeFormatUtil formatter = mock(DateTimeFormatUtil.class);
        OracleDialectHandler handler = createHandlerWithMeta(formatter,
                mock(DbUnitConfigFactory.class), "TBL", new ColumnDef("TIME_COL", DataType.TIME));
        LocalTime expected = LocalTime.of(14, 5, 30);
        when(formatter.parseConfiguredTime("14:05:30")).thenReturn(expected);

        Object actual = handler.convertCsvValueToDbType("TBL", "TIME_COL", "14:05:30");
        assertEquals(Time.valueOf(expected), actual);
    }

    @Test
    void convertCsvValueToDbType_正常ケース_TIME型でコロン形式を指定する_Timeが返ること() throws Exception {
        OracleDialectHandler handler =
                createHandlerWithMeta("TBL", new ColumnDef("TIME_COL", DataType.TIME));

        Object actual = handler.convertCsvValueToDbType("TBL", "TIME_COL", "14:05:30");

        assertEquals(Time.valueOf(LocalTime.of(14, 5, 30)), actual);
    }

    @Test
    void convertCsvValueToDbType_正常ケース_DATE型で設定済みフォーマッタ成功_設定値のDateが返ること() throws Exception {
        DateTimeFormatUtil formatter = mock(DateTimeFormatUtil.class);
        OracleDialectHandler handler = createHandlerWithMeta(formatter,
                mock(DbUnitConfigFactory.class), "TBL", new ColumnDef("DATE_COL", DataType.DATE));
        LocalDate expected = LocalDate.of(2026, 2, 15);
        when(formatter.parseConfiguredDate("2026-02-15")).thenReturn(expected);

        Object actual = handler.convertCsvValueToDbType("TBL", "DATE_COL", "2026-02-15");
        assertEquals(Date.valueOf(expected), actual);
    }

    @Test
    void convertCsvValueToDbType_正常ケース_TIMESTAMP型で設定済みフォーマッタ成功_設定値のTimestampが返ること()
            throws Exception {
        DateTimeFormatUtil formatter = mock(DateTimeFormatUtil.class);
        OracleDialectHandler handler = createHandlerWithMeta(formatter,
                mock(DbUnitConfigFactory.class), "TBL",
                new ColumnDef("TS_COL", DataType.TIMESTAMP));
        LocalDateTime expected = LocalDateTime.of(2026, 2, 15, 1, 2, 3);
        when(formatter.parseConfiguredTimestamp("2026-02-15 01:02:03")).thenReturn(expected);

        Object actual = handler.convertCsvValueToDbType("TBL", "TS_COL", "2026-02-15 01:02:03");
        assertEquals(Timestamp.valueOf(expected), actual);
    }

    @Test
    void convertCsvValueToDbType_異常ケース_TIME型に不正値を指定する_DataSetExceptionが送出されること() throws Exception {
        OracleDialectHandler handler =
                createHandlerWithMeta("TBL", new ColumnDef("TIME_COL", DataType.TIME));

        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("TBL", "TIME_COL", "xx"));
    }

    @Test
    void convertCsvValueToDbType_正常ケース_sqlTypeNameがnullのINTERVALとOTHERを指定する_型に応じた値が返ること()
            throws Exception {
        OracleDialectHandler handler = createHandlerWithMeta("TBL",
                new ColumnDef("IVYM_COL", -103, null), new ColumnDef("IVDS_COL", -104, null),
                new ColumnDef("OTHER_COL", Types.OTHER, null));

        Object ym = handler.convertCsvValueToDbType("TBL", "IVYM_COL", "1-2");
        Object ds = handler.convertCsvValueToDbType("TBL", "IVDS_COL", "1 01:02:03");
        Object other = handler.convertCsvValueToDbType("TBL", "OTHER_COL", "x");

        assertEquals("oracle.sql.INTERVALYM", ym.getClass().getName());
        assertEquals("oracle.sql.INTERVALDS", ds.getClass().getName());
        assertEquals("x", other);
    }

    @Test
    void readLobFile_異常ケース_読み取り時IOExceptionが発生する_IOExceptionが送出されること() throws Exception {
        OracleDialectHandler handler =
                createHandlerWithMeta("TBL", new ColumnDef("BLOB_COL", DataType.BLOB));
        File base = tempDir.toFile();
        File filesDir = new File(base, "files");
        Files.createDirectories(filesDir.toPath());
        Files.createDirectories(new File(filesDir, "dir_lob").toPath());

        assertThrows(java.io.IOException.class,
                () -> handler.readLobFile("dir_lob", "TBL", "BLOB_COL", base));
    }

    @Test
    void readLobFile_異常ケース_lob以外列にfile参照を指定する_DataSetExceptionが送出されること() throws Exception {
        OracleDialectHandler handler =
                createHandlerWithMeta("TBL", new ColumnDef("TXT", DataType.VARCHAR));
        File base = tempDir.toFile();
        File filesDir = new File(base, "files");
        Files.createDirectories(filesDir.toPath());
        Files.writeString(new File(filesDir, "a.txt").toPath(), "x", StandardCharsets.UTF_8);

        assertThrows(DataSetException.class,
                () -> handler.readLobFile("a.txt", "TBL", "TXT", base));
    }

    @Test
    void readLobFile_正常ケース_sqlType判定でblobclobを指定する_値が返ること() throws Exception {
        OracleDialectHandler handler = createHandlerWithMeta("TBL",
                new ColumnDef("BLOB_COL", Types.BLOB, "BLOB"),
                new ColumnDef("CLOB_COL", Types.CLOB, "CLOB"));
        File base = tempDir.toFile();
        File filesDir = new File(base, "files");
        Files.createDirectories(filesDir.toPath());
        Files.write(new File(filesDir, "s.bin").toPath(), new byte[] {0x01});
        Files.writeString(new File(filesDir, "s.txt").toPath(), "abc", StandardCharsets.UTF_8);

        Object blob = handler.readLobFile("s.bin", "TBL", "BLOB_COL", base);
        Object clob = handler.readLobFile("s.txt", "TBL", "CLOB_COL", base);
        assertArrayEquals(new byte[] {0x01}, (byte[]) blob);
        assertEquals("abc", clob);
    }

    @Test
    void shouldUseRawValueForComparison_正常ケース_sqlTypeNameがINTERVALである_trueが返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        assertTrue(handler.shouldUseRawValueForComparison("INTERVAL DAY TO SECOND"));
        assertFalse(handler.shouldUseRawValueForComparison("VARCHAR2"));
        assertFalse(handler.shouldUseRawValueForComparison(null));
    }

    @Test
    void normalizeValueForComparison_正常ケース_引数に応じてINTERVAL正規化する_期待形式に整形されること() throws Exception {
        OracleDialectHandler handler = createHandler();

        assertNull(handler.normalizeValueForComparison("COL", "INTERVAL YEAR TO MONTH", null));
        assertEquals("abc", handler.normalizeValueForComparison("COL", "VARCHAR2", "abc"));
        assertEquals("+01-02",
                handler.normalizeValueForComparison("COL", "INTERVAL YEAR TO MONTH", "1-2"));
        assertEquals("-01-02",
                handler.normalizeValueForComparison("COL", "INTERVAL YEAR TO MONTH", "-1-2"));
        assertEquals("+01 02:03:04", handler.normalizeValueForComparison("COL",
                "INTERVAL DAY TO SECOND", "1 2:3:4.999"));
        assertEquals("-01 02:03:04",
                handler.normalizeValueForComparison("COL", "INTERVAL DAY TO SECOND", "-1 2:3:4"));
        assertEquals("bad",
                handler.normalizeValueForComparison("COL", "INTERVAL YEAR TO MONTH", " bad "));
        assertEquals("bad",
                handler.normalizeValueForComparison("COL", "INTERVAL DAY TO SECOND", " bad "));
    }


    @Test
    void isDateTimeTypeForDump_正常ケース_oracle固有時刻型を指定する_trueが返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        assertTrue(handler.isDateTimeTypeForDump(Types.DATE, "DATE"));
        assertTrue(handler.isDateTimeTypeForDump(-101, "TIMESTAMP WITH TIME ZONE"));
        assertTrue(handler.isDateTimeTypeForDump(-102, "TIMESTAMP WITH LOCAL TIME ZONE"));
        assertTrue(handler.isDateTimeTypeForDump(Types.TIMESTAMP_WITH_TIMEZONE,
                "TIMESTAMP WITH TIME ZONE"));
        assertTrue(handler.isDateTimeTypeForDump(Types.OTHER, "TIMESTAMP WITH TIME ZONE"));
        assertTrue(handler.isDateTimeTypeForDump(Types.OTHER, "TIMESTAMP WITH LOCAL TIME ZONE"));
        assertFalse(handler.isDateTimeTypeForDump(Types.VARCHAR, "VARCHAR2"));
        assertFalse(handler.isDateTimeTypeForDump(Types.OTHER, null));
    }

    @Test
    void isBinaryTypeForDump_正常ケース_raw系型名を指定する_trueが返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        assertTrue(handler.isBinaryTypeForDump(Types.BINARY, "BINARY"));
        assertTrue(handler.isBinaryTypeForDump(Types.OTHER, "RAW"));
        assertTrue(handler.isBinaryTypeForDump(Types.OTHER, "LONG RAW"));
        assertFalse(handler.isBinaryTypeForDump(Types.VARCHAR, "VARCHAR2"));
        assertFalse(handler.isBinaryTypeForDump(Types.OTHER, null));
    }

    private OracleDialectHandler createHandler() throws Exception {
        return createHandler(mock(DateTimeFormatUtil.class), mock(DbUnitConfigFactory.class));
    }

    private OracleDialectHandler createHandler(DateTimeFormatUtil formatter,
            DbUnitConfigFactory configFactory) throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DbUnitConfig dbUnitConfig = new DbUnitConfig();
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

        return new OracleDialectHandler(dbConn, dumpConfig, dbUnitConfig, configFactory, formatter,
                pathsConfig);
    }

    private static final class ColumnDef {

        final String name;
        final int sqlType;
        final String sqlTypeName;

        ColumnDef(String name, DataType dataType) {
            this.name = name;
            this.sqlType = dataType.getSqlType();
            this.sqlTypeName = dataType.toString();
        }

        ColumnDef(String name, int sqlType, String sqlTypeName) {
            this.name = name;
            this.sqlType = sqlType;
            this.sqlTypeName = sqlTypeName;
        }
    }

    private static final class JdbcColumnDef {

        final String name;
        final int sqlType;
        final String sqlTypeName;

        JdbcColumnDef(String name, int sqlType, String sqlTypeName) {
            this.name = name;
            this.sqlType = sqlType;
            this.sqlTypeName = sqlTypeName;
        }
    }

    private OracleDialectHandler createHandlerWithMeta(String tableName, ColumnDef... columns)
            throws Exception {
        return createHandlerWithMeta(mock(DateTimeFormatUtil.class),
                mock(DbUnitConfigFactory.class), tableName, columns);
    }

    private OracleDialectHandler createHandlerWithMeta(DateTimeFormatUtil formatter,
            DbUnitConfigFactory configFactory, String tableName, ColumnDef... columns)
            throws Exception {
        return createHandlerWithMetaAndJdbc(formatter, configFactory, tableName, columns,
                new JdbcColumnDef[0], false);
    }

    private OracleDialectHandler createHandlerWithMetaAndJdbc(String tableName, ColumnDef[] columns,
            JdbcColumnDef[] jdbcColumns) throws Exception {
        return createHandlerWithMetaAndJdbc(mock(DateTimeFormatUtil.class),
                mock(DbUnitConfigFactory.class), tableName, columns, jdbcColumns);
    }

    private OracleDialectHandler createHandlerWithMetaAndJdbc(DateTimeFormatUtil formatter,
            DbUnitConfigFactory configFactory, String tableName, ColumnDef[] columns,
            JdbcColumnDef[] jdbcColumns) throws Exception {
        return createHandlerWithMetaAndJdbc(formatter, configFactory, tableName, columns,
                jdbcColumns, true);
    }

    private OracleDialectHandler createHandlerWithMetaAndJdbc(DateTimeFormatUtil formatter,
            DbUnitConfigFactory configFactory, String tableName, ColumnDef[] columns,
            JdbcColumnDef[] jdbcColumns, boolean includeDbUnitColumnsInJdbcMeta) throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(new ArrayList<>());
        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        Connection jdbc = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        IDataSet dataSet = mock(IDataSet.class);
        when(dbConn.getConnection()).thenReturn(jdbc);
        when(jdbc.getSchema()).thenReturn("APP");
        when(jdbc.getMetaData()).thenReturn(meta);
        ResultSet rsTables = mock(ResultSet.class);
        when(meta.getTables(eq(null), eq("APP"), eq("%"), any(String[].class)))
                .thenReturn(rsTables);
        when(rsTables.next()).thenReturn(true, false);
        when(rsTables.getString("TABLE_NAME")).thenReturn(tableName);
        Column[] dbunitColumns = new Column[columns.length];
        for (int i = 0; i < columns.length; i++) {
            dbunitColumns[i] = mock(Column.class);
            DataType dt = mock(DataType.class);
            when(dbunitColumns[i].getColumnName()).thenReturn(columns[i].name);
            when(dt.getSqlType()).thenReturn(columns[i].sqlType);
            when(dt.getSqlTypeName()).thenReturn(columns[i].sqlTypeName);
            when(dbunitColumns[i].getDataType()).thenReturn(dt);
        }
        ITableMetaData tableMeta = mock(ITableMetaData.class);
        when(tableMeta.getColumns()).thenReturn(dbunitColumns);
        when(dataSet.getTableMetaData(tableName)).thenReturn(tableMeta);
        when(dbConn.createDataSet()).thenReturn(dataSet);
        List<String> allJdbcNames = new ArrayList<>();
        List<Integer> allJdbcTypes = new ArrayList<>();
        List<String> allJdbcTypeNames = new ArrayList<>();
        if (includeDbUnitColumnsInJdbcMeta) {
            for (ColumnDef c : columns) {
                allJdbcNames.add(c.name);
                allJdbcTypes.add(c.sqlType);
                allJdbcTypeNames.add(c.sqlTypeName);
            }
        }
        for (JdbcColumnDef jc : jdbcColumns) {
            allJdbcNames.add(jc.name);
            allJdbcTypes.add(jc.sqlType);
            allJdbcTypeNames.add(jc.sqlTypeName);
        }
        ResultSet rsColumns = mock(ResultSet.class);
        when(meta.getColumns(eq(null), eq("APP"), eq(tableName), eq("%"))).thenReturn(rsColumns);
        if (allJdbcNames.isEmpty()) {
            when(rsColumns.next()).thenReturn(false);
        } else {
            Boolean[] nextRest = new Boolean[allJdbcNames.size()];
            for (int i = 0; i < allJdbcNames.size() - 1; i++) {
                nextRest[i] = Boolean.TRUE;
            }
            nextRest[allJdbcNames.size() - 1] = Boolean.FALSE;
            when(rsColumns.next()).thenReturn(true, nextRest);
            String[] names = allJdbcNames.toArray(new String[0]);
            Integer[] types = allJdbcTypes.toArray(new Integer[0]);
            String[] typeNames = allJdbcTypeNames.toArray(new String[0]);
            when(rsColumns.getString("COLUMN_NAME")).thenReturn(names[0],
                    Arrays.copyOfRange(names, 1, names.length));
            when(rsColumns.getInt("DATA_TYPE")).thenReturn(types[0],
                    Arrays.copyOfRange(types, 1, types.length));
            when(rsColumns.getString("TYPE_NAME")).thenReturn(typeNames[0],
                    Arrays.copyOfRange(typeNames, 1, typeNames.length));
        }
        return new OracleDialectHandler(dbConn, dumpConfig, dbUnitConfig, configFactory, formatter,
                pathsConfig);
    }

    @Test
    public void getPrimaryKeyColumns_正常ケース_主キーが存在しない_空リストが返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);

        when(conn.getMetaData()).thenReturn(metaData);
        when(metaData.getPrimaryKeys(null, "APP", "TBL")).thenReturn(rs);
        when(rs.next()).thenReturn(false);

        List<String> actual = handler.getPrimaryKeyColumns(conn, "APP", "TBL");
        assertEquals(List.of(), actual);
    }

    @Test
    public void formatDateTimeColumn_正常ケース_OracleTIMESTAMPTZでrawがnull_nullが返ること() throws Exception {
        OracleDialectHandler handler = createHandler();

        Object oracleValue = newDynamicOracleTemporalObject("oracle.jdbc.OracleTIMESTAMPTZ", null);

        String actual = handler.formatDateTimeColumn("COL", oracleValue, mock(Connection.class));
        assertNull(actual);
    }

    @Test
    public void formatDateTimeColumn_正常ケース_OracleTIMESTAMPTZでリージョン形式を指定する_数値オフセットへ変換されること()
            throws Exception {
        OracleDialectHandler handler = createHandler();

        Object oracleValue = newDynamicOracleTemporalObject("oracle.jdbc.OracleTIMESTAMPTZ",
                "2026-02-15 01:02:03 Asia/Tokyo");

        String actual = handler.formatDateTimeColumn("COL", oracleValue, mock(Connection.class));
        assertEquals("2026-02-15 01:02:03 +0900", actual);
    }

    @Test
    public void formatDateTimeColumn_異常ケース_OracleTIMESTAMPTZでリージョン形式だが日付が不正_trimmedが返ること()
            throws Exception {
        OracleDialectHandler handler = createHandler();

        Object oracleValue = newDynamicOracleTemporalObject("oracle.jdbc.OracleTIMESTAMPTZ",
                " 2026-13-15 01:02:03 Asia/Tokyo ");

        String actual = handler.formatDateTimeColumn("COL", oracleValue, mock(Connection.class));
        assertEquals("2026-13-15 01:02:03 Asia/Tokyo", actual);
    }

    @Test
    public void normalizeRawTemporalValueForDump_正常ケース_rawがnull_空文字が返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        assertEquals("", handler.normalizeRawTemporalValueForDump("COL", null));
    }

    @Test
    public void normalizeRawTemporalValueForDump_正常ケース_DaySecond規約列_末尾のdot0が削除されること()
            throws Exception {
        OracleDialectHandler handler = createHandler();
        assertEquals("1 02:03:04",
                handler.normalizeRawTemporalValueForDump("INTERVAL_DS_COL", "1 02:03:04.0"));
        assertEquals("1 02:03:04",
                handler.normalizeRawTemporalValueForDump("IV_DS_COL", "1 02:03:04.000"));
    }

    @Test
    public void normalizeRawTemporalValueForDump_正常ケース_DaySecond規約列以外_そのまま返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        assertEquals("1 02:03:04.0",
                handler.normalizeRawTemporalValueForDump("OTHER_COL", "1 02:03:04.0"));
    }

    @Test
    public void shouldUseRawTemporalValueForDump_正常ケース_sqlTypeNameがINTERVALの時true_nullや非INTERVALはfalseであること()
            throws Exception {
        OracleDialectHandler handler = createHandler();

        assertTrue(handler.shouldUseRawTemporalValueForDump("COL", Types.OTHER,
                "INTERVAL DAY TO SECOND"));
        assertFalse(handler.shouldUseRawTemporalValueForDump("COL", Types.OTHER, "VARCHAR2"));
        assertFalse(handler.shouldUseRawTemporalValueForDump("COL", Types.OTHER, null));
    }

    @Test
    public void readLobFile_正常ケース_DBUnitに列が無いがJDBCメタでBLOB判定できる_bytesが返ること() throws Exception {

        // PathsConfig は dump setter が無いケースがあるため mock で合わせる
        PathsConfig pathsConfig = mock(PathsConfig.class);
        when(pathsConfig.getDataPath()).thenReturn(tempDir.toString());
        when(pathsConfig.getDump()).thenReturn(tempDir.resolve("dump").toString());

        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(new ArrayList<>());

        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        Connection jdbc = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rsTables = mock(ResultSet.class);
        ResultSet rsColumns = mock(ResultSet.class);
        IDataSet dataSet = mock(IDataSet.class);

        when(dbConn.getConnection()).thenReturn(jdbc);
        when(jdbc.getSchema()).thenReturn("APP");
        when(jdbc.getMetaData()).thenReturn(meta);

        // 対象テーブルを1つ返す
        when(meta.getTables(eq(null), eq("APP"), eq("%"), any(String[].class)))
                .thenReturn(rsTables);
        when(rsTables.next()).thenReturn(true, false);
        when(rsTables.getString("TABLE_NAME")).thenReturn("TBL");

        // JDBCメタ: BLOB_COL を返す
        when(meta.getColumns(eq(null), eq("APP"), eq("TBL"), eq("%"))).thenReturn(rsColumns);
        when(rsColumns.next()).thenReturn(true, false);
        when(rsColumns.getString("COLUMN_NAME")).thenReturn("BLOB_COL");
        when(rsColumns.getInt("DATA_TYPE")).thenReturn(Types.BLOB);
        when(rsColumns.getString("TYPE_NAME")).thenReturn("BLOB");

        // DBUnitメタ: BLOB_COL を含めない（IDのみ）
        ITableMetaData md = mock(ITableMetaData.class);
        when(md.getColumns()).thenReturn(new Column[] {new Column("ID", DataType.INTEGER)});
        when(dbConn.createDataSet()).thenReturn(dataSet);
        when(dataSet.getTableMetaData("TBL")).thenReturn(md);

        OracleDialectHandler handler = new OracleDialectHandler(dbConn, dumpConfig, dbUnitConfig,
                mock(DbUnitConfigFactory.class), mock(DateTimeFormatUtil.class), pathsConfig);

        // baseDir/files 配下に LOB ファイルを作成
        File baseDir = tempDir.toFile();
        File filesDir = new File(baseDir, "files");
        Files.createDirectories(filesDir.toPath());
        Files.write(new File(filesDir, "a.bin").toPath(), new byte[] {0x01, 0x02, 0x03});

        Object actual = handler.readLobFile("a.bin", "TBL", "BLOB_COL", baseDir);

        assertArrayEquals(new byte[] {0x01, 0x02, 0x03}, (byte[]) actual);
    }

    @Test
    public void hasNotNullLobColumn_異常ケース_closeでSQLExceptionが発生する_SQLExceptionが再スローされること()
            throws Exception {
        OracleDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);

        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getColumns(null, "APP", "TBL", null)).thenReturn(rs);

        // 走査はすぐ終わる
        when(rs.next()).thenReturn(false);

        // finally の close で例外
        doThrow(new SQLException("rs-close-error")).when(rs).close();

        SQLException ex = assertThrows(SQLException.class, () -> handler.hasNotNullLobColumn(conn,
                "APP", "TBL", new Column[] {new Column("CLOB_COL", DataType.CLOB)}));
        assertEquals("rs-close-error", ex.getMessage());
    }

    @Test
    public void formatDateTimeColumn_正常ケース_通常のTimestampを指定する_フォーマット値が返ること() throws Exception {
        DateTimeFormatUtil formatter = mock(DateTimeFormatUtil.class);
        OracleDialectHandler handler = createHandler(formatter, mock(DbUnitConfigFactory.class));
        when(formatter.formatJdbcDateTime(any(String.class), any(), any()))
                .thenReturn("2026-02-15 01:02:03");

        Timestamp ts = new Timestamp(System.currentTimeMillis());
        String result = handler.formatDateTimeColumn("COL", ts, mock(Connection.class));

        assertEquals("2026-02-15 01:02:03", result);
    }

    @Test
    public void formatDateTimeColumn_異常ケース_formatJdbcDateTimeが例外をスローする_toStringが返ること()
            throws Exception {
        DateTimeFormatUtil formatter = mock(DateTimeFormatUtil.class);
        OracleDialectHandler handler = createHandler(formatter, mock(DbUnitConfigFactory.class));
        when(formatter.formatJdbcDateTime(any(String.class), any(), any()))
                .thenThrow(new RuntimeException("format-fail"));

        Timestamp ts = Timestamp.valueOf("2026-02-15 01:02:03");
        String result = handler.formatDateTimeColumn("COL", ts, mock(Connection.class));

        assertEquals(ts.toString(), result);
    }

    @Test
    public void logTableDefinition_正常ケース_列メタデータが空である_例外なく処理されること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getColumns(null, "APP", "TBL", null)).thenReturn(rs);
        when(rs.next()).thenReturn(false);

        handler.logTableDefinition(conn, "APP", "TBL", "db1");
        verify(meta).getColumns(null, "APP", "TBL", null);
    }

    @Test
    public void logTableDefinition_異常ケース_getColumnsでSQLExceptionが発生する_SQLExceptionが再スローされること()
            throws Exception {
        OracleDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getColumns(null, "APP", "TBL", null)).thenThrow(new SQLException("meta-error"));

        assertThrows(SQLException.class,
                () -> handler.logTableDefinition(conn, "APP", "TBL", "db1"));
    }

    @Test
    void formatDateTimeColumn_正常ケース_oracleSqlTIMESTAMPTZ形式を指定する_フォールバックで結果が返ること() throws Exception {
        OracleDialectHandler handler = createHandler();
        Constructor<?> ctor = Class.forName("oracle.sql.TIMESTAMPTZ").getConstructor(byte[].class);
        Object oracleValue = ctor
                .newInstance((Object) new byte[] {120, 121, 1, 1, 13, 35, 57, 0, 0, 0, 0, 9, 60});
        String actual = handler.formatDateTimeColumn("COL", oracleValue, mock(Connection.class));
        assertNotNull(actual);
    }

    @Test
    void formatDateTimeColumn_正常ケース_oracleSqlTIMESTAMPLTZ形式を指定する_フォールバックで結果が返ること()
            throws Exception {
        OracleDialectHandler handler = createHandler();
        Constructor<?> ctor = Class.forName("oracle.sql.TIMESTAMPLTZ").getConstructor(byte[].class);
        Object oracleValue =
                ctor.newInstance((Object) new byte[] {120, 121, 1, 1, 13, 35, 57, 0, 0, 0, 0});
        String actual = handler.formatDateTimeColumn("COL", oracleValue, mock(Connection.class));
        assertNotNull(actual);
    }

    @Test
    void formatDateTimeColumn_正常ケース_OracleTIMESTAMPTZでオフセットもリージョンもない形式を指定する_末尾のdot0が除去されること()
            throws Exception {
        OracleDialectHandler handler = createHandler();
        Object oracleValue = newDynamicOracleTemporalObject("oracle.jdbc.OracleTIMESTAMPTZ",
                "2020-01-01 12:34:56.000");
        String actual = handler.formatDateTimeColumn("COL", oracleValue, mock(Connection.class));
        assertEquals("2020-01-01 12:34:56", actual);
    }

    @Test
    void convertCsvValueToDbType_正常ケース_DBUnit既知型かつJDBC情報ありでINTEGERを指定する_DBUnit型が優先されること()
            throws Exception {
        OracleDialectHandler handler = createHandlerWithMetaAndJdbc("TBL",
                new ColumnDef[] {new ColumnDef("NUM_COL", Types.INTEGER, "NUMBER")},
                new JdbcColumnDef[] {new JdbcColumnDef("NUM_COL", Types.VARCHAR, "VARCHAR2")});
        Object result = handler.convertCsvValueToDbType("TBL", "NUM_COL", "42");
        assertInstanceOf(Integer.class, result);
    }

    /**
     * Builds a runtime class with a specific Oracle JDBC-like FQCN and a
     * {@code stringValue(Connection)} method.
     *
     * @param className fully qualified class name to expose through {@code getClass().getName()}
     * @param rawValue return value of {@code stringValue(Connection)}
     * @return instantiated dynamic object
     * @throws Exception if bytecode generation or class loading fails
     */
    private Object newDynamicOracleTemporalObject(String className, String rawValue)
            throws Exception {
        Class<?> dynamicClass;
        try {
            dynamicClass = ORACLE_TEMPORAL_CLASS_CACHE.computeIfAbsent(className, name -> {
                try {
                    return new ByteBuddy().subclass(Object.class).name(name)
                            .defineField("__raw", String.class, Visibility.PRIVATE)
                            .defineConstructor(Visibility.PUBLIC).withParameters(String.class)
                            .intercept(MethodCall.invoke(Object.class.getConstructor())
                                    .andThen(FieldAccessor.ofField("__raw").setsArgumentAt(0)))
                            .defineMethod("stringValue", String.class, Visibility.PUBLIC)
                            .withParameters(Connection.class)
                            .intercept(FieldAccessor.ofField("__raw")).make()
                            .load(getClass().getClassLoader(), ClassLoadingStrategy.Default.WRAPPER)
                            .getLoaded();
                } catch (Exception e) {
                    throw new IllegalStateException("failed to build dynamic oracle temporal class",
                            e);
                }
            });
        } catch (IllegalStateException e) {
            throw (e.getCause() instanceof Exception) ? (Exception) e.getCause() : e;
        }

        return dynamicClass.getDeclaredConstructor(String.class).newInstance(rawValue);
    }
}
