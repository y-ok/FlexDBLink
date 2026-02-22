package io.github.yok.flexdblink.db.sqlserver;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
import io.github.yok.flexdblink.util.DateTimeFormatSupport;
import io.github.yok.flexdblink.util.LobPathConstants;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.IDataTypeFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class SqlServerDialectHandlerTest {

    @TempDir
    public Path tempDir;

    @Test
    public void prepareConnection_正常ケース_接続初期化を実行する_SQLServer向けSQLが実行されること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        when(connection.createStatement()).thenReturn(statement);

        handler.prepareConnection(connection);

        verify(statement).execute("SET LANGUAGE us_english");
        verify(statement).execute("SET DATEFORMAT ymd");
    }

    @Test
    public void resolveSchema_正常ケース_接続情報を指定する_dboが返ること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
        assertEquals("dbo", handler.resolveSchema(new ConnectionConfig.Entry()));
    }

    @Test
    public void applyPagination_正常ケース_offsetlimitを指定する_OffsetFetch形式が返ること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
        assertEquals("SELECT * FROM T OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY",
                handler.applyPagination("SELECT * FROM T", 5, 10));
    }

    @Test
    public void quoteIdentifier_正常ケース_識別子を指定する_角括弧で囲まれること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
        assertEquals("[ID]", handler.quoteIdentifier("ID"));
    }

    @Test
    public void booleanLiteral_正常ケース_真偽値リテラルを取得する_10が返ること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
        assertEquals("1", handler.getBooleanTrueLiteral());
        assertEquals("0", handler.getBooleanFalseLiteral());
    }

    @Test
    public void getCurrentTimestampFunction_正常ケース_関数を取得する_SYSDATETIMEが返ること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
        assertEquals("SYSDATETIME()", handler.getCurrentTimestampFunction());
    }

    @Test
    public void formatDateLiteral_正常ケース_日時を指定する_DATETIME2キャスト文字列が返ること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
        String actual = handler.formatDateLiteral(LocalDateTime.of(2026, 2, 19, 7, 0, 1));
        assertEquals("CAST('2026-02-19 07:00:01' AS DATETIME2)", actual);
    }

    @Test
    public void buildUpsertSql_正常ケース_列定義を指定する_MERGE文が返ること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
        String sql = handler.buildUpsertSql("T_MAIN", List.of("ID"), List.of("ID", "NAME"),
                List.of("NAME"));
        assertTrue(sql.startsWith("MERGE INTO [T_MAIN] AS tgt"));
        assertTrue(sql.contains("WHEN MATCHED THEN UPDATE SET"));
        assertTrue(sql.contains("WHEN NOT MATCHED THEN INSERT ([ID], [NAME]) VALUES"));
    }

    @Test
    public void getCreateTempTableSql_正常ケース_列定義を指定する_ハッシュ始まりの作成SQLが返ること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
        Map<String, String> columns = new LinkedHashMap<>();
        columns.put("ID", "BIGINT");
        columns.put("NAME", "NVARCHAR(20)");
        String sql = handler.getCreateTempTableSql("TMP_MAIN", columns);
        assertEquals("CREATE TABLE #TMP_MAIN ([ID] BIGINT, [NAME] NVARCHAR(20))", sql);
    }

    @Test
    public void applyForUpdate_正常ケース_select文を指定する_ロックヒント付きSQLが返ること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
        assertEquals("SELECT * FROM T WITH (UPDLOCK, ROWLOCK)",
                handler.applyForUpdate("SELECT * FROM T"));
    }

    @Test
    public void supportsFlags_正常ケース_各種サポートフラグを確認する_true系と空文字が返ること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
        assertTrue(handler.supportsGetGeneratedKeys());
        assertTrue(handler.supportsSequences());
        assertTrue(handler.supportsIdentityColumns());
        assertTrue(handler.supportsBatchUpdates());
        assertTrue(handler.supportsLobStreamByStream());
        assertEquals("", handler.getGeneratedKeyRetrievalSql());
        assertEquals("SELECT NEXT VALUE FOR [SEQ1]", handler.getNextSequenceSql("SEQ1"));
    }

    @Test
    public void parseDateTimeValue_正常ケース_offset付きを指定する_Timestampが返ること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
        Object actual = handler.parseDateTimeValue("COL", "2026-02-15 01:02:03+0900");
        assertInstanceOf(Timestamp.class, actual);
    }

    @Test
    public void parseDateTimeValue_正常ケース_localDatetimeを指定する_Timestampが返ること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
        Object actual = handler.parseDateTimeValue("COL", "2026-02-15 01:02:03");
        assertInstanceOf(Timestamp.class, actual);
    }

    @Test
    public void parseDateTimeValue_正常ケース_dateを指定する_Dateが返ること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
        Object actual = handler.parseDateTimeValue("COL", "2026/02/15");
        assertInstanceOf(Date.class, actual);
    }

    @Test
    public void parseDateTimeValue_正常ケース_timeを指定する_Timeが返ること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
        Object actual = handler.parseDateTimeValue("COL", "01:02:03.123");
        assertInstanceOf(Time.class, actual);
    }

    @Test
    public void parseDateTimeValue_異常ケース_短い不正値を指定する_IllegalArgumentExceptionが送出されること()
            throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
        assertThrows(IllegalArgumentException.class, () -> handler.parseDateTimeValue("COL", "1"));
    }

    @Test
    public void parseDateTimeValue_異常ケース_不正オフセットを指定する_IllegalArgumentExceptionが送出されること()
            throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
        assertThrows(IllegalArgumentException.class,
                () -> handler.parseDateTimeValue("COL", "2026-02-15 01:02:03+ABCD"));
    }

    @Test
    public void isDateTimeTypeForDump_正常ケース_型コードを指定する_標準型とDATETIMEOFFSETがtrueであること()
            throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
        assertTrue(handler.isDateTimeTypeForDump(Types.TIMESTAMP, "timestamp"));
        assertTrue(handler.isDateTimeTypeForDump(-155, "datetimeoffset"));
    }

    @Test
    public void getColumnTypeName_正常ケース_型情報が存在する_TYPE_NAMEが返ること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getColumns(null, "dbo", "t1", "c1")).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getString("TYPE_NAME")).thenReturn("int");

        assertEquals("int", handler.getColumnTypeName(conn, "dbo", "t1", "c1"));
    }

    @Test
    public void getColumnTypeName_異常ケース_型情報が存在しない_SQLExceptionが送出されること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getColumns(null, "dbo", "t1", "c1")).thenReturn(rs);
        when(rs.next()).thenReturn(false);

        assertThrows(SQLException.class, () -> handler.getColumnTypeName(conn, "dbo", "t1", "c1"));
    }

    @Test
    public void getColumnTypeName_異常ケース_ResultSetCloseで例外が発生する_SQLExceptionが送出されること()
            throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());

        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);

        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getColumns(null, "dbo", "t1", "c1")).thenReturn(rs);

        // ループ本体は成功させない（throw 経路）
        when(rs.next()).thenReturn(false);

        // try-with-resources の close 分岐を踏む（close が例外）
        doThrow(new SQLException("close failed")).when(rs).close();

        assertThrows(SQLException.class, () -> handler.getColumnTypeName(conn, "dbo", "t1", "c1"));
    }

    @Test
    public void getColumnTypeName_異常ケース_getString呼び出しでSQLExceptionが発生する_SQLExceptionが送出されること()
            throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());

        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);

        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getColumns(null, "dbo", "t1", "c1")).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        doThrow(new SQLException("get type failed")).when(rs).getString("TYPE_NAME");

        assertThrows(SQLException.class, () -> handler.getColumnTypeName(conn, "dbo", "t1", "c1"));
    }

    @Test
    public void getColumnTypeName_異常ケース_getColumns呼び出しでSQLExceptionが発生する_SQLExceptionが送出されること()
            throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        doThrow(new SQLException("getColumns failed")).when(meta).getColumns(null, "dbo", "t1",
                "c1");
        assertThrows(SQLException.class, () -> handler.getColumnTypeName(conn, "dbo", "t1", "c1"));
    }

    @Test
    public void getPrimaryKeyColumns_正常ケース_PK列が存在する_列名リストが返ること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getPrimaryKeys(null, "dbo", "T1")).thenReturn(rs);
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getString("COLUMN_NAME")).thenReturn("ID", "SUB_ID");

        assertEquals(List.of("ID", "SUB_ID"), handler.getPrimaryKeyColumns(conn, "dbo", "T1"));
    }

    @Test
    public void hasPrimaryKey_正常ケース_PKがある場合_trueが返ること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getPrimaryKeys(null, "dbo", "T1")).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getString("COLUMN_NAME")).thenReturn("ID");

        assertTrue(handler.hasPrimaryKey(conn, "dbo", "T1"));
    }

    @Test
    public void countRows_正常ケース_件数が取得できる_intが返ること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
        Connection conn = mock(Connection.class);
        Statement st = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.createStatement()).thenReturn(st);
        when(st.executeQuery("SELECT COUNT(*) FROM [T1]")).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getInt(1)).thenReturn(123);

        assertEquals(123, handler.countRows(conn, "T1"));
    }

    @Test
    public void countRows_正常ケース_RSが空の場合_0が返ること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
        Connection conn = mock(Connection.class);
        Statement st = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.createStatement()).thenReturn(st);
        when(st.executeQuery("SELECT COUNT(*) FROM [T1]")).thenReturn(rs);
        when(rs.next()).thenReturn(false);

        assertEquals(0, handler.countRows(conn, "T1"));
    }

    @Test
    public void countRows_異常ケース_ResultSetCloseで例外が発生する_SQLExceptionが送出されること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());

        Connection conn = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);

        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery("SELECT COUNT(*) FROM [T1]")).thenReturn(rs);

        // try本体は return 0 へ進ませる
        when(rs.next()).thenReturn(false);

        // try-with-resources の close 分岐（ResultSet close が例外）
        doThrow(new SQLException("close failed")).when(rs).close();

        assertThrows(SQLException.class, () -> handler.countRows(conn, "T1"));
    }

    @Test
    public void countRows_異常ケース_StatementCloseで例外が発生する_SQLExceptionが送出されること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());

        Connection conn = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);

        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery("SELECT COUNT(*) FROM [T1]")).thenReturn(rs);

        // try本体は return rs.getInt(1) に進ませる
        when(rs.next()).thenReturn(true);
        when(rs.getInt(1)).thenReturn(1);

        // try-with-resources の close 分岐（Statement close が例外）
        doThrow(new SQLException("stmt close failed")).when(stmt).close();

        assertThrows(SQLException.class, () -> handler.countRows(conn, "T1"));
    }

    @Test
    public void countRows_異常ケース_getInt呼び出しでSQLExceptionが発生する_SQLExceptionが送出されること()
            throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());

        Connection conn = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);

        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery("SELECT COUNT(*) FROM [T1]")).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        doThrow(new SQLException("getInt failed")).when(rs).getInt(1);

        assertThrows(SQLException.class, () -> handler.countRows(conn, "T1"));
    }

    @Test
    public void countRows_異常ケース_ResultSetとStatementCloseで両方例外が発生する_SQLExceptionが送出されること()
            throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());

        Connection conn = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);

        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery("SELECT COUNT(*) FROM [T1]")).thenReturn(rs);
        when(rs.next()).thenReturn(false);
        doThrow(new SQLException("rs close failed")).when(rs).close();
        doThrow(new SQLException("stmt close failed")).when(stmt).close();

        assertThrows(SQLException.class, () -> handler.countRows(conn, "T1"));
    }

    @Test
    public void countRows_異常ケース_executeQuery呼び出しでSQLExceptionが発生する_SQLExceptionが送出されること()
            throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
        Connection conn = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmt);
        doThrow(new SQLException("executeQuery failed")).when(stmt)
                .executeQuery("SELECT COUNT(*) FROM [T1]");
        assertThrows(SQLException.class, () -> handler.countRows(conn, "T1"));
    }

    @Test
    public void countRows_異常ケース_createStatement呼び出しでSQLExceptionが発生する_SQLExceptionが送出されること()
            throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
        Connection conn = mock(Connection.class);
        doThrow(new SQLException("createStatement failed")).when(conn).createStatement();
        assertThrows(SQLException.class, () -> handler.countRows(conn, "T1"));
    }

    @Test
    public void createDbUnitConnection_正常ケース_configureが呼ばれ_escapePatternが設定されること()
            throws Exception {
        DbUnitConfigFactory factory = mock(DbUnitConfigFactory.class);
        SqlServerDialectHandler handler = createHandlerDefault(factory,
                mock(DateTimeFormatSupport.class), List.of(), Map.of(), Map.of());

        Connection jdbc = mock(Connection.class);

        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(jdbc.getMetaData()).thenReturn(meta);

        // schemaExists() が参照する ResultSet をスタブ
        ResultSet schemasRs = mock(ResultSet.class);
        when(meta.getSchemas()).thenReturn(schemasRs);
        when(schemasRs.next()).thenReturn(true, false);
        when(schemasRs.getString("TABLE_SCHEM")).thenReturn("dbo");

        // 念のため（schema が見つからない場合に catalogExists() が呼ばれる）
        ResultSet catalogsRs = mock(ResultSet.class);
        when(meta.getCatalogs()).thenReturn(catalogsRs);
        when(catalogsRs.next()).thenReturn(false);

        // correctCase() などが参照し得る最低限（既に入れているなら重複不要）
        when(meta.getIdentifierQuoteString()).thenReturn("[");
        when(meta.getDatabaseProductName()).thenReturn("Microsoft SQL Server");
        when(meta.storesLowerCaseIdentifiers()).thenReturn(false);
        when(meta.storesUpperCaseIdentifiers()).thenReturn(true);
        when(meta.storesMixedCaseIdentifiers()).thenReturn(false);
        when(meta.storesLowerCaseQuotedIdentifiers()).thenReturn(false);
        when(meta.storesUpperCaseQuotedIdentifiers()).thenReturn(false);
        when(meta.storesMixedCaseQuotedIdentifiers()).thenReturn(true);

        DatabaseConnection dbConn = handler.createDbUnitConnection(jdbc, "dbo");
        assertNotNull(dbConn);

        DatabaseConfig config = dbConn.getConfig();
        Object escape = config.getProperty(DatabaseConfig.PROPERTY_ESCAPE_PATTERN);
        assertEquals("[?]", escape);

        verify(factory).configure(eq(config), any(IDataTypeFactory.class));
    }

    @Test
    public void formatDateTimeColumn_正常ケース_小数秒000を含む文字列を返す_小数秒が正規化されること() throws Exception {
        DateTimeFormatSupport support = mock(DateTimeFormatSupport.class);
        when(support.formatJdbcDateTime(eq("COL"), any(), any()))
                .thenReturn("2026-02-15 01:02:03.000");

        SqlServerDialectHandler handler = createHandlerDefault(mock(DbUnitConfigFactory.class),
                support, List.of(), Map.of(), Map.of());

        String actual = handler.formatDateTimeColumn("COL", new Timestamp(0), null);
        assertEquals("2026-02-15 01:02:03", actual);
    }

    @Test
    public void formatDbValueForCsv_正常ケース_nullは空文字_LOBも空文字_日時は整形されること() throws Exception {
        DateTimeFormatSupport support = mock(DateTimeFormatSupport.class);
        when(support.formatJdbcDateTime(eq("COL"), any(), any()))
                .thenReturn("2026-02-15 01:02:03.000");
        SqlServerDialectHandler handler = createHandlerDefault(mock(DbUnitConfigFactory.class),
                support, List.of(), Map.of(), Map.of());

        assertEquals("", handler.formatDbValueForCsv("COL", null));
        assertEquals("", handler.formatDbValueForCsv("COL", new byte[] {1, 2}));

        Blob blob = new SerialBlob(new byte[] {3, 4});
        assertEquals("", handler.formatDbValueForCsv("COL", blob));

        Clob clob = new SerialClob("abc".toCharArray());
        assertEquals("", handler.formatDbValueForCsv("COL", clob));

        assertEquals("2026-02-15 01:02:03",
                handler.formatDbValueForCsv("COL", Timestamp.valueOf("2026-02-15 01:02:03")));
        assertEquals("x", handler.formatDbValueForCsv("COL", "x"));
    }

    @Test
    public void writeLobFile_正常ケース_byteBlobClobその他_全分岐でファイルが書けること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
        Path outDir = tempDir.resolve("out");
        Path p1 = outDir.resolve("b1.bin");
        Path p2 = outDir.resolve("b2.bin");
        Path p3 = outDir.resolve("c1.txt");
        Path p4 = outDir.resolve("s1.txt");

        handler.writeLobFile("dbo", "T1", new byte[] {1, 2}, p1);
        assertArrayEquals(new byte[] {1, 2}, Files.readAllBytes(p1));

        handler.writeLobFile("dbo", "T1", new SerialBlob(new byte[] {3, 4}), p2);
        assertArrayEquals(new byte[] {3, 4}, Files.readAllBytes(p2));

        handler.writeLobFile("dbo", "T1", new SerialClob("abc".toCharArray()), p3);
        assertEquals("abc", Files.readString(p3, StandardCharsets.UTF_8));

        handler.writeLobFile("dbo", "T1", "xyz", p4);
        assertEquals("xyz", Files.readString(p4, StandardCharsets.UTF_8));
    }

    @Test
    public void readLobFile_正常ケース_バイナリとテキストで返却型が変わること() throws Exception {
        // Table/column metadata for BIN_COL and TXT_COL
        Map<String, Column[]> dbUnitCols = Map.of("T1",
                new Column[] {colMock("BIN_COL", dataTypeMock(Types.BLOB, "varbinary")),
                        colMock("TXT_COL", dataTypeMock(Types.CLOB, "nvarchar"))});
        Map<String, List<JdbcRow>> jdbcCols =
                Map.of("T1", List.of(new JdbcRow("BIN_COL", Types.BLOB, "varbinary"),
                        new JdbcRow("TXT_COL", Types.CLOB, "nvarchar")));
        SqlServerDialectHandler handler = createHandlerDefault(List.of("T1"), dbUnitCols, jdbcCols);

        Path lobDir = tempDir.resolve(LobPathConstants.DIRECTORY_NAME);
        Files.createDirectories(lobDir);
        Files.write(lobDir.resolve("bin.dat"), new byte[] {9, 8});
        Files.writeString(lobDir.resolve("txt.txt"), "hello", StandardCharsets.UTF_8);

        Object bin = handler.readLobFile("bin.dat", "T1", "BIN_COL", tempDir.toFile());
        assertInstanceOf(byte[].class, bin);

        Object txt = handler.readLobFile("txt.txt", "T1", "TXT_COL", tempDir.toFile());
        assertInstanceOf(String.class, txt);
        assertEquals("hello", txt);
    }

    @Test
    public void readLobFile_異常ケース_ファイルが存在しない_DataSetExceptionが送出されること() throws Exception {
        Map<String, Column[]> dbUnitCols = Map.of("T1",
                new Column[] {colMock("BIN_COL", dataTypeMock(Types.BLOB, "varbinary"))});
        Map<String, List<JdbcRow>> jdbcCols =
                Map.of("T1", List.of(new JdbcRow("BIN_COL", Types.BLOB, "varbinary")));
        SqlServerDialectHandler handler = createHandlerDefault(List.of("T1"), dbUnitCols, jdbcCols);

        assertThrows(DataSetException.class,
                () -> handler.readLobFile("missing.bin", "T1", "BIN_COL", tempDir.toFile()));
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_nullと空白はnullが返ること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of("T1"),
                Map.of("T1", new Column[] {colMock("C1", dataTypeMock(Types.INTEGER, "int"))}),
                Map.of("T1", List.of(new JdbcRow("C1", Types.INTEGER, "int"))));

        assertEquals(null, handler.convertCsvValueToDbType("T1", "C1", null));
        assertEquals(null, handler.convertCsvValueToDbType("T1", "C1", "   "));
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_主要型の分岐を網羅すること() throws Exception {
        Map<String, Column[]> dbUnitCols = Map.of("T1",
                new Column[] {colMock("B", dataTypeMock(Types.BIT, "bit")),
                        colMock("I", dataTypeMock(Types.INTEGER, "int")),
                        colMock("L", dataTypeMock(Types.BIGINT, "bigint")),
                        colMock("N", dataTypeMock(Types.DECIMAL, "decimal")),
                        colMock("R", dataTypeMock(Types.DOUBLE, "float")),
                        colMock("BIN", dataTypeMock(Types.BLOB, "varbinary")),
                        colMock("DT", dataTypeMock(Types.TIMESTAMP, "datetime2")),
                        colMock("S", dataTypeMock(Types.VARCHAR, "varchar"))});
        SqlServerDialectHandler handler = createHandlerDefault(List.of("T1"), dbUnitCols, Map.of());

        assertEquals(Boolean.TRUE, handler.convertCsvValueToDbType("T1", "B", "1"));
        assertEquals(Boolean.FALSE, handler.convertCsvValueToDbType("T1", "B", "0"));

        assertEquals(123, handler.convertCsvValueToDbType("T1", "I", "123"));
        assertEquals(123L, handler.convertCsvValueToDbType("T1", "L", "123"));

        assertEquals("123.45", handler.convertCsvValueToDbType("T1", "N", "123.45").toString());
        assertEquals("1.25", handler.convertCsvValueToDbType("T1", "R", "1.25").toString());

        // parseBinaryHex: with \x prefix
        Object bin = handler.convertCsvValueToDbType("T1", "BIN", "\\x0A0B");
        assertInstanceOf(byte[].class, bin);

        Object dt = handler.convertCsvValueToDbType("T1", "DT", "2026-02-15 01:02:03");
        assertInstanceOf(Timestamp.class, dt);

        assertEquals("abc", handler.convertCsvValueToDbType("T1", "S", "abc"));
    }

    @Test
    public void convertCsvValueToDbType_異常ケース_Integer変換不能_RuntimeException経由でDataSetExceptionになること()
            throws Exception {
        Map<String, Column[]> dbUnitCols =
                Map.of("T1", new Column[] {colMock("I", dataTypeMock(Types.INTEGER, "int"))});
        SqlServerDialectHandler handler = createHandlerDefault(List.of("T1"), dbUnitCols, Map.of());

        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("T1", "I", "not-an-int"));
    }

    @Test
    public void convertCsvValueToDbType_異常ケース_バイナリHEX不正_Exception経由でDataSetExceptionになること()
            throws Exception {
        Map<String, Column[]> dbUnitCols =
                Map.of("T1", new Column[] {colMock("BIN", dataTypeMock(Types.BLOB, "varbinary"))});
        SqlServerDialectHandler handler = createHandlerDefault(List.of("T1"), dbUnitCols, Map.of());

        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("T1", "BIN", "\\xZZ"));
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_DBUnit型がUNKNOWNでJDBCにフォールバックすること() throws Exception {
        // DBUnit UNKNOWN, JDBC says INTEGER => should return Integer, not String
        Map<String, Column[]> dbUnitCols =
                Map.of("T1", new Column[] {colMock("C1", DataType.UNKNOWN)});
        Map<String, List<JdbcRow>> jdbcCols =
                Map.of("T1", List.of(new JdbcRow("C1", Types.INTEGER, "int")));

        SqlServerDialectHandler handler = createHandlerDefault(List.of("T1"), dbUnitCols, jdbcCols);
        Object actual = handler.convertCsvValueToDbType("T1", "C1", "123");
        assertEquals(123, actual);
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_DBUnitに列が無くJDBCのみで解決できること() throws Exception {
        Map<String, Column[]> dbUnitCols =
                Map.of("T1", new Column[] {colMock("X", dataTypeMock(Types.VARCHAR, "varchar"))});
        Map<String, List<JdbcRow>> jdbcCols =
                Map.of("T1", List.of(new JdbcRow("JDBC_ONLY", Types.BIGINT, "bigint")));

        SqlServerDialectHandler handler = createHandlerDefault(List.of("T1"), dbUnitCols, jdbcCols);
        Object actual = handler.convertCsvValueToDbType("T1", "JDBC_ONLY", "999");
        assertEquals(999L, actual);
    }

    @Test
    public void convertCsvValueToDbType_異常ケース_列メタデータが無い_DataSetExceptionが送出されること()
            throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of("T1"),
                Map.of("T1", new Column[] {colMock("A", dataTypeMock(Types.INTEGER, "int"))}),
                Map.of("T1", List.of()));
        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("T1", "NO_SUCH_COL", "1"));
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_file参照_バイナリとテキストが読み込めること() throws Exception {
        Map<String, Column[]> dbUnitCols = Map.of("T1",
                new Column[] {colMock("BINFILE", dataTypeMock(Types.BLOB, "varbinary")),
                        colMock("TXTFILE", dataTypeMock(Types.CLOB, "nvarchar"))});
        SqlServerDialectHandler handler = createHandlerDefault(List.of("T1"), dbUnitCols, Map.of());

        Path lobDir = tempDir.resolve("dump").resolve(LobPathConstants.DIRECTORY_NAME);
        Files.createDirectories(lobDir);
        Files.write(lobDir.resolve("bin.dat"), new byte[] {1, 2, 3});
        Files.writeString(lobDir.resolve("txt.txt"), "hello", StandardCharsets.UTF_8);

        Object bin = handler.convertCsvValueToDbType("T1", "BINFILE", "file:bin.dat");
        assertInstanceOf(byte[].class, bin);
        assertArrayEquals(new byte[] {1, 2, 3}, (byte[]) bin);

        Object txt = handler.convertCsvValueToDbType("T1", "TXTFILE", "file:txt.txt");
        assertEquals("hello", txt);
    }

    @Test
    public void convertCsvValueToDbType_異常ケース_file参照_ファイルが存在しない_DataSetExceptionが送出されること()
            throws Exception {
        Map<String, Column[]> dbUnitCols = Map.of("T1",
                new Column[] {colMock("BINFILE", dataTypeMock(Types.BLOB, "varbinary"))});
        SqlServerDialectHandler handler = createHandlerDefault(List.of("T1"), dbUnitCols, Map.of());

        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("T1", "BINFILE", "file:missing.bin"));
    }

    @Test
    public void convertCsvValueToDbType_異常ケース_file参照_未対応型_DataSetExceptionが送出されること()
            throws Exception {
        Map<String, Column[]> dbUnitCols =
                Map.of("T1", new Column[] {colMock("I", dataTypeMock(Types.INTEGER, "int"))});
        SqlServerDialectHandler handler = createHandlerDefault(List.of("T1"), dbUnitCols, Map.of());

        Path lobDir = tempDir.resolve("dump").resolve(LobPathConstants.DIRECTORY_NAME);
        Files.createDirectories(lobDir);
        Files.writeString(lobDir.resolve("x.txt"), "x", StandardCharsets.UTF_8);

        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("T1", "I", "file:x.txt"));
    }

    @Test
    public void getLobColumns_正常ケース_CSVヘッダとメタからLOBのみ抽出できること() throws Exception {
        Column lob1 = new Column("BIN_COL", DataType.BLOB);
        Column nonLob = new Column("NAME", DataType.VARCHAR);

        DataType xmlType = mock(DataType.class);
        when(xmlType.getSqlType()).thenReturn(Types.LONGVARCHAR);
        when(xmlType.getSqlTypeName()).thenReturn("xml");
        Column lob2 = new Column("XML_COL", xmlType);

        Map<String, Column[]> dbUnitCols = Map.of("T_LOB", new Column[] {lob1, nonLob, lob2});
        Map<String, List<JdbcRow>> jdbcCols =
                Map.of("T_LOB", List.of(new JdbcRow("XML_COL", Types.LONGVARCHAR, "xml")));

        SqlServerDialectHandler handler =
                createHandlerDefault(List.of("T_LOB"), dbUnitCols, jdbcCols);

        Path csvDir = tempDir.resolve("csv");
        Files.createDirectories(csvDir);
        Files.writeString(csvDir.resolve("T_LOB.csv"), "BIN_COL,NAME,XML_COL\n1,a,<_/>\n",
                StandardCharsets.UTF_8);

        Column[] actual = handler.getLobColumns(csvDir, "T_LOB");
        assertEquals(2, actual.length);
    }

    @Test
    public void getLobColumns_正常ケース_CSVが無い場合_空配列が返ること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of("T1"),
                Map.of("T1", new Column[] {new Column("C1", DataType.VARCHAR)}), Map.of());
        Column[] actual = handler.getLobColumns(tempDir, "T1");
        assertEquals(0, actual.length);
    }

    @Test
    public void hasNotNullLobColumn_正常ケース_NOTNULLのLOBがあればtrueが返ること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
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
    public void hasNotNullLobColumn_正常ケース_NULL許容のLOB列のみである_falseであること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
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
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
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
    public void hasNotNullLobColumn_異常ケース_closeでSQLExceptionが発生する_SQLExceptionが再スローされること()
            throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());

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
    public void hasNotNullLobColumn_異常ケース_getColumns呼び出しでSQLExceptionが発生する_SQLExceptionが送出されること()
            throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
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
    public void logTableDefinition_正常ケース_メタ取得が走り例外が出ないこと() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getColumns(null, "dbo", "T1", null)).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getString("COLUMN_NAME")).thenReturn("C1");
        when(rs.getString("TYPE_NAME")).thenReturn("int");
        when(rs.getInt("COLUMN_SIZE")).thenReturn(10);
        when(rs.getInt("CHAR_OCTET_LENGTH")).thenReturn(10);

        handler.logTableDefinition(conn, "dbo", "T1", "logger");
        verify(meta).getColumns(null, "dbo", "T1", null);
    }

    @Test
    public void constructor_正常ケース_getSchema例外_dboフォールバックされること() throws Exception {
        DumpConfig dump = mock(DumpConfig.class);
        when(dump.getExcludeTables()).thenReturn(List.of());

        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        Connection jdbc = mock(Connection.class);
        when(dbConn.getConnection()).thenReturn(jdbc);
        when(jdbc.getSchema()).thenThrow(new SQLException("no schema"));

        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(jdbc.getMetaData()).thenReturn(meta);

        ResultSet tableRs = mock(ResultSet.class);
        when(meta.getTables(null, "dbo", "%", new String[] {"TABLE"})).thenReturn(tableRs);
        when(tableRs.next()).thenReturn(false);

        IDataSet ds = mock(IDataSet.class);
        when(dbConn.createDataSet()).thenReturn(ds);

        PathsConfig paths = mock(PathsConfig.class);
        when(paths.getDump()).thenReturn(tempDir.resolve("dump").toString());
        when(paths.getDataPath()).thenReturn(tempDir.toString());

        new SqlServerDialectHandler(dbConn, dump, new DbUnitConfig(),
                mock(DbUnitConfigFactory.class), mock(DateTimeFormatSupport.class), paths);

        verify(meta).getTables(null, "dbo", "%", new String[] {"TABLE"});
    }

    @Test
    public void constructor_正常ケース_excludeTablesにより対象が除外されること() throws Exception {
        DumpConfig dump = mock(DumpConfig.class);
        when(dump.getExcludeTables()).thenReturn(List.of("T2"));

        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        Connection jdbc = mock(Connection.class);
        when(dbConn.getConnection()).thenReturn(jdbc);
        when(jdbc.getSchema()).thenReturn("dbo");

        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(jdbc.getMetaData()).thenReturn(meta);

        ResultSet tableRs = mock(ResultSet.class);
        when(meta.getTables(null, "dbo", "%", new String[] {"TABLE"})).thenReturn(tableRs);
        when(tableRs.next()).thenReturn(true, true, false);
        when(tableRs.getString("TABLE_NAME")).thenReturn("T1", "T2");

        IDataSet ds = mock(IDataSet.class);
        when(dbConn.createDataSet()).thenReturn(ds);

        ITableMetaData md1 = mock(ITableMetaData.class);
        when(ds.getTableMetaData("T1")).thenReturn(md1);

        ResultSet colRs1 = mock(ResultSet.class);
        when(meta.getColumns(null, "dbo", "T1", "%")).thenReturn(colRs1);
        when(colRs1.next()).thenReturn(false);

        PathsConfig paths = mock(PathsConfig.class);
        when(paths.getDump()).thenReturn(tempDir.resolve("dump").toString());
        when(paths.getDataPath()).thenReturn(tempDir.toString());

        new SqlServerDialectHandler(dbConn, dump, new DbUnitConfig(),
                mock(DbUnitConfigFactory.class), mock(DateTimeFormatSupport.class), paths);

        verify(ds).getTableMetaData("T1");
        verify(ds, never()).getTableMetaData("T2");
    }

    @Test
    public void constructor_正常ケース_schemaがnull_dboへ補正されること() throws Exception {
        DumpConfig dump = mock(DumpConfig.class);
        when(dump.getExcludeTables()).thenReturn(List.of());

        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        Connection jdbc = mock(Connection.class);
        when(dbConn.getConnection()).thenReturn(jdbc);

        // schema == null の分岐
        when(jdbc.getSchema()).thenReturn(null);

        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(jdbc.getMetaData()).thenReturn(meta);

        ResultSet tableRs = mock(ResultSet.class);
        when(meta.getTables(null, "dbo", "%", new String[] {"TABLE"})).thenReturn(tableRs);
        when(tableRs.next()).thenReturn(false);

        when(dbConn.createDataSet()).thenReturn(mock(IDataSet.class));

        PathsConfig paths = mock(PathsConfig.class);
        when(paths.getDump()).thenReturn(tempDir.resolve("dump").toString());
        when(paths.getDataPath()).thenReturn(tempDir.toString());

        new SqlServerDialectHandler(dbConn, dump, new DbUnitConfig(),
                mock(DbUnitConfigFactory.class), mock(DateTimeFormatSupport.class), paths);

        verify(meta).getTables(null, "dbo", "%", new String[] {"TABLE"});
    }

    @Test
    public void constructor_正常ケース_schemaが空白_dboへ補正されること() throws Exception {
        DumpConfig dump = mock(DumpConfig.class);
        when(dump.getExcludeTables()).thenReturn(List.of());

        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        Connection jdbc = mock(Connection.class);
        when(dbConn.getConnection()).thenReturn(jdbc);

        // schema.isBlank() の分岐
        when(jdbc.getSchema()).thenReturn("   ");

        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(jdbc.getMetaData()).thenReturn(meta);

        ResultSet tableRs = mock(ResultSet.class);
        when(meta.getTables(null, "dbo", "%", new String[] {"TABLE"})).thenReturn(tableRs);
        when(tableRs.next()).thenReturn(false);

        when(dbConn.createDataSet()).thenReturn(mock(IDataSet.class));

        PathsConfig paths = mock(PathsConfig.class);
        when(paths.getDump()).thenReturn(tempDir.resolve("dump").toString());
        when(paths.getDataPath()).thenReturn(tempDir.toString());

        new SqlServerDialectHandler(dbConn, dump, new DbUnitConfig(),
                mock(DbUnitConfigFactory.class), mock(DateTimeFormatSupport.class), paths);

        verify(meta).getTables(null, "dbo", "%", new String[] {"TABLE"});
    }

    @Test
    public void formatDbValueForCsv_正常ケース_日時型の全分岐を通すこと() throws Exception {
        DateTimeFormatSupport support = mock(DateTimeFormatSupport.class);
        when(support.formatJdbcDateTime(eq("COL"), any(), any()))
                .thenReturn("2026-02-15 01:02:03.000");

        SqlServerDialectHandler handler = createHandlerDefault(mock(DbUnitConfigFactory.class),
                support, List.of(), Map.of(), Map.of());

        // Date / Time / Timestamp / LocalDate / LocalDateTime / OffsetDateTime を全部踏む
        assertEquals("2026-02-15 01:02:03",
                handler.formatDbValueForCsv("COL", Date.valueOf("2026-02-15")));
        assertEquals("2026-02-15 01:02:03",
                handler.formatDbValueForCsv("COL", Time.valueOf("01:02:03")));
        assertEquals("2026-02-15 01:02:03",
                handler.formatDbValueForCsv("COL", Timestamp.valueOf("2026-02-15 01:02:03")));
        assertEquals("2026-02-15 01:02:03",
                handler.formatDbValueForCsv("COL", LocalDate.of(2026, 2, 15)));
        assertEquals("2026-02-15 01:02:03",
                handler.formatDbValueForCsv("COL", LocalDateTime.of(2026, 2, 15, 1, 2, 3)));
        assertEquals("2026-02-15 01:02:03", handler.formatDbValueForCsv("COL",
                OffsetDateTime.parse("2026-02-15T01:02:03+09:00")));
    }

    @Test
    public void isDateTimeTypeForDump_正常ケース_falseケース_intはfalseであること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
        assertFalse(handler.isDateTimeTypeForDump(Types.INTEGER, "int"));
    }

    @Test
    public void hasPrimaryKey_正常ケース_PK無し_falseが返ること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());

        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getPrimaryKeys(null, "dbo", "T1")).thenReturn(rs);
        when(rs.next()).thenReturn(false);

        assertFalse(handler.hasPrimaryKey(conn, "dbo", "T1"));
    }

    @Test
    public void getLobColumns_正常ケース_mdが無い場合_空配列が返ること() throws Exception {
        // tables を空にして tableMetaMap を空にする
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());

        Path csvDir = tempDir.resolve("csv_md_null");
        Files.createDirectories(csvDir);
        Files.writeString(csvDir.resolve("T_NO_MD.csv"), "A\n1\n", StandardCharsets.UTF_8);

        Column[] actual = handler.getLobColumns(csvDir, "T_NO_MD");
        assertEquals(0, actual.length);
    }

    @Test
    public void getLobColumns_正常ケース_ヘッダに無い列は無視されること() throws Exception {
        Column inCsv = new Column("IN_CSV", DataType.BLOB);
        Column notInCsv = new Column("NOT_IN_CSV", DataType.BLOB);

        Map<String, Column[]> dbUnitCols = Map.of("T_HDR", new Column[] {inCsv, notInCsv});
        SqlServerDialectHandler handler =
                createHandlerDefault(List.of("T_HDR"), dbUnitCols, Map.of());

        Path csvDir = tempDir.resolve("csv_hdr");
        Files.createDirectories(csvDir);
        // NOT_IN_CSV をヘッダに入れない -> idx==null continue を踏む
        Files.writeString(csvDir.resolve("T_HDR.csv"), "IN_CSV\nx\n", StandardCharsets.UTF_8);

        Column[] actual = handler.getLobColumns(csvDir, "T_HDR");
        assertEquals(1, actual.length);
        assertEquals("IN_CSV", actual[0].getColumnName());
    }

    @Test
    public void parseDateTimeValue_正常ケース_コロン無し時刻を指定する_Timeが返ること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());
        Object actual = handler.parseDateTimeValue("COL", "0102");
        assertInstanceOf(Time.class, actual);
        assertEquals(Time.valueOf("01:02:00"), actual);
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_boolean表現を網羅すること() throws Exception {
        Map<String, Column[]> dbUnitCols =
                Map.of("TBOOL", new Column[] {colMock("B", dataTypeMock(Types.BIT, "bit"))});
        SqlServerDialectHandler handler =
                createHandlerDefault(List.of("TBOOL"), dbUnitCols, Map.of());

        assertEquals(Boolean.TRUE, handler.convertCsvValueToDbType("TBOOL", "B", "true"));
        assertEquals(Boolean.TRUE, handler.convertCsvValueToDbType("TBOOL", "B", "1"));
        assertEquals(Boolean.TRUE, handler.convertCsvValueToDbType("TBOOL", "B", "t"));

        assertEquals(Boolean.FALSE, handler.convertCsvValueToDbType("TBOOL", "B", "false"));
        assertEquals(Boolean.FALSE, handler.convertCsvValueToDbType("TBOOL", "B", "0"));
        assertEquals(Boolean.FALSE, handler.convertCsvValueToDbType("TBOOL", "B", "f"));

        // fallback（Boolean.valueOf）を踏む
        assertEquals(Boolean.FALSE, handler.convertCsvValueToDbType("TBOOL", "B", "yes"));
    }

    @Test
    public void convertCsvValueToDbType_正常ケース_16進文字列_prefix無しも扱えること() throws Exception {
        Map<String, Column[]> dbUnitCols = Map.of("TBIN",
                new Column[] {colMock("BIN", dataTypeMock(Types.BLOB, "varbinary"))});
        SqlServerDialectHandler handler =
                createHandlerDefault(List.of("TBIN"), dbUnitCols, Map.of());

        Object bin = handler.convertCsvValueToDbType("TBIN", "BIN", "0A0B");
        assertInstanceOf(byte[].class, bin);
        assertArrayEquals(new byte[] {0x0A, 0x0B}, (byte[]) bin);
    }

    @Test
    public void convertCsvValueToDbType_異常ケース_file参照_ディレクトリでIOExceptionとなりDataSetExceptionが送出されること()
            throws Exception {
        Map<String, Column[]> dbUnitCols = Map.of("T1",
                new Column[] {colMock("BINFILE", dataTypeMock(Types.BLOB, "varbinary"))});
        SqlServerDialectHandler handler = createHandlerDefault(List.of("T1"), dbUnitCols, Map.of());

        Path lobDir = tempDir.resolve("dump").resolve(LobPathConstants.DIRECTORY_NAME);
        Files.createDirectories(lobDir);

        // “ファイル” の代わりにディレクトリを作る -> readAllBytes が IOException
        Files.createDirectories(lobDir.resolve("dir1"));

        DataSetException ex = assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("T1", "BINFILE", "file:dir1"));
        assertTrue(ex.getMessage().contains("Failed to read LOB file"));
    }

    @Test
    public void privateHelpers_正常ケース_未到達分岐を直接踏むこと() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());

        // startsWithFileReference(s == null)
        assertFalse((Boolean) invokePrivate(handler, "startsWithFileReference",
                new Class<?>[] {String.class}, new Object[] {null}));

        // fetchTargetTables(excludeTables == null)
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getTables(null, "dbo", "%", new String[] {"TABLE"})).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getString("TABLE_NAME")).thenReturn("T1");

        @SuppressWarnings("unchecked")
        List<String> tables = (List<String>) invokePrivate(handler, "fetchTargetTables",
                new Class<?>[] {Connection.class, String.class, List.class},
                new Object[] {conn, "dbo", null});
        assertEquals(List.of("T1"), tables);

        // findJdbcColumnSpec(byColumn == null)
        assertEquals(null, invokePrivate(handler, "findJdbcColumnSpec",
                new Class<?>[] {String.class, String.class}, new Object[] {"no_such_table", "C1"}));

        // findColumnInDbUnitMeta(md == null) -> DataSetException
        assertThrows(DataSetException.class, () -> invokePrivate(handler, "findColumnInDbUnitMeta",
                new Class<?>[] {String.class, String.class}, new Object[] {"no_such_table", "C1"}));

        // try-with-resources の分岐（ResultSet/Statement が null）
        Connection jdbc = mock(Connection.class);
        DatabaseMetaData m = mock(DatabaseMetaData.class);
        when(jdbc.getMetaData()).thenReturn(m);
        when(m.getColumns(null, "dbo", "t1", "c1")).thenReturn(null);
        assertThrows(NullPointerException.class,
                () -> handler.getColumnTypeName(jdbc, "dbo", "t1", "c1"));

        Connection c = mock(Connection.class);
        when(c.createStatement()).thenReturn(null);
        assertThrows(NullPointerException.class, () -> handler.countRows(c, "T1"));

        Connection c2 = mock(Connection.class);
        Statement st = mock(Statement.class);
        when(c2.createStatement()).thenReturn(st);
        when(st.executeQuery("SELECT COUNT(*) FROM [T1]")).thenReturn(null);
        assertThrows(NullPointerException.class, () -> handler.countRows(c2, "T1"));

        // isUnknownDbUnitType / isTextLikeType の早期return群
        assertTrue((Boolean) invokePrivate(handler, "isUnknownDbUnitType",
                new Class<?>[] {DataType.class}, new Object[] {null}));
        assertTrue((Boolean) invokePrivate(handler, "isUnknownDbUnitType",
                new Class<?>[] {DataType.class}, new Object[] {DataType.UNKNOWN}));

        DataType other = mock(DataType.class);
        when(other.getSqlType()).thenReturn(Types.OTHER);
        when(other.getSqlTypeName()).thenReturn("whatever");
        assertTrue((Boolean) invokePrivate(handler, "isUnknownDbUnitType",
                new Class<?>[] {DataType.class}, new Object[] {other}));

        assertTrue((Boolean) invokePrivate(handler, "isTextLikeType",
                new Class<?>[] {int.class, String.class, DataType.class},
                new Object[] {Types.VARCHAR, "varchar", null}));

        assertTrue((Boolean) invokePrivate(handler, "isTextLikeType",
                new Class<?>[] {int.class, String.class, DataType.class},
                new Object[] {Types.NCLOB, "nclob", null}));

        assertFalse((Boolean) invokePrivate(handler, "isTextLikeType",
                new Class<?>[] {int.class, String.class, DataType.class},
                new Object[] {Types.INTEGER, "x", null}));

        assertTrue((Boolean) invokePrivate(handler, "isTextLikeType",
                new Class<?>[] {int.class, String.class, DataType.class},
                new Object[] {Types.INTEGER, "x", DataType.CLOB}));
    }

    @Test
    public void isUnknownDbUnitType_正常ケース_sqlTypeNameがnull_falseが返ること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());

        DataType dt = mock(DataType.class);
        when(dt.getSqlType()).thenReturn(Types.INTEGER);
        when(dt.getSqlTypeName()).thenReturn(null);

        boolean actual = (boolean) invokePrivate(handler, "isUnknownDbUnitType",
                new Class<?>[] {DataType.class}, new Object[] {dt});
        assertFalse(actual);
    }

    @Test
    public void privateTypeChecks_正常ケース_booleanIntegerRealDateTimeの分岐を埋めること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());

        // isBooleanType: BOOLEAN / BIT / neither(true/false/false)
        assertTrue((boolean) invokePrivate(handler, "isBooleanType",
                new Class<?>[] {int.class, String.class}, new Object[] {Types.BOOLEAN, "x"}));
        assertTrue((boolean) invokePrivate(handler, "isBooleanType",
                new Class<?>[] {int.class, String.class}, new Object[] {Types.BIT, "x"}));
        assertFalse((boolean) invokePrivate(handler, "isBooleanType",
                new Class<?>[] {int.class, String.class}, new Object[] {Types.INTEGER, "varchar"}));

        // isIntegerType: INTEGER / SMALLINT / TINYINT / neither
        assertTrue((boolean) invokePrivate(handler, "isIntegerType",
                new Class<?>[] {int.class, String.class}, new Object[] {Types.INTEGER, "x"}));
        assertTrue((boolean) invokePrivate(handler, "isIntegerType",
                new Class<?>[] {int.class, String.class}, new Object[] {Types.SMALLINT, "x"}));
        assertTrue((boolean) invokePrivate(handler, "isIntegerType",
                new Class<?>[] {int.class, String.class}, new Object[] {Types.TINYINT, "x"}));
        assertFalse((boolean) invokePrivate(handler, "isIntegerType",
                new Class<?>[] {int.class, String.class}, new Object[] {Types.VARCHAR, "varchar"}));

        // isRealType: REAL / FLOAT / DOUBLE（if側） + contains側（左true/右true/両false）
        assertTrue((boolean) invokePrivate(handler, "isRealType",
                new Class<?>[] {int.class, String.class}, new Object[] {Types.REAL, "x"}));
        assertTrue((boolean) invokePrivate(handler, "isRealType",
                new Class<?>[] {int.class, String.class}, new Object[] {Types.FLOAT, "x"}));
        assertTrue((boolean) invokePrivate(handler, "isRealType",
                new Class<?>[] {int.class, String.class}, new Object[] {Types.DOUBLE, "x"}));

        // contains側を通すため、sqlType は REAL/FLOAT/DOUBLE 以外にする
        assertTrue((boolean) invokePrivate(handler, "isRealType",
                new Class<?>[] {int.class, String.class}, new Object[] {Types.VARCHAR, "real"})); // 左true
        assertTrue((boolean) invokePrivate(handler, "isRealType",
                new Class<?>[] {int.class, String.class}, new Object[] {Types.VARCHAR, "double"})); // 右true
        assertFalse((boolean) invokePrivate(handler, "isRealType",
                new Class<?>[] {int.class, String.class}, new Object[] {Types.VARCHAR, "varchar"})); // 両false

        // isDateTimeType: DATE / TIME / TIMESTAMP / TIMESTAMP_WITH_TIMEZONE（if側） + typeName側
        assertTrue((boolean) invokePrivate(handler, "isDateTimeType",
                new Class<?>[] {int.class, String.class}, new Object[] {Types.DATE, "x"}));
        assertTrue((boolean) invokePrivate(handler, "isDateTimeType",
                new Class<?>[] {int.class, String.class}, new Object[] {Types.TIME, "x"}));
        assertTrue((boolean) invokePrivate(handler, "isDateTimeType",
                new Class<?>[] {int.class, String.class}, new Object[] {Types.TIMESTAMP, "x"}));
        assertTrue((boolean) invokePrivate(handler, "isDateTimeType",
                new Class<?>[] {int.class, String.class},
                new Object[] {Types.TIMESTAMP_WITH_TIMEZONE, "x"}));

        // typeName集合側（DATE_TIME_TYPE_NAMES.contains）
        assertTrue((boolean) invokePrivate(handler, "isDateTimeType",
                new Class<?>[] {int.class, String.class},
                new Object[] {Types.INTEGER, "datetime2"}));
        assertFalse((boolean) invokePrivate(handler, "isDateTimeType",
                new Class<?>[] {int.class, String.class}, new Object[] {Types.INTEGER, "varchar"}));
    }

    @Test
    public void privateBinaryAndLobChecks_正常ケース_OR分岐を全て埋めること() throws Exception {
        SqlServerDialectHandler handler = createHandlerDefault(List.of(), Map.of(), Map.of());

        // isBinaryLobType: OR の各項目を1回ずつ true にする（短絡枝を埋める）
        String[] trueTypeNames = new String[] {"myblob", // endsWith("blob")
                "binary", "varbinary", "image", "rowversion", "timestamp", "bit"};
        for (String typeName : trueTypeNames) {
            assertTrue((boolean) invokePrivate(handler, "isBinaryLobType",
                    new Class<?>[] {int.class, String.class},
                    new Object[] {Types.INTEGER, typeName}));
        }

        // BINARY_SQL_TYPES.contains(sqlType) 側（typeName OR が全部 false でも true）
        assertTrue((boolean) invokePrivate(handler, "isBinaryLobType",
                new Class<?>[] {int.class, String.class}, new Object[] {Types.BLOB, "x"}));

        // 全部 false
        assertFalse((boolean) invokePrivate(handler, "isBinaryLobType",
                new Class<?>[] {int.class, String.class}, new Object[] {Types.INTEGER, "varchar"}));

        // isLobType: text/ntext/xml OR の各項目
        assertTrue((boolean) invokePrivate(handler, "isLobType",
                new Class<?>[] {int.class, String.class}, new Object[] {Types.VARCHAR, "text"}));
        assertTrue((boolean) invokePrivate(handler, "isLobType",
                new Class<?>[] {int.class, String.class}, new Object[] {Types.VARCHAR, "ntext"}));
        assertTrue((boolean) invokePrivate(handler, "isLobType",
                new Class<?>[] {int.class, String.class}, new Object[] {Types.VARCHAR, "xml"}));

        // LOB_SQL_TYPES.contains(sqlType) 側（text/ntext/xml ではないが true）
        assertTrue((boolean) invokePrivate(handler, "isLobType",
                new Class<?>[] {int.class, String.class}, new Object[] {Types.CLOB, "varchar"}));

        // 全部 false
        assertFalse((boolean) invokePrivate(handler, "isLobType",
                new Class<?>[] {int.class, String.class}, new Object[] {Types.INTEGER, "varchar"}));
    }

    /**
     * Invokes a private method for coverage-oriented tests.
     *
     * @param target target instance
     * @param methodName method name
     * @param paramTypes parameter types
     * @param args arguments
     * @return invocation result
     * @throws Exception on reflection errors or target exceptions
     */
    private static Object invokePrivate(Object target, String methodName, Class<?>[] paramTypes,
            Object[] args) throws Exception {
        Method m = target.getClass().getDeclaredMethod(methodName, paramTypes);
        m.setAccessible(true);
        try {
            return m.invoke(target, args);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            }
            throw e;
        }
    }

    private static final class JdbcRow {
        private final String columnName;
        private final int sqlType;
        private final String typeName;

        private JdbcRow(String columnName, int sqlType, String typeName) {
            this.columnName = columnName;
            this.sqlType = sqlType;
            this.typeName = typeName;
        }
    }

    private SqlServerDialectHandler createHandlerDefault(List<String> tables,
            Map<String, Column[]> dbUnitColumnsByTable,
            Map<String, List<JdbcRow>> jdbcColumnsByTable) throws Exception {
        return createHandlerDefault(mock(DbUnitConfigFactory.class),
                mock(DateTimeFormatSupport.class), tables, dbUnitColumnsByTable,
                jdbcColumnsByTable);
    }

    private SqlServerDialectHandler createHandlerDefault(DbUnitConfigFactory factory,
            DateTimeFormatSupport dateTimeSupport, List<String> tables,
            Map<String, Column[]> dbUnitColumnsByTable,
            Map<String, List<JdbcRow>> jdbcColumnsByTable) throws Exception {

        DumpConfig dump = mock(DumpConfig.class);
        when(dump.getExcludeTables()).thenReturn(List.of());

        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        Connection jdbc = mock(Connection.class);
        when(dbConn.getConnection()).thenReturn(jdbc);
        when(jdbc.getSchema()).thenReturn("dbo");

        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(jdbc.getMetaData()).thenReturn(meta);

        ResultSet tableRs = mock(ResultSet.class);
        when(meta.getTables(null, "dbo", "%", new String[] {"TABLE"})).thenReturn(tableRs);

        final int[] tIdx = new int[] {-1};
        when(tableRs.next()).thenAnswer(inv -> {
            tIdx[0]++;
            return tables != null && tIdx[0] < tables.size();
        });
        when(tableRs.getString("TABLE_NAME")).thenAnswer(inv -> tables.get(tIdx[0]));

        IDataSet ds = mock(IDataSet.class);
        when(dbConn.createDataSet()).thenReturn(ds);

        for (String t : tables) {
            ITableMetaData md = mock(ITableMetaData.class);
            Column[] cols = dbUnitColumnsByTable.getOrDefault(t, new Column[0]);
            when(md.getColumns()).thenReturn(cols);
            when(ds.getTableMetaData(t)).thenReturn(md);

            List<JdbcRow> jdbcRows = jdbcColumnsByTable.getOrDefault(t, List.of());
            ResultSet colRs = mock(ResultSet.class);
            when(meta.getColumns(null, "dbo", t, "%")).thenReturn(colRs);

            if (jdbcRows.isEmpty()) {
                when(colRs.next()).thenReturn(false);
            } else {
                final int[] idx = new int[] {-1};
                when(colRs.next()).thenAnswer(inv -> {
                    idx[0]++;
                    return idx[0] < jdbcRows.size();
                });
                when(colRs.getString("COLUMN_NAME"))
                        .thenAnswer(inv -> jdbcRows.get(idx[0]).columnName);
                when(colRs.getInt("DATA_TYPE")).thenAnswer(inv -> jdbcRows.get(idx[0]).sqlType);
                when(colRs.getString("TYPE_NAME")).thenAnswer(inv -> jdbcRows.get(idx[0]).typeName);
            }
        }

        PathsConfig paths = mock(PathsConfig.class);
        when(paths.getDump()).thenReturn(tempDir.resolve("dump").toString());
        when(paths.getDataPath()).thenReturn(tempDir.toString());

        return new SqlServerDialectHandler(dbConn, dump, new DbUnitConfig(), factory,
                dateTimeSupport, paths);
    }

    private static Column colMock(String name, DataType dt) {
        Column col = mock(Column.class);
        when(col.getColumnName()).thenReturn(name);
        when(col.getDataType()).thenReturn(dt);
        return col;
    }

    private static DataType dataTypeMock(int sqlType, String sqlTypeName) {
        DataType dt = mock(DataType.class);
        when(dt.getSqlType()).thenReturn(sqlType);
        when(dt.getSqlTypeName()).thenReturn(sqlTypeName);
        return dt;
    }
}
