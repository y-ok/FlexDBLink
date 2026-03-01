package io.github.yok.flexdblink.db.postgresql;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.db.DbUnitConfigFactory;
import io.github.yok.flexdblink.util.DateTimeFormatUtil;
import java.io.ByteArrayInputStream;
import java.io.StringReader;
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
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.datatype.DataType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class PostgresqlDialectHandlerTest {

    @TempDir
    Path tempDir;

    @Test
    void quoteIdentifier_正常ケース_識別子を指定する_ダブルクォート付き文字列が返ること() throws Exception {
        PostgresqlDialectHandler handler = createHandler();
        assertEquals("\"A1\"", handler.quoteIdentifier("A1"));
    }

    @Test
    void formatDbValueForCsv_正常ケース_nullを指定する_空文字が返ること() throws Exception {
        PostgresqlDialectHandler handler = createHandler();
        assertEquals("", handler.formatDbValueForCsv("COL", null));
    }

    @Test
    void formatDbValueForCsv_正常ケース_LOB型を指定する_空文字が返ること() throws Exception {
        PostgresqlDialectHandler handler = createHandler();
        Blob blob = mock(Blob.class);
        Clob clob = mock(Clob.class);
        assertEquals("", handler.formatDbValueForCsv("COL", new byte[] {0x01}));
        assertEquals("", handler.formatDbValueForCsv("COL", blob));
        assertEquals("", handler.formatDbValueForCsv("COL", clob));
    }

    @Test
    void formatDbValueForCsv_正常ケース_日時型を指定する_フォーマッタ経由の文字列が返ること() throws Exception {
        DateTimeFormatUtil formatter = mock(DateTimeFormatUtil.class);
        when(formatter.formatJdbcDateTime(eq("TS_COL"), any(), eq(null))).thenReturn("fmt");
        PostgresqlDialectHandler handler =
                createHandler(formatter, mock(DbUnitConfigFactory.class));
        assertEquals("fmt",
                handler.formatDbValueForCsv("TS_COL", Timestamp.valueOf("2026-02-15 01:02:03")));
    }

    @Test
    void formatDbValueForCsv_正常ケース_通常値を指定する_toString値が返ること() throws Exception {
        PostgresqlDialectHandler handler = createHandler();
        assertEquals("123", handler.formatDbValueForCsv("COL", 123));
    }

    @Test
    void formatDbValueForCsv_正常ケース_SQLXMLを指定する_XML文字列が返ること() throws Exception {
        PostgresqlDialectHandler handler = createHandler();
        SQLXML sqlxml = mock(SQLXML.class);
        when(sqlxml.getString()).thenReturn("<root/>");
        assertEquals("<root/>", handler.formatDbValueForCsv("XML_COL", sqlxml));
    }

    @Test
    void formatDateTimeColumn_正常ケース_ミリ秒ゼロを含む日時文字列を指定する_末尾の000が除去された値が返ること() throws Exception {
        DateTimeFormatUtil formatter = mock(DateTimeFormatUtil.class);
        when(formatter.formatJdbcDateTime(eq("TS_COL"), any(), eq(null)))
                .thenReturn("2026-02-15 01:02:03.000");
        PostgresqlDialectHandler handler =
                createHandler(formatter, mock(DbUnitConfigFactory.class));
        assertEquals("2026-02-15 01:02:03", handler.formatDateTimeColumn("TS_COL",
                Timestamp.valueOf("2026-02-15 01:02:03"), null));
    }

    @Test
    void formatDateTimeColumn_正常ケース_タイムゾーン付きミリ秒ゼロを指定する_末尾の000が除去された値が返ること() throws Exception {
        DateTimeFormatUtil formatter = mock(DateTimeFormatUtil.class);
        when(formatter.formatJdbcDateTime(eq("TSZ_COL"), any(), eq(null)))
                .thenReturn("2026-02-15 01:02:03.000+0900");
        PostgresqlDialectHandler handler =
                createHandler(formatter, mock(DbUnitConfigFactory.class));
        assertEquals("2026-02-15 01:02:03+0900", handler.formatDateTimeColumn("TSZ_COL",
                Timestamp.valueOf("2026-02-15 01:02:03"), null));
    }

    @Test
    void resolveSchema_正常ケース_接続設定を指定する_publicが返ること() throws Exception {
        PostgresqlDialectHandler handler = createHandler();
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setUser("app");
        assertEquals("public", handler.resolveSchema(entry));
    }

    @Test
    void prepareConnection_正常ケース_接続を初期化する_searchPath設定SQLが実行されること() throws Exception {
        PostgresqlDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmt);
        handler.prepareConnection(conn);
        verify(stmt).execute("SET search_path TO \"public\"");
    }

    @Test
    void getPrimaryKeyColumns_正常ケース_PK列が存在する_PK列一覧が返ること() throws Exception {
        PostgresqlDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getPrimaryKeys(null, "public", "t1")).thenReturn(rs);
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getString("COLUMN_NAME")).thenReturn("id", "seq");
        assertEquals(List.of("id", "seq"), handler.getPrimaryKeyColumns(conn, "public", "t1"));
    }

    @Test
    void getColumnTypeName_正常ケース_型情報が存在する_型名が返ること() throws Exception {
        PostgresqlDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getColumns(null, "public", "t1", "c1")).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getString("TYPE_NAME")).thenReturn("varchar");
        assertEquals("varchar", handler.getColumnTypeName(conn, "public", "t1", "c1"));
    }

    @Test
    void getColumnTypeName_異常ケース_型情報が存在しない_SQLExceptionが送出されること() throws Exception {
        PostgresqlDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getColumns(null, "public", "t1", "c1")).thenReturn(rs);
        when(rs.next()).thenReturn(false);
        assertThrows(SQLException.class,
                () -> handler.getColumnTypeName(conn, "public", "t1", "c1"));
    }

    @Test
    void getColumnTypeName_異常ケース_getString呼び出しでSQLExceptionが発生する_SQLExceptionが送出されること()
            throws Exception {
        PostgresqlDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);

        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getColumns(null, "public", "t1", "c1")).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        doThrow(new SQLException("get type failed")).when(rs).getString("TYPE_NAME");

        assertThrows(SQLException.class,
                () -> handler.getColumnTypeName(conn, "public", "t1", "c1"));
    }

    @Test
    void getColumnTypeName_異常ケース_getColumns呼び出しでSQLExceptionが発生する_SQLExceptionが送出されること()
            throws Exception {
        PostgresqlDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        doThrow(new SQLException("getColumns failed")).when(meta).getColumns(null, "public", "t1",
                "c1");
        assertThrows(SQLException.class,
                () -> handler.getColumnTypeName(conn, "public", "t1", "c1"));
    }

    @Test
    void parseDateTimeValue_正常ケース_各形式を指定する_日時型が返ること() throws Exception {
        PostgresqlDialectHandler handler = createHandler();
        assertInstanceOf(Timestamp.class,
                handler.parseDateTimeValue("tstz_col", "2026-02-15T01:02:03+09:00"));
        assertInstanceOf(Timestamp.class,
                handler.parseDateTimeValue("ts_col", "2026-02-15 01:02:03.123"));
        assertInstanceOf(Date.class, handler.parseDateTimeValue("date_col", "2026/02/15"));
        assertInstanceOf(Time.class, handler.parseDateTimeValue("time_col", "010203"));
    }

    @Test
    void parseDateTimeValue_異常ケース_不正値を指定する_IllegalArgumentExceptionが送出されること() throws Exception {
        PostgresqlDialectHandler handler = createHandler();
        assertThrows(IllegalArgumentException.class,
                () -> handler.parseDateTimeValue("col", "not-datetime"));
    }

    @Test
    void writeLobFile_正常ケース_各型を指定する_ファイルへ書き込まれること() throws Exception {
        PostgresqlDialectHandler handler = createHandler();
        Path p1 = tempDir.resolve("a").resolve("b.bin");
        handler.writeLobFile("public", "t1", "abc", p1);
        assertEquals("abc", Files.readString(p1, StandardCharsets.UTF_8));

        Path p2 = tempDir.resolve("a").resolve("b2.bin");
        handler.writeLobFile("public", "t1", new byte[] {0x01, 0x02}, p2);
        assertArrayEquals(new byte[] {0x01, 0x02}, Files.readAllBytes(p2));

        Blob blob = mock(Blob.class);
        when(blob.getBinaryStream())
                .thenReturn(new ByteArrayInputStream("blob".getBytes(StandardCharsets.UTF_8)));
        Path p3 = tempDir.resolve("a").resolve("b3.bin");
        handler.writeLobFile("public", "t1", blob, p3);
        assertEquals("blob", Files.readString(p3, StandardCharsets.UTF_8));

        Clob clob = mock(Clob.class);
        when(clob.getCharacterStream()).thenReturn(new StringReader("clob"));
        Path p4 = tempDir.resolve("a").resolve("b4.txt");
        handler.writeLobFile("public", "t1", clob, p4);
        assertEquals("clob", Files.readString(p4, StandardCharsets.UTF_8));
    }

    @Test
    void readLobFile_正常ケース_bytea列を指定する_byte配列が返ること() throws Exception {
        PostgresqlDialectHandler handler =
                createHandlerWithMeta("t1", new ColumnDef("blob_col", Types.BINARY, "bytea"));
        Path filesDir = tempDir.resolve("base").resolve("files");
        Files.createDirectories(filesDir);
        Files.write(filesDir.resolve("a.bin"), new byte[] {0x0A, 0x0B});
        Object actual =
                handler.readLobFile("a.bin", "t1", "blob_col", tempDir.resolve("base").toFile());
        assertArrayEquals(new byte[] {0x0A, 0x0B}, (byte[]) actual);
    }

    @Test
    void readLobFile_正常ケース_text列を指定する_文字列が返ること() throws Exception {
        PostgresqlDialectHandler handler =
                createHandlerWithMeta("t1", new ColumnDef("clob_col", Types.CLOB, "text"));
        Path filesDir = tempDir.resolve("base2").resolve("files");
        Files.createDirectories(filesDir);
        Files.writeString(filesDir.resolve("a.txt"), "hello", StandardCharsets.UTF_8);
        Object actual =
                handler.readLobFile("a.txt", "t1", "clob_col", tempDir.resolve("base2").toFile());
        assertEquals("hello", actual);
    }

    @Test
    void readLobFile_異常ケース_ファイルが存在しない_DataSetExceptionが送出されること() throws Exception {
        PostgresqlDialectHandler handler =
                createHandlerWithMeta("t1", new ColumnDef("clob_col", Types.CLOB, "text"));
        assertThrows(DataSetException.class,
                () -> handler.readLobFile("missing.txt", "t1", "clob_col", tempDir.toFile()));
    }

    @Test
    void createDbUnitConnection_正常ケース_DBUnit設定を行う_設定ファクトリが呼び出されること() throws Exception {
        DateTimeFormatUtil formatter = mock(DateTimeFormatUtil.class);
        DbUnitConfigFactory factory = mock(DbUnitConfigFactory.class);
        PostgresqlDialectHandler handler = createHandler(formatter, factory);
        Connection jdbc = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet schemas = mock(ResultSet.class);
        ResultSet catalogs = mock(ResultSet.class);
        when(jdbc.getMetaData()).thenReturn(meta);
        when(meta.getIdentifierQuoteString()).thenReturn("\"");
        when(meta.getSchemas()).thenReturn(schemas);
        when(meta.getCatalogs()).thenReturn(catalogs);
        when(schemas.next()).thenReturn(true, false);
        when(catalogs.next()).thenReturn(false);
        when(schemas.getString("TABLE_SCHEM")).thenReturn("PUBLIC");
        when(schemas.getString(1)).thenReturn("PUBLIC");
        DatabaseConnection dbConn = handler.createDbUnitConnection(jdbc, "public");
        assertNotNull(dbConn);
        verify(factory).configure(any(DatabaseConfig.class), any());
    }

    @Test
    void convertCsvValueToDbType_正常ケース_各型を指定する_期待型へ変換されること() throws Exception {
        PostgresqlDialectHandler handler =
                createHandlerWithMeta("t1", new ColumnDef("b1", Types.BOOLEAN, "boolean"),
                        new ColumnDef("i1", Types.INTEGER, "int4"),
                        new ColumnDef("l1", Types.BIGINT, "int8"),
                        new ColumnDef("n1", Types.NUMERIC, "numeric"),
                        new ColumnDef("r1", Types.REAL, "float4"),
                        new ColumnDef("d1", Types.DOUBLE, "float8"),
                        new ColumnDef("u1", Types.OTHER, "uuid"),
                        new ColumnDef("t1", Types.TIMESTAMP, "timestamp"));

        assertEquals(Boolean.TRUE, handler.convertCsvValueToDbType("t1", "b1", "t"));
        assertEquals(10, handler.convertCsvValueToDbType("t1", "i1", "10"));
        assertEquals(10L, handler.convertCsvValueToDbType("t1", "l1", "10"));
        assertEquals("12.34", handler.convertCsvValueToDbType("t1", "n1", "12.34").toString());
        assertEquals(1.25f, handler.convertCsvValueToDbType("t1", "r1", "1.25"));
        assertEquals(2.5d, handler.convertCsvValueToDbType("t1", "d1", "2.5"));
        assertEquals(UUID.fromString("123e4567-e89b-12d3-a456-426614174000"), handler
                .convertCsvValueToDbType("t1", "u1", "123e4567-e89b-12d3-a456-426614174000"));
        assertInstanceOf(Timestamp.class,
                handler.convertCsvValueToDbType("t1", "t1", "2026-02-15 01:02:03"));
    }

    @Test
    void convertCsvValueToDbType_正常ケース_空文字を指定する_nullが返ること() throws Exception {
        PostgresqlDialectHandler handler =
                createHandlerWithMeta("t1", new ColumnDef("c1", Types.VARCHAR, "varchar"));
        assertNull(handler.convertCsvValueToDbType("t1", "c1", " "));
    }

    @Test
    void convertCsvValueToDbType_正常ケース_nullを指定する_nullが返ること() throws Exception {
        PostgresqlDialectHandler handler =
                createHandlerWithMeta("t1", new ColumnDef("c1", Types.VARCHAR, "varchar"));
        assertNull(handler.convertCsvValueToDbType("t1", "c1", null));
    }

    @Test
    void convertCsvValueToDbType_正常ケース_file参照を指定する_LOB内容が返ること() throws Exception {
        PostgresqlDialectHandler handler =
                createHandlerWithMeta("t1", new ColumnDef("blob_col", Types.BINARY, "bytea"));
        Path filesDir = tempDir.resolve("dump").resolve("files");
        Files.createDirectories(filesDir);
        Files.write(filesDir.resolve("x.bin"), new byte[] {0x11, 0x12});
        Object actual = handler.convertCsvValueToDbType("t1", "blob_col", "file:x.bin");
        assertArrayEquals(new byte[] {0x11, 0x12}, (byte[]) actual);
    }

    @Test
    void convertCsvValueToDbType_正常ケース_xml型でfile参照を指定する_文字列として読み込まれること() throws Exception {
        PostgresqlDialectHandler handler =
                createHandlerWithMeta("t1", new ColumnDef("xml_col", Types.OTHER, "xml"));
        Path filesDir = tempDir.resolve("dump").resolve("files");
        Files.createDirectories(filesDir);
        Files.writeString(filesDir.resolve("x.xml"), "<root><v>1</v></root>",
                StandardCharsets.UTF_8);

        Object actual = handler.convertCsvValueToDbType("t1", "xml_col", "file:x.xml");
        assertEquals("<root><v>1</v></root>", actual);
    }

    @Test
    void convertCsvValueToDbType_異常ケース_不正数値を指定する_DataSetExceptionが送出されること() throws Exception {
        PostgresqlDialectHandler handler =
                createHandlerWithMeta("t1", new ColumnDef("n1", Types.NUMERIC, "numeric"));
        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("t1", "n1", "abc"));
    }

    @Test
    void convertCsvValueToDbType_異常ケース_不正hexを指定する_DataSetExceptionが送出されること() throws Exception {
        PostgresqlDialectHandler handler =
                createHandlerWithMeta("t1", new ColumnDef("blob_col", Types.BINARY, "bytea"));
        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("t1", "blob_col", "XYZ"));
    }

    @Test
    void convertCsvValueToDbType_正常ケース_boolean別名値を指定する_期待真偽値へ変換されること() throws Exception {
        PostgresqlDialectHandler handler =
                createHandlerWithMeta("t1", new ColumnDef("b1", Types.OTHER, "bool"));
        assertEquals(Boolean.TRUE, handler.convertCsvValueToDbType("t1", "b1", "true"));
        assertEquals(Boolean.TRUE, handler.convertCsvValueToDbType("t1", "b1", "1"));
        assertEquals(Boolean.TRUE, handler.convertCsvValueToDbType("t1", "b1", "t"));
        assertEquals(Boolean.FALSE, handler.convertCsvValueToDbType("t1", "b1", "false"));
        assertEquals(Boolean.FALSE, handler.convertCsvValueToDbType("t1", "b1", "0"));
        assertEquals(Boolean.FALSE, handler.convertCsvValueToDbType("t1", "b1", "f"));
    }

    @Test
    void convertCsvValueToDbType_正常ケース_型別名を指定する_数値型へ変換されること() throws Exception {
        PostgresqlDialectHandler handler =
                createHandlerWithMeta("t1", new ColumnDef("n1", Types.OTHER, "decimal"),
                        new ColumnDef("r1", Types.OTHER, "real"),
                        new ColumnDef("d1", Types.OTHER, "double precision"));
        assertEquals("12.5", handler.convertCsvValueToDbType("t1", "n1", "12.5").toString());
        assertEquals(1.5f, handler.convertCsvValueToDbType("t1", "r1", "1.5"));
        assertEquals(2.5d, handler.convertCsvValueToDbType("t1", "d1", "2.5"));
    }

    @Test
    void convertCsvValueToDbType_正常ケース_日時型別名を指定する_日時型へ変換されること() throws Exception {
        PostgresqlDialectHandler handler = createHandlerWithMeta("t1",
                new ColumnDef("d1", Types.OTHER, "date"), new ColumnDef("t1", Types.OTHER, "time"),
                new ColumnDef("z1", Types.OTHER, "timestamptz"));
        assertInstanceOf(Date.class, handler.convertCsvValueToDbType("t1", "d1", "2026-02-15"));
        assertInstanceOf(Time.class, handler.convertCsvValueToDbType("t1", "t1", "01:02:03"));
        assertInstanceOf(Timestamp.class,
                handler.convertCsvValueToDbType("t1", "z1", "2026-02-15T01:02:03+09:00"));
    }

    @Test
    void hasNotNullLobColumn_正常ケース_NOTNULLLOB列が存在する_trueが返ること() throws Exception {
        PostgresqlDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getColumns(null, "public", "t1", null)).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getString("IS_NULLABLE")).thenReturn("NO");
        when(rs.getString("TYPE_NAME")).thenReturn("bytea");
        when(rs.getInt("DATA_TYPE")).thenReturn(Types.BINARY);
        assertTrue(handler.hasNotNullLobColumn(conn, "public", "t1", new Column[0]));
    }

    @Test
    void hasNotNullLobColumn_正常ケース_NOTNULLLOB列が存在しない_falseが返ること() throws Exception {
        PostgresqlDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getColumns(null, "public", "t1", null)).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getString("IS_NULLABLE")).thenReturn("YES");
        when(rs.getString("TYPE_NAME")).thenReturn("bytea");
        when(rs.getInt("DATA_TYPE")).thenReturn(Types.BINARY);
        assertFalse(handler.hasNotNullLobColumn(conn, "public", "t1", new Column[0]));
    }

    @Test
    void hasNotNullLobColumn_正常ケース_列定義が存在しない_falseが返ること() throws Exception {
        PostgresqlDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getColumns(null, "public", "t1", null)).thenReturn(rs);
        when(rs.next()).thenReturn(false);
        assertFalse(handler.hasNotNullLobColumn(conn, "public", "t1", new Column[0]));
    }

    @Test
    void hasNotNullLobColumn_正常ケース_NOTNULLかつ非LOB列を指定する_falseが返ること() throws Exception {
        PostgresqlDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getColumns(null, "public", "t1", null)).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getString("IS_NULLABLE")).thenReturn("NO");
        when(rs.getString("TYPE_NAME")).thenReturn("integer");
        when(rs.getInt("DATA_TYPE")).thenReturn(Types.INTEGER);
        assertFalse(handler.hasNotNullLobColumn(conn, "public", "t1", new Column[0]));
    }

    @Test
    void hasPrimaryKey_正常ケース_PKが存在する_trueが返ること() throws Exception {
        PostgresqlDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getPrimaryKeys(null, "public", "t1")).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        assertTrue(handler.hasPrimaryKey(conn, "public", "t1"));
    }

    @Test
    void countRows_正常ケース_件数を取得する_COUNT値が返ること() throws Exception {
        PostgresqlDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        Statement st = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.createStatement()).thenReturn(st);
        when(st.executeQuery("SELECT COUNT(*) FROM t1")).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getInt(1)).thenReturn(9);
        assertEquals(9, handler.countRows(conn, "t1"));
    }

    @Test
    void getLobColumns_正常ケース_file参照列が存在する_LOB列が返ること() throws Exception {
        PostgresqlDialectHandler handler = createHandler();
        Path dir = tempDir.resolve("csv");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("TBL.csv"), "ID,BLOB_COL,CLOB_COL\n1,file:a.bin,\n2,,text\n",
                StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("table-ordering.txt"), "TBL\n", StandardCharsets.UTF_8);
        Column[] cols = handler.getLobColumns(dir, "TBL");
        assertEquals(1, cols.length);
        assertEquals("BLOB_COL", cols[0].getColumnName());
    }

    @Test
    void getLobColumns_正常ケース_CSVが存在しない_空配列が返ること() throws Exception {
        PostgresqlDialectHandler handler = createHandler();
        Column[] cols = handler.getLobColumns(tempDir, "MISSING");
        assertEquals(0, cols.length);
    }

    @Test
    void getLobColumns_正常ケース_file参照がないCSVを指定する_空配列が返ること() throws Exception {
        PostgresqlDialectHandler handler = createHandler();
        Path dir = tempDir.resolve("csv_no_file");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("TBL.csv"), "ID,C1\n1,text\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("table-ordering.txt"), "TBL\n", StandardCharsets.UTF_8);
        Column[] cols = handler.getLobColumns(dir, "TBL");
        assertEquals(0, cols.length);
    }

    @Test
    void logTableDefinition_正常ケース_列情報を取得する_例外なく完了すること() throws Exception {
        PostgresqlDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getColumns(null, "public", "t1", null)).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getString("COLUMN_NAME")).thenReturn("id");
        when(rs.getString("TYPE_NAME")).thenReturn("bigint");
        when(rs.getInt("DATA_TYPE")).thenReturn(Types.BIGINT);
        handler.logTableDefinition(conn, "public", "t1", "test");
    }

    @Test
    void constructor_正常ケース_getSchemaで例外が発生する_publicスキーマで初期化されること() throws Exception {
        DateTimeFormatUtil formatter = mock(DateTimeFormatUtil.class);
        DbUnitConfigFactory configFactory = mock(DbUnitConfigFactory.class);
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
        IDataSet dataSet = mock(IDataSet.class);

        when(dbConn.getConnection()).thenReturn(jdbc);
        when(jdbc.getSchema()).thenThrow(new SQLException("schema-error"));
        when(jdbc.getMetaData()).thenReturn(meta);
        when(meta.getTables(eq(null), eq("public"), eq("%"), any(String[].class)))
                .thenReturn(rsTables);
        when(rsTables.next()).thenReturn(false);
        when(dbConn.createDataSet()).thenReturn(dataSet);

        PostgresqlDialectHandler handler = new PostgresqlDialectHandler(dbConn, dumpConfig,
                dbUnitConfig, configFactory, formatter, pathsConfig);
        assertNotNull(handler);
    }

    @Test
    void formatDbValueForCsv_正常ケース_日付時刻型を複数指定する_フォーマッタ結果が返ること() throws Exception {
        DateTimeFormatUtil formatter = mock(DateTimeFormatUtil.class);
        when(formatter.formatJdbcDateTime(eq("D_COL"), any(), eq(null))).thenReturn("d");
        when(formatter.formatJdbcDateTime(eq("ODT_COL"), any(), eq(null))).thenReturn("odt");
        PostgresqlDialectHandler handler =
                createHandler(formatter, mock(DbUnitConfigFactory.class));

        assertEquals("d", handler.formatDbValueForCsv("D_COL", Date.valueOf("2026-02-15")));
        assertEquals("odt", handler.formatDbValueForCsv("ODT_COL",
                OffsetDateTime.parse("2026-02-15T01:02:03+09:00")));
    }

    private PostgresqlDialectHandler createHandler() throws Exception {
        return createHandler(mock(DateTimeFormatUtil.class), mock(DbUnitConfigFactory.class));
    }

    private PostgresqlDialectHandler createHandler(DateTimeFormatUtil formatter,
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
        when(jdbc.getSchema()).thenReturn("public");
        when(jdbc.getMetaData()).thenReturn(meta);
        when(meta.getTables(eq(null), eq("public"), eq("%"), any(String[].class)))
                .thenReturn(rsTables);
        when(rsTables.next()).thenReturn(false);
        when(dbConn.createDataSet()).thenReturn(dataSet);

        return new PostgresqlDialectHandler(dbConn, dumpConfig, dbUnitConfig, configFactory,
                formatter, pathsConfig);
    }

    private static final class ColumnDef {

        final String name;
        final int sqlType;
        final String sqlTypeName;

        ColumnDef(String name, int sqlType, String sqlTypeName) {
            this.name = name;
            this.sqlType = sqlType;
            this.sqlTypeName = sqlTypeName;
        }
    }

    private PostgresqlDialectHandler createHandlerWithMeta(String tableName, ColumnDef... columns)
            throws Exception {
        return createHandlerWithMeta(mock(DateTimeFormatUtil.class),
                mock(DbUnitConfigFactory.class), tableName, columns);
    }

    private PostgresqlDialectHandler createHandlerWithMeta(String tableName, Column[] dbUnitColumns,
            ColumnDef... columns) throws Exception {
        return createHandlerWithMeta(mock(DateTimeFormatUtil.class),
                mock(DbUnitConfigFactory.class), tableName, dbUnitColumns, columns);
    }

    private PostgresqlDialectHandler createHandlerWithMeta(DateTimeFormatUtil formatter,
            DbUnitConfigFactory configFactory, String tableName, ColumnDef... columns)
            throws Exception {
        Column[] dbUnitColumns = new Column[columns.length];
        for (int i = 0; i < columns.length; i++) {
            dbUnitColumns[i] = colMock(columns[i].name,
                    dataTypeMock(columns[i].sqlType, columns[i].sqlTypeName));
        }
        return createHandlerWithMeta(formatter, configFactory, tableName, dbUnitColumns, columns);
    }

    private PostgresqlDialectHandler createHandlerWithMeta(DateTimeFormatUtil formatter,
            DbUnitConfigFactory configFactory, String tableName, Column[] dbUnitColumns,
            ColumnDef... columns) throws Exception {
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
        when(jdbc.getSchema()).thenReturn("public");
        when(jdbc.getMetaData()).thenReturn(meta);
        ResultSet rsTables = mock(ResultSet.class);
        when(meta.getTables(eq(null), eq("public"), eq("%"), any(String[].class)))
                .thenReturn(rsTables);
        when(rsTables.next()).thenReturn(true, false);
        when(rsTables.getString("TABLE_NAME")).thenReturn(tableName);
        ITableMetaData tableMeta = mock(ITableMetaData.class);
        when(tableMeta.getColumns()).thenReturn(dbUnitColumns);
        when(dataSet.getTableMetaData(tableName)).thenReturn(tableMeta);
        when(dbConn.createDataSet()).thenReturn(dataSet);
        ResultSet rsColumns = mock(ResultSet.class);
        when(meta.getColumns(eq(null), eq("public"), eq(tableName), eq(null)))
                .thenReturn(rsColumns);
        if (columns.length == 0) {
            when(rsColumns.next()).thenReturn(false);
        } else {
            Boolean[] nextRest = new Boolean[columns.length];
            for (int i = 0; i < columns.length - 1; i++) {
                nextRest[i] = Boolean.TRUE;
            }
            nextRest[columns.length - 1] = Boolean.FALSE;
            when(rsColumns.next()).thenReturn(true, nextRest);
            String[] names = Arrays.stream(columns).map(c -> c.name).toArray(String[]::new);
            Integer[] types =
                    Arrays.stream(columns).mapToInt(c -> c.sqlType).boxed().toArray(Integer[]::new);
            String[] typeNames =
                    Arrays.stream(columns).map(c -> c.sqlTypeName).toArray(String[]::new);
            when(rsColumns.getString("COLUMN_NAME")).thenReturn(names[0],
                    Arrays.copyOfRange(names, 1, names.length));
            when(rsColumns.getInt("DATA_TYPE")).thenReturn(types[0],
                    Arrays.copyOfRange(types, 1, types.length));
            when(rsColumns.getString("TYPE_NAME")).thenReturn(typeNames[0],
                    Arrays.copyOfRange(typeNames, 1, typeNames.length));
        }
        return new PostgresqlDialectHandler(dbConn, dumpConfig, dbUnitConfig, configFactory,
                formatter, pathsConfig);
    }

    @Test
    void constructor_正常ケース_テーブルが存在しJDBC列メタをキャッシュする_cacheJdbcColumnSpecsが実行されること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());

        DbUnitConfig dbUnitConfig = new DbUnitConfig();

        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(new ArrayList<>());

        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        Connection jdbc = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rsTables = mock(ResultSet.class);
        ResultSet rsCols = mock(ResultSet.class);
        IDataSet dataSet = mock(IDataSet.class);
        ITableMetaData tableMeta = mock(ITableMetaData.class);

        when(dbConn.getConnection()).thenReturn(jdbc);
        when(jdbc.getSchema()).thenReturn("public");
        when(jdbc.getMetaData()).thenReturn(meta);
        when(meta.getTables(eq(null), eq("public"), eq("%"), any(String[].class)))
                .thenReturn(rsTables);
        when(rsTables.next()).thenReturn(true, false);
        when(rsTables.getString("TABLE_NAME")).thenReturn("MY_TABLE");
        when(dbConn.createDataSet()).thenReturn(dataSet);
        when(dataSet.getTableMetaData("MY_TABLE")).thenReturn(tableMeta);
        when(tableMeta.getColumns()).thenReturn(new Column[0]);
        when(meta.getColumns(eq(null), eq("public"), eq("MY_TABLE"), eq(null))).thenReturn(rsCols);
        when(rsCols.next()).thenReturn(true, false);
        when(rsCols.getString("COLUMN_NAME")).thenReturn("ID");
        when(rsCols.getInt("DATA_TYPE")).thenReturn(Types.INTEGER);
        when(rsCols.getString("TYPE_NAME")).thenReturn("int4");

        PostgresqlDialectHandler handler = new PostgresqlDialectHandler(dbConn, dumpConfig,
                dbUnitConfig, mock(DbUnitConfigFactory.class), mock(DateTimeFormatUtil.class),
                pathsConfig);
        assertNotNull(handler);
    }

    @Test
    void parseDateTimeValue_正常ケース_設定済み日付フォーマットに一致する_Dateが返ること() throws Exception {
        DateTimeFormatUtil formatter = mock(DateTimeFormatUtil.class);
        when(formatter.parseConfiguredDate("2026/02/15")).thenReturn(LocalDate.of(2026, 2, 15));
        PostgresqlDialectHandler handler =
                createHandler(formatter, mock(DbUnitConfigFactory.class));
        Object result = handler.parseDateTimeValue("date_col", "2026/02/15");
        assertInstanceOf(java.sql.Date.class, result);
    }

    @Test
    void parseDateTimeValue_正常ケース_設定済み時刻フォーマットに一致する_Timeが返ること() throws Exception {
        DateTimeFormatUtil formatter = mock(DateTimeFormatUtil.class);
        when(formatter.parseConfiguredTime("12:30:00")).thenReturn(LocalTime.of(12, 30, 0));
        PostgresqlDialectHandler handler =
                createHandler(formatter, mock(DbUnitConfigFactory.class));
        Object result = handler.parseDateTimeValue("time_col", "12:30:00");
        assertInstanceOf(Time.class, result);
    }

    @Test
    void parseDateTimeValue_正常ケース_設定済みタイムスタンプフォーマットに一致する_Timestampが返ること() throws Exception {
        DateTimeFormatUtil formatter = mock(DateTimeFormatUtil.class);
        when(formatter.parseConfiguredTimestamp("2026-02-15 01:02:03"))
                .thenReturn(LocalDateTime.of(2026, 2, 15, 1, 2, 3));
        PostgresqlDialectHandler handler =
                createHandler(formatter, mock(DbUnitConfigFactory.class));
        Object result = handler.parseDateTimeValue("ts_col", "2026-02-15 01:02:03");
        assertInstanceOf(Timestamp.class, result);
    }

    @Test
    void convertCsvValueToDbType_正常ケース_TIMESTAMP型の日付文字列を指定する_Timestampが返ること() throws Exception {
        PostgresqlDialectHandler handler =
                createHandlerWithMeta("T1", new ColumnDef("TS_COL", Types.TIMESTAMP, "OTHER"));
        Object result = handler.convertCsvValueToDbType("T1", "TS_COL", "2026-02-15 01:02:03");
        assertInstanceOf(Timestamp.class, result);
    }

    @Test
    void convertCsvValueToDbType_正常ケース_BINARY型のhex文字列を指定する_byte配列が返ること() throws Exception {
        PostgresqlDialectHandler handler =
                createHandlerWithMeta("T1", new ColumnDef("BIN_COL", Types.BINARY, "OTHER"));
        Object result = handler.convertCsvValueToDbType("T1", "BIN_COL", "\\x0102");
        assertInstanceOf(byte[].class, result);
    }

    @Test
    void convertCsvValueToDbType_異常ケース_日時型で不正値を指定する_DataSetExceptionが送出されること() throws Exception {
        DateTimeFormatUtil formatter = mock(DateTimeFormatUtil.class);
        when(formatter.parseConfiguredTimestamp(any())).thenReturn(null);
        when(formatter.parseConfiguredDate(any())).thenReturn(null);
        when(formatter.parseConfiguredTime(any())).thenReturn(null);
        PostgresqlDialectHandler handler =
                createHandlerWithMeta(formatter, mock(DbUnitConfigFactory.class), "T1",
                        new ColumnDef("DT_COL", Types.TIMESTAMP, "TIMESTAMP"));
        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("T1", "DT_COL", "not-a-date"));
    }

    @Test
    void convertCsvValueToDbType_正常ケース_DBUnit型がUNKNOWNである_JDBCメタデータへフォールバックすること() throws Exception {
        PostgresqlDialectHandler handler = createHandlerWithMeta(mock(DateTimeFormatUtil.class),
                mock(DbUnitConfigFactory.class), "T1",
                new Column[] {colMock("C1", DataType.UNKNOWN)},
                new ColumnDef("C1", Types.INTEGER, "int4"));

        assertEquals(123, handler.convertCsvValueToDbType("T1", "C1", "123"));
    }

    @Test
    void convertCsvValueToDbType_正常ケース_DBUnit型がOTHERである_JDBCメタデータへフォールバックすること() throws Exception {
        PostgresqlDialectHandler handler =
                createHandlerWithMeta(mock(DateTimeFormatUtil.class),
                        mock(DbUnitConfigFactory.class), "T1",
                        new Column[] {colMock("C1", dataTypeMock(Types.OTHER, "json"))},
                        new ColumnDef("C1", Types.INTEGER, "int4"));

        assertEquals(123, handler.convertCsvValueToDbType("T1", "C1", "123"));
    }

    @Test
    void convertCsvValueToDbType_正常ケース_DBUnit型名がUNKNOWNである_JDBCメタデータへフォールバックすること() throws Exception {
        PostgresqlDialectHandler handler =
                createHandlerWithMeta(mock(DateTimeFormatUtil.class),
                        mock(DbUnitConfigFactory.class), "T1",
                        new Column[] {colMock("C1", dataTypeMock(Types.INTEGER, "UNKNOWN"))},
                        new ColumnDef("C1", Types.INTEGER, "int4"));

        assertEquals(123, handler.convertCsvValueToDbType("T1", "C1", "123"));
    }

    @Test
    void convertCsvValueToDbType_正常ケース_DBUnit列が存在しない_JDBCメタデータで変換されること() throws Exception {
        PostgresqlDialectHandler handler = createHandlerWithMeta("T1", new Column[0],
                new ColumnDef("JDBC_ONLY", Types.BIGINT, "int8"));

        assertEquals(999L, handler.convertCsvValueToDbType("T1", "JDBC_ONLY", "999"));
    }

    @Test
    void convertCsvValueToDbType_正常ケース_DBUnit列のみ存在する_DBUnitメタデータで文字列が返ること() throws Exception {
        PostgresqlDialectHandler handler = createHandlerWithMeta("T1",
                new Column[] {colMock("DBUNIT_ONLY", dataTypeMock(Types.VARCHAR, "varchar"))});

        assertEquals("plain", handler.convertCsvValueToDbType("T1", "DBUNIT_ONLY", "plain"));
    }

    @Test
    void convertCsvValueToDbType_異常ケース_テーブルメタデータが存在しない_DataSetExceptionが送出されること() throws Exception {
        PostgresqlDialectHandler handler = createHandler();

        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("NO_TABLE", "C1", "1"));
    }

    @Test
    void convertCsvValueToDbType_異常ケース_列メタデータが存在しない_DataSetExceptionが送出されること() throws Exception {
        PostgresqlDialectHandler handler = createHandlerWithMeta("T1", new Column[0]);

        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("T1", "NO_COL", "1"));
    }

    @Test
    void convertCsvValueToDbType_異常ケース_file参照先が存在しない_DataSetExceptionが送出されること() throws Exception {
        PostgresqlDialectHandler handler =
                createHandlerWithMeta("T1", new ColumnDef("BYTEA_COL", Types.BINARY, "bytea"));

        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("T1", "BYTEA_COL", "file:missing.bin"));
    }

    @Test
    void convertCsvValueToDbType_異常ケース_file参照先がディレクトリである_DataSetExceptionが送出されること() throws Exception {
        PostgresqlDialectHandler handler =
                createHandlerWithMeta("T1", new ColumnDef("BYTEA_COL", Types.BINARY, "bytea"));
        Path filesDir = tempDir.resolve("dump").resolve("files");
        Files.createDirectories(filesDir.resolve("dir1"));

        DataSetException ex = assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("T1", "BYTEA_COL", "file:dir1"));

        assertTrue(ex.getMessage().contains("Failed to read LOB file"));
    }

    @Test
    void convertCsvValueToDbType_異常ケース_file参照で非LOB型を指定する_DataSetExceptionが送出されること() throws Exception {
        PostgresqlDialectHandler handler =
                createHandlerWithMeta("T1", new ColumnDef("INT_COL", Types.INTEGER, "int4"));
        Path filesDir = tempDir.resolve("dump").resolve("files");
        Files.createDirectories(filesDir);
        Files.writeString(filesDir.resolve("x.txt"), "x", StandardCharsets.UTF_8);

        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("T1", "INT_COL", "file:x.txt"));
    }

    @Test
    void convertCsvValueToDbType_正常ケース_CLOB型のfile参照を指定する_文字列として読み込まれること() throws Exception {
        PostgresqlDialectHandler handler =
                createHandlerWithMeta("T1", new ColumnDef("CLOB_COL", Types.CLOB, "clob"));
        Path filesDir = tempDir.resolve("dump").resolve("files");
        Files.createDirectories(filesDir);
        Files.writeString(filesDir.resolve("clob.txt"), "hello", StandardCharsets.UTF_8);

        assertEquals("hello",
                handler.convertCsvValueToDbType("T1", "CLOB_COL", "file:clob.txt"));
    }

    @Test
    void convertCsvValueToDbType_正常ケース_boolean別名に非標準値を指定する_falseが返ること() throws Exception {
        PostgresqlDialectHandler handler =
                createHandlerWithMeta("T1", new ColumnDef("B1", Types.OTHER, "bool"));

        assertEquals(Boolean.FALSE, handler.convertCsvValueToDbType("T1", "B1", "yes"));
    }

    @Test
    void convertCsvValueToDbType_正常ケース_DATE型とTIME型を指定する_日時型へ変換されること() throws Exception {
        PostgresqlDialectHandler handler = createHandlerWithMeta("T1",
                new ColumnDef("DATE_COL", Types.DATE, "date"),
                new ColumnDef("TIME_COL", Types.TIME, "time"));

        assertInstanceOf(Date.class,
                handler.convertCsvValueToDbType("T1", "DATE_COL", "2026-02-15"));
        assertInstanceOf(Time.class,
                handler.convertCsvValueToDbType("T1", "TIME_COL", "01:02:03"));
    }

    @Test
    void parseDateTimeValue_正常ケース_4桁時刻を指定する_Timeが返ること() throws Exception {
        PostgresqlDialectHandler handler = createHandler();

        assertEquals(Time.valueOf("01:02:00"),
                handler.parseDateTimeValue("time_col", "0102"));
    }

    @Test
    void parseDateTimeValue_正常ケース_コロン無しオフセットを指定する_Timestampが返ること() throws Exception {
        PostgresqlDialectHandler handler = createHandler();

        assertInstanceOf(Timestamp.class,
                handler.parseDateTimeValue("col", "2026-02-15 01:02:03+0900"));
    }

    @Test
    void parseDateTimeValue_異常ケース_オフセット末尾が非数値である_IllegalArgumentExceptionが送出されること() throws Exception {
        PostgresqlDialectHandler handler = createHandler();

        assertThrows(IllegalArgumentException.class,
                () -> handler.parseDateTimeValue("col", "2026-02-15 01:02:03+ABCD"));
    }

    @Test
    void constructor_正常ケース_excludeTablesがnullである_初期化できること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());

        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(null);

        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        Connection jdbc = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rsTables = mock(ResultSet.class);

        when(dbConn.getConnection()).thenReturn(jdbc);
        when(jdbc.getSchema()).thenReturn("public");
        when(jdbc.getMetaData()).thenReturn(meta);
        when(meta.getTables(eq(null), eq("public"), eq("%"), any(String[].class)))
                .thenReturn(rsTables);
        when(rsTables.next()).thenReturn(false);
        when(dbConn.createDataSet()).thenReturn(mock(IDataSet.class));

        PostgresqlDialectHandler handler = new PostgresqlDialectHandler(dbConn, dumpConfig,
                new DbUnitConfig(), mock(DbUnitConfigFactory.class), mock(DateTimeFormatUtil.class),
                pathsConfig);

        assertNotNull(handler);
    }

    @Test
    void constructor_正常ケース_excludeTablesに一致するテーブルがある_除外テーブルのメタ取得が行われないこと() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());

        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(List.of("T2"));

        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        Connection jdbc = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rsTables = mock(ResultSet.class);
        ResultSet rsCols = mock(ResultSet.class);
        IDataSet dataSet = mock(IDataSet.class);
        ITableMetaData tableMeta = mock(ITableMetaData.class);

        when(dbConn.getConnection()).thenReturn(jdbc);
        when(jdbc.getSchema()).thenReturn("public");
        when(jdbc.getMetaData()).thenReturn(meta);
        when(meta.getTables(eq(null), eq("public"), eq("%"), any(String[].class)))
                .thenReturn(rsTables);
        when(rsTables.next()).thenReturn(true, true, false);
        when(rsTables.getString("TABLE_NAME")).thenReturn("T1", "T2");
        when(dbConn.createDataSet()).thenReturn(dataSet);
        when(dataSet.getTableMetaData("T1")).thenReturn(tableMeta);
        when(tableMeta.getColumns()).thenReturn(new Column[0]);
        when(meta.getColumns(eq(null), eq("public"), eq("T1"), eq(null))).thenReturn(rsCols);
        when(rsCols.next()).thenReturn(false);

        new PostgresqlDialectHandler(dbConn, dumpConfig, new DbUnitConfig(),
                mock(DbUnitConfigFactory.class), mock(DateTimeFormatUtil.class), pathsConfig);

        verify(dataSet).getTableMetaData("T1");
        verify(dataSet, org.mockito.Mockito.never()).getTableMetaData("T2");
    }

    @Test
    void hasNotNullLobColumn_正常ケース_CLOB型を指定する_trueが返ること() throws Exception {
        PostgresqlDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getColumns(null, "public", "t1", null)).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getString("IS_NULLABLE")).thenReturn("NO");
        when(rs.getString("TYPE_NAME")).thenReturn("clob");
        when(rs.getInt("DATA_TYPE")).thenReturn(Types.CLOB);

        assertTrue(handler.hasNotNullLobColumn(conn, "public", "t1", new Column[0]));
    }

    @Test
    void hasNotNullLobColumn_正常ケース_TEXT型を指定する_trueが返ること() throws Exception {
        PostgresqlDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getColumns(null, "public", "t1", null)).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getString("IS_NULLABLE")).thenReturn("NO");
        when(rs.getString("TYPE_NAME")).thenReturn("text");
        when(rs.getInt("DATA_TYPE")).thenReturn(Types.LONGVARCHAR);

        assertTrue(handler.hasNotNullLobColumn(conn, "public", "t1", new Column[0]));
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
