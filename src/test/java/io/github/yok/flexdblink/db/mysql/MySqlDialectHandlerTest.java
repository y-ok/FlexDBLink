package io.github.yok.flexdblink.db.mysql;

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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.db.DbUnitConfigFactory;
import io.github.yok.flexdblink.util.OracleDateTimeFormatUtil;
import java.io.StringReader;
import java.lang.reflect.Field;
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
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

public class MySqlDialectHandlerTest {

    @TempDir
    Path tempDir;

    @Test
    void quoteIdentifier_正常ケース_識別子を指定する_バッククォート付き文字列が返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        assertEquals("`A1`", handler.quoteIdentifier("A1"));
    }

    @Test
    void applyPagination_正常ケース_offsetlimitを指定する_MySQL形式SQLが返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        assertEquals("SELECT * FROM T LIMIT 5, 10",
                handler.applyPagination("SELECT * FROM T", 5, 10));
    }

    @Test
    void formatDateLiteral_正常ケース_LocalDateTimeを指定する_TIMESTAMPリテラルが返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        assertEquals("TIMESTAMP '2026-02-15 16:00:01'",
                handler.formatDateLiteral(LocalDateTime.of(2026, 2, 15, 16, 0, 1)));
    }

    @Test
    void buildUpsertSql_正常ケース_keyinsertupdateを指定する_ONDUPLICATEKEY文が返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        String sql =
                handler.buildUpsertSql("t1", List.of("id"), List.of("id", "name"), List.of("name"));
        assertTrue(sql.contains("INSERT INTO t1"));
        assertTrue(sql.contains("ON DUPLICATE KEY UPDATE"));
        assertTrue(sql.contains("name = VALUES(name)"));
    }

    @Test
    void getCreateTempTableSql_正常ケース_列定義を指定する_CREATETEMP文が返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        String sql = handler.getCreateTempTableSql("tmp_t", Map.of("id", "bigint", "name", "text"));
        assertTrue(sql.contains("CREATE TEMPORARY TABLE tmp_t"));
        assertTrue(sql.contains("id bigint"));
        assertTrue(sql.contains("name text"));
    }

    @Test
    void applyForUpdate_正常ケース_SELECT文を指定する_FORUPDATE句が付与されること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        assertEquals("SELECT 1 FOR UPDATE", handler.applyForUpdate("SELECT 1"));
    }

    @Test
    void formatDbValueForCsv_正常ケース_nullを指定する_空文字が返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        assertEquals("", handler.formatDbValueForCsv("COL", null));
    }

    @Test
    void formatDbValueForCsv_正常ケース_LOB型を指定する_空文字が返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        Blob blob = mock(Blob.class);
        Clob clob = mock(Clob.class);
        assertEquals("", handler.formatDbValueForCsv("COL", new byte[] {0x01}));
        assertEquals("", handler.formatDbValueForCsv("COL", blob));
        assertEquals("", handler.formatDbValueForCsv("COL", clob));
    }

    @Test
    void formatDbValueForCsv_正常ケース_BIT列にバイト配列を指定する_10進文字列が返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        assertEquals("1", handler.formatDbValueForCsv("BIT_COL", new byte[] {0x01}));
        assertEquals("240", handler.formatDbValueForCsv("BIT_COL", new byte[] {(byte) 0xF0}));
    }

    @Test
    void formatDbValueForCsv_正常ケース_日時型を指定する_フォーマッタ経由の文字列が返ること() throws Exception {
        OracleDateTimeFormatUtil formatter = mock(OracleDateTimeFormatUtil.class);
        when(formatter.formatJdbcDateTime(eq("TS_COL"), any(), eq(null))).thenReturn("fmt");
        MySqlDialectHandler handler =
                createHandler(formatter, mock(DbUnitConfigFactory.class));
        assertEquals("fmt",
                handler.formatDbValueForCsv("TS_COL", Timestamp.valueOf("2026-02-15 01:02:03")));
    }

    @Test
    void formatDbValueForCsv_正常ケース_YEAR列にDateを指定する_年のみの文字列が返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        assertEquals("2026",
                handler.formatDbValueForCsv("YEAR_COL", Date.valueOf("2026-01-01")));
        assertEquals("2026", handler.formatDbValueForCsv("YEAR_COL", "2026-01-01"));
        assertEquals("2026", handler.formatDbValueForCsv("YEAR_COL", "2026"));
    }

    @Test
    void formatDbValueForCsv_正常ケース_YEAR列に非年形式文字列を指定する_文字列がそのまま返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        assertEquals("year-unknown", handler.formatDbValueForCsv("YEAR_COL", "year-unknown"));
    }

    @Test
    void formatDbValueForCsv_正常ケース_通常値を指定する_toString値が返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        assertEquals("123", handler.formatDbValueForCsv("COL", 123));
    }

    @Test
    void formatDbValueForCsv_正常ケース_SQLXMLを指定する_XML文字列が返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        SQLXML sqlxml = mock(SQLXML.class);
        when(sqlxml.getString()).thenReturn("<root/>");
        assertEquals("<root/>", handler.formatDbValueForCsv("XML_COL", sqlxml));
    }

    @Test
    void formatDateTimeColumn_正常ケース_ミリ秒ゼロを含む日時文字列を指定する_末尾の000が除去された値が返ること() throws Exception {
        OracleDateTimeFormatUtil formatter = mock(OracleDateTimeFormatUtil.class);
        when(formatter.formatJdbcDateTime(eq("TS_COL"), any(), eq(null)))
                .thenReturn("2026-02-15 01:02:03.000");
        MySqlDialectHandler handler =
                createHandler(formatter, mock(DbUnitConfigFactory.class));
        assertEquals("2026-02-15 01:02:03", handler.formatDateTimeColumn("TS_COL",
                Timestamp.valueOf("2026-02-15 01:02:03"), null));
    }

    @Test
    void formatDateTimeColumn_正常ケース_タイムゾーン付きミリ秒ゼロを指定する_末尾の000が除去された値が返ること() throws Exception {
        OracleDateTimeFormatUtil formatter = mock(OracleDateTimeFormatUtil.class);
        when(formatter.formatJdbcDateTime(eq("TSZ_COL"), any(), eq(null)))
                .thenReturn("2026-02-15 01:02:03.000+0900");
        MySqlDialectHandler handler =
                createHandler(formatter, mock(DbUnitConfigFactory.class));
        assertEquals("2026-02-15 01:02:03+0900", handler.formatDateTimeColumn("TSZ_COL",
                Timestamp.valueOf("2026-02-15 01:02:03"), null));
    }

    @Test
    void resolveSchema_正常ケース_接続設定を指定する_DB名が返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setUrl("jdbc:mysql://localhost:3306/testdb?useSSL=false");
        assertEquals("testdb", handler.resolveSchema(entry));
    }

    @Test
    void resolveSchema_正常ケース_URLが不足形式である_testdbが返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();

        entry.setUrl(null);
        assertEquals("testdb", handler.resolveSchema(entry));

        entry.setUrl("plain-url");
        assertEquals("testdb", handler.resolveSchema(entry));

        entry.setUrl("jdbc:mysql://localhost");
        assertEquals("localhost", handler.resolveSchema(entry));

        entry.setUrl("jdbc:mysql://localhost/");
        assertEquals("testdb", handler.resolveSchema(entry));

        entry.setUrl("jdbc:mysql://localhost/?useSSL=false");
        assertEquals("testdb", handler.resolveSchema(entry));
    }

    @Test
    void prepareConnection_正常ケース_接続を初期化する_timezoneと文字コード設定SQLが実行されること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmt);
        handler.prepareConnection(conn);
        verify(stmt).execute("SET time_zone = '+00:00'");
        verify(stmt).execute("SET NAMES utf8mb4");
    }

    @Test
    void getPrimaryKeyColumns_正常ケース_PK列が存在する_PK列一覧が返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
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
        MySqlDialectHandler handler = createHandler();
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
    void getColumnTypeName_正常ケース_型情報が存在しない_nullが返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getColumns(null, "public", "t1", "c1")).thenReturn(rs);
        when(rs.next()).thenReturn(false);
        assertNull(handler.getColumnTypeName(conn, "public", "t1", "c1"));
    }

    @Test
    void parseDateTimeValue_正常ケース_各形式を指定する_日時型が返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        assertInstanceOf(Timestamp.class,
                handler.parseDateTimeValue("tstz_col", "2026-02-15T01:02:03+09:00"));
        assertInstanceOf(Timestamp.class,
                handler.parseDateTimeValue("ts_col", "2026-02-15 01:02:03.123"));
        assertInstanceOf(java.sql.Date.class, handler.parseDateTimeValue("date_col", "2026/02/15"));
        assertInstanceOf(Time.class, handler.parseDateTimeValue("time_col", "010203"));
    }

    @Test
    void parseDateTimeValue_正常ケース_YEAR列に4桁年を指定する_年初日のDateが返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        Object actual = handler.parseDateTimeValue("YEAR_COL", "2026");
        assertEquals(Date.valueOf("2026-01-01"), actual);
    }

    @Test
    void parseDateTimeValue_正常ケース_YEAR列に日付形式を指定する_DATEとして返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        Object actual = handler.parseDateTimeValue("YEAR_COL", "2026-02-15");
        assertEquals(Date.valueOf("2026-02-15"), actual);
    }

    @Test
    void parseDateTimeValue_異常ケース_不正値を指定する_IllegalArgumentExceptionが送出されること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        assertThrows(IllegalArgumentException.class,
                () -> handler.parseDateTimeValue("col", "not-datetime"));
    }

    @Test
    void writeLobFile_正常ケース_各型を指定する_ファイルへ書き込まれること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        Path p1 = tempDir.resolve("a").resolve("b.bin");
        handler.writeLobFile("public", "t1", "abc", p1);
        assertEquals("abc", Files.readString(p1, StandardCharsets.UTF_8));

        Path p2 = tempDir.resolve("a").resolve("b2.bin");
        handler.writeLobFile("public", "t1", new byte[] {0x01, 0x02}, p2);
        assertArrayEquals(new byte[] {0x01, 0x02}, Files.readAllBytes(p2));

        Blob blob = mock(Blob.class);
        when(blob.getBinaryStream()).thenReturn(
                new java.io.ByteArrayInputStream("blob".getBytes(StandardCharsets.UTF_8)));
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
        MySqlDialectHandler handler = createHandler();
        putTableMetaBySqlType(handler, "t1", "blob_col", Types.BINARY, "bytea");
        Path filesDir = tempDir.resolve("base").resolve("files");
        Files.createDirectories(filesDir);
        Files.write(filesDir.resolve("a.bin"), new byte[] {0x0A, 0x0B});
        Object actual =
                handler.readLobFile("a.bin", "t1", "blob_col", tempDir.resolve("base").toFile());
        assertArrayEquals(new byte[] {0x0A, 0x0B}, (byte[]) actual);
    }

    @Test
    void readLobFile_正常ケース_text列を指定する_文字列が返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        putTableMetaBySqlType(handler, "t1", "clob_col", Types.CLOB, "text");
        Path filesDir = tempDir.resolve("base2").resolve("files");
        Files.createDirectories(filesDir);
        Files.writeString(filesDir.resolve("a.txt"), "hello", StandardCharsets.UTF_8);
        Object actual =
                handler.readLobFile("a.txt", "t1", "clob_col", tempDir.resolve("base2").toFile());
        assertEquals("hello", actual);
    }

    @Test
    void readLobFile_異常ケース_ファイルが存在しない_DataSetExceptionが送出されること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        putTableMetaBySqlType(handler, "t1", "clob_col", Types.CLOB, "text");
        assertThrows(DataSetException.class,
                () -> handler.readLobFile("missing.txt", "t1", "clob_col", tempDir.toFile()));
    }

    @Test
    void createDbUnitConnection_正常ケース_DBUnit設定を行う_設定ファクトリが呼び出されること() throws Exception {
        OracleDateTimeFormatUtil formatter = mock(OracleDateTimeFormatUtil.class);
        DbUnitConfigFactory factory = mock(DbUnitConfigFactory.class);
        MySqlDialectHandler handler = createHandler(formatter, factory);
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
        assertEquals("`?`", dbConn.getConfig().getProperty(DatabaseConfig.PROPERTY_ESCAPE_PATTERN));
    }

    @Test
    void convertCsvValueToDbType_正常ケース_各型を指定する_期待型へ変換されること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        putTableMetaBySqlType(handler, "t1", "b1", Types.BOOLEAN, "boolean");
        putTableMetaBySqlType(handler, "t1", "i1", Types.INTEGER, "int4");
        putTableMetaBySqlType(handler, "t1", "l1", Types.BIGINT, "int8");
        putTableMetaBySqlType(handler, "t1", "n1", Types.NUMERIC, "numeric");
        putTableMetaBySqlType(handler, "t1", "r1", Types.REAL, "float4");
        putTableMetaBySqlType(handler, "t1", "d1", Types.DOUBLE, "float8");
        putTableMetaBySqlType(handler, "t1", "u1", Types.OTHER, "uuid");
        putTableMetaBySqlType(handler, "t1", "t1", Types.TIMESTAMP, "timestamp");

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
        MySqlDialectHandler handler = createHandler();
        putTableMetaBySqlType(handler, "t1", "c1", Types.VARCHAR, "varchar");
        assertNull(handler.convertCsvValueToDbType("t1", "c1", " "));
    }

    @Test
    void convertCsvValueToDbType_正常ケース_nullを指定する_nullが返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        putTableMetaBySqlType(handler, "t1", "c1", Types.VARCHAR, "varchar");
        assertNull(handler.convertCsvValueToDbType("t1", "c1", null));
    }

    @Test
    void convertCsvValueToDbType_正常ケース_file参照を指定する_LOB内容が返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        putTableMetaBySqlType(handler, "t1", "blob_col", Types.BINARY, "bytea");
        Path filesDir = tempDir.resolve("dump").resolve("files");
        Files.createDirectories(filesDir);
        Files.write(filesDir.resolve("x.bin"), new byte[] {0x11, 0x12});
        Object actual = handler.convertCsvValueToDbType("t1", "blob_col", "file:x.bin");
        assertArrayEquals(new byte[] {0x11, 0x12}, (byte[]) actual);
    }

    @Test
    void convertCsvValueToDbType_正常ケース_xml型でfile参照を指定する_文字列として読み込まれること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        putTableMetaBySqlType(handler, "t1", "xml_col", Types.OTHER, "xml");
        Path filesDir = tempDir.resolve("dump").resolve("files");
        Files.createDirectories(filesDir);
        Files.writeString(filesDir.resolve("x.xml"), "<root><v>1</v></root>",
                StandardCharsets.UTF_8);

        Object actual = handler.convertCsvValueToDbType("t1", "xml_col", "file:x.xml");
        assertEquals("<root><v>1</v></root>", actual);
    }

    @Test
    void convertCsvValueToDbType_異常ケース_不正数値を指定する_DataSetExceptionが送出されること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        putTableMetaBySqlType(handler, "t1", "n1", Types.NUMERIC, "numeric");
        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("t1", "n1", "abc"));
    }

    @Test
    void convertCsvValueToDbType_異常ケース_不正hexを指定する_DataSetExceptionが送出されること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        putTableMetaBySqlType(handler, "t1", "blob_col", Types.BINARY, "bytea");
        assertThrows(DataSetException.class,
                () -> handler.convertCsvValueToDbType("t1", "blob_col", "XYZ"));
    }

    @Test
    void convertCsvValueToDbType_正常ケース_boolean別名値を指定する_期待真偽値へ変換されること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        putTableMetaBySqlType(handler, "t1", "b1", Types.OTHER, "bool");
        assertEquals(Boolean.TRUE, handler.convertCsvValueToDbType("t1", "b1", "true"));
        assertEquals(Boolean.TRUE, handler.convertCsvValueToDbType("t1", "b1", "1"));
        assertEquals(Boolean.TRUE, handler.convertCsvValueToDbType("t1", "b1", "t"));
        assertEquals(Boolean.FALSE, handler.convertCsvValueToDbType("t1", "b1", "false"));
        assertEquals(Boolean.FALSE, handler.convertCsvValueToDbType("t1", "b1", "0"));
        assertEquals(Boolean.FALSE, handler.convertCsvValueToDbType("t1", "b1", "f"));
    }

    @Test
    void convertCsvValueToDbType_正常ケース_型別名を指定する_数値型へ変換されること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        putTableMetaBySqlType(handler, "t1", "n1", Types.OTHER, "decimal");
        putTableMetaBySqlType(handler, "t1", "r1", Types.OTHER, "real");
        putTableMetaBySqlType(handler, "t1", "d1", Types.OTHER, "double precision");
        assertEquals("12.5", handler.convertCsvValueToDbType("t1", "n1", "12.5").toString());
        assertEquals(1.5f, handler.convertCsvValueToDbType("t1", "r1", "1.5"));
        assertEquals(2.5d, handler.convertCsvValueToDbType("t1", "d1", "2.5"));
    }

    @Test
    void convertCsvValueToDbType_正常ケース_日時型別名を指定する_日時型へ変換されること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        putTableMetaBySqlType(handler, "t1", "d1", Types.OTHER, "date");
        putTableMetaBySqlType(handler, "t1", "t1", Types.OTHER, "time");
        putTableMetaBySqlType(handler, "t1", "z1", Types.OTHER, "timestamptz");
        assertInstanceOf(java.sql.Date.class,
                handler.convertCsvValueToDbType("t1", "d1", "2026-02-15"));
        assertInstanceOf(Time.class, handler.convertCsvValueToDbType("t1", "t1", "01:02:03"));
        assertInstanceOf(Timestamp.class,
                handler.convertCsvValueToDbType("t1", "z1", "2026-02-15T01:02:03+09:00"));
    }

    @Test
    void convertCsvValueToDbType_正常ケース_YEAR型とVARBINARY型を指定する_期待値へ変換されること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        putTableMetaBySqlType(handler, "t1", "year_col", Types.DATE, "year");
        putTableMetaBySqlType(handler, "t1", "bin_col", Types.VARBINARY, "varbinary");

        assertEquals(2026, handler.convertCsvValueToDbType("t1", "year_col", "2026"));
        assertEquals("raw-bytes", handler.convertCsvValueToDbType("t1", "bin_col", "raw-bytes"));
    }

    @Test
    void convertCsvValueToDbType_正常ケース_BLOB型に16進文字列を指定する_byte配列へ変換されること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        putTableMetaBySqlType(handler, "t1", "blob_col", Types.BLOB, "blob");
        Object actual = handler.convertCsvValueToDbType("t1", "blob_col", "\\x4142");
        assertArrayEquals(new byte[] {0x41, 0x42}, (byte[]) actual);
    }

    @Test
    void hasNotNullLobColumn_正常ケース_NOTNULLLOB列が存在する_trueが返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
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
        MySqlDialectHandler handler = createHandler();
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
        MySqlDialectHandler handler = createHandler();
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
        MySqlDialectHandler handler = createHandler();
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
        MySqlDialectHandler handler = createHandler();
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
        MySqlDialectHandler handler = createHandler();
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
        MySqlDialectHandler handler = createHandler();
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
        MySqlDialectHandler handler = createHandler();
        Column[] cols = handler.getLobColumns(tempDir, "MISSING");
        assertEquals(0, cols.length);
    }

    @Test
    void getLobColumns_正常ケース_file参照がないCSVを指定する_空配列が返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        Path dir = tempDir.resolve("csv_no_file");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("TBL.csv"), "ID,C1\n1,text\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("table-ordering.txt"), "TBL\n", StandardCharsets.UTF_8);
        Column[] cols = handler.getLobColumns(dir, "TBL");
        assertEquals(0, cols.length);
    }

    @Test
    void logTableDefinition_正常ケース_列情報を取得する_例外なく完了すること() throws Exception {
        MySqlDialectHandler handler = createHandler();
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
    void private_正常ケース_型判定系を呼び出す_想定真偽値が返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        assertTrue((boolean) invokePrivate(handler, "isBooleanType",
                new Class<?>[] {int.class, String.class}, Types.BOOLEAN, "boolean"));
        assertTrue((boolean) invokePrivate(handler, "isBitType",
                new Class<?>[] {int.class, String.class}, Types.BIT, "bit"));
        assertTrue((boolean) invokePrivate(handler, "isIntegerType",
                new Class<?>[] {int.class, String.class}, Types.INTEGER, "int4"));
        assertTrue((boolean) invokePrivate(handler, "isBigIntType",
                new Class<?>[] {int.class, String.class}, Types.BIGINT, "int8"));
        assertTrue((boolean) invokePrivate(handler, "isNumericType",
                new Class<?>[] {int.class, String.class}, Types.NUMERIC, "numeric"));
        assertTrue((boolean) invokePrivate(handler, "isRealType",
                new Class<?>[] {int.class, String.class}, Types.REAL, "real"));
        assertTrue((boolean) invokePrivate(handler, "isDoubleType",
                new Class<?>[] {int.class, String.class}, Types.DOUBLE, "float8"));
        assertTrue((boolean) invokePrivate(handler, "isDateTimeType",
                new Class<?>[] {int.class, String.class}, Types.TIMESTAMP, "timestamp"));
        assertTrue((boolean) invokePrivate(handler, "isByteaType",
                new Class<?>[] {int.class, String.class}, Types.BINARY, "bytea"));
        assertTrue((boolean) invokePrivate(handler, "isLobType",
                new Class<?>[] {int.class, String.class}, Types.CLOB, "text"));
        assertTrue((boolean) invokePrivate(handler, "isUuidType",
                new Class<?>[] {int.class, String.class}, Types.OTHER, "uuid"));
    }

    @Test
    void private_正常ケース_補助メソッドを呼び出す_期待値が返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        assertTrue((boolean) invokePrivate(handler, "startsWithFileReference",
                new Class<?>[] {String.class}, "file:a.txt"));
        assertFalse((boolean) invokePrivate(handler, "startsWithFileReference",
                new Class<?>[] {String.class}, (Object) null));
        assertEquals("", invokePrivate(handler, "normalizeTypeName", new Class<?>[] {String.class},
                (Object) null));
        assertEquals("abc", invokePrivate(handler, "normalizeTypeName",
                new Class<?>[] {String.class}, "  AbC  "));
        assertEquals("2026-02-15T01:02:03+09:00",
                invokePrivate(handler, "normalizeOffsetNoColonToColon",
                        new Class<?>[] {String.class}, "2026-02-15T01:02:03+0900"));
    }

    @Test
    void private_正常ケース_parseBooleanを呼び出す_真偽値へ変換されること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        assertEquals(Boolean.TRUE,
                invokePrivate(handler, "parseBoolean", new Class<?>[] {String.class}, "1"));
        assertEquals(Boolean.FALSE,
                invokePrivate(handler, "parseBoolean", new Class<?>[] {String.class}, "f"));
        assertEquals(Boolean.FALSE,
                invokePrivate(handler, "parseBoolean", new Class<?>[] {String.class}, "other"));
    }

    @Test
    void private_正常ケース_parseBitValueを呼び出す_入力形式に応じた数値へ変換されること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        assertEquals(Integer.valueOf(1),
                invokePrivate(handler, "parseBitValue", new Class<?>[] {String.class}, "true"));
        assertEquals(Integer.valueOf(1),
                invokePrivate(handler, "parseBitValue", new Class<?>[] {String.class}, "t"));
        assertEquals(Integer.valueOf(0),
                invokePrivate(handler, "parseBitValue", new Class<?>[] {String.class}, "f"));
        assertEquals(Integer.valueOf(255),
                invokePrivate(handler, "parseBitValue", new Class<?>[] {String.class}, "11111111"));
        assertEquals(Integer.valueOf(10),
                invokePrivate(handler, "parseBitValue", new Class<?>[] {String.class}, "0x0A"));
        assertEquals(Integer.valueOf(10),
                invokePrivate(handler, "parseBitValue", new Class<?>[] {String.class}, "0X0A"));
        assertEquals(Integer.valueOf(7),
                invokePrivate(handler, "parseBitValue", new Class<?>[] {String.class}, "7"));
    }

    @Test
    void private_正常ケース_tryParse系を呼び出す_解析結果が返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        assertNotNull(invokePrivate(handler, "tryParseOffsetDateTime",
                new Class<?>[] {String.class}, "2026-02-15 01:02:03+0900"));
        assertNotNull(invokePrivate(handler, "tryParseLocalDateTime", new Class<?>[] {String.class},
                "2026-02-15 01:02:03.123"));
        assertNotNull(invokePrivate(handler, "tryParseLocalDate", new Class<?>[] {String.class},
                "2026/02/15"));
        assertNotNull(invokePrivate(handler, "tryParseLocalTime", new Class<?>[] {String.class},
                "010203"));
    }

    @Test
    void private_異常ケース_parseByteaで不正HEXを指定する_DataSetExceptionが送出されること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        Method m = MySqlDialectHandler.class.getDeclaredMethod("parseBytea", String.class,
                String.class, String.class);
        m.setAccessible(true);
        Exception ex = assertThrows(Exception.class, () -> m.invoke(handler, "xyz", "t1", "c1"));
        assertTrue(ex.getCause() instanceof DataSetException);
    }

    @Test
    void private_正常ケース_isUnknownDbUnitTypeを呼び出す_期待真偽値が返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        Method m = MySqlDialectHandler.class.getDeclaredMethod("isUnknownDbUnitType",
                DataType.class);
        m.setAccessible(true);
        DataType unknownName = mock(DataType.class);
        DataType otherType = mock(DataType.class);
        when(unknownName.getSqlType()).thenReturn(Types.VARCHAR);
        when(unknownName.getSqlTypeName()).thenReturn("UNKNOWN");
        when(otherType.getSqlType()).thenReturn(Types.OTHER);
        when(otherType.getSqlTypeName()).thenReturn("jsonb");
        assertTrue((boolean) m.invoke(handler, (Object) null));
        assertTrue((boolean) m.invoke(handler, DataType.UNKNOWN));
        assertTrue((boolean) m.invoke(handler, otherType));
        assertTrue((boolean) m.invoke(handler, unknownName));
        assertFalse((boolean) m.invoke(handler, DataType.VARCHAR));
    }

    @Test
    void publicApi_正常ケース_定数系メソッドを呼び出す_期待値が返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        assertTrue(handler.supportsLobStreamByStream());
        assertThrows(UnsupportedOperationException.class,
                () -> handler.getNextSequenceSql("seq_01"));
        assertEquals("", handler.getGeneratedKeyRetrievalSql());
        assertTrue(handler.supportsGetGeneratedKeys());
        assertFalse(handler.supportsSequences());
        assertTrue(handler.supportsIdentityColumns());
        assertEquals("TRUE", handler.getBooleanTrueLiteral());
        assertEquals("FALSE", handler.getBooleanFalseLiteral());
        assertEquals("CURRENT_TIMESTAMP", handler.getCurrentTimestampFunction());
        assertTrue(handler.supportsBatchUpdates());
    }

    @Test
    void constructor_正常ケース_getSchemaで例外が発生する_catalogで初期化されること() throws Exception {
        OracleDateTimeFormatUtil formatter = mock(OracleDateTimeFormatUtil.class);
        DbUnitConfigFactory configFactory = mock(DbUnitConfigFactory.class);
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
        IDataSet dataSet = mock(IDataSet.class);

        when(dbConn.getConnection()).thenReturn(jdbc);
        when(jdbc.getSchema()).thenThrow(new java.sql.SQLException("schema-error"));
        when(jdbc.getCatalog()).thenReturn("testdb");
        when(jdbc.getMetaData()).thenReturn(meta);
        when(meta.getTables(eq(null), eq("testdb"), eq("%"), any(String[].class)))
                .thenReturn(rsTables);
        when(rsTables.next()).thenReturn(false);
        when(dbConn.createDataSet()).thenReturn(dataSet);

        MySqlDialectHandler handler = new MySqlDialectHandler(dbConn, dumpConfig,
                dbUnitConfig, configFactory, formatter, pathsConfig);
        assertNotNull(handler);
    }

    @Test
    void private_正常ケース_fetchTargetTablesを呼び出す_除外対象以外のテーブル一覧が返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        Method method = MySqlDialectHandler.class.getDeclaredMethod("fetchTargetTables",
                Connection.class, String.class, List.class);
        method.setAccessible(true);

        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getTables(null, "public", "%", new String[] {"TABLE"})).thenReturn(rs);
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getString("TABLE_NAME")).thenReturn("T1", "T2");

        @SuppressWarnings("unchecked")
        List<String> tables = (List<String>) method.invoke(handler, conn, "public", List.of("t2"));
        assertEquals(List.of("T1"), tables);
    }

    @Test
    void private_正常ケース_resolveColumnSpecを呼び出す_DBUnit未知型とJDBC型を優先して返すこと() throws Exception {
        MySqlDialectHandler handler = createHandler();
        putTableMetaBySqlType(handler, "t1", "c1", Types.OTHER, "unknown");

        Field jdbcMapField = MySqlDialectHandler.class.getDeclaredField("jdbcColumnSpecMap");
        jdbcMapField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> jdbcMap =
                (Map<String, Map<String, Object>>) jdbcMapField.get(handler);
        Class<?> specClass = Class
                .forName("io.github.yok.flexdblink.db.mysql.MySqlDialectHandler$JdbcColumnSpec");
        java.lang.reflect.Constructor<?> ctor =
                specClass.getDeclaredConstructor(int.class, String.class);
        ctor.setAccessible(true);
        Map<String, Object> byCol = new java.util.HashMap<>();
        byCol.put("c1", ctor.newInstance(Types.VARCHAR, "varchar"));
        jdbcMap.put("t1", byCol);

        Method method = MySqlDialectHandler.class.getDeclaredMethod("resolveColumnSpec",
                String.class, String.class);
        method.setAccessible(true);
        Object spec = method.invoke(handler, "t1", "c1");

        Field sqlTypeField = spec.getClass().getDeclaredField("sqlType");
        Field typeNameField = spec.getClass().getDeclaredField("typeName");
        sqlTypeField.setAccessible(true);
        typeNameField.setAccessible(true);
        assertEquals(Types.VARCHAR, sqlTypeField.getInt(spec));
        assertEquals("varchar", typeNameField.get(spec));
    }

    @Test
    void private_正常ケース_loadLobFromFileを呼び出す_text型ファイル参照が文字列で返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        Path lob = tempDir.resolve("dump").resolve("files").resolve("t.txt");
        Files.createDirectories(lob.getParent());
        Files.writeString(lob, "hello", StandardCharsets.UTF_8);

        Method method = MySqlDialectHandler.class.getDeclaredMethod("loadLobFromFile",
                String.class, String.class, String.class, int.class, String.class, DataType.class);
        method.setAccessible(true);
        Object actual =
                method.invoke(handler, "t.txt", "t1", "c1", Types.CLOB, "text", DataType.CLOB);
        assertEquals("hello", actual);
    }

    @Test
    void private_異常ケース_loadLobFromFileで非LOB型を指定する_DataSetExceptionが送出されること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        Path lob = tempDir.resolve("dump").resolve("files").resolve("n.txt");
        Files.createDirectories(lob.getParent());
        Files.writeString(lob, "x", StandardCharsets.UTF_8);

        Method method = MySqlDialectHandler.class.getDeclaredMethod("loadLobFromFile",
                String.class, String.class, String.class, int.class, String.class, DataType.class);
        method.setAccessible(true);
        Exception ex = assertThrows(Exception.class, () -> method.invoke(handler, "n.txt", "t1",
                "c1", Types.INTEGER, "int4", DataType.INTEGER));
        assertTrue(ex.getCause() instanceof DataSetException);
    }

    @Test
    void private_正常ケース_isTextLikeTypeと補助判定を呼び出す_分岐網羅した真偽値が返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();

        assertTrue((boolean) invokePrivate(handler, "isTextLikeType",
                new Class<?>[] {int.class, String.class, DataType.class}, Types.VARCHAR, "varchar",
                null));
        assertTrue((boolean) invokePrivate(handler, "isTextLikeType",
                new Class<?>[] {int.class, String.class, DataType.class}, Types.CLOB, "unknown",
                null));
        assertTrue((boolean) invokePrivate(handler, "isTextLikeType",
                new Class<?>[] {int.class, String.class, DataType.class}, Types.OTHER, "unknown",
                DataType.CLOB));
        assertFalse((boolean) invokePrivate(handler, "isTextLikeType",
                new Class<?>[] {int.class, String.class, DataType.class}, Types.INTEGER, "int4",
                null));

        assertTrue((boolean) invokePrivate(handler, "isUuidType",
                new Class<?>[] {int.class, String.class}, Types.OTHER, "uuid"));
        assertFalse((boolean) invokePrivate(handler, "isUuidType",
                new Class<?>[] {int.class, String.class}, Types.OTHER, "jsonb"));

        assertTrue((boolean) invokePrivate(handler, "isDateTimeType",
                new Class<?>[] {int.class, String.class}, Types.OTHER, "timestamp with time zone"));
        assertFalse((boolean) invokePrivate(handler, "isDateTimeType",
                new Class<?>[] {int.class, String.class}, Types.INTEGER, "int4"));
        assertFalse((boolean) invokePrivate(handler, "isLobType",
                new Class<?>[] {int.class, String.class}, Types.INTEGER, "int4"));
    }

    @Test
    void private_正常ケース_解析不能パターンを指定する_nullまたは入力値のまま返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        assertNull(invokePrivate(handler, "tryParseLocalDateTime", new Class<?>[] {String.class},
                "not-datetime"));
        assertEquals("abc", invokePrivate(handler, "normalizeOffsetNoColonToColon",
                new Class<?>[] {String.class}, "abc"));
        assertEquals("2026-02-15T01:02:03+09AB",
                invokePrivate(handler, "normalizeOffsetNoColonToColon",
                        new Class<?>[] {String.class}, "2026-02-15T01:02:03+09AB"));
    }

    @Test
    void private_正常ケース_resolveColumnSpecを呼び出す_DBUnit既知型が優先して返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        putTableMetaBySqlType(handler, "t1", "c1", Types.INTEGER, "int4");

        Method method = MySqlDialectHandler.class.getDeclaredMethod("resolveColumnSpec",
                String.class, String.class);
        method.setAccessible(true);
        Object spec = method.invoke(handler, "t1", "c1");

        Field sqlTypeField = spec.getClass().getDeclaredField("sqlType");
        Field typeNameField = spec.getClass().getDeclaredField("typeName");
        sqlTypeField.setAccessible(true);
        typeNameField.setAccessible(true);
        assertEquals(Types.INTEGER, sqlTypeField.getInt(spec));
        assertEquals("int4", typeNameField.get(spec));
    }

    @Test
    void private_正常ケース_resolveColumnSpecを呼び出す_DBUnit未登録でJDBC型が返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        putTableMetaBySqlType(handler, "t1", "other", Types.VARCHAR, "varchar");

        Field jdbcMapField = MySqlDialectHandler.class.getDeclaredField("jdbcColumnSpecMap");
        jdbcMapField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> jdbcMap =
                (Map<String, Map<String, Object>>) jdbcMapField.get(handler);
        Class<?> specClass = Class
                .forName("io.github.yok.flexdblink.db.mysql.MySqlDialectHandler$JdbcColumnSpec");
        java.lang.reflect.Constructor<?> ctor =
                specClass.getDeclaredConstructor(int.class, String.class);
        ctor.setAccessible(true);
        Map<String, Object> byCol = new java.util.HashMap<>();
        byCol.put("c1", ctor.newInstance(Types.BIGINT, "int8"));
        jdbcMap.put("t1", byCol);

        Method method = MySqlDialectHandler.class.getDeclaredMethod("resolveColumnSpec",
                String.class, String.class);
        method.setAccessible(true);
        Object spec = method.invoke(handler, "t1", "c1");

        Field sqlTypeField = spec.getClass().getDeclaredField("sqlType");
        Field typeNameField = spec.getClass().getDeclaredField("typeName");
        sqlTypeField.setAccessible(true);
        typeNameField.setAccessible(true);
        assertEquals(Types.BIGINT, sqlTypeField.getInt(spec));
        assertEquals("int8", typeNameField.get(spec));
    }

    @Test
    void private_正常ケース_resolveColumnSpecを呼び出す_DBUnit未知型ではJDBC型が優先されること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        putTableMetaBySqlType(handler, "t1", "c1", Types.OTHER, "unknown");

        Field jdbcMapField = MySqlDialectHandler.class.getDeclaredField("jdbcColumnSpecMap");
        jdbcMapField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> jdbcMap =
                (Map<String, Map<String, Object>>) jdbcMapField.get(handler);
        Class<?> specClass = Class
                .forName("io.github.yok.flexdblink.db.mysql.MySqlDialectHandler$JdbcColumnSpec");
        java.lang.reflect.Constructor<?> ctor =
                specClass.getDeclaredConstructor(int.class, String.class);
        ctor.setAccessible(true);
        Map<String, Object> byCol = new java.util.HashMap<>();
        byCol.put("c1", ctor.newInstance(Types.BIGINT, "int8"));
        jdbcMap.put("t1", byCol);

        Method method = MySqlDialectHandler.class.getDeclaredMethod("resolveColumnSpec",
                String.class, String.class);
        method.setAccessible(true);
        Object spec = method.invoke(handler, "t1", "c1");

        Field sqlTypeField = spec.getClass().getDeclaredField("sqlType");
        Field typeNameField = spec.getClass().getDeclaredField("typeName");
        sqlTypeField.setAccessible(true);
        typeNameField.setAccessible(true);
        assertEquals(Types.BIGINT, sqlTypeField.getInt(spec));
        assertEquals("int8", typeNameField.get(spec));
    }

    @Test
    void private_正常ケース_resolveColumnSpecを呼び出す_DBUnit既知型かつJDBC情報ありではDBUnit型が優先されること()
            throws Exception {
        MySqlDialectHandler handler = createHandler();
        putTableMetaBySqlType(handler, "t1", "c1", Types.INTEGER, "int4");

        Field jdbcMapField = MySqlDialectHandler.class.getDeclaredField("jdbcColumnSpecMap");
        jdbcMapField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> jdbcMap =
                (Map<String, Map<String, Object>>) jdbcMapField.get(handler);
        Class<?> specClass = Class
                .forName("io.github.yok.flexdblink.db.mysql.MySqlDialectHandler$JdbcColumnSpec");
        java.lang.reflect.Constructor<?> ctor =
                specClass.getDeclaredConstructor(int.class, String.class);
        ctor.setAccessible(true);
        Map<String, Object> byCol = new java.util.HashMap<>();
        byCol.put("c1", ctor.newInstance(Types.BIGINT, "int8"));
        jdbcMap.put("t1", byCol);

        Method method = MySqlDialectHandler.class.getDeclaredMethod("resolveColumnSpec",
                String.class, String.class);
        method.setAccessible(true);
        Object spec = method.invoke(handler, "t1", "c1");

        Field sqlTypeField = spec.getClass().getDeclaredField("sqlType");
        Field typeNameField = spec.getClass().getDeclaredField("typeName");
        sqlTypeField.setAccessible(true);
        typeNameField.setAccessible(true);
        assertEquals(Types.INTEGER, sqlTypeField.getInt(spec));
        assertEquals("int4", typeNameField.get(spec));
    }

    @Test
    void private_異常ケース_resolveColumnSpecを呼び出す_メタ情報未解決でDataSetExceptionが送出されること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        Method method = MySqlDialectHandler.class.getDeclaredMethod("resolveColumnSpec",
                String.class, String.class);
        method.setAccessible(true);

        Exception ex = assertThrows(Exception.class, () -> method.invoke(handler, "unknown", "c1"));
        assertTrue(ex.getCause() instanceof DataSetException);
    }

    @Test
    void private_異常ケース_resolveColumnSpecを呼び出す_既存テーブルの未知列でDataSetExceptionが送出されること()
            throws Exception {
        MySqlDialectHandler handler = createHandler();
        putTableMetaBySqlType(handler, "t1", "known", Types.INTEGER, "int4");
        Method method = MySqlDialectHandler.class.getDeclaredMethod("resolveColumnSpec",
                String.class, String.class);
        method.setAccessible(true);

        Exception ex = assertThrows(Exception.class, () -> method.invoke(handler, "t1", "unknown"));
        assertTrue(ex.getCause() instanceof DataSetException);
    }

    @Test
    void private_異常ケース_findColumnInDbUnitMetaを呼び出す_テーブルメタなしでDataSetExceptionが送出されること()
            throws Exception {
        MySqlDialectHandler handler = createHandler();
        Method method = MySqlDialectHandler.class.getDeclaredMethod("findColumnInDbUnitMeta",
                String.class, String.class);
        method.setAccessible(true);

        Exception ex = assertThrows(Exception.class, () -> method.invoke(handler, "missing", "c1"));
        assertTrue(ex.getCause() instanceof DataSetException);
    }

    @Test
    void private_正常ケース_findColumnInDbUnitMetaを呼び出す_列未存在でnullが返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        putTableMetaBySqlType(handler, "t1", "c1", Types.INTEGER, "int4");
        Method method = MySqlDialectHandler.class.getDeclaredMethod("findColumnInDbUnitMeta",
                String.class, String.class);
        method.setAccessible(true);

        Object col = method.invoke(handler, "t1", "unknown");
        assertNull(col);
    }

    @Test
    void private_正常ケース_loadLobFromFileを呼び出す_bytea型ファイル参照がbyte配列で返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        Path lob = tempDir.resolve("dump").resolve("files").resolve("b.bin");
        Files.createDirectories(lob.getParent());
        Files.write(lob, new byte[] {0x01, 0x02});

        Method method = MySqlDialectHandler.class.getDeclaredMethod("loadLobFromFile",
                String.class, String.class, String.class, int.class, String.class, DataType.class);
        method.setAccessible(true);
        Object actual =
                method.invoke(handler, "b.bin", "t1", "c1", Types.BINARY, "bytea", DataType.BINARY);
        assertArrayEquals(new byte[] {0x01, 0x02}, (byte[]) actual);
    }

    @Test
    void private_異常ケース_loadLobFromFileで存在しないファイルを指定する_DataSetExceptionが送出されること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        Method method = MySqlDialectHandler.class.getDeclaredMethod("loadLobFromFile",
                String.class, String.class, String.class, int.class, String.class, DataType.class);
        method.setAccessible(true);

        Exception ex = assertThrows(Exception.class, () -> method.invoke(handler, "missing.bin",
                "t1", "c1", Types.BINARY, "bytea", DataType.BINARY));
        assertTrue(ex.getCause() instanceof DataSetException);
    }

    @Test
    void private_異常ケース_loadLobFromFileでディレクトリを指定する_DataSetExceptionが送出されること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        Path dir = tempDir.resolve("dump").resolve("files").resolve("as_dir");
        Files.createDirectories(dir);

        Method method = MySqlDialectHandler.class.getDeclaredMethod("loadLobFromFile",
                String.class, String.class, String.class, int.class, String.class, DataType.class);
        method.setAccessible(true);

        Exception ex = assertThrows(Exception.class, () -> method.invoke(handler, "as_dir", "t1",
                "c1", Types.CLOB, "text", DataType.CLOB));
        assertTrue(ex.getCause() instanceof DataSetException);
    }

    @Test
    void private_正常ケース_fetchTargetTablesを呼び出す_除外リストnullで全件返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        Method method = MySqlDialectHandler.class.getDeclaredMethod("fetchTargetTables",
                Connection.class, String.class, List.class);
        method.setAccessible(true);

        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getTables(null, "public", "%", new String[] {"TABLE"})).thenReturn(rs);
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getString("TABLE_NAME")).thenReturn("T1", "T2");

        @SuppressWarnings("unchecked")
        List<String> tables = (List<String>) method.invoke(handler, conn, "public", null);
        assertEquals(List.of("T1", "T2"), tables);
    }

    @Test
    void private_正常ケース_型名側判定分岐を呼び出す_期待真偽値が返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        assertTrue((boolean) invokePrivate(handler, "isBooleanType",
                new Class<?>[] {int.class, String.class}, Types.INTEGER, "bool"));
        assertTrue((boolean) invokePrivate(handler, "isIntegerType",
                new Class<?>[] {int.class, String.class}, Types.BIGINT, "integer"));
        assertTrue((boolean) invokePrivate(handler, "isBigIntType",
                new Class<?>[] {int.class, String.class}, Types.INTEGER, "bigint"));
        assertTrue((boolean) invokePrivate(handler, "isNumericType",
                new Class<?>[] {int.class, String.class}, Types.INTEGER, "decimal"));
        assertTrue((boolean) invokePrivate(handler, "isRealType",
                new Class<?>[] {int.class, String.class}, Types.INTEGER, "real"));
        assertTrue((boolean) invokePrivate(handler, "isDoubleType",
                new Class<?>[] {int.class, String.class}, Types.INTEGER, "double precision"));
        assertTrue((boolean) invokePrivate(handler, "isDateTimeType",
                new Class<?>[] {int.class, String.class}, Types.INTEGER, "date"));
        assertTrue((boolean) invokePrivate(handler, "isDateTimeType",
                new Class<?>[] {int.class, String.class}, Types.INTEGER, "time"));
        assertTrue((boolean) invokePrivate(handler, "isDateTimeType",
                new Class<?>[] {int.class, String.class}, Types.INTEGER, "timestamptz"));
        assertTrue((boolean) invokePrivate(handler, "isByteaType",
                new Class<?>[] {int.class, String.class}, Types.INTEGER, "bytea"));
        assertTrue((boolean) invokePrivate(handler, "isLobType",
                new Class<?>[] {int.class, String.class}, Types.BLOB, "unknown"));
        assertFalse((boolean) invokePrivate(handler, "isTextLikeType",
                new Class<?>[] {int.class, String.class, DataType.class}, Types.INTEGER, "unknown",
                DataType.INTEGER));
    }

    @Test
    void private_正常ケース_型判定系の偽分岐を呼び出す_偽値が返ること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        assertFalse((boolean) invokePrivate(handler, "isBitType",
                new Class<?>[] {int.class, String.class}, Types.INTEGER, "int"));
        assertTrue((boolean) invokePrivate(handler, "isBitType",
                new Class<?>[] {int.class, String.class}, Types.INTEGER, "bit"));
        assertFalse((boolean) invokePrivate(handler, "isRealType",
                new Class<?>[] {int.class, String.class}, Types.INTEGER, "int"));
        assertTrue((boolean) invokePrivate(handler, "isRealType",
                new Class<?>[] {int.class, String.class}, Types.REAL, "int"));
        assertTrue((boolean) invokePrivate(handler, "isRealType",
                new Class<?>[] {int.class, String.class}, Types.FLOAT, "int"));
        assertFalse((boolean) invokePrivate(handler, "isByteaType",
                new Class<?>[] {int.class, String.class}, Types.INTEGER, "int"));
        assertTrue((boolean) invokePrivate(handler, "isByteaType",
                new Class<?>[] {int.class, String.class}, Types.INTEGER, "bit"));
        assertFalse((boolean) invokePrivate(handler, "isHexBinarySqlType",
                new Class<?>[] {int.class, String.class}, Types.BINARY, "bytea"));
        assertTrue((boolean) invokePrivate(handler, "isHexBinarySqlType",
                new Class<?>[] {int.class, String.class}, Types.BINARY, "binary"));
        assertTrue((boolean) invokePrivate(handler, "isHexBinarySqlType",
                new Class<?>[] {int.class, String.class}, Types.VARBINARY, "varbinary"));
        assertTrue((boolean) invokePrivate(handler, "isHexBinarySqlType",
                new Class<?>[] {int.class, String.class}, Types.LONGVARBINARY, "varbinary"));
        assertFalse((boolean) invokePrivate(handler, "isHexBinarySqlType",
                new Class<?>[] {int.class, String.class}, Types.INTEGER, "int"));
        assertTrue((boolean) invokePrivate(handler, "isYearType",
                new Class<?>[] {int.class, String.class, String.class}, Types.DATE, "date",
                "YEAR_COL"));
        assertFalse((boolean) invokePrivate(handler, "isYearType",
                new Class<?>[] {int.class, String.class, String.class}, Types.DATE, "date",
                "TS_COL"));
    }

    @Test
    void private_正常ケース_parseByteaを呼び出す_接頭辞付きHEXがデコードされること() throws Exception {
        MySqlDialectHandler handler = createHandler();
        Method method = MySqlDialectHandler.class.getDeclaredMethod("parseBytea", String.class,
                String.class, String.class);
        method.setAccessible(true);
        Object bytes = method.invoke(handler, "\\x4142", "t1", "c1");
        assertArrayEquals(new byte[] {0x41, 0x42}, (byte[]) bytes);
    }

    @Test
    void formatDbValueForCsv_正常ケース_日付時刻型を複数指定する_フォーマッタ結果が返ること() throws Exception {
        OracleDateTimeFormatUtil formatter = mock(OracleDateTimeFormatUtil.class);
        when(formatter.formatJdbcDateTime(eq("D_COL"), any(), eq(null))).thenReturn("d");
        when(formatter.formatJdbcDateTime(eq("ODT_COL"), any(), eq(null))).thenReturn("odt");
        MySqlDialectHandler handler =
                createHandler(formatter, mock(DbUnitConfigFactory.class));

        assertEquals("d", handler.formatDbValueForCsv("D_COL", Date.valueOf("2026-02-15")));
        assertEquals("odt", handler.formatDbValueForCsv("ODT_COL",
                OffsetDateTime.parse("2026-02-15T01:02:03+09:00")));
    }

    @Test
    void constructor_正常ケース_getSchema取得が失敗する_testdbフォールバックで初期化されること() throws Exception {
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
        when(jdbc.getSchema()).thenThrow(new java.sql.SQLException("schema-fail"));
        when(jdbc.getCatalog()).thenReturn(" ");
        when(jdbc.getMetaData()).thenReturn(meta);
        when(meta.getTables(eq(null), eq("testdb"), eq("%"), any(String[].class)))
                .thenReturn(rsTables);
        when(rsTables.next()).thenReturn(false);
        when(dbConn.createDataSet()).thenReturn(dataSet);

        MySqlDialectHandler handler = new MySqlDialectHandler(dbConn, dumpConfig, dbUnitConfig,
                mock(DbUnitConfigFactory.class), mock(OracleDateTimeFormatUtil.class), pathsConfig);
        assertNotNull(handler);
    }

    private MySqlDialectHandler createHandler() throws Exception {
        return createHandler(mock(OracleDateTimeFormatUtil.class), mock(DbUnitConfigFactory.class));
    }

    private MySqlDialectHandler createHandler(OracleDateTimeFormatUtil formatter,
            DbUnitConfigFactory configFactory) throws Exception {
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
        when(jdbc.getSchema()).thenReturn("public");
        when(jdbc.getMetaData()).thenReturn(meta);
        when(meta.getTables(eq(null), eq("public"), eq("%"), any(String[].class)))
                .thenReturn(rsTables);
        when(rsTables.next()).thenReturn(false);
        when(dbConn.createDataSet()).thenReturn(dataSet);

        return new MySqlDialectHandler(dbConn, dumpConfig, dbUnitConfig, configFactory,
                formatter, pathsConfig);
    }

    private void putTableMetaBySqlType(MySqlDialectHandler handler, String table,
            String column, int sqlType, String sqlTypeName) throws Exception {
        Column col = mock(Column.class);
        when(col.getColumnName()).thenReturn(column);
        DataType dt = mock(DataType.class);
        when(dt.getSqlType()).thenReturn(sqlType);
        when(dt.getSqlTypeName()).thenReturn(sqlTypeName);
        when(col.getDataType()).thenReturn(dt);

        Field f = MySqlDialectHandler.class.getDeclaredField("tableMetaMap");
        f.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, ITableMetaData> map = (Map<String, ITableMetaData>) f.get(handler);
        String tableKey = table.toLowerCase();
        ITableMetaData existing = map.get(tableKey);
        List<Column> mergedColumns = new ArrayList<>();
        if (existing != null) {
            mergedColumns.addAll(List.of(existing.getColumns()));
        }
        mergedColumns.add(col);

        ITableMetaData tableMetaData = mock(ITableMetaData.class);
        when(tableMetaData.getColumns()).thenReturn(mergedColumns.toArray(new Column[0]));
        map.put(tableKey, tableMetaData);
    }

    private Object invokePrivate(MySqlDialectHandler handler, String methodName,
            Class<?>[] argTypes, Object... args) throws Exception {
        Method m = MySqlDialectHandler.class.getDeclaredMethod(methodName, argTypes);
        m.setAccessible(true);
        return m.invoke(handler, args);
    }
}
