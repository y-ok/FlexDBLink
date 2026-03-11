package io.github.yok.flexdblink.db.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLXML;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.TypeCastException;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class CustomMySqlDataTypeFactoryTest {

    @Test
    void createDataType_正常ケース_SQLXML型を指定する_SQLXML型が返ること() throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType actual = factory.createDataType(Types.SQLXML, "xml");
        assertEquals(Types.SQLXML, actual.getSqlType());
    }

    @Test
    void createDataType_正常ケース_型名xmlを指定する_SQLXML型が返ること() throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType actual = factory.createDataType(Types.OTHER, "xml");
        assertEquals(Types.SQLXML, actual.getSqlType());
    }

    @Test
    void createDataType_正常ケース_通常型を指定する_親実装の型が返ること() throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType actual = factory.createDataType(Types.INTEGER, "int4");
        assertEquals(Types.INTEGER, actual.getSqlType());
    }

    @Test
    void createDataType_正常ケース_year型を指定する_INTEGER型が返ること() throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType actual = factory.createDataType(Types.DATE, "year");
        assertEquals(Types.INTEGER, actual.getSqlType());
    }

    @Test
    void createDataType_正常ケース_型名nullを指定する_親実装の型が返ること() throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        assertThrows(NullPointerException.class, () -> factory.createDataType(Types.INTEGER, null));
    }

    @Test
    void createDataType_正常ケース_TIMESTAMP_WITH_TIMEZONE型を指定する_TIMESTAMP型が返ること() throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType actual = factory.createDataType(Types.TIMESTAMP_WITH_TIMEZONE, "ignored");
        assertEquals(Types.TIMESTAMP, actual.getSqlType());
    }

    @Test
    void createDataType_正常ケース_型名datetimeを指定する_TIMESTAMP型が返ること() throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType actual = factory.createDataType(Types.OTHER, "datetime");
        assertEquals(Types.TIMESTAMP, actual.getSqlType());
    }

    @Test
    void createDataType_正常ケース_型名timestampを指定する_TIMESTAMP型が返ること() throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType actual = factory.createDataType(Types.OTHER, "timestamp");
        assertEquals(Types.TIMESTAMP, actual.getSqlType());
    }

    @Test
    void createDataType_正常ケース_TIMESTAMP型に空文字を設定する_setNullが呼ばれること() throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType timestampType = factory.createDataType(Types.TIMESTAMP, "timestamp");
        PreparedStatement statement = mock(PreparedStatement.class);

        timestampType.setSqlValue("   ", 10, statement);

        verify(statement).setNull(10, Types.TIMESTAMP);
    }

    @Test
    void createDataType_正常ケース_TIMESTAMP型にnullを設定する_setNullが呼ばれること() throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType timestampType = factory.createDataType(Types.TIMESTAMP, "timestamp");
        PreparedStatement statement = mock(PreparedStatement.class);

        timestampType.setSqlValue(null, 16, statement);

        verify(statement).setNull(16, Types.TIMESTAMP);
    }

    @Test
    void createDataType_正常ケース_TIMESTAMP型にNO_VALUEを設定する_setNullが呼ばれること() throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType timestampType = factory.createDataType(Types.TIMESTAMP, "timestamp");
        PreparedStatement statement = mock(PreparedStatement.class);

        timestampType.setSqlValue(ITable.NO_VALUE, 11, statement);

        verify(statement).setNull(11, Types.TIMESTAMP);
    }

    @Test
    void createDataType_正常ケース_TIMESTAMP型にTimestampを設定する_setTimestampが呼ばれること() throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType timestampType = factory.createDataType(Types.TIMESTAMP, "timestamp");
        PreparedStatement statement = mock(PreparedStatement.class);
        Timestamp timestamp = Timestamp.valueOf("2026-03-12 00:00:00");

        timestampType.setSqlValue(timestamp, 12, statement);

        verify(statement).setTimestamp(12, timestamp);
    }

    @Test
    void createDataType_正常ケース_TIMESTAMP型に日時文字列を設定する_setTimestampが呼ばれること() throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType timestampType = factory.createDataType(Types.TIMESTAMP, "timestamp");
        PreparedStatement statement = mock(PreparedStatement.class);
        Timestamp timestamp = Timestamp.valueOf("2026-03-12 01:02:03");

        timestampType.setSqlValue("2026-03-12 01:02:03", 13, statement);

        verify(statement).setTimestamp(13, timestamp);
    }

    @Test
    void createDataType_異常ケース_TIMESTAMP型に不正文字列を設定する_TypeCastExceptionが送出されること() throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType timestampType = factory.createDataType(Types.TIMESTAMP, "timestamp");
        PreparedStatement statement = mock(PreparedStatement.class);

        assertThrows(TypeCastException.class,
                () -> timestampType.setSqlValue("bad-value", 14, statement));
    }

    @Test
    void createDataType_正常ケース_xml型のsetSqlValueを実行する_setSQLXMLが呼ばれること() throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType xmlType = factory.createDataType(Types.SQLXML, "xml");
        PreparedStatement statement = mock(PreparedStatement.class);
        Connection connection = mock(Connection.class);
        SQLXML sqlxml = mock(SQLXML.class);
        when(statement.getConnection()).thenReturn(connection);
        when(connection.createSQLXML()).thenReturn(sqlxml);

        xmlType.setSqlValue("<root/>", 1, statement);

        verify(sqlxml).setString("<root/>");
        verify(statement).setSQLXML(1, sqlxml);
    }

    @Test
    void createDataType_正常ケース_xml型でnullを設定する_setNullが呼ばれること() throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType xmlType = factory.createDataType(Types.SQLXML, "xml");
        PreparedStatement statement = mock(PreparedStatement.class);

        xmlType.setSqlValue(null, 2, statement);

        verify(statement).setNull(2, Types.SQLXML);
    }

    @Test
    void createDataType_正常ケース_xml型でNO_VALUEを設定する_setNullが呼ばれること() throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType xmlType = factory.createDataType(Types.SQLXML, "xml");
        PreparedStatement statement = mock(PreparedStatement.class);

        xmlType.setSqlValue(ITable.NO_VALUE, 3, statement);

        verify(statement).setNull(3, Types.SQLXML);
    }

    @Test
    void createDataType_正常ケース_BINARY型にbyte配列を設定する_setBytesが呼ばれること() throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType binaryType = factory.createDataType(Types.BINARY, "binary");
        PreparedStatement statement = mock(PreparedStatement.class);
        byte[] value = new byte[] {0x01, 0x02, 0x03};

        binaryType.setSqlValue(value, 15, statement);

        verify(statement).setBytes(15, value);
    }

    @Test
    void createDataType_正常ケース_BINARY型にHEX文字列を設定する_setBytesが呼ばれること() throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType binaryType = factory.createDataType(Types.BINARY, "binary");
        PreparedStatement statement = mock(PreparedStatement.class);

        binaryType.setSqlValue("31323334", 1, statement);

        verify(statement).setBytes(eq(1),
                argThat(v -> matches(v, new byte[] {0x31, 0x32, 0x33, 0x34})));
    }

    @Test
    void createDataType_正常ケース_VARBINARY型に0x接頭辞付きHEX文字列を設定する_setBytesが呼ばれること() throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType binaryType = factory.createDataType(Types.VARBINARY, "varbinary");
        PreparedStatement statement = mock(PreparedStatement.class);

        binaryType.setSqlValue("0x0A0B0C21", 2, statement);

        verify(statement).setBytes(eq(2),
                argThat(v -> matches(v, new byte[] {0x0A, 0x0B, 0x0C, 0x21})));
    }

    @Test
    void createDataType_正常ケース_BINARY型に非HEX文字列を設定する_UTF8バイトでsetBytesが呼ばれること() throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType binaryType = factory.createDataType(Types.BINARY, "binary");
        PreparedStatement statement = mock(PreparedStatement.class);

        binaryType.setSqlValue("text-bytes", 3, statement);

        verify(statement).setBytes(eq(3),
                argThat(v -> matches(v, "text-bytes".getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    void createDataType_正常ケース_BINARY型にnullを設定する_setNullが呼ばれること() throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType binaryType = factory.createDataType(Types.BINARY, "binary");
        PreparedStatement statement = mock(PreparedStatement.class);

        binaryType.setSqlValue(null, 4, statement);

        verify(statement).setNull(4, Types.BINARY);
    }

    @Test
    void createDataType_正常ケース_BINARY型にNO_VALUEを設定する_setNullが呼ばれること() throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType binaryType = factory.createDataType(Types.BINARY, "binary");
        PreparedStatement statement = mock(PreparedStatement.class);

        binaryType.setSqlValue(ITable.NO_VALUE, 5, statement);

        verify(statement).setNull(5, Types.BINARY);
    }

    @Test
    void createDataType_正常ケース_BINARY型に空文字を設定する_空バイトでsetBytesが呼ばれること() throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType binaryType = factory.createDataType(Types.BINARY, "binary");
        PreparedStatement statement = mock(PreparedStatement.class);

        binaryType.setSqlValue("   ", 6, statement);

        verify(statement).setBytes(eq(6), argThat(v -> matches(v, new byte[0])));
    }

    @Test
    void createDataType_正常ケース_BINARY型にバックスラッシュx接頭辞付きHEXを設定する_setBytesが呼ばれること() throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType binaryType = factory.createDataType(Types.BINARY, "binary");
        PreparedStatement statement = mock(PreparedStatement.class);

        binaryType.setSqlValue("\\x414243", 7, statement);

        verify(statement).setBytes(eq(7), argThat(v -> matches(v, new byte[] {0x41, 0x42, 0x43})));
    }

    @Test
    void createDataType_正常ケース_BINARY型に奇数長HEX文字列を設定する_UTF8バイトでsetBytesが呼ばれること() throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType binaryType = factory.createDataType(Types.BINARY, "binary");
        PreparedStatement statement = mock(PreparedStatement.class);

        binaryType.setSqlValue("ABC", 8, statement);

        verify(statement).setBytes(eq(8),
                argThat(v -> matches(v, "ABC".getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    void createDataType_正常ケース_BINARY型でHEXデコード例外が発生する_UTF8バイトでsetBytesが呼ばれること() throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType binaryType = factory.createDataType(Types.BINARY, "binary");
        PreparedStatement statement = mock(PreparedStatement.class);

        try (MockedStatic<Hex> mockedHex = mockStatic(Hex.class)) {
            mockedHex.when(() -> Hex.decodeHex("4142")).thenThrow(new DecoderException("x"));
            binaryType.setSqlValue("4142", 9, statement);
        }

        verify(statement).setBytes(eq(9),
                argThat(v -> matches(v, "4142".getBytes(StandardCharsets.UTF_8))));
    }

    /**
     * Compares two byte arrays for equality.
     *
     * @param actual actual byte array
     * @param expected expected byte array
     * @return true if both arrays have identical content
     */
    private static boolean matches(byte[] actual, byte[] expected) {
        return Arrays.equals(actual, expected);
    }
}
