package io.github.yok.flexdblink.db.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLXML;
import java.sql.Types;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.datatype.DataType;
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
    void createDataType_正常ケース_BINARY型にHEX文字列を設定する_setBytesが呼ばれること() throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType binaryType = factory.createDataType(Types.BINARY, "binary");
        PreparedStatement statement = mock(PreparedStatement.class);

        binaryType.setSqlValue("31323334", 1, statement);

        verify(statement).setBytes(eq(1),
                argThat(v -> matches(v, new byte[] {0x31, 0x32, 0x33, 0x34})));
    }

    @Test
    void createDataType_正常ケース_VARBINARY型に0x接頭辞付きHEX文字列を設定する_setBytesが呼ばれること()
            throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType binaryType = factory.createDataType(Types.VARBINARY, "varbinary");
        PreparedStatement statement = mock(PreparedStatement.class);

        binaryType.setSqlValue("0x0A0B0C21", 2, statement);

        verify(statement).setBytes(eq(2),
                argThat(v -> matches(v, new byte[] {0x0A, 0x0B, 0x0C, 0x21})));
    }

    @Test
    void createDataType_正常ケース_BINARY型に非HEX文字列を設定する_UTF8バイトでsetBytesが呼ばれること()
            throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType binaryType = factory.createDataType(Types.BINARY, "binary");
        PreparedStatement statement = mock(PreparedStatement.class);

        binaryType.setSqlValue("text-bytes", 3, statement);

        verify(statement).setBytes(eq(3),
                argThat(v -> matches(v, "text-bytes".getBytes(java.nio.charset.StandardCharsets.UTF_8))));
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
    void createDataType_正常ケース_BINARY型にバックスラッシュx接頭辞付きHEXを設定する_setBytesが呼ばれること()
            throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType binaryType = factory.createDataType(Types.BINARY, "binary");
        PreparedStatement statement = mock(PreparedStatement.class);

        binaryType.setSqlValue("\\x414243", 7, statement);

        verify(statement).setBytes(eq(7),
                argThat(v -> matches(v, new byte[] {0x41, 0x42, 0x43})));
    }

    @Test
    void createDataType_正常ケース_BINARY型に奇数長HEX文字列を設定する_UTF8バイトでsetBytesが呼ばれること()
            throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType binaryType = factory.createDataType(Types.BINARY, "binary");
        PreparedStatement statement = mock(PreparedStatement.class);

        binaryType.setSqlValue("ABC", 8, statement);

        verify(statement).setBytes(eq(8),
                argThat(v -> matches(v, "ABC".getBytes(java.nio.charset.StandardCharsets.UTF_8))));
    }

    @Test
    void createDataType_正常ケース_BINARY型でHEXデコード例外が発生する_UTF8バイトでsetBytesが呼ばれること()
            throws Exception {
        CustomMySqlDataTypeFactory factory = new CustomMySqlDataTypeFactory();
        DataType binaryType = factory.createDataType(Types.BINARY, "binary");
        PreparedStatement statement = mock(PreparedStatement.class);

        try (MockedStatic<Hex> mockedHex = mockStatic(Hex.class)) {
            mockedHex.when(() -> Hex.decodeHex("4142")).thenThrow(new DecoderException("x"));
            binaryType.setSqlValue("4142", 9, statement);
        }

        verify(statement).setBytes(eq(9),
                argThat(v -> matches(v, "4142".getBytes(java.nio.charset.StandardCharsets.UTF_8))));
    }

    private static boolean matches(byte[] actual, byte[] expected) {
        return java.util.Arrays.equals(actual, expected);
    }
}
