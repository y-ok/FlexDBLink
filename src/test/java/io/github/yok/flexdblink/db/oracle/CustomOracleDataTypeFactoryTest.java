package io.github.yok.flexdblink.db.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Types;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.datatype.DataType;
import org.junit.jupiter.api.Test;

class CustomOracleDataTypeFactoryTest {

    @Test
    void createDataType_正常ケース_blob型を指定する_専用BlobDataTypeが返ること() throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();
        DataType actual = factory.createDataType(Types.BLOB, "BLOB");
        assertEquals(
                "io.github.yok.flexdblink.db.oracle.CustomOracleDataTypeFactory$SafeOracleBlobDataType",
                actual.getClass().getName());
    }

    @Test
    void createDataType_正常ケース_clob型を指定する_専用ClobDataTypeが返ること() throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();
        DataType actual = factory.createDataType(Types.CLOB, "CLOB");
        assertEquals(
                "io.github.yok.flexdblink.db.oracle.CustomOracleDataTypeFactory$SafeOracleClobDataType",
                actual.getClass().getName());
    }

    @Test
    void createDataType_正常ケース_interval型を指定する_varchar型が返ること() throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();
        assertEquals(DataType.VARCHAR,
                factory.createDataType(Types.OTHER, "INTERVAL YEAR TO MONTH"));
        assertEquals(DataType.VARCHAR,
                factory.createDataType(Types.OTHER, "INTERVAL DAY TO SECOND"));
    }

    @Test
    void safeBlobDataType_setSqlValue_正常ケース_nullとNO_VALUEを指定する_setNullが呼ばれること() throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();
        DataType blobType = factory.createDataType(Types.BLOB, "BLOB");
        PreparedStatement statement = mock(PreparedStatement.class);

        blobType.setSqlValue(null, 1, statement);
        blobType.setSqlValue(ITable.NO_VALUE, 2, statement);

        verify(statement).setNull(1, Types.BLOB);
        verify(statement).setNull(2, Types.BLOB);
    }

    @Test
    void safeBlobDataType_setSqlValue_正常ケース_byte配列を指定する_setBytesが呼ばれること() throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();
        DataType blobType = factory.createDataType(Types.BLOB, "BLOB");
        PreparedStatement statement = mock(PreparedStatement.class);

        byte[] bytes = new byte[] {0x01, 0x02};
        blobType.setSqlValue(bytes, 1, statement);

        verify(statement).setBytes(1, bytes);
    }

    @Test
    void safeBlobDataType_setSqlValue_正常ケース_文字列値を指定する_変換後bytesでsetBytesが呼ばれること() throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();
        DataType blobType = factory.createDataType(Types.BLOB, "BLOB");
        PreparedStatement statement = mock(PreparedStatement.class);

        blobType.setSqlValue("x", 1, statement);
        verify(statement).setBytes(eq(1), any(byte[].class));
    }

    @Test
    void safeClobDataType_setSqlValue_正常ケース_nullとNO_VALUEを指定する_setNullが呼ばれること() throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();
        DataType clobType = factory.createDataType(Types.CLOB, "CLOB");
        PreparedStatement statement = mock(PreparedStatement.class);

        clobType.setSqlValue(null, 1, statement);
        clobType.setSqlValue(ITable.NO_VALUE, 2, statement);

        verify(statement).setNull(1, Types.CLOB);
        verify(statement).setNull(2, Types.CLOB);
    }

    @Test
    void safeClobDataType_setSqlValue_正常ケース_文字列を指定する_createClob経由でsetClobが呼ばれること()
            throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();
        DataType clobType = factory.createDataType(Types.CLOB, "CLOB");
        PreparedStatement statement = mock(PreparedStatement.class);
        Connection connection = mock(Connection.class);
        Clob clob = mock(Clob.class);
        when(statement.getConnection()).thenReturn(connection);
        when(connection.createClob()).thenReturn(clob);

        clobType.setSqlValue("abc", 1, statement);

        verify(clob).setString(1, "abc");
        verify(statement).setClob(1, clob);
    }

    @Test
    void createDataType_正常ケース_sqlTypeNameが小文字blobを指定する_専用BlobDataTypeが返ること() throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();
        DataType actual = factory.createDataType(Types.VARBINARY, "blob");
        assertEquals(
                "io.github.yok.flexdblink.db.oracle.CustomOracleDataTypeFactory$SafeOracleBlobDataType",
                actual.getClass().getName());
    }

    @Test
    void createDataType_正常ケース_sqlTypeNameが小文字clobを指定する_専用ClobDataTypeが返ること() throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();
        DataType actual = factory.createDataType(Types.VARCHAR, "clob");
        assertEquals(
                "io.github.yok.flexdblink.db.oracle.CustomOracleDataTypeFactory$SafeOracleClobDataType",
                actual.getClass().getName());
    }

    @Test
    void createDataType_異常ケース_sqlTypeNameがnullを指定する_NullPointerExceptionが送出されること()
            throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();
        assertThrows(NullPointerException.class, () -> factory.createDataType(Types.VARCHAR, null));
    }

    @Test
    void createDataType_正常ケース_通常型を指定する_親実装のDataTypeが返ること() throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();
        DataType actual = factory.createDataType(Types.INTEGER, "NUMBER");
        assertEquals(Types.INTEGER, actual.getSqlType());
    }

    @Test
    void equalsIgnoreCase_正常ケース_nullと値を比較する_falseが返ること() throws Exception {
        java.lang.reflect.Method method = CustomOracleDataTypeFactory.class
                .getDeclaredMethod("equalsIgnoreCase", String.class, String.class);
        method.setAccessible(true);

        boolean bothNull = (boolean) method.invoke(null, null, null);
        boolean leftNull = (boolean) method.invoke(null, null, "A");
        boolean equalIgnoreCase = (boolean) method.invoke(null, "AbC", "aBc");

        assertTrue(bothNull);
        assertFalse(leftNull);
        assertTrue(equalIgnoreCase);
    }
}
