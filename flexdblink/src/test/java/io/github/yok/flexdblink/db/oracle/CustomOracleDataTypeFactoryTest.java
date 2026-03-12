package io.github.yok.flexdblink.db.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.OffsetDateTime;
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
    void createDataType_正常ケース_sqlTypeNameのみblob小文字を指定する_専用BlobDataTypeが返ること() throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();

        DataType actual = factory.createDataType(Types.OTHER, "blob");

        assertEquals(
                "io.github.yok.flexdblink.db.oracle.CustomOracleDataTypeFactory$SafeOracleBlobDataType",
                actual.getClass().getName());
    }

    @Test
    void createDataType_正常ケース_sqlTypeNameのみclob小文字を指定する_専用ClobDataTypeが返ること() throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();

        DataType actual = factory.createDataType(Types.OTHER, "clob");

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
    void createDataType_正常ケース_tstz型を指定する_専用TimestampWithTimeZoneDataTypeが返ること() throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();

        DataType actual =
                factory.createDataType(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP WITH TIME ZONE");

        assertEquals(Types.TIMESTAMP_WITH_TIMEZONE, actual.getSqlType());
        assertEquals("TIMESTAMP WITH TIME ZONE", actual.toString());
    }

    @Test
    void createDataType_正常ケース_tsltz型を指定する_専用TimestampWithLocalTimeZoneDataTypeが返ること()
            throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();

        DataType actual = factory.createDataType(-102, "TIMESTAMP WITH LOCAL TIME ZONE");

        assertEquals(-102, actual.getSqlType());
        assertEquals("TIMESTAMP WITH LOCAL TIME ZONE", actual.toString());
    }

    @Test
    void createDataType_正常ケース_timestamp型を指定する_親実装のTimestamp型が返ること() throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();

        DataType actual = factory.createDataType(Types.TIMESTAMP, "TIMESTAMP");

        assertEquals(Types.TIMESTAMP, actual.getSqlType());
    }

    @Test
    void createDataType_正常ケース_localを含む曖昧なtstz名を指定する_親実装のTimestamp型が返ること() throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();

        DataType actual = factory.createDataType(Types.TIMESTAMP, "TIMESTAMP WITH TIME ZONE LOCAL");

        assertEquals(Types.TIMESTAMP, actual.getSqlType());
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
    void timestampWithTimeZoneDataType_setSqlValue_正常ケース_OffsetDateTimeを指定する_TIMESTAMPTZが設定されること()
            throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();
        DataType tstzType =
                factory.createDataType(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP WITH TIME ZONE");
        PreparedStatement statement = mock(PreparedStatement.class);
        OffsetDateTime value = OffsetDateTime.parse("2026-02-15T01:02:03+09:00");

        tstzType.setSqlValue(value, 1, statement);

        org.mockito.ArgumentCaptor<Object> captor =
                org.mockito.ArgumentCaptor.forClass(Object.class);
        verify(statement).setObject(eq(1), captor.capture());
        assertEquals(value, captor.getValue());
    }

    @Test
    void timestampWithTimeZoneDataType_typeCast_正常ケース_nullを指定する_nullが返ること() throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();
        DataType tstzType =
                factory.createDataType(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP WITH TIME ZONE");

        assertEquals(null, tstzType.typeCast(null));
        assertEquals(null, tstzType.typeCast(ITable.NO_VALUE));
    }

    @Test
    void timestampWithTimeZoneDataType_typeCast_正常ケース_OffsetDateTimeを指定する_同じ値が返ること()
            throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();
        DataType tstzType =
                factory.createDataType(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP WITH TIME ZONE");
        OffsetDateTime value = OffsetDateTime.parse("2026-02-15T01:02:03+09:00");

        assertEquals(value, tstzType.typeCast(value));
    }

    @Test
    void timestampWithTimeZoneDataType_isDateTime_正常ケース_判定する_trueが返ること() throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();
        DataType tstzType =
                factory.createDataType(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP WITH TIME ZONE");

        assertEquals(true, tstzType.isDateTime());
    }

    @Test
    void timestampWithTimeZoneDataType_setSqlValue_正常ケース_nullを指定する_setNullが呼ばれること()
            throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();
        DataType tstzType =
                factory.createDataType(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP WITH TIME ZONE");
        PreparedStatement statement = mock(PreparedStatement.class);

        tstzType.setSqlValue(null, 1, statement);

        verify(statement).setNull(1, Types.TIMESTAMP_WITH_TIMEZONE);
    }

    @Test
    void timestampWithTimeZoneDataType_setSqlValue_正常ケース_NO_VALUEを指定する_setNullが呼ばれること()
            throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();
        DataType tstzType =
                factory.createDataType(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP WITH TIME ZONE");
        PreparedStatement statement = mock(PreparedStatement.class);

        tstzType.setSqlValue(ITable.NO_VALUE, 1, statement);

        verify(statement).setNull(1, Types.TIMESTAMP_WITH_TIMEZONE);
    }

    @Test
    void timestampWithTimeZoneDataType_setSqlValue_正常ケース_Timestampを指定する_setObjectが呼ばれること()
            throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();
        DataType tstzType =
                factory.createDataType(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP WITH TIME ZONE");
        PreparedStatement statement = mock(PreparedStatement.class);
        Timestamp value = Timestamp.valueOf("2026-02-15 01:02:03");

        tstzType.setSqlValue(value, 1, statement);

        verify(statement).setObject(1, value);
    }

    @Test
    void timestampWithTimeZoneDataType_setSqlValue_正常ケース_Dateを指定する_setObjectが呼ばれること()
            throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();
        DataType tstzType =
                factory.createDataType(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP WITH TIME ZONE");
        PreparedStatement statement = mock(PreparedStatement.class);
        java.util.Date value =
                new java.util.Date(Timestamp.valueOf("2026-02-15 01:02:03").getTime());

        tstzType.setSqlValue(value, 1, statement);

        verify(statement).setObject(1, value);
    }

    @Test
    void timestampWithTimeZoneDataType_setSqlValue_正常ケース_Stringを指定する_setObjectが呼ばれること()
            throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();
        DataType tstzType =
                factory.createDataType(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP WITH TIME ZONE");
        PreparedStatement statement = mock(PreparedStatement.class);

        tstzType.setSqlValue("2026-02-15 01:02:03 +0900", 1, statement);

        verify(statement).setObject(1, "2026-02-15 01:02:03 +0900");
    }

    @Test
    void timestampWithTimeZoneDataType_setSqlValue_異常ケース_非対応型を指定する_TypeCastExceptionが送出されること()
            throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();
        DataType tstzType =
                factory.createDataType(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP WITH TIME ZONE");
        PreparedStatement statement = mock(PreparedStatement.class);

        assertThrows(org.dbunit.dataset.datatype.TypeCastException.class,
                () -> tstzType.setSqlValue(123, 1, statement));
    }

    @Test
    void timestampWithLocalTimeZoneDataType_setSqlValue_正常ケース_nullを指定する_setNullが呼ばれること()
            throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();
        DataType tsltzType = factory.createDataType(-102, "TIMESTAMP WITH LOCAL TIME ZONE");
        PreparedStatement statement = mock(PreparedStatement.class);

        tsltzType.setSqlValue(null, 1, statement);

        verify(statement).setNull(1, -102);
    }

    @Test
    void timestampWithLocalTimeZoneDataType_setSqlValue_正常ケース_NO_VALUEを指定する_setNullが呼ばれること()
            throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();
        DataType tsltzType = factory.createDataType(-102, "TIMESTAMP WITH LOCAL TIME ZONE");
        PreparedStatement statement = mock(PreparedStatement.class);

        tsltzType.setSqlValue(ITable.NO_VALUE, 1, statement);

        verify(statement).setNull(1, -102);
    }

    @Test
    void timestampWithLocalTimeZoneDataType_setSqlValue_正常ケース_OffsetDateTimeを指定する_セッション時差基準Timestampが設定されること()
            throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();
        DataType tsltzType = factory.createDataType(-102, "TIMESTAMP WITH LOCAL TIME ZONE");
        PreparedStatement statement = mock(PreparedStatement.class);
        OffsetDateTime value = OffsetDateTime.parse("2026-02-15T01:02:03+09:00");

        tsltzType.setSqlValue(value, 1, statement);

        verify(statement).setTimestamp(1, Timestamp.valueOf("2026-02-15 01:02:03"));
    }

    @Test
    void timestampWithLocalTimeZoneDataType_typeCast_正常ケース_nullを指定する_nullが返ること() throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();
        DataType tsltzType = factory.createDataType(-102, "TIMESTAMP WITH LOCAL TIME ZONE");

        assertEquals(null, tsltzType.typeCast(null));
        assertEquals(null, tsltzType.typeCast(ITable.NO_VALUE));
    }

    @Test
    void timestampWithLocalTimeZoneDataType_isDateTime_正常ケース_判定する_trueが返ること() throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();
        DataType tsltzType = factory.createDataType(-102, "TIMESTAMP WITH LOCAL TIME ZONE");

        assertEquals(true, tsltzType.isDateTime());
    }

    @Test
    void timestampWithLocalTimeZoneDataType_setSqlValue_正常ケース_Timestampを指定する_setTimestampが呼ばれること()
            throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();
        DataType tsltzType = factory.createDataType(-102, "TIMESTAMP WITH LOCAL TIME ZONE");
        PreparedStatement statement = mock(PreparedStatement.class);
        Timestamp value = Timestamp.valueOf("2026-02-15 01:02:03");

        tsltzType.setSqlValue(value, 1, statement);

        verify(statement).setTimestamp(1, value);
    }

    @Test
    void timestampWithLocalTimeZoneDataType_setSqlValue_正常ケース_Dateを指定する_setTimestampが呼ばれること()
            throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();
        DataType tsltzType = factory.createDataType(-102, "TIMESTAMP WITH LOCAL TIME ZONE");
        PreparedStatement statement = mock(PreparedStatement.class);
        java.util.Date value =
                new java.util.Date(Timestamp.valueOf("2026-02-15 01:02:03").getTime());

        tsltzType.setSqlValue(value, 1, statement);

        verify(statement).setTimestamp(1, new Timestamp(value.getTime()));
    }

    @Test
    void timestampWithLocalTimeZoneDataType_setSqlValue_正常ケース_Stringを指定する_setObjectが呼ばれること()
            throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();
        DataType tsltzType = factory.createDataType(-102, "TIMESTAMP WITH LOCAL TIME ZONE");
        PreparedStatement statement = mock(PreparedStatement.class);

        tsltzType.setSqlValue("2026-02-15 01:02:03", 1, statement);

        verify(statement).setObject(1, "2026-02-15 01:02:03");
    }

    @Test
    void timestampWithLocalTimeZoneDataType_setSqlValue_異常ケース_非対応型を指定する_TypeCastExceptionが送出されること()
            throws Exception {
        CustomOracleDataTypeFactory factory = new CustomOracleDataTypeFactory();
        DataType tsltzType = factory.createDataType(-102, "TIMESTAMP WITH LOCAL TIME ZONE");
        PreparedStatement statement = mock(PreparedStatement.class);

        assertThrows(org.dbunit.dataset.datatype.TypeCastException.class,
                () -> tsltzType.setSqlValue(123, 1, statement));
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

}
