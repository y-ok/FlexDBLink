package io.github.yok.flexdblink.db.postgresql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLXML;
import java.sql.Timestamp;
import java.sql.Types;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.TypeCastException;
import org.junit.jupiter.api.Test;

class CustomPostgresqlDataTypeFactoryTest {

    @Test
    void createDataType_正常ケース_SQLXML型を指定する_SQLXML型が返ること() throws Exception {
        CustomPostgresqlDataTypeFactory factory = new CustomPostgresqlDataTypeFactory();
        DataType actual = factory.createDataType(Types.SQLXML, "xml");
        assertEquals(Types.SQLXML, actual.getSqlType());
    }

    @Test
    void createDataType_正常ケース_型名xmlを指定する_SQLXML型が返ること() throws Exception {
        CustomPostgresqlDataTypeFactory factory = new CustomPostgresqlDataTypeFactory();
        DataType actual = factory.createDataType(Types.OTHER, "xml");
        assertEquals(Types.SQLXML, actual.getSqlType());
    }

    @Test
    void createDataType_正常ケース_通常型を指定する_親実装の型が返ること() throws Exception {
        CustomPostgresqlDataTypeFactory factory = new CustomPostgresqlDataTypeFactory();
        DataType actual = factory.createDataType(Types.INTEGER, "int4");
        assertEquals(Types.INTEGER, actual.getSqlType());
    }

    @Test
    void createDataType_正常ケース_型名nullを指定する_親実装の型が返ること() throws Exception {
        CustomPostgresqlDataTypeFactory factory = new CustomPostgresqlDataTypeFactory();
        DataType actual = factory.createDataType(Types.INTEGER, null);
        assertEquals(Types.INTEGER, actual.getSqlType());
    }

    @Test
    void createDataType_正常ケース_TIMESTAMP_WITH_TIMEZONE型を指定する_TIMESTAMP型が返ること() throws Exception {
        CustomPostgresqlDataTypeFactory factory = new CustomPostgresqlDataTypeFactory();
        DataType actual = factory.createDataType(Types.TIMESTAMP_WITH_TIMEZONE, "ignored");
        assertEquals(Types.TIMESTAMP, actual.getSqlType());
    }

    @Test
    void createDataType_正常ケース_型名timestampを指定する_TIMESTAMP型が返ること() throws Exception {
        CustomPostgresqlDataTypeFactory factory = new CustomPostgresqlDataTypeFactory();
        DataType actual = factory.createDataType(Types.OTHER, "timestamp");
        assertEquals(Types.TIMESTAMP, actual.getSqlType());
    }

    @Test
    void createDataType_正常ケース_型名timestamptzを指定する_TIMESTAMP型が返ること() throws Exception {
        CustomPostgresqlDataTypeFactory factory = new CustomPostgresqlDataTypeFactory();
        DataType actual = factory.createDataType(Types.OTHER, "timestamptz");
        assertEquals(Types.TIMESTAMP, actual.getSqlType());
    }

    @Test
    void createDataType_正常ケース_型名timestampwithtimezoneを指定する_TIMESTAMP型が返ること() throws Exception {
        CustomPostgresqlDataTypeFactory factory = new CustomPostgresqlDataTypeFactory();
        DataType actual = factory.createDataType(Types.OTHER, "timestamp with time zone");
        assertEquals(Types.TIMESTAMP, actual.getSqlType());
    }

    @Test
    void createDataType_正常ケース_TIMESTAMP型に空文字を設定する_setNullが呼ばれること() throws Exception {
        CustomPostgresqlDataTypeFactory factory = new CustomPostgresqlDataTypeFactory();
        DataType timestampType = factory.createDataType(Types.TIMESTAMP, "timestamptz");
        PreparedStatement statement = mock(PreparedStatement.class);

        timestampType.setSqlValue("   ", 4, statement);

        verify(statement).setNull(4, Types.TIMESTAMP);
    }

    @Test
    void createDataType_正常ケース_TIMESTAMP型にnullを設定する_setNullが呼ばれること() throws Exception {
        CustomPostgresqlDataTypeFactory factory = new CustomPostgresqlDataTypeFactory();
        DataType timestampType = factory.createDataType(Types.TIMESTAMP, "timestamptz");
        PreparedStatement statement = mock(PreparedStatement.class);

        timestampType.setSqlValue(null, 9, statement);

        verify(statement).setNull(9, Types.TIMESTAMP);
    }

    @Test
    void createDataType_正常ケース_TIMESTAMP型にNO_VALUEを設定する_setNullが呼ばれること() throws Exception {
        CustomPostgresqlDataTypeFactory factory = new CustomPostgresqlDataTypeFactory();
        DataType timestampType = factory.createDataType(Types.TIMESTAMP, "timestamptz");
        PreparedStatement statement = mock(PreparedStatement.class);

        timestampType.setSqlValue(ITable.NO_VALUE, 5, statement);

        verify(statement).setNull(5, Types.TIMESTAMP);
    }

    @Test
    void createDataType_正常ケース_TIMESTAMP型にTimestampを設定する_setTimestampが呼ばれること() throws Exception {
        CustomPostgresqlDataTypeFactory factory = new CustomPostgresqlDataTypeFactory();
        DataType timestampType = factory.createDataType(Types.TIMESTAMP, "timestamptz");
        PreparedStatement statement = mock(PreparedStatement.class);
        Timestamp timestamp = Timestamp.valueOf("2026-03-12 00:00:00");

        timestampType.setSqlValue(timestamp, 6, statement);

        verify(statement).setTimestamp(6, timestamp);
    }

    @Test
    void createDataType_正常ケース_TIMESTAMP型に日時文字列を設定する_setTimestampが呼ばれること() throws Exception {
        CustomPostgresqlDataTypeFactory factory = new CustomPostgresqlDataTypeFactory();
        DataType timestampType = factory.createDataType(Types.TIMESTAMP, "timestamptz");
        PreparedStatement statement = mock(PreparedStatement.class);
        Timestamp timestamp = Timestamp.valueOf("2026-03-12 01:02:03");

        timestampType.setSqlValue("2026-03-12 01:02:03", 7, statement);

        verify(statement).setTimestamp(7, timestamp);
    }

    @Test
    void createDataType_異常ケース_TIMESTAMP型に不正文字列を設定する_TypeCastExceptionが送出されること() throws Exception {
        CustomPostgresqlDataTypeFactory factory = new CustomPostgresqlDataTypeFactory();
        DataType timestampType = factory.createDataType(Types.TIMESTAMP, "timestamptz");
        PreparedStatement statement = mock(PreparedStatement.class);

        assertThrows(TypeCastException.class,
                () -> timestampType.setSqlValue("bad-value", 8, statement));
    }

    @Test
    void createDataType_正常ケース_xml型のsetSqlValueを実行する_setSQLXMLが呼ばれること() throws Exception {
        CustomPostgresqlDataTypeFactory factory = new CustomPostgresqlDataTypeFactory();
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
        CustomPostgresqlDataTypeFactory factory = new CustomPostgresqlDataTypeFactory();
        DataType xmlType = factory.createDataType(Types.SQLXML, "xml");
        PreparedStatement statement = mock(PreparedStatement.class);

        xmlType.setSqlValue(null, 2, statement);

        verify(statement).setNull(2, Types.SQLXML);
    }

    @Test
    void createDataType_正常ケース_xml型でNO_VALUEを設定する_setNullが呼ばれること() throws Exception {
        CustomPostgresqlDataTypeFactory factory = new CustomPostgresqlDataTypeFactory();
        DataType xmlType = factory.createDataType(Types.SQLXML, "xml");
        PreparedStatement statement = mock(PreparedStatement.class);

        xmlType.setSqlValue(ITable.NO_VALUE, 3, statement);

        verify(statement).setNull(3, Types.SQLXML);
    }
}
