package io.github.yok.flexdblink.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLXML;
import java.sql.Types;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.datatype.DataType;
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
