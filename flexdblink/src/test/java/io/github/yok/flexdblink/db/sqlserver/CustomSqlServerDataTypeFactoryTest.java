package io.github.yok.flexdblink.db.sqlserver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.sql.Types;
import org.dbunit.dataset.datatype.DataType;
import org.junit.jupiter.api.Test;

class CustomSqlServerDataTypeFactoryTest {

    @Test
    void createDataType_正常ケース_sqlxml型を指定する_VARCHARが返ること() throws Exception {
        CustomSqlServerDataTypeFactory factory = new CustomSqlServerDataTypeFactory();
        DataType actual = factory.createDataType(Types.SQLXML, "xml");
        assertEquals(DataType.VARCHAR, actual);
    }

    @Test
    void createDataType_正常ケース_xml型名を指定する_VARCHARが返ること() throws Exception {
        CustomSqlServerDataTypeFactory factory = new CustomSqlServerDataTypeFactory();
        DataType actual = factory.createDataType(Types.VARCHAR, "XML");
        assertEquals(DataType.VARCHAR, actual);
    }

    @Test
    void createDataType_正常ケース_xml型名にnullを指定する_親クラスの型が返ること() throws Exception {
        CustomSqlServerDataTypeFactory factory = new CustomSqlServerDataTypeFactory();
        DataType actual = factory.createDataType(Types.VARCHAR, null);
        assertEquals(DataType.VARCHAR, actual);
    }

    @Test
    void createDataType_正常ケース_varbinary型を指定する_BYTESが返ること() throws Exception {
        CustomSqlServerDataTypeFactory factory = new CustomSqlServerDataTypeFactory();
        DataType actual = factory.createDataType(Types.VARBINARY, "varbinary");
        assertEquals(DataType.BINARY, actual);
    }

    @Test
    void createDataType_正常ケース_datetimeoffset型コードを指定する_VARCHARが返ること() throws Exception {
        CustomSqlServerDataTypeFactory factory = new CustomSqlServerDataTypeFactory();
        // -155 = microsoft.sql.Types.DATETIMEOFFSET
        DataType actual = factory.createDataType(-155, "datetimeoffset");
        assertEquals(DataType.VARCHAR, actual);
    }

    @Test
    void createDataType_正常ケース_datetimeoffset型名を指定する_VARCHARが返ること() throws Exception {
        CustomSqlServerDataTypeFactory factory = new CustomSqlServerDataTypeFactory();
        DataType actual = factory.createDataType(Types.OTHER, "DATETIMEOFFSET");
        assertEquals(DataType.VARCHAR, actual);
    }

    @Test
    void createDataType_正常ケース_通常型を指定する_親クラスの型が返ること() throws Exception {
        CustomSqlServerDataTypeFactory factory = new CustomSqlServerDataTypeFactory();
        DataType actual = factory.createDataType(Types.INTEGER, "int");
        assertEquals(DataType.INTEGER, actual);
    }
}
