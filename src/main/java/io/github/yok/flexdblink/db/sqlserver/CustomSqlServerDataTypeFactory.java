package io.github.yok.flexdblink.db.sqlserver;

import java.sql.Types;
import java.util.Set;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.DataTypeException;
import org.dbunit.ext.mssql.MsSqlDataTypeFactory;

/**
 * Custom {@link org.dbunit.dataset.datatype.IDataTypeFactory} implementation for SQL Server.
 *
 * <p>
 * This factory normalizes a few SQL Server specific type mappings used by FlexDBLink.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
public class CustomSqlServerDataTypeFactory extends MsSqlDataTypeFactory {

    private static final Set<Integer> BINARY_SQL_TYPES =
            Set.of(Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY);

    /**
     * Creates DBUnit data type for SQL Server.
     *
     * @param sqlType JDBC SQL type
     * @param sqlTypeName database type name
     * @return resolved DBUnit data type
     * @throws DataTypeException if mapping fails
     */
    @Override
    public DataType createDataType(int sqlType, String sqlTypeName) throws DataTypeException {
        if (sqlType == Types.SQLXML || isXmlTypeName(sqlTypeName)) {
            return DataType.VARCHAR;
        }
        if (BINARY_SQL_TYPES.contains(sqlType)) {
            return DataType.BINARY;
        }
        return super.createDataType(sqlType, sqlTypeName);
    }

    /**
     * Returns true when SQL type name represents XML.
     *
     * @param sqlTypeName database type name
     * @return true if XML
     */
    private boolean isXmlTypeName(String sqlTypeName) {
        if (sqlTypeName == null) {
            return false;
        }
        return "xml".equalsIgnoreCase(sqlTypeName);
    }
}
