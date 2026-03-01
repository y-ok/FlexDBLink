package io.github.yok.flexdblink.db.postgresql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Types;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.DataTypeException;
import org.dbunit.dataset.datatype.StringDataType;
import org.dbunit.dataset.datatype.TypeCastException;
import org.dbunit.ext.postgresql.PostgresqlDataTypeFactory;

/**
 * Custom {@link org.dbunit.dataset.datatype.IDataTypeFactory} implementation for PostgreSQL.
 *
 * <p>
 * DBUnit's default PostgreSQL factory may treat {@code xml} as unsupported and drop the column from
 * metadata. This implementation maps {@code xml}/{@link Types#SQLXML} to {@link DataType#VARCHAR}
 * so CSV load/dump can handle XML columns as plain text.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
public class CustomPostgresqlDataTypeFactory extends PostgresqlDataTypeFactory {
    private static final DataType XML_DATA_TYPE = new SqlXmlDataType();

    /**
     * Creates DBUnit data type for PostgreSQL.
     *
     * @param sqlType JDBC SQL type
     * @param sqlTypeName database type name
     * @return resolved DBUnit data type
     * @throws DataTypeException if mapping fails
     */
    @Override
    public DataType createDataType(int sqlType, String sqlTypeName) throws DataTypeException {
        if (sqlType == Types.SQLXML) {
            return XML_DATA_TYPE;
        }
        if (isXmlTypeName(sqlTypeName)) {
            return XML_DATA_TYPE;
        }
        return super.createDataType(sqlType, sqlTypeName);
    }

    /**
     * Returns true when the SQL type name represents PostgreSQL XML.
     *
     * @param sqlTypeName database type name
     * @return true if XML type name
     */
    private boolean isXmlTypeName(String sqlTypeName) {
        if (sqlTypeName == null) {
            return false;
        }
        return "xml".equalsIgnoreCase(sqlTypeName);
    }

    /**
     * DBUnit datatype implementation for PostgreSQL XML.
     */
    private static final class SqlXmlDataType extends StringDataType {

        /**
         * Creates SQLXML datatype.
         */
        private SqlXmlDataType() {
            super("SQLXML", Types.SQLXML);
        }

        /**
         * Sets SQLXML value with JDBC standard API.
         *
         * @param value value to bind
         * @param column column index
         * @param statement prepared statement
         * @throws SQLException when JDBC operation fails
         * @throws TypeCastException when value cannot be cast to String
         */
        @Override
        public void setSqlValue(Object value, int column, PreparedStatement statement)
                throws SQLException, TypeCastException {
            if (value == null || value == ITable.NO_VALUE) {
                statement.setNull(column, Types.SQLXML);
                return;
            }

            String xml = (String) typeCast(value);
            Connection connection = statement.getConnection();
            SQLXML sqlxml = connection.createSQLXML();
            sqlxml.setString(xml);
            statement.setSQLXML(column, sqlxml);
        }
    }
}
