package io.github.yok.flexdblink.db.postgresql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Timestamp;
import java.sql.Types;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.datatype.AbstractDataType;
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
    private static final DataType NULL_SAFE_TIMESTAMP_DATA_TYPE =
            new NullSafeTimestampDataType(Types.TIMESTAMP);

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
        if (isTimestampType(sqlType, sqlTypeName)) {
            return NULL_SAFE_TIMESTAMP_DATA_TYPE;
        }
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
     * Returns true when the SQL type represents a timestamp-like column.
     *
     * @param sqlType JDBC SQL type
     * @param sqlTypeName database type name
     * @return true if timestamp-like
     */
    private boolean isTimestampType(int sqlType, String sqlTypeName) {
        if (sqlType == Types.TIMESTAMP || sqlType == Types.TIMESTAMP_WITH_TIMEZONE) {
            return true;
        }
        if (sqlTypeName == null) {
            return false;
        }
        return "timestamp".equalsIgnoreCase(sqlTypeName)
                || "timestamptz".equalsIgnoreCase(sqlTypeName)
                || "timestamp with time zone".equalsIgnoreCase(sqlTypeName);
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

    /**
     * DBUnit datatype that treats blank timestamp strings as SQL NULL.
     */
    private static final class NullSafeTimestampDataType extends AbstractDataType {

        /**
         * Creates datatype for a timestamp-like SQL type.
         *
         * @param sqlType JDBC SQL type
         */
        private NullSafeTimestampDataType(int sqlType) {
            super("TIMESTAMP", sqlType, Timestamp.class, false);
        }

        @Override
        public Object typeCast(Object value) throws TypeCastException {
            if (value == null || value == ITable.NO_VALUE) {
                return null;
            }
            if (value instanceof Timestamp) {
                return value;
            }
            String text = DataType.asString(value).trim();
            if (text.isEmpty()) {
                return null;
            }
            try {
                return Timestamp.valueOf(text);
            } catch (IllegalArgumentException ex) {
                throw new TypeCastException("Unable to typecast value <" + value + "> of type <"
                        + value.getClass().getName() + "> to TIMESTAMP", ex);
            }
        }

        @Override
        public void setSqlValue(Object value, int column, PreparedStatement statement)
                throws SQLException, TypeCastException {
            Timestamp timestamp = (Timestamp) typeCast(value);
            if (timestamp == null) {
                statement.setNull(column, getSqlType());
                return;
            }
            statement.setTimestamp(column, timestamp);
        }
    }
}
