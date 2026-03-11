package io.github.yok.flexdblink.db.mysql;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Set;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.datatype.AbstractDataType;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.DataTypeException;
import org.dbunit.dataset.datatype.StringDataType;
import org.dbunit.dataset.datatype.TypeCastException;
import org.dbunit.ext.mysql.MySqlDataTypeFactory;

/**
 * Custom {@link org.dbunit.dataset.datatype.IDataTypeFactory} implementation for MySQL.
 *
 * <p>
 * DBUnit's default MySQL factory may treat {@code xml} as unsupported and drop the column from
 * metadata. This implementation maps {@code xml}/{@link Types#SQLXML} to {@link DataType#VARCHAR}
 * so CSV load/dump can handle XML columns as plain text.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
public class CustomMySqlDataTypeFactory extends MySqlDataTypeFactory {
    private static final DataType XML_DATA_TYPE = new SqlXmlDataType();
    private static final DataType NULL_SAFE_TIMESTAMP_DATA_TYPE =
            new NullSafeTimestampDataType(Types.TIMESTAMP);
    private static final Set<Integer> HEX_BINARY_SQL_TYPES =
            Set.of(Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB);

    /**
     * Creates DBUnit data type for MySQL.
     *
     * @param sqlType JDBC SQL type
     * @param sqlTypeName database type name
     * @return resolved DBUnit data type
     * @throws DataTypeException if mapping fails
     */
    @Override
    public DataType createDataType(int sqlType, String sqlTypeName) throws DataTypeException {
        if (isYearTypeName(sqlTypeName)) {
            return DataType.INTEGER;
        }
        if (isTimestampType(sqlType, sqlTypeName)) {
            return NULL_SAFE_TIMESTAMP_DATA_TYPE;
        }
        if (HEX_BINARY_SQL_TYPES.contains(sqlType)) {
            return new HexBinaryDataType(sqlTypeName, sqlType);
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
     * Returns true when the SQL type name represents MySQL XML.
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
     * Returns true when the SQL type name represents MySQL YEAR.
     *
     * @param sqlTypeName database type name
     * @return true if YEAR type name
     */
    private boolean isYearTypeName(String sqlTypeName) {
        if (sqlTypeName == null) {
            return false;
        }
        return "year".equalsIgnoreCase(sqlTypeName);
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
                || "datetime".equalsIgnoreCase(sqlTypeName);
    }

    /**
     * DBUnit datatype implementation for MySQL XML.
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

    /**
     * DBUnit datatype for hex-friendly binary columns.
     */
    private static final class HexBinaryDataType extends StringDataType {

        /**
         * Creates datatype with given SQL type.
         *
         * @param sqlTypeName SQL type name
         * @param sqlType SQL type
         */
        private HexBinaryDataType(String sqlTypeName, int sqlType) {
            super(sqlTypeName, sqlType);
        }

        /**
         * Binds binary bytes. Raw {@code byte[]} values (from LOB file references) are bound
         * directly; hex strings are decoded before binding.
         *
         * @param value input value
         * @param column column index
         * @param statement prepared statement
         * @throws SQLException on JDBC errors
         * @throws TypeCastException when cast fails
         */
        @Override
        public void setSqlValue(Object value, int column, PreparedStatement statement)
                throws SQLException, TypeCastException {
            if (value == null || value == ITable.NO_VALUE) {
                statement.setNull(column, getSqlType());
                return;
            }

            if (value instanceof byte[]) {
                statement.setBytes(column, (byte[]) value);
                return;
            }

            String text = ((String) typeCast(value)).trim();
            if (text.isEmpty()) {
                statement.setBytes(column, new byte[0]);
                return;
            }
            statement.setBytes(column, parseBinary(text));
        }

        /**
         * Parses string to binary bytes.
         *
         * @param value raw input string
         * @return parsed bytes
         */
        private byte[] parseBinary(String value) {
            String normalized = value;
            if (normalized.startsWith("\\x") || normalized.startsWith("0x")) {
                normalized = normalized.substring(2);
            }

            if (isHex(normalized)) {
                try {
                    return Hex.decodeHex(normalized);
                } catch (DecoderException ignore) {
                    return value.getBytes(StandardCharsets.UTF_8);
                }
            }
            return value.getBytes(StandardCharsets.UTF_8);
        }

        /**
         * Returns true when string is even-length hex.
         *
         * @param value input string
         * @return true if hex text
         */
        private boolean isHex(String value) {
            if ((value.length() % 2) != 0) {
                return false;
            }
            return value.chars().allMatch(ch -> Character.digit(ch, 16) >= 0);
        }
    }
}
