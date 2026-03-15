package io.github.yok.flexdblink.db.oracle;

import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Locale;
import lombok.extern.slf4j.Slf4j;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.datatype.AbstractDataType;
import org.dbunit.dataset.datatype.BlobDataType;
import org.dbunit.dataset.datatype.ClobDataType;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.DataTypeException;
import org.dbunit.dataset.datatype.TypeCastException;
import org.dbunit.ext.oracle.OracleDataTypeFactory;

/**
 * Custom {@link org.dbunit.dataset.datatype.IDataTypeFactory} implementation for Oracle.
 *
 * <p>
 * <b>Goals</b>
 * </p>
 * <ul>
 * <li>Avoid {@link ClassCastException} caused by DBUnit’s default Oracle BLOB/CLOB implementation
 * attempting to cast to {@code oracle.jdbc.OracleConnection} when the connection is wrapped by a
 * pool (e.g., HikariCP’s {@code HikariProxyConnection}).</li>
 * <li>Insert BLOB/CLOB using <b>pure JDBC</b> only, without any Oracle-specific API (no
 * {@code oracle.sql.BLOB/CLOB}).</li>
 * <li>Keep mapping of INTERVAL YEAR TO MONTH / INTERVAL DAY TO SECOND to {@link DataType#VARCHAR},
 * same as the conventional behavior.</li>
 * </ul>
 *
 * <p>
 * <b>Approach</b>
 * </p>
 * <ul>
 * <li><b>BLOB:</b> use {@link PreparedStatement#setBytes(int, byte[])} (no InputStream).</li>
 * <li><b>CLOB:</b> {@link java.sql.Connection#createClob()} → {@link Clob#setString(long, String)}
 * → {@link PreparedStatement#setClob(int, Clob)} in that order.</li>
 * <li>Extend {@link OracleDataTypeFactory} and replace only BLOB/CLOB with safe implementations;
 * delegate all other types to the parent.</li>
 * </ul>
 *
 * <p>
 * <b>Exception policy</b>
 * </p>
 * <ul>
 * <li>In DBUnit 3.0.0, {@code setSqlValue} declares {@code throws SQLException, TypeCastException}.
 * This class strictly follows that signature and deliberately avoids code that could throw
 * {@code IOException} (e.g., try-with-resources on {@code InputStream}).</li>
 * </ul>
 *
 * @author Yasuharu.Okawauchi
 */
@Slf4j
public class CustomOracleDataTypeFactory extends OracleDataTypeFactory {
    private static final int ORACLE_TIMESTAMPLTZ_SQL_TYPE = -102;
    private static final DataType ORACLE_TIMESTAMPTZ_DATA_TYPE =
            new OracleTimestampWithTimeZoneDataType();
    private static final DataType ORACLE_TIMESTAMPLTZ_DATA_TYPE =
            new OracleTimestampWithLocalTimeZoneDataType();

    /**
     * {@inheritDoc}
     */
    @Override
    public DataType createDataType(int sqlType, String sqlTypeName) throws DataTypeException {
        // ---- Replace BLOB with a safe implementation (no InputStream; use setBytes) ----
        if (sqlType == Types.BLOB || "BLOB".equalsIgnoreCase(sqlTypeName)) {
            log.debug("Mapping Oracle BLOB -> SafeOracleBlobDataType (JDBC only, setBytes)");
            return new SafeOracleBlobDataType();
        }
        // ---- Replace CLOB with a safe implementation (use JDBC-standard Clob) ----
        if (sqlType == Types.CLOB || "CLOB".equalsIgnoreCase(sqlTypeName)) {
            log.debug("Mapping Oracle CLOB -> SafeOracleClobDataType (JDBC only, createClob)");
            return new SafeOracleClobDataType();
        }
        if (isTimestampWithTimeZoneType(sqlTypeName)) {
            log.debug("Mapping Oracle TIMESTAMP WITH TIME ZONE -> dedicated data type");
            return ORACLE_TIMESTAMPTZ_DATA_TYPE;
        }
        if (isTimestampWithLocalTimeZoneType(sqlTypeName)) {
            log.debug("Mapping Oracle TIMESTAMP WITH LOCAL TIME ZONE -> dedicated data type");
            return ORACLE_TIMESTAMPLTZ_DATA_TYPE;
        }

        // ---- Keep INTERVAL types mapped to VARCHAR (as per existing behavior) ----
        if (sqlTypeName != null) {
            // e.g., "INTERVAL YEAR TO MONTH"
            if (sqlTypeName.startsWith("INTERVAL YEAR")) {
                log.debug("Mapping Oracle INTERVAL YEAR TO MONTH -> VARCHAR");
                return DataType.VARCHAR;
            }
            // e.g., "INTERVAL DAY TO SECOND"
            if (sqlTypeName.startsWith("INTERVAL DAY")) {
                log.debug("Mapping Oracle INTERVAL DAY TO SECOND -> VARCHAR");
                return DataType.VARCHAR;
            }
        }

        // Delegate all others to the parent
        return super.createDataType(sqlType, sqlTypeName);
    }

    /**
     * {@link BlobDataType} derivative that sets BLOBs using pure JDBC without Oracle-specific APIs.
     *
     * <p>
     * <b>Accepted values</b>
     * </p>
     * <ul>
     * <li>{@code byte[]} (for CSV {@code file:...}, it is assumed that
     * {@code LobResolvingTableWrapper} or similar has already converted to {@code byte[]}).</li>
     * <li>{@code null} or {@link ITable#NO_VALUE} → bind {@code NULL}.</li>
     * </ul>
     *
     * <p>
     * <b>Implementation details</b>
     * </p>
     * <ul>
     * <li>Do <b>not</b> use {@code InputStream}; closing streams can throw {@code IOException},
     * which is not part of the declared throws clause.</li>
     * <li>JDBC drivers support {@link PreparedStatement#setBytes(int, byte[])} for BLOB columns, so
     * we use that to insert values.</li>
     * </ul>
     */
    private static final class SafeOracleBlobDataType extends BlobDataType {

        @Override
        public void setSqlValue(Object value, int column, PreparedStatement statement)
                throws SQLException, TypeCastException {
            // NULL/NO_VALUE → bind NULL
            if (value == null || value == ITable.NO_VALUE) {
                statement.setNull(column, Types.BLOB);
                return;
            }

            // Convert via BlobDataType to get byte[] (throws TypeCastException on failure)
            byte[] bytes = (byte[]) typeCast(value);

            // Use JDBC-standard setBytes (no IOException involved)
            statement.setBytes(column, bytes);
        }
    }

    /**
     * {@link ClobDataType} derivative that sets CLOBs using pure JDBC without Oracle-specific APIs.
     *
     * <p>
     * <b>Accepted values</b>
     * </p>
     * <ul>
     * <li>{@code String} (for CSV {@code file:...}, it is assumed conversion to String is done in
     * advance).</li>
     * <li>{@code null} or {@link ITable#NO_VALUE} → bind {@code NULL}.</li>
     * </ul>
     *
     * <p>
     * <b>Implementation details</b>
     * </p>
     * <ul>
     * <li>Create an empty CLOB with {@link java.sql.Connection#createClob()}, set content via
     * {@link Clob#setString(long, String)}, then bind with
     * {@link PreparedStatement#setClob(int, Clob)}.</li>
     * <li>Drivers differ in how they handle {@code free()}; for compatibility, this method does not
     * call it explicitly. Cleanup is expected on Statement/Connection close.</li>
     * </ul>
     */
    private static final class SafeOracleClobDataType extends ClobDataType {

        @Override
        public void setSqlValue(Object value, int column, PreparedStatement statement)
                throws SQLException, TypeCastException {
            // NULL/NO_VALUE → bind NULL
            if (value == null || value == ITable.NO_VALUE) {
                statement.setNull(column, Types.CLOB);
                return;
            }

            // Convert via ClobDataType to get String (throws TypeCastException on failure)
            String s = (String) typeCast(value);

            // JDBC 4.0 standard: Connection#createClob → setString → setClob
            Clob clob = statement.getConnection().createClob();
            try {
                clob.setString(1, s);
                statement.setClob(column, clob);
            } finally {
                // Most drivers clean up the Clob at Statement/Connection close.
                // For maximum compatibility, do not call free() explicitly here.
            }
        }
    }

    /**
     * Returns true when the type name represents Oracle TIMESTAMP WITH TIME ZONE.
     *
     * @param sqlTypeName database type name
     * @return true for Oracle TSTZ
     */
    private static boolean isTimestampWithTimeZoneType(String sqlTypeName) {
        if (sqlTypeName == null) {
            return false;
        }
        String normalized = sqlTypeName.toUpperCase(Locale.ROOT);
        return normalized.startsWith("TIMESTAMP") && normalized.contains("WITH TIME ZONE")
                && !normalized.contains("LOCAL");
    }

    /**
     * Returns true when the type name represents Oracle TIMESTAMP WITH LOCAL TIME ZONE.
     *
     * @param sqlTypeName database type name
     * @return true for Oracle TSLTZ
     */
    private static boolean isTimestampWithLocalTimeZoneType(String sqlTypeName) {
        if (sqlTypeName == null) {
            return false;
        }
        String normalized = sqlTypeName.toUpperCase(Locale.ROOT);
        return normalized.startsWith("TIMESTAMP") && normalized.contains("WITH LOCAL TIME ZONE");
    }

    /**
     * DBUnit datatype for Oracle TIMESTAMP WITH TIME ZONE.
     */
    private static final class OracleTimestampWithTimeZoneDataType extends AbstractDataType {

        /**
         * Creates the datatype.
         */
        private OracleTimestampWithTimeZoneDataType() {
            super("TIMESTAMP WITH TIME ZONE", Types.TIMESTAMP_WITH_TIMEZONE, OffsetDateTime.class,
                    false);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Object typeCast(Object value) throws TypeCastException {
            if (value == null || value == ITable.NO_VALUE) {
                return null;
            }
            return value;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isDateTime() {
            return true;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void setSqlValue(Object value, int column, PreparedStatement statement)
                throws SQLException, TypeCastException {
            if (value == null || value == ITable.NO_VALUE) {
                statement.setNull(column, Types.TIMESTAMP_WITH_TIMEZONE);
                return;
            }
            if (value instanceof OffsetDateTime || value instanceof Timestamp
                    || value instanceof java.util.Date || value instanceof String) {
                statement.setObject(column, value);
                return;
            }
            throw new TypeCastException(value, this);
        }
    }

    /**
     * DBUnit datatype for Oracle TIMESTAMP WITH LOCAL TIME ZONE.
     */
    private static final class OracleTimestampWithLocalTimeZoneDataType extends AbstractDataType {

        /**
         * Creates the datatype.
         */
        private OracleTimestampWithLocalTimeZoneDataType() {
            super("TIMESTAMP WITH LOCAL TIME ZONE", ORACLE_TIMESTAMPLTZ_SQL_TYPE, Timestamp.class,
                    false);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Object typeCast(Object value) throws TypeCastException {
            if (value == null || value == ITable.NO_VALUE) {
                return null;
            }
            if (value instanceof OffsetDateTime) {
                OffsetDateTime offsetValue = (OffsetDateTime) value;
                LocalDateTime localDateTime =
                        offsetValue.atZoneSameInstant(OracleTimeZoneSupport.SESSION_ZONE_OFFSET)
                                .toLocalDateTime();
                return Timestamp.valueOf(localDateTime);
            }
            return value;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isDateTime() {
            return true;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void setSqlValue(Object value, int column, PreparedStatement statement)
                throws SQLException, TypeCastException {
            if (value == null || value == ITable.NO_VALUE) {
                statement.setNull(column, ORACLE_TIMESTAMPLTZ_SQL_TYPE);
                return;
            }
            Object typedValue = typeCast(value);
            if (typedValue instanceof Timestamp) {
                statement.setTimestamp(column, (Timestamp) typedValue);
                return;
            }
            if (typedValue instanceof java.util.Date) {
                statement.setTimestamp(column,
                        new Timestamp(((java.util.Date) typedValue).getTime()));
                return;
            }
            if (typedValue instanceof String) {
                statement.setObject(column, typedValue);
                return;
            }
            throw new TypeCastException(value, this);
        }
    }
}
