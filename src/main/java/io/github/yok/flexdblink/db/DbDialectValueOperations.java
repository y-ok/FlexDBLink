package io.github.yok.flexdblink.db;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import org.dbunit.dataset.DataSetException;

/**
 * Value conversion and LOB operations for each database dialect.
 */
public interface DbDialectValueOperations {

    /**
     * Converts CSV value to JDBC-bindable object.
     *
     * @param table table name
     * @param column column name
     * @param value CSV value
     * @return JDBC-bindable object
     * @throws DataSetException if conversion fails
     */
    Object convertCsvValueToDbType(String table, String column, String value)
            throws DataSetException;

    /**
     * Formats JDBC value for CSV output.
     *
     * @param columnName column name
     * @param value JDBC value
     * @return CSV string value
     * @throws SQLException if formatting fails
     */
    String formatDbValueForCsv(String columnName, Object value) throws SQLException;

    /**
     * Writes LOB value to file.
     *
     * @param schema schema name
     * @param table table name
     * @param value LOB value
     * @param outputPath destination file path
     * @throws Exception if write fails
     */
    void writeLobFile(String schema, String table, Object value, Path outputPath) throws Exception;

    /**
     * Reads LOB file and converts it to JDBC-bindable object.
     *
     * @param fileRef file reference
     * @param table table name
     * @param column column name
     * @param baseDir dataset base directory
     * @return JDBC-bindable object
     * @throws IOException if read fails
     * @throws DataSetException if conversion fails
     */
    Object readLobFile(String fileRef, String table, String column, File baseDir)
            throws IOException, DataSetException;

    /**
     * Returns whether stream-based LOB operations are supported.
     *
     * @return true when supported
     */
    boolean supportsLobStreamByStream();

    /**
     * Formats temporal value for CSV.
     *
     * @param columnName column name
     * @param value JDBC temporal value
     * @param connection JDBC connection
     * @return formatted temporal text
     * @throws SQLException if formatting fails
     */
    String formatDateTimeColumn(String columnName, Object value, Connection connection)
            throws SQLException;

    /**
     * Returns whether raw temporal text should be used in dump.
     *
     * @param columnName column name
     * @param sqlType JDBC SQL type
     * @param sqlTypeName dialect SQL type name
     * @return true when raw temporal text should be used
     */
    default boolean shouldUseRawTemporalValueForDump(String columnName, int sqlType,
            String sqlTypeName) {
        return false;
    }

    /**
     * Normalizes raw temporal text for dump output.
     *
     * @param columnName column name
     * @param rawValue raw JDBC text
     * @return normalized value
     */
    default String normalizeRawTemporalValueForDump(String columnName, String rawValue) {
        if (rawValue == null) {
            return "";
        }
        return rawValue;
    }

    /**
     * Parses CSV temporal value.
     *
     * @param columnName column name
     * @param value CSV temporal value
     * @return JDBC-bindable temporal object
     * @throws Exception if parsing fails
     */
    Object parseDateTimeValue(String columnName, String value) throws Exception;

    /**
     * Returns whether SQL type is treated as datetime in dump.
     *
     * @param sqlType JDBC SQL type
     * @param sqlTypeName dialect SQL type name
     * @return true when treated as datetime
     */
    default boolean isDateTimeTypeForDump(int sqlType, String sqlTypeName) {
        return sqlType == Types.DATE || sqlType == Types.TIME || sqlType == Types.TIMESTAMP;
    }

    /**
     * Returns whether SQL type is treated as binary in dump.
     *
     * @param sqlType JDBC SQL type
     * @param sqlTypeName dialect SQL type name
     * @return true when treated as binary
     */
    default boolean isBinaryTypeForDump(int sqlType, String sqlTypeName) {
        return sqlType == Types.BINARY || sqlType == Types.VARBINARY
                || sqlType == Types.LONGVARBINARY;
    }

    /**
     * Returns whether row comparison should use raw DB value.
     *
     * @param sqlTypeName dialect SQL type name
     * @return true when raw comparison is required
     */
    default boolean shouldUseRawValueForComparison(String sqlTypeName) {
        return false;
    }

    /**
     * Normalizes row-comparison value.
     *
     * @param columnName column name
     * @param sqlTypeName dialect SQL type name
     * @param value value to normalize
     * @return normalized value
     */
    default String normalizeValueForComparison(String columnName, String sqlTypeName,
            String value) {
        return value;
    }
}
