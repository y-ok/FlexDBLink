package io.github.yok.flexdblink.db.sqlserver;

import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.db.DbDialectHandler;
import io.github.yok.flexdblink.db.DbUnitConfigFactory;
import io.github.yok.flexdblink.db.FlexibleDateTimeParsers;
import io.github.yok.flexdblink.util.DateTimeFormatSupport;
import io.github.yok.flexdblink.util.LobPathConstants;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileAttribute;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.IDataTypeFactory;

/**
 * SQL Server dialect handler.
 *
 * <p>
 * This implementation contains only SQL Server behavior and shared DB-agnostic routines.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
@Slf4j
public class SqlServerDialectHandler implements DbDialectHandler {

    private static final IDataTypeFactory SQLSERVER_TYPE_FACTORY =
            new CustomSqlServerDataTypeFactory();
    private static final Set<String> TEXT_LIKE_TYPE_NAMES =
            Set.of("char", "varchar", "nvarchar", "nchar", "text", "ntext", "xml");
    private static final Set<String> TEXT_LOB_TYPE_NAMES = Set.of("text", "ntext", "xml");
    private static final Set<String> BOOLEAN_TYPE_NAMES = Set.of("bit", "boolean", "bool");
    private static final Set<Integer> BOOLEAN_SQL_TYPES = Set.of(Types.BOOLEAN, Types.BIT);
    private static final Set<String> INTEGER_TYPE_NAMES =
            Set.of("tinyint", "smallint", "int", "integer");
    private static final Set<Integer> INTEGER_SQL_TYPES =
            Set.of(Types.INTEGER, Types.SMALLINT, Types.TINYINT);
    private static final Set<String> BIGINT_TYPE_NAMES = Set.of("bigint");
    private static final Set<String> NUMERIC_TYPE_NAMES = Set.of("numeric", "decimal", "money");
    private static final Set<String> FLOATING_POINT_TYPE_NAMES = Set.of("real", "float", "double");
    private static final Set<Integer> REAL_SQL_TYPES =
            Set.of(Types.REAL, Types.FLOAT, Types.DOUBLE);
    private static final Set<String> DATE_TIME_TYPE_NAMES = Set.of("date", "time", "datetime",
            "datetime2", "smalldatetime", "datetimeoffset", "timestamp");
    private static final Set<Integer> DATE_TIME_SQL_TYPES =
            Set.of(Types.DATE, Types.TIME, Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE);
    private static final Set<Integer> CLOB_SQL_TYPES = Set.of(Types.CLOB, Types.NCLOB);
    private static final Set<Integer> NUMERIC_SQL_TYPES = Set.of(Types.NUMERIC, Types.DECIMAL);
    private static final Set<String> BINARY_LIKE_TYPE_NAMES =
            Set.of("binary", "varbinary", "image", "rowversion", "timestamp", "bit");
    private static final Set<Integer> BINARY_SQL_TYPES =
            Set.of(Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB, Types.BIT);
    private static final Set<Integer> LOB_SQL_TYPES = Set.of(Types.CLOB, Types.BLOB, Types.NCLOB);
    private static final Set<Integer> UNKNOWN_DB_UNIT_SQL_TYPES = Set.of(Types.OTHER);
    private static final Set<String> UNKNOWN_DB_UNIT_TYPE_NAMES = Set.of("unknown");

    private final Path baseLobDir;
    private final DateTimeFormatSupport dateTimeFormatter;
    private final Map<String, Column[]> tableColumnsMap = new HashMap<>();
    private final Map<String, Map<String, JdbcColumnSpec>> jdbcColumnSpecMap = new HashMap<>();
    private final DbUnitConfigFactory configFactory;
    private final PathsConfig pathsConfig;

    /**
     * JDBC metadata snapshot for one column.
     */
    private static final class JdbcColumnSpec {
        private final int sqlType;
        private final String typeName;

        /**
         * Creates a column spec.
         *
         * @param sqlType JDBC SQL type
         * @param typeName database type name
         */
        private JdbcColumnSpec(int sqlType, String typeName) {
            this.sqlType = sqlType;
            this.typeName = typeName;
        }
    }

    /**
     * Resolved column type information used during CSV-to-DB conversion.
     */
    private static final class ResolvedColumnSpec {
        private final int sqlType;
        private final String typeName;

        /**
         * Creates a resolved column spec.
         *
         * @param sqlType JDBC SQL type
         * @param typeName database type name
         */
        private ResolvedColumnSpec(int sqlType, String typeName) {
            this.sqlType = sqlType;
            this.typeName = typeName;
        }
    }

    /**
     * Constructor. Caches table metadata from a DBUnit dataset while honoring exclusion settings.
     *
     * @param dbConn DBUnit connection
     * @param dumpConfig dump.exclude-tables configuration
     * @param dbUnitConfig dbunit configuration
     * @param configFactory DBUnit config factory
     * @param dateTimeFormatter date/time formatter utility
     * @param pathsConfig path settings
     * @throws Exception if metadata retrieval fails
     */
    public SqlServerDialectHandler(DatabaseConnection dbConn, DumpConfig dumpConfig,
            DbUnitConfig dbUnitConfig, DbUnitConfigFactory configFactory,
            DateTimeFormatSupport dateTimeFormatter, PathsConfig pathsConfig) throws Exception {
        this.configFactory = configFactory;
        this.dateTimeFormatter = dateTimeFormatter;
        this.pathsConfig = pathsConfig;

        Path dumpBase = Paths.get(pathsConfig.getDump());
        this.baseLobDir = dumpBase.resolve(LobPathConstants.DIRECTORY_NAME);

        Connection jdbcConn = dbConn.getConnection();
        String schema;
        try {
            schema = jdbcConn.getSchema();
        } catch (SQLException e) {
            schema = "dbo";
        }
        if (schema == null || schema.isBlank()) {
            schema = "dbo";
        }

        List<String> excludeTables = dumpConfig.getExcludeTables();
        List<String> targetTables = fetchTargetTables(jdbcConn, schema, excludeTables);

        IDataSet ds = dbConn.createDataSet();
        for (String tbl : targetTables) {
            tableColumnsMap.put(tbl.toLowerCase(Locale.ROOT),
                    ds.getTableMetaData(tbl).getColumns());
        }
        cacheJdbcColumnSpecs(jdbcConn, schema, targetTables);
    }

    /**
     * Returns SQL type name for a given schema/table/column.
     *
     * @param jdbc raw JDBC connection
     * @param schema schema name
     * @param table table name
     * @param column column name
     * @return SQL type name
     * @throws SQLException when metadata retrieval fails
     */
    @Override
    public String getColumnTypeName(Connection jdbc, String schema, String table, String column)
            throws SQLException {
        DatabaseMetaData meta = jdbc.getMetaData();
        try (ResultSet rs = meta.getColumns(null, schema, table, column)) {
            if (rs.next()) {
                return rs.getString("TYPE_NAME");
            } else {
                throw new SQLException(String.format("Column metadata not found: %s.%s.%s", schema,
                        table, column));
            }
        }
    }

    /**
     * Retrieves the list of primary-key column names for the specified table.
     *
     * @param conn JDBC connection
     * @param schema schema name
     * @param table table name
     * @return list of primary-key column names; empty if no PK
     * @throws SQLException on JDBC errors
     */
    @Override
    public List<String> getPrimaryKeyColumns(Connection conn, String schema, String table)
            throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        List<String> cols = new ArrayList<>();
        try (ResultSet rs = meta.getPrimaryKeys(null, schema, table)) {
            while (rs.next()) {
                cols.add(rs.getString("COLUMN_NAME"));
            }
        }
        return cols;
    }

    /**
     * Applies SQL Server session settings.
     *
     * @param connection JDBC connection
     * @throws SQLException on SQL errors
     */
    @Override
    public void prepareConnection(Connection connection) throws SQLException {
        try (Statement st = connection.createStatement()) {
            st.execute("SET LANGUAGE us_english");
            st.execute("SET DATEFORMAT ymd");
        }
    }

    /**
     * Converts a CSV value into a JDBC-bindable object.
     *
     * @param table table name
     * @param column column name
     * @param value CSV value
     * @return JDBC-bindable object
     * @throws DataSetException if conversion fails
     */
    @Override
    public Object convertCsvValueToDbType(String table, String column, String value)
            throws DataSetException {
        if (value == null) {
            return null;
        }
        String str = value.trim();
        if (str.isEmpty()) {
            return null;
        }

        ResolvedColumnSpec resolved = resolveColumnSpec(table, column);
        String typeName = normalizeTypeName(resolved.typeName);
        int sqlType = resolved.sqlType;

        if (startsWithFileReference(str)) {
            return loadLobFromFile(str.substring("file:".length()), table, column, sqlType,
                    typeName);
        }

        try {
            if (isBooleanType(sqlType, typeName)) {
                return parseBoolean(str);
            }
            if (isIntegerType(sqlType, typeName)) {
                return Integer.valueOf(str);
            }
            if (isBigIntType(sqlType, typeName)) {
                return Long.valueOf(str);
            }
            if (isNumericType(sqlType, typeName)) {
                return new BigDecimal(str);
            }
            if (isRealType(sqlType, typeName)) {
                return Double.valueOf(str);
            }
            if (isBinaryLobType(sqlType, typeName)) {
                return parseBinaryHex(str, table, column);
            }
            if (isDateTimeType(sqlType, typeName)) {
                return parseDateTimeValue(column, str);
            }
            return str;
        } catch (Exception ex) {
            throw new DataSetException("Failed to convert CSV value. table=" + table + " column="
                    + column + " value=" + str, ex);
        }
    }

    /**
     * Formats a JDBC value for CSV output.
     *
     * <ul>
     * <li>Returns an empty string for {@code null}.</li>
     * <li>Returns an empty string for LOB values
     * ({@code byte[]}/{@link java.sql.Blob}/{@link java.sql.Clob}), because LOBs are not written
     * into CSV (handled as separate files).</li>
     * <li>Formats date/time values using the date-time formatter.</li>
     * <li>For other values, uses {@code toString()} and normalizes fractional seconds
     * representation.</li>
     * </ul>
     *
     * @param columnName column name
     * @param value JDBC object
     * @return string for CSV
     * @throws SQLException if formatting requires DB access and fails
     */
    @Override
    public String formatDbValueForCsv(String columnName, Object value) throws SQLException {
        if (value == null) {
            return "";
        }
        if (value instanceof byte[]) {
            return "";
        }
        if (value instanceof Blob) {
            return "";
        }
        if (value instanceof Clob) {
            return "";
        }
        if (value instanceof Date || value instanceof Time || value instanceof Timestamp
                || value instanceof LocalDate || value instanceof LocalDateTime
                || value instanceof OffsetDateTime) {
            return formatDateTimeColumn(columnName, value, null);
        }
        return normalizeTimestampFraction(value.toString());
    }

    /**
     * Resolves SQL Server schema name.
     *
     * @param entry connection config entry
     * @return SQL Server default schema
     */
    @Override
    public String resolveSchema(ConnectionConfig.Entry entry) {
        return "dbo";
    }

    /**
     * Creates a DBUnit connection with SQL Server settings.
     *
     * @param jdbc JDBC connection
     * @param schema schema name
     * @return initialized DBUnit connection
     * @throws Exception if initialization fails
     */
    @Override
    public DatabaseConnection createDbUnitConnection(Connection jdbc, String schema)
            throws Exception {
        DatabaseConnection dbConn = new DatabaseConnection(jdbc, schema);
        DatabaseConfig config = dbConn.getConfig();
        configFactory.configure(config, getDataTypeFactory());
        config.setProperty(DatabaseConfig.PROPERTY_ESCAPE_PATTERN, "[?]");
        return dbConn;
    }

    /**
     * Writes LOB value to file.
     *
     * @param schema schema name
     * @param table table name
     * @param value LOB value
     * @param outputPath destination file
     * @throws Exception if file operation fails
     */
    @Override
    public void writeLobFile(String schema, String table, Object value, Path outputPath)
            throws Exception {
        Files.createDirectories(outputPath.getParent(), new FileAttribute<?>[0]);
        if (value instanceof byte[]) {
            Files.write(outputPath, (byte[]) value);
            logWrittenLobPath(outputPath, table);
            return;
        }
        if (value instanceof Blob) {
            try (InputStream in = ((Blob) value).getBinaryStream()) {
                Files.copy(in, outputPath, StandardCopyOption.REPLACE_EXISTING);
            }
            logWrittenLobPath(outputPath, table);
            return;
        }
        if (value instanceof Clob) {
            try (Reader reader = ((Clob) value).getCharacterStream()) {
                writeReaderAsUtf8(reader, outputPath);
            }
            logWrittenLobPath(outputPath, table);
            return;
        }
        Files.writeString(outputPath, value.toString(), StandardCharsets.UTF_8);
        logWrittenLobPath(outputPath, table);
    }

    /**
     * Reads a LOB file and converts it into a JDBC-bindable object.
     *
     * @param fileRef file name
     * @param table table name
     * @param column column name
     * @param baseDir base directory where LOB files are stored
     * @return {@code byte[]} for binary/blob, otherwise UTF-8 string
     * @throws IOException if file reading fails
     * @throws DataSetException if metadata cannot be resolved
     */
    @Override
    public Object readLobFile(String fileRef, String table, String column, File baseDir)
            throws IOException, DataSetException {
        File lobFile = new File(new File(baseDir, LobPathConstants.DIRECTORY_NAME), fileRef);
        if (!lobFile.exists()) {
            throw new DataSetException("LOB file not found: " + lobFile.getAbsolutePath());
        }
        ResolvedColumnSpec resolved = resolveColumnSpec(table, column);
        String typeName = normalizeTypeName(resolved.typeName);

        if (isBinaryLobType(resolved.sqlType, typeName)) {
            return Files.readAllBytes(lobFile.toPath());
        }
        return Files.readString(lobFile.toPath(), StandardCharsets.UTF_8);
    }

    /**
     * Formats date/time value using shared formatter.
     *
     * @param columnName column name
     * @param value JDBC value
     * @param connection JDBC connection (nullable)
     * @return formatted datetime text
     * @throws SQLException if formatting fails
     */
    @Override
    public String formatDateTimeColumn(String columnName, Object value, Connection connection)
            throws SQLException {
        String formatted = dateTimeFormatter.formatJdbcDateTime(columnName, value, connection);
        return normalizeTimestampFraction(formatted);
    }

    /**
     * Returns whether SQL type should be treated as datetime for dump.
     *
     * @param sqlType JDBC SQL type
     * @param sqlTypeName dialect SQL type name
     * @return {@code true} when treated as datetime
     */
    @Override
    public boolean isDateTimeTypeForDump(int sqlType, String sqlTypeName) {
        if (DbDialectHandler.super.isDateTimeTypeForDump(sqlType, sqlTypeName)) {
            return true;
        }
        return sqlType == -155;
    }

    /**
     * Parses CSV datetime value.
     *
     * @param columnName column name
     * @param value CSV value
     * @return parsed temporal object
     * @throws Exception if parsing fails
     */
    @Override
    public Object parseDateTimeValue(String columnName, String value) throws Exception {
        String str = value.trim();

        LocalDateTime configuredTs = dateTimeFormatter.parseConfiguredTimestamp(str);
        if (configuredTs != null) {
            return Timestamp.valueOf(configuredTs);
        }
        LocalDate configuredDate = dateTimeFormatter.parseConfiguredDate(str);
        if (configuredDate != null) {
            return Date.valueOf(configuredDate);
        }
        LocalTime configuredTime = dateTimeFormatter.parseConfiguredTime(str);
        if (configuredTime != null) {
            return Time.valueOf(configuredTime);
        }

        OffsetDateTime odt = tryParseOffsetDateTime(str);
        if (odt != null) {
            return Timestamp.from(odt.toInstant());
        }

        LocalDateTime ldt = tryParseLocalDateTime(str);
        if (ldt != null) {
            return Timestamp.valueOf(ldt);
        }

        LocalDate d = tryParseLocalDate(str);
        if (d != null) {
            return Date.valueOf(d);
        }

        LocalTime t = tryParseLocalTime(str);
        if (t != null) {
            return Time.valueOf(t);
        }

        throw new IllegalArgumentException(
                "Invalid date/time format. column=" + columnName + " value=" + str);
    }

    /**
     * Quotes SQL Server identifier.
     *
     * @param identifier identifier
     * @return quoted identifier
     */
    @Override
    public String quoteIdentifier(String identifier) {
        return "[" + identifier + "]";
    }

    /**
     * Returns SQL Server specific DBUnit data type factory.
     *
     * @return data type factory
     */
    @Override
    public IDataTypeFactory getDataTypeFactory() {
        return SQLSERVER_TYPE_FACTORY;
    }

    /**
     * Returns whether table has NOT NULL LOB column.
     *
     * @param conn JDBC connection
     * @param schema schema name
     * @param table table name
     * @param lobCols table columns
     * @return true if NOT NULL LOB exists
     * @throws SQLException on metadata errors
     */
    @Override
    public boolean hasNotNullLobColumn(Connection conn, String schema, String table,
            Column[] lobCols) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet rs = meta.getColumns(null, schema, table, null);
        try {
            while (rs.next()) {
                String colName = rs.getString("COLUMN_NAME");
                int nullable = rs.getInt("NULLABLE");
                for (Column lob : lobCols) {
                    if (lob.getColumnName().equalsIgnoreCase(colName)
                            && nullable == DatabaseMetaData.columnNoNulls) {
                        return true;
                    }
                }
            }
        } finally {
            rs.close();
        }
        return false;
    }

    /**
     * Returns whether table has primary key.
     *
     * @param connection JDBC connection
     * @param schema schema name
     * @param table table name
     * @return true when primary key exists
     * @throws SQLException if metadata retrieval fails
     */
    @Override
    public boolean hasPrimaryKey(Connection connection, String schema, String table)
            throws SQLException {
        return !getPrimaryKeyColumns(connection, schema, table).isEmpty();
    }

    /**
     * Returns row count for table.
     *
     * @param conn JDBC connection
     * @param table table name
     * @return row count
     * @throws SQLException if SQL execution fails
     */
    @Override
    public int countRows(Connection conn, String table) throws SQLException {
        String sql = String.format("SELECT COUNT(*) FROM %s", quoteIdentifier(table));
        Statement stmt = conn.createStatement();
        try {
            ResultSet rs = stmt.executeQuery(sql);
            try {
                // Read a single row and return the count
                if (rs.next()) {
                    return rs.getInt(1);
                } else {
                    // If the result set is unexpectedly empty, return 0
                    return 0;
                }
            } finally {
                rs.close();
            }
        } finally {
            stmt.close();
        }
    }

    /**
     * Returns LOB column definitions from input CSV and cached table metadata.
     *
     * @param csvDirPath CSV directory
     * @param tableName table name
     * @return LOB columns
     * @throws IOException if CSV read fails
     * @throws DataSetException if DBUnit metadata operation fails
     */
    @Override
    public Column[] getLobColumns(Path csvDirPath, String tableName)
            throws IOException, DataSetException {
        File csv = csvDirPath.resolve(tableName + ".csv").toFile();
        if (!csv.exists()) {
            return new Column[0];
        }
        CSVFormat fmt = CSVFormat.DEFAULT.builder().setHeader(new String[0])
                .setSkipHeaderRecord(true).get();

        String[] headers;
        try (BufferedReader reader = Files.newBufferedReader(csv.toPath(), StandardCharsets.UTF_8);
                CSVParser parser = CSVParser.builder().setReader(reader).setFormat(fmt).get()) {
            headers = parser.getHeaderMap().keySet().toArray(new String[0]);
        }

        Column[] allCols = tableColumnsMap.get(tableName.toLowerCase(Locale.ROOT));
        if (allCols == null) {
            return new Column[0];
        }
        Map<String, Integer> headerIndex = new HashMap<>();
        for (int i = 0; i < headers.length; i++) {
            headerIndex.put(headers[i], i);
        }

        List<Column> result = new ArrayList<>();
        for (Column col : allCols) {
            Integer idx = headerIndex.get(col.getColumnName());
            if (idx == null) {
                continue;
            }
            int sqlType = col.getDataType().getSqlType();
            String typeName = normalizeTypeName(col.getDataType().getSqlTypeName());
            if (isLobType(sqlType, typeName)) {
                result.add(col);
            }
        }
        return result.toArray(new Column[0]);
    }

    /**
     * Logs table definition (column names and type names).
     *
     * @param connection JDBC connection
     * @param schema schema name
     * @param table table name
     * @param loggerName logger context
     * @throws SQLException on metadata retrieval errors
     */
    @Override
    public void logTableDefinition(Connection connection, String schema, String table,
            String loggerName) throws SQLException {
        DatabaseMetaData meta = connection.getMetaData();
        try (ResultSet rs = meta.getColumns(null, schema, table, null)) {
            while (rs.next()) {
                String colName = rs.getString("COLUMN_NAME");
                String typeName = rs.getString("TYPE_NAME");
                int columnSize = rs.getInt("COLUMN_SIZE");
                int charOctetLength = rs.getInt("CHAR_OCTET_LENGTH");
                log.debug("[{}] def: tbl={} col={} type={} lenC={} lenB={}", loggerName, table,
                        colName, typeName, columnSize, charOctetLength);
            }
        }
    }

    /**
     * Retrieves all table names and filters out excluded tables.
     *
     * @param conn JDBC connection
     * @param schema schema name
     * @param excludeTables excluded table names
     * @return target table names
     * @throws SQLException if metadata retrieval fails
     */
    private List<String> fetchTargetTables(Connection conn, String schema,
            List<String> excludeTables) throws SQLException {
        List<String> tables = new ArrayList<>();
        List<String> effectiveExcludeTables = Optional.ofNullable(excludeTables).orElse(List.of());
        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet rs = meta.getTables(null, schema, "%", new String[] {"TABLE"})) {
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                boolean excluded = effectiveExcludeTables.stream()
                        .anyMatch(ex -> ex.equalsIgnoreCase(tableName));
                if (!excluded) {
                    tables.add(tableName);
                }
            }
        }
        return tables;
    }

    /**
     * Caches JDBC column metadata per table for conversion fallback.
     *
     * @param conn JDBC connection
     * @param schema schema name
     * @param targetTables table names
     * @throws SQLException if metadata retrieval fails
     */
    private void cacheJdbcColumnSpecs(Connection conn, String schema, List<String> targetTables)
            throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        for (String table : targetTables) {
            Map<String, JdbcColumnSpec> byColumn = new HashMap<>();
            try (ResultSet rs = meta.getColumns(null, schema, table, "%")) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    int sqlType = rs.getInt("DATA_TYPE");
                    String typeName = rs.getString("TYPE_NAME");
                    byColumn.put(columnName.toLowerCase(Locale.ROOT),
                            new JdbcColumnSpec(sqlType, typeName));
                }
            }
            jdbcColumnSpecMap.put(table.toLowerCase(Locale.ROOT), byColumn);
        }
    }

    /**
     * Resolves column type information using DBUnit metadata and JDBC fallback.
     *
     * @param table table name
     * @param column column name
     * @return resolved column type information
     * @throws DataSetException if metadata cannot be resolved
     */
    private ResolvedColumnSpec resolveColumnSpec(String table, String column)
            throws DataSetException {
        String tableKey = table.toLowerCase(Locale.ROOT);
        Column dbUnitColumn = findColumnInDbUnitMeta(tableKey, column);
        JdbcColumnSpec jdbcSpec = findJdbcColumnSpec(tableKey, column);

        if (dbUnitColumn != null) {
            DataType dataType = dbUnitColumn.getDataType();
            if (jdbcSpec != null && isUnknownDbUnitType(dataType)) {
                return new ResolvedColumnSpec(jdbcSpec.sqlType, jdbcSpec.typeName);
            }
            return new ResolvedColumnSpec(dataType.getSqlType(), dataType.getSqlTypeName());
        }

        if (jdbcSpec != null) {
            return new ResolvedColumnSpec(jdbcSpec.sqlType, jdbcSpec.typeName);
        }

        throw new DataSetException(
                "Column metadata not found: table=" + table + " column=" + column);
    }

    /**
     * Finds column metadata from cached DBUnit metadata.
     *
     * @param tableKey lower-cased table key
     * @param column column name
     * @return DBUnit column metadata, or null
     * @throws DataSetException if table metadata is missing
     */
    private Column findColumnInDbUnitMeta(String tableKey, String column) throws DataSetException {
        Column[] columns = tableColumnsMap.get(tableKey);
        if (columns == null) {
            throw new DataSetException("Table metadata not found: " + tableKey);
        }
        for (Column col : columns) {
            if (col.getColumnName().equalsIgnoreCase(column)) {
                return col;
            }
        }
        return null;
    }

    /**
     * Finds column metadata from cached JDBC metadata.
     *
     * @param tableKey lower-cased table key
     * @param column column name
     * @return JDBC column metadata, or null
     */
    private JdbcColumnSpec findJdbcColumnSpec(String tableKey, String column) {
        return jdbcColumnSpecMap.get(tableKey).get(column.toLowerCase(Locale.ROOT));
    }

    /**
     * Writes text data from reader into UTF-8 file.
     *
     * @param reader source reader
     * @param outputPath destination path
     * @throws IOException if file writing fails
     */
    private void writeReaderAsUtf8(Reader reader, Path outputPath) throws IOException {
        StringBuilder sb = new StringBuilder();
        char[] buf = new char[8192];
        int n;
        while ((n = reader.read(buf)) >= 0) {
            sb.append(buf, 0, n);
        }
        Files.writeString(outputPath, sb.toString(), StandardCharsets.UTF_8);
    }

    /**
     * Logs LOB write path as dataPath-relative UNIX-style path.
     *
     * @param outputPath output file path
     * @param table table name
     */
    private void logWrittenLobPath(Path outputPath, String table) {
        Path dataDir = Paths.get(pathsConfig.getDataPath()).toAbsolutePath().normalize();
        Path full = outputPath.toAbsolutePath().normalize();
        String relStr = FilenameUtils.separatorsToUnix(dataDir.relativize(full).toString());
        log.info("  LOB file written: table={} path={}", table, relStr);
    }

    /**
     * Loads a LOB value from file.
     *
     * @param fileName relative file name
     * @param table table name
     * @param column column name
     * @param sqlType JDBC SQL type
     * @param typeName normalized type name
     * @return binary bytes or text string
     * @throws DataSetException if loading fails
     */
    private Object loadLobFromFile(String fileName, String table, String column, int sqlType,
            String typeName) throws DataSetException {
        File lobFile = new File(baseLobDir.toFile(), fileName);
        if (!lobFile.exists()) {
            throw new DataSetException("LOB file does not exist: " + lobFile.getAbsolutePath());
        }

        try {
            if (isBinaryLobType(sqlType, typeName)) {
                return Files.readAllBytes(lobFile.toPath());
            }
            if (isTextLikeType(sqlType, typeName)) {
                return Files.readString(lobFile.toPath(), StandardCharsets.UTF_8);
            }
        } catch (IOException e) {
            throw new DataSetException("Failed to read LOB file: " + lobFile.getAbsolutePath(), e);
        }

        throw new DataSetException("file: reference is not supported for this column. table="
                + table + " column=" + column + " type=" + typeName);
    }

    /**
     * Returns whether input starts with file reference prefix.
     *
     * @param s input string
     * @return true when prefixed by {@code file:}
     */
    private boolean startsWithFileReference(String s) {
        return s.startsWith("file:");
    }

    /**
     * Normalizes SQL type name for comparison.
     *
     * @param typeName raw type name
     * @return lower-cased normalized name
     */
    private String normalizeTypeName(String typeName) {
        return StringUtils.defaultString(typeName).toLowerCase(Locale.ROOT).trim();
    }

    /**
     * Parses boolean-like text.
     *
     * @param str input text
     * @return parsed boolean
     */
    private Boolean parseBoolean(String str) {
        if ("true".equalsIgnoreCase(str) || "1".equals(str) || "t".equalsIgnoreCase(str)) {
            return Boolean.TRUE;
        }
        if ("false".equalsIgnoreCase(str) || "0".equals(str) || "f".equalsIgnoreCase(str)) {
            return Boolean.FALSE;
        }
        return Boolean.valueOf(str);
    }

    /**
     * Parses binary hex text.
     *
     * @param str hex string
     * @param table table name
     * @param column column name
     * @return decoded bytes
     * @throws DataSetException if decoding fails
     */
    private byte[] parseBinaryHex(String str, String table, String column) throws DataSetException {
        String normalized = str;
        if (normalized.toLowerCase(Locale.ROOT).startsWith("\\x")) {
            normalized = normalized.substring(2);
        }
        try {
            return Hex.decodeHex(normalized.toCharArray());
        } catch (DecoderException e) {
            throw new DataSetException("Failed to decode binary hex. table=" + table + " column="
                    + column + " value=" + str, e);
        }
    }

    /**
     * Tries parsing offset datetime.
     *
     * @param s input text
     * @return parsed offset datetime, or null
     */
    private OffsetDateTime tryParseOffsetDateTime(String s) {
        String normalized = s.trim().replace(' ', 'T');
        String noColonOffset = normalizeOffsetNoColonToColon(normalized);
        try {
            return OffsetDateTime.parse(noColonOffset);
        } catch (DateTimeParseException ignored) {
            return null;
        }
    }

    /**
     * Tries parsing local datetime.
     *
     * @param s input text
     * @return parsed local datetime, or null
     */
    private LocalDateTime tryParseLocalDateTime(String s) {
        String normalized = s.trim().replace(' ', 'T');
        try {
            return LocalDateTime.parse(normalized);
        } catch (DateTimeParseException ignored) {
            return null;
        }
    }

    /**
     * Tries parsing local date.
     *
     * @param s input text
     * @return parsed local date, or null
     */
    private LocalDate tryParseLocalDate(String s) {
        String normalized = s.trim();
        for (DateTimeFormatter formatter : FlexibleDateTimeParsers.DATE_ONLY_FORMATTERS) {
            try {
                return LocalDate.parse(normalized, formatter);
            } catch (DateTimeParseException ignored) {
                // Try next formatter.
            }
        }
        return null;
    }

    /**
     * Tries parsing local time.
     *
     * @param s input text
     * @return parsed local time, or null
     */
    private LocalTime tryParseLocalTime(String s) {
        String normalized = s.trim();
        try {
            return LocalTime.parse(normalized,
                    FlexibleDateTimeParsers.FLEXIBLE_LOCAL_TIME_PARSER_COLON);
        } catch (DateTimeParseException ignored) {
            // Try next parser.
        }
        try {
            return LocalTime.parse(normalized,
                    FlexibleDateTimeParsers.FLEXIBLE_LOCAL_TIME_PARSER_NO_COLON);
        } catch (DateTimeParseException ignored) {
            return null;
        }
    }

    /**
     * Converts offset format +0900 into +09:00 when possible.
     *
     * @param s input text
     * @return normalized text
     */
    private String normalizeOffsetNoColonToColon(String s) {
        int len = s.length();
        if (len < 5) {
            return s;
        }
        char sign = s.charAt(len - 5);
        if ("+-".indexOf(sign) < 0) {
            return s;
        }
        String hhmm = s.substring(len - 4);
        if (!hhmm.chars().allMatch(Character::isDigit)) {
            return s;
        }
        String hh = hhmm.substring(0, 2);
        String mm = hhmm.substring(2, 4);
        return s.substring(0, len - 5) + sign + hh + ":" + mm;
    }

    /**
     * Normalizes timestamp fraction (drops trailing .000).
     *
     * @param value formatted value
     * @return normalized value
     */
    private String normalizeTimestampFraction(String value) {
        return value.replaceFirst(
                "^(\\d{4}-\\d{2}-\\d{2}[ T]\\d{2}:\\d{2}:\\d{2})\\.000((?:Z|[+-]\\d{2}:?\\d{2})?)$",
                "$1$2");
    }

    /**
     * Returns whether DBUnit type is unresolved.
     *
     * @param dataType DBUnit type
     * @return true when unresolved
     */
    private boolean isUnknownDbUnitType(DataType dataType) {
        return DataType.UNKNOWN.equals(dataType)
                || UNKNOWN_DB_UNIT_SQL_TYPES.contains(dataType.getSqlType())
                || UNKNOWN_DB_UNIT_TYPE_NAMES
                        .contains(normalizeTypeName(dataType.getSqlTypeName()));
    }

    /**
     * Returns whether type is text-like.
     *
     * @param sqlType JDBC SQL type
     * @param typeName normalized type name
     * @return true when text-like
     */
    private boolean isTextLikeType(int sqlType, String typeName) {
        return TEXT_LIKE_TYPE_NAMES.contains(typeName) || CLOB_SQL_TYPES.contains(sqlType);
    }

    /**
     * Returns whether type is boolean.
     *
     * @param sqlType JDBC SQL type
     * @param typeName normalized type name
     * @return true when boolean
     */
    private boolean isBooleanType(int sqlType, String typeName) {
        return BOOLEAN_SQL_TYPES.contains(sqlType) || BOOLEAN_TYPE_NAMES.contains(typeName);
    }

    /**
     * Returns whether type is integer.
     *
     * @param sqlType JDBC SQL type
     * @param typeName normalized type name
     * @return true when integer
     */
    private boolean isIntegerType(int sqlType, String typeName) {
        return INTEGER_SQL_TYPES.contains(sqlType) || INTEGER_TYPE_NAMES.contains(typeName);
    }

    /**
     * Returns whether type is bigint.
     *
     * @param sqlType JDBC SQL type
     * @param typeName normalized type name
     * @return true when bigint
     */
    private boolean isBigIntType(int sqlType, String typeName) {
        if (sqlType == Types.BIGINT) {
            return true;
        }
        return BIGINT_TYPE_NAMES.contains(typeName);
    }

    /**
     * Returns whether type is numeric.
     *
     * @param sqlType JDBC SQL type
     * @param typeName normalized type name
     * @return true when numeric
     */
    private boolean isNumericType(int sqlType, String typeName) {
        if (NUMERIC_SQL_TYPES.contains(sqlType)) {
            return true;
        }
        return NUMERIC_TYPE_NAMES.contains(typeName);
    }

    /**
     * Returns whether type is real/double.
     *
     * @param sqlType JDBC SQL type
     * @param typeName normalized type name
     * @return true when floating point
     */
    private boolean isRealType(int sqlType, String typeName) {
        return REAL_SQL_TYPES.contains(sqlType) || FLOATING_POINT_TYPE_NAMES.contains(typeName);
    }

    /**
     * Returns whether type is datetime.
     *
     * @param sqlType JDBC SQL type
     * @param typeName normalized type name
     * @return true when datetime
     */
    private boolean isDateTimeType(int sqlType, String typeName) {
        return DATE_TIME_SQL_TYPES.contains(sqlType) || DATE_TIME_TYPE_NAMES.contains(typeName);
    }

    /**
     * Returns whether the given JDBC type should be treated as a binary LOB (BLOB).
     *
     * <p>
     * This method assumes {@code typeName} is normalized (for example, lowercased) and checks:
     * </p>
     * <ul>
     * <li>whether {@code typeName} ends with {@code "blob"}</li>
     * <li>whether {@code typeName} is in {@code BINARY_LIKE_TYPE_NAMES}</li>
     * <li>whether {@code sqlType} is in {@code BINARY_SQL_TYPES}</li>
     * </ul>
     *
     * @param sqlType JDBC SQL type
     * @param typeName normalized type name (nullable)
     * @return {@code true} when the type is binary/blob; {@code false} otherwise
     */
    private boolean isBinaryLobType(int sqlType, String typeName) {
        return Strings.CS.endsWith(typeName, "blob") || BINARY_LIKE_TYPE_NAMES.contains(typeName)
                || BINARY_SQL_TYPES.contains(sqlType);
    }

    /**
     * Returns whether type is treated as LOB.
     *
     * @param sqlType JDBC SQL type
     * @param typeName normalized type name
     * @return true when LOB
     */
    private boolean isLobType(int sqlType, String typeName) {
        return isBinaryLobType(sqlType, typeName) || TEXT_LOB_TYPE_NAMES.contains(typeName)
                || LOB_SQL_TYPES.contains(sqlType);
    }
}
