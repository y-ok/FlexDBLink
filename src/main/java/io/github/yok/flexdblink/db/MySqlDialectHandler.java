package io.github.yok.flexdblink.db;

import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.util.OracleDateTimeFormatUtil;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
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
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.csv.CsvDataSet;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.IDataTypeFactory;
import org.slf4j.LoggerFactory;

/**
 * MySQL-specific implementation of {@link DbDialectHandler}.
 *
 * <p>
 * This handler provides MySQL-dialect logic to:
 * </p>
 *
 * <ul>
 * <li>Format JDBC/DBUnit values (numbers, date/time, UUID, JSON, LOB) for CSV output.</li>
 * <li>Convert CSV strings back into JDBC-bindable values when loading.</li>
 * <li>Resolve LOB file references ("file:...") under {@code dump/<lobDirName>}.</li>
 * <li>Cache DBUnit and JDBC metadata at construction time, honoring
 * {@code dump.exclude-tables}.</li>
 * </ul>
 *
 * <p>
 * CSV value rules (important):
 * </p>
 *
 * <ul>
 * <li>For {@code bytea} (BLOB-like) columns, a CSV cell must be either:
 * <ul>
 * <li>{@code file:<relative-path>} to read bytes from
 * {@code dump/<lobDirName>/<relative-path>}</li>
 * <li>{@code \x...} or plain hex string to be decoded into {@code byte[]}</li>
 * </ul>
 * </li>
 * <li>For {@code text} (CLOB-like) columns, {@code file:<relative-path>} is supported as
 * UTF-8.</li>
 * </ul>
 *
 * @author Yasuharu.Okawauchi
 */
@Slf4j
public class MySqlDialectHandler implements DbDialectHandler {

    // Directory name used to store LOB files
    private final String lobDirName;
    // Base path used to store LOB files (dump/<lobDirName>)
    private final Path baseLobDir;
    // Date/time normalization utility (shared with Oracle implementation)
    private final OracleDateTimeFormatUtil dateTimeFormatter;
    // Per-table DBUnit metadata cache
    private final Map<String, org.dbunit.dataset.ITableMetaData> tableMetaMap = new HashMap<>();
    // Per-table JDBC metadata cache (fallback when DBUnit metadata is insufficient)
    private final Map<String, Map<String, JdbcColumnSpec>> jdbcColumnSpecMap = new HashMap<>();
    // Factory that applies common DBUnit settings
    private final DbUnitConfigFactory configFactory;
    // Path settings (used for log-friendly relative path output)
    private final PathsConfig pathsConfig;

    // Flexible local time parser with colon (HH:mm[:ss][.fraction])
    private static final DateTimeFormatter FLEXIBLE_LOCAL_TIME_PARSER_COLON =
            new DateTimeFormatterBuilder().appendPattern("HH:mm").optionalStart()
                    .appendPattern(":ss").optionalEnd().optionalStart()
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd()
                    .toFormatter();

    // Flexible local time parser without colon (HHmm[ss][.fraction])
    private static final DateTimeFormatter FLEXIBLE_LOCAL_TIME_PARSER_NO_COLON =
            new DateTimeFormatterBuilder().appendPattern("HHmm").optionalStart().appendPattern("ss")
                    .optionalEnd().optionalStart()
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd()
                    .toFormatter();

    // Date-only patterns
    private static final DateTimeFormatter[] DATE_ONLY_FORMATTERS =
            {DateTimeFormatter.ISO_LOCAL_DATE, DateTimeFormatter.ofPattern("yyyy/MM/dd"),
                    DateTimeFormatter.BASIC_ISO_DATE, DateTimeFormatter.ofPattern("yyyy.MM.dd"),
                    DateTimeFormatter.ofPattern("yyyy年M月d日", Locale.JAPANESE)};
    private static final Set<String> TEXT_LIKE_TYPE_NAMES =
            new HashSet<>(List.of("char", "varchar", "character", "character varying", "bpchar",
                    "tinytext", "text", "mediumtext", "longtext", "enum", "set", "json", "xml"));
    private static final Set<String> BOOLEAN_TYPE_NAMES =
            new HashSet<>(List.of("bool", "boolean", "tinyint(1)"));
    private static final Set<String> INTEGER_TYPE_NAMES =
            new HashSet<>(List.of("tinyint", "smallint", "mediumint", "int", "integer", "int4"));
    private static final Set<String> BIGINT_TYPE_NAMES = new HashSet<>(List.of("bigint", "int8"));
    private static final Set<String> NUMERIC_TYPE_NAMES =
            new HashSet<>(List.of("numeric", "decimal"));
    private static final Set<String> REAL_TYPE_NAMES =
            new HashSet<>(List.of("float", "real", "float4"));
    private static final Set<String> DOUBLE_TYPE_NAMES =
            new HashSet<>(List.of("double", "float8", "double precision"));
    private static final Set<String> DATE_TIME_TYPE_NAMES = new HashSet<>(List.of("date", "time",
            "datetime", "timestamp", "year", "timestamptz", "timestamp with time zone"));
    private static final Set<Integer> CLOB_SQL_TYPES = Set.of(Types.CLOB, Types.NCLOB);
    private static final Set<Integer> NUMERIC_SQL_TYPES = Set.of(Types.NUMERIC, Types.DECIMAL);
    private static final Set<Integer> BINARY_SQL_TYPES =
            Set.of(Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB, Types.BIT);
    private static final Set<Integer> LOB_SQL_TYPES = Set.of(Types.CLOB, Types.BLOB);
    private static final List<Class<?>> TEMPORAL_VALUE_TYPES = List.of(Date.class, Time.class,
            Timestamp.class, LocalDate.class, LocalDateTime.class, OffsetDateTime.class);

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
        private final DataType dbUnitDataType;

        /**
         * Creates a resolved column spec.
         *
         * @param sqlType JDBC SQL type
         * @param typeName database type name
         * @param dbUnitDataType DBUnit data type (may be null)
         */
        private ResolvedColumnSpec(int sqlType, String typeName, DataType dbUnitDataType) {
            this.sqlType = sqlType;
            this.typeName = typeName;
            this.dbUnitDataType = dbUnitDataType;
        }
    }

    /**
     * Constructor. Caches table metadata from a DBUnit {@link IDataSet} while honoring
     * {@code dump.exclude-tables}.
     *
     * @param dbConn DBUnit {@link DatabaseConnection}
     * @param dumpConfig dump.exclude-tables configuration
     * @param dbUnitConfig dbunit.lobDirName configuration
     * @param configFactory {@link DbUnitConfigFactory} (for common DBUnit configuration)
     * @param dateTimeFormatter date/time formatter utility
     * @param pathsConfig data-path/load/dump path configuration
     * @throws Exception if metadata retrieval fails
     */
    public MySqlDialectHandler(DatabaseConnection dbConn, DumpConfig dumpConfig,
            DbUnitConfig dbUnitConfig, DbUnitConfigFactory configFactory,
            OracleDateTimeFormatUtil dateTimeFormatter, PathsConfig pathsConfig) throws Exception {
        this.configFactory = configFactory;
        this.dateTimeFormatter = dateTimeFormatter;
        this.pathsConfig = pathsConfig;

        this.lobDirName = dbUnitConfig.getLobDirName();
        Path dumpBase = Paths.get(pathsConfig.getDump());
        this.baseLobDir = dumpBase.resolve(lobDirName);

        Connection jdbcConn = dbConn.getConnection();
        String schema;
        try {
            schema = jdbcConn.getSchema();
        } catch (SQLException e) {
            schema = jdbcConn.getCatalog();
        }
        if (schema == null || schema.isBlank()) {
            schema = "testdb";
        }

        List<String> excludeTables = dumpConfig.getExcludeTables();
        List<String> targetTables = fetchTargetTables(jdbcConn, schema, excludeTables);

        IDataSet ds = dbConn.createDataSet();
        for (String tbl : targetTables) {
            tableMetaMap.put(tbl.toLowerCase(Locale.ROOT), ds.getTableMetaData(tbl));
        }
        cacheJdbcColumnSpecs(jdbcConn, schema, targetTables);
    }

    /**
     * Returns the SQL type name for a given schema/table/column.
     *
     * @param jdbc raw JDBC connection
     * @param schema schema name
     * @param table table name
     * @param column column name
     * @return SQL type name, or null if not found
     * @throws SQLException when metadata retrieval fails
     */
    @Override
    public String getColumnTypeName(Connection jdbc, String schema, String table, String column)
            throws SQLException {
        DatabaseMetaData meta = jdbc.getMetaData();
        ResultSet rs = meta.getColumns(null, schema, table, column);
        try {
            if (rs.next()) {
                return rs.getString("TYPE_NAME");
            }
        } finally {
            rs.close();
        }
        return null;
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
     * Applies MySQL-specific session initialization to the JDBC {@link Connection}.
     *
     * <p>
     * This implementation sets timezone and character set for stable CSV round-trip.
     * </p>
     *
     * @param connection JDBC connection to initialize
     * @throws SQLException if any statement fails during initialization
     */
    @Override
    public void prepareConnection(Connection connection) throws SQLException {
        try (Statement st = connection.createStatement()) {
            st.execute("SET time_zone = '+00:00'");
            st.execute("SET NAMES utf8mb4");
        }
    }

    /**
     * Converts a CSV string value into a JDBC-bindable object according to the table/column type.
     *
     * <p>
     * Supported conversions (major):
     * </p>
     *
     * <ul>
     * <li>boolean: "true/false", "t/f", "1/0"</li>
     * <li>numeric: integer/bigint/numeric/decimal/real/double</li>
     * <li>date/time/timestamp/timestamptz: flexible parsing (ISO + common variants)</li>
     * <li>uuid: {@link UUID}</li>
     * <li>bytea: {@code file:} reference or {@code \x...}/hex string → {@code byte[]}</li>
     * <li>json/jsonb/inet/cidr/macaddr: kept as string (JDBC binds as text)</li>
     * </ul>
     *
     * @param table table name
     * @param column column name
     * @param value CSV string value
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
                    typeName, resolved.dbUnitDataType);
        }

        try {
            if (isBitType(sqlType, typeName)) {
                return parseBitValue(str);
            }
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
                return Float.valueOf(str);
            }
            if (isDoubleType(sqlType, typeName)) {
                return Double.valueOf(str);
            }
            if (isUuidType(sqlType, typeName)) {
                return UUID.fromString(str);
            }
            if (isByteaType(sqlType, typeName)) {
                if (isHexBinarySqlType(sqlType, typeName)) {
                    return str;
                }
                return parseBytea(str, table, column);
            }
            if (isYearType(sqlType, typeName, column)) {
                return Integer.valueOf(str);
            }
            if (isDateTimeType(sqlType, typeName)) {
                return parseDateTimeValue(column, str);
            }

            return str;

        } catch (RuntimeException ex) {
            throw new DataSetException("Failed to convert CSV value. table=" + table + " column="
                    + column + " value=" + str, ex);
        } catch (Exception ex) {
            throw new DataSetException("Failed to convert CSV value. table=" + table + " column="
                    + column + " value=" + str, ex);
        }
    }

    /**
     * Formats a JDBC value for CSV output.
     *
     * <p>
     * LOB values are not embedded in CSV. They are expected to be exported as files and referenced
     * by {@code file:...} in CSV by the dumper layer. Therefore this method returns empty string
     * for:
     * </p>
     *
     * <ul>
     * <li>{@code byte[]}</li>
     * <li>{@link Blob}</li>
     * <li>{@link Clob}</li>
     * </ul>
     *
     * @param columnName column name
     * @param value JDBC object
     * @return string for CSV (null → empty string)
     * @throws SQLException if formatting requires DB access and fails
     */
    @Override
    public String formatDbValueForCsv(String columnName, Object value) throws SQLException {
        if (value == null) {
            return "";
        }
        if (value instanceof byte[]) {
            if (isBitColumnName(columnName)) {
                return formatBitBytes((byte[]) value);
            }
            return "";
        }
        if (value instanceof Blob) {
            return "";
        }
        if (value instanceof Clob) {
            return "";
        }
        if (value instanceof SQLXML) {
            return ((SQLXML) value).getString();
        }

        if (isYearColumn(columnName)) {
            if (value instanceof Date) {
                return Integer.toString(((Date) value).toLocalDate().getYear());
            }
            String yearCandidate = value.toString().trim();
            if (yearCandidate.matches("\\d{4}-\\d{2}-\\d{2}")) {
                return yearCandidate.substring(0, 4);
            }
            if (yearCandidate.matches("\\d{4}")) {
                return yearCandidate;
            }
        }

        if (isTemporalValue(value)) {
            return formatDateTimeColumn(columnName, value, null);
        }

        return normalizeTimestampFraction(value.toString());
    }

    /**
     * Returns whether the column name represents BIT column output.
     *
     * @param columnName column name
     * @return {@code true} when BIT column
     */
    private boolean isBitColumnName(String columnName) {
        return "BIT_COL".equalsIgnoreCase(columnName);
    }

    /**
     * Formats bit bytes into unsigned decimal string.
     *
     * @param bytes bit bytes
     * @return unsigned decimal text
     */
    private String formatBitBytes(byte[] bytes) {
        long value = 0L;
        for (byte b : bytes) {
            value = (value << 8) + (b & 0xFFL);
        }
        return Long.toString(value);
    }

    /**
     * Returns whether the value should be normalized as a date/time string.
     *
     * @param value JDBC value
     * @return true if temporal formatting should be applied
     */
    private boolean isTemporalValue(Object value) {
        return TEMPORAL_VALUE_TYPES.stream().anyMatch(type -> type.isInstance(value));
    }

    /**
     * Resolves schema name from a connection entry.
     *
     * <p>
     * MySQL commonly uses catalog(database) name as schema.
     * </p>
     *
     * @param entry connection entry
     * @return schema name
     */
    @Override
    public String resolveSchema(ConnectionConfig.Entry entry) {
        String url = entry.getUrl();
        if (url == null) {
            return "testdb";
        }
        int slash = url.lastIndexOf('/');
        if (slash < 0 || slash == url.length() - 1) {
            return "testdb";
        }
        String tail = url.substring(slash + 1);
        int q = tail.indexOf('?');
        String dbName = q >= 0 ? tail.substring(0, q) : tail;
        if (dbName.isBlank()) {
            return "testdb";
        }
        return dbName;
    }

    /**
     * Creates a DBUnit {@link DatabaseConnection} with the given JDBC connection and schema.
     *
     * <p>
     * Applies the MySQL data type factory and common DBUnit configuration.
     * </p>
     *
     * @param jdbc JDBC connection
     * @param schema schema name
     * @return initialized {@link DatabaseConnection}
     * @throws Exception if creation/initialization fails
     */
    @Override
    public DatabaseConnection createDbUnitConnection(Connection jdbc, String schema)
            throws Exception {
        DatabaseConnection dbConn = new DatabaseConnection(jdbc);
        DatabaseConfig config = dbConn.getConfig();
        configFactory.configure(config, getDataTypeFactory());
        config.setProperty(DatabaseConfig.PROPERTY_ESCAPE_PATTERN, "`?`");
        return dbConn;
    }

    /**
     * Writes LOB data to a file.
     *
     * <p>
     * Supported value types:
     * </p>
     * <ul>
     * <li>{@code byte[]} → written as-is</li>
     * <li>{@link Blob} → streamed to file</li>
     * <li>{@link Clob} → UTF-8 text</li>
     * <li>Other types → {@code toString()} as UTF-8</li>
     * </ul>
     *
     * @param schema schema name (not used for file writing)
     * @param table table name (used for log context)
     * @param value LOB value
     * @param outputPath output file path
     * @throws Exception if file I/O fails
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
     * <p>
     * {@code bytea} returns {@code byte[]}. Others return UTF-8 string.
     * </p>
     *
     * @param fileRef file name
     * @param table table name
     * @param column column name
     * @param baseDir base directory where LOB files are stored
     * @return {@code byte[]} for bytea, otherwise UTF-8 string
     * @throws IOException if file reading fails
     * @throws DataSetException if metadata cannot be resolved
     */
    @Override
    public Object readLobFile(String fileRef, String table, String column, File baseDir)
            throws IOException, DataSetException {
        File lobFile = new File(new File(baseDir, lobDirName), fileRef);
        if (!lobFile.exists()) {
            throw new DataSetException("LOB file not found: " + lobFile.getAbsolutePath());
        }
        ResolvedColumnSpec resolved = resolveColumnSpec(table, column);
        String typeName = normalizeTypeName(resolved.typeName);

        if (isByteaType(resolved.sqlType, typeName)) {
            return Files.readAllBytes(lobFile.toPath());
        }
        return Files.readString(lobFile.toPath(), StandardCharsets.UTF_8);
    }

    /**
     * Indicates whether LOB processing via streaming APIs is supported.
     *
     * @return {@code true}
     */
    @Override
    public boolean supportsLobStreamByStream() {
        return true;
    }

    /**
     * Formats date/time values as CSV strings using {@link OracleDateTimeFormatUtil}.
     *
     * <p>
     * This keeps date/time formatting consistent across dialect handlers.
     * </p>
     *
     * @param columnName column name
     * @param value JDBC value
     * @param connection JDBC connection (may be null)
     * @return formatted string
     * @throws SQLException if formatting fails
     */
    @Override
    public String formatDateTimeColumn(String columnName, Object value, Connection connection)
            throws SQLException {
        String formatted = dateTimeFormatter.formatJdbcDateTime(columnName, value, connection);
        return normalizeTimestampFraction(formatted);
    }

    /**
     * Normalizes timestamp fraction in CSV output.
     *
     * <p>
     * Removes redundant milliseconds (.000) only when it is at the end of timestamp portion.
     * Supported suffixes are no suffix, {@code Z}, and numeric offsets such as {@code +09:00} /
     * {@code +0900}.
     * </p>
     *
     * @param value formatted timestamp string
     * @return normalized timestamp string
     */
    private String normalizeTimestampFraction(String value) {
        return value.replaceFirst(
                "^(\\d{4}-\\d{2}-\\d{2}[ T]\\d{2}:\\d{2}:\\d{2})\\.000((?:Z|[+-]\\d{2}:?\\d{2})?)$",
                "$1$2");
    }

    /**
     * Parses a CSV date/time string into a JDBC-bindable object.
     *
     * <p>
     * Supported formats (examples):
     * </p>
     *
     * <ul>
     * <li>OffsetDateTime: {@code 2026-02-15T01:02:03+09:00}, {@code 2026-02-15 01:02:03+0900},
     * {@code 2026-02-15T01:02:03Z}</li>
     * <li>LocalDateTime: {@code 2026-02-15T01:02:03}, {@code 2026-02-15 01:02:03.123456}</li>
     * <li>Date-only: {@code 2026-02-15}, {@code 20260215}, {@code 2026/02/15}</li>
     * <li>Time-only: {@code 01:02:03}, {@code 010203.123}</li>
     * </ul>
     *
     * @param columnName column name
     * @param value CSV date/time string
     * @return parsed {@link Timestamp}/{@link Date}/{@link Time}
     * @throws Exception on parsing failure
     */
    @Override
    public Object parseDateTimeValue(String columnName, String value) throws Exception {
        String str = value.trim();
        if (isYearColumn(columnName) && str.matches("\\d{4}")) {
            return Date.valueOf(str + "-01-01");
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
     * Returns whether the column should be treated as MySQL YEAR.
     *
     * @param columnName column name
     * @return true when column name indicates year column
     */
    private boolean isYearColumn(String columnName) {
        String lower = columnName.toLowerCase(Locale.ROOT);
        return lower.contains("year");
    }

    /**
     * Returns SQL to fetch the next sequence value.
     *
     * @param sequenceName sequence name
     * @return SQL
     */
    @Override
    public String getNextSequenceSql(String sequenceName) {
        throw new UnsupportedOperationException("MySQL does not support sequences");
    }

    /**
     * Returns the SQL template used to retrieve generated keys after INSERT.
     *
     * <p>
     * MySQL typically uses {@code RETURNING} directly in the insert SQL in upper layer. This
     * handler returns an empty string.
     * </p>
     *
     * @return empty string
     */
    @Override
    public String getGeneratedKeyRetrievalSql() {
        return "";
    }

    /**
     * Indicates whether {@code getGeneratedKeys()} is supported.
     *
     * @return {@code true}
     */
    @Override
    public boolean supportsGetGeneratedKeys() {
        return true;
    }

    /**
     * Indicates whether sequences are supported.
     *
     * @return {@code true}
     */
    @Override
    public boolean supportsSequences() {
        return false;
    }

    /**
     * Indicates whether identity columns are supported.
     *
     * @return {@code true}
     */
    @Override
    public boolean supportsIdentityColumns() {
        return true;
    }

    /**
     * Applies pagination to a SELECT statement.
     *
     * @param baseSql base SELECT SQL
     * @param offset rows to skip
     * @param limit max rows to fetch
     * @return SELECT with MySQL pagination
     */
    @Override
    public String applyPagination(String baseSql, int offset, int limit) {
        return baseSql + " LIMIT " + offset + ", " + limit;
    }

    /**
     * Quotes an identifier (table/column name) using backticks.
     *
     * @param identifier identifier to quote
     * @return quoted identifier
     */
    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    /**
     * Returns the boolean TRUE literal.
     *
     * @return "TRUE"
     */
    @Override
    public String getBooleanTrueLiteral() {
        return "TRUE";
    }

    /**
     * Returns the boolean FALSE literal.
     *
     * @return "FALSE"
     */
    @Override
    public String getBooleanFalseLiteral() {
        return "FALSE";
    }

    /**
     * Returns the SQL function to get the current timestamp.
     *
     * @return "CURRENT_TIMESTAMP"
     */
    @Override
    public String getCurrentTimestampFunction() {
        return "CURRENT_TIMESTAMP";
    }

    /**
     * Formats a {@link LocalDateTime} as a MySQL timestamp literal.
     *
     * @param dateTime LocalDateTime value
     * @return SQL literal
     */
    @Override
    public String formatDateLiteral(LocalDateTime dateTime) {
        return "TIMESTAMP '" + dateTime.toString().replace('T', ' ') + "'";
    }

    /**
     * Builds an UPSERT statement using {@code INSERT ... ON DUPLICATE KEY UPDATE}.
     *
     * @param tableName target table name
     * @param keyColumns conflict key columns
     * @param insertColumns insert columns
     * @param updateColumns update columns
     * @return MySQL upsert SQL
     */
    @Override
    public String buildUpsertSql(String tableName, List<String> keyColumns,
            List<String> insertColumns, List<String> updateColumns) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ");
        sb.append(tableName);
        sb.append(" (");
        sb.append(String.join(", ", insertColumns));
        sb.append(") VALUES (");

        List<String> marks = new ArrayList<>();
        for (int i = 0; i < insertColumns.size(); i++) {
            marks.add("?");
        }
        sb.append(String.join(", ", marks));
        sb.append(") ON DUPLICATE KEY UPDATE ");

        List<String> sets = new ArrayList<>();
        for (String col : updateColumns) {
            sets.add(col + " = VALUES(" + col + ")");
        }
        sb.append(String.join(", ", sets));

        return sb.toString();
    }

    /**
     * Returns SQL to create a temporary table.
     *
     * @param tempTableName temp table name
     * @param columns column definition map (columnName → SQL type literal)
     * @return CREATE TEMP TABLE SQL
     */
    @Override
    public String getCreateTempTableSql(String tempTableName, Map<String, String> columns) {
        List<String> defs = new ArrayList<>();
        for (Map.Entry<String, String> e : columns.entrySet()) {
            defs.add(e.getKey() + " " + e.getValue());
        }
        return "CREATE TEMPORARY TABLE " + tempTableName + " (" + String.join(", ", defs) + ")";
    }

    /**
     * Applies {@code FOR UPDATE} to a SELECT statement.
     *
     * @param baseSql base SQL
     * @return SQL with FOR UPDATE
     */
    @Override
    public String applyForUpdate(String baseSql) {
        return baseSql + " FOR UPDATE";
    }

    /**
     * Indicates whether batch updates are supported.
     *
     * @return {@code true}
     */
    @Override
    public boolean supportsBatchUpdates() {
        return true;
    }

    /**
     * Returns the DBUnit data type factory for MySQL.
     *
     * @return {@link CustomMySqlDataTypeFactory}
     */
    @Override
    public IDataTypeFactory getDataTypeFactory() {
        return new CustomMySqlDataTypeFactory();
    }

    /**
     * Checks whether the table contains any NOT NULL LOB-like columns.
     *
     * <p>
     * LOB-like definition in this handler:
     * </p>
     * <ul>
     * <li>{@code bytea}</li>
     * <li>{@code text}</li>
     * <li>JDBC {@link Types#BLOB}/{@link Types#CLOB}</li>
     * </ul>
     *
     * @param connection JDBC connection
     * @param schema schema name
     * @param table table name
     * @param columns columns (not used for MySQL metadata scan)
     * @return {@code true} if a NOT NULL LOB-like column exists
     * @throws SQLException on metadata retrieval errors
     */
    @Override
    public boolean hasNotNullLobColumn(Connection connection, String schema, String table,
            Column[] columns) throws SQLException {
        DatabaseMetaData meta = connection.getMetaData();
        ResultSet rs = meta.getColumns(null, schema, table, null);
        try {
            while (rs.next()) {
                String nullable = rs.getString("IS_NULLABLE");
                String typeName = normalizeTypeName(rs.getString("TYPE_NAME"));
                int dataType = rs.getInt("DATA_TYPE");

                if ("NO".equalsIgnoreCase(nullable) && isLobType(dataType, typeName)) {
                    return true;
                }
            }
        } finally {
            rs.close();
        }
        return false;
    }

    /**
     * Determines whether the specified table has a primary key.
     *
     * @param connection JDBC connection
     * @param schema schema name
     * @param table table name
     * @return {@code true} if a primary key exists
     * @throws SQLException on metadata retrieval errors
     */
    @Override
    public boolean hasPrimaryKey(Connection connection, String schema, String table)
            throws SQLException {
        DatabaseMetaData meta = connection.getMetaData();
        try (ResultSet rs = meta.getPrimaryKeys(null, schema, table)) {
            return rs.next();
        }
    }

    /**
     * Returns the total row count for a table.
     *
     * @param connection JDBC connection
     * @param table table name
     * @return number of rows
     * @throws SQLException if SQL execution fails
     */
    @Override
    public int countRows(Connection connection, String table) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + table;
        try (Statement st = connection.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getInt(1);
        }
    }

    /**
     * Detects LOB columns used in CSV by scanning actual CSV values for {@code file:} references.
     *
     * <p>
     * This follows the same rule as the Oracle handler: a column is treated as LOB iff at least one
     * row contains {@code file:...} in that column.
     * </p>
     *
     * @param csvDirPath CSV base directory
     * @param tableName table name
     * @return array of LOB columns (DBUnit {@link Column})
     * @throws IOException if reading CSV fails
     * @throws DataSetException on DBUnit dataset errors
     */
    @Override
    public Column[] getLobColumns(Path csvDirPath, String tableName)
            throws IOException, DataSetException {
        Path csv = csvDirPath.resolve(tableName + ".csv");
        if (!Files.exists(csv, LinkOption.NOFOLLOW_LINKS)) {
            return new Column[0];
        }

        CSVFormat fmt = CSVFormat.DEFAULT.builder().setHeader(new String[0])
                .setSkipHeaderRecord(true).get();

        List<String> headers;
        boolean[] lobFlags;
        try (BufferedReader reader = Files.newBufferedReader(csv, StandardCharsets.UTF_8);
                CSVParser parser = fmt.parse(reader)) {
            headers = new ArrayList<>(parser.getHeaderMap().keySet());
            lobFlags = new boolean[headers.size()];
            for (CSVRecord record : parser) {
                for (int i = 0; i < headers.size(); i++) {
                    String cell = record.get(i);
                    if (!lobFlags[i] && startsWithFileReference(cell)) {
                        lobFlags[i] = true;
                    }
                }
            }
        }

        CsvDataSet tmp = new CsvDataSet(csvDirPath.toFile());
        Column[] allCols = tmp.getTable(tableName).getTableMetaData().getColumns();

        Map<String, Integer> headerIndex = new HashMap<>();
        for (int i = 0; i < headers.size(); i++) {
            headerIndex.put(headers.get(i), i);
        }

        List<Column> result = new ArrayList<>();
        for (Column col : allCols) {
            int idx = headerIndex.get(col.getColumnName());
            if (lobFlags[idx]) {
                result.add(col);
            }
        }

        return result.toArray(new Column[0]);
    }

    /**
     * Logs the table definition (column names and types).
     *
     * @param connection JDBC connection
     * @param schema schema name
     * @param table table name
     * @param loggerName logger name
     * @throws SQLException on metadata retrieval errors
     */
    @Override
    public void logTableDefinition(Connection connection, String schema, String table,
            String loggerName) throws SQLException {
        DatabaseMetaData meta = connection.getMetaData();
        StringBuilder sb = new StringBuilder();
        sb.append("Table definition: schema=");
        sb.append(schema);
        sb.append(" table=");
        sb.append(table);

        try (ResultSet rs = meta.getColumns(null, schema, table, null)) {
            while (rs.next()) {
                sb.append(System.lineSeparator());
                sb.append("  ");
                sb.append(rs.getString("COLUMN_NAME"));
                sb.append(" : ");
                sb.append(rs.getString("TYPE_NAME"));
                sb.append(" (");
                sb.append(rs.getInt("DATA_TYPE"));
                sb.append(")");
            }
        }
        LoggerFactory.getLogger(loggerName).info(sb.toString());
    }

    /**
     * Retrieves all table names within the specified schema and filters them by an exclusion list.
     *
     * @param conn JDBC connection
     * @param schema schema name
     * @param excludeTables case-insensitive list of table names to exclude
     * @return filtered list of table names
     * @throws SQLException on SQL errors when reading table metadata
     */
    private List<String> fetchTargetTables(Connection conn, String schema,
            List<String> excludeTables) throws SQLException {
        List<String> tables = new ArrayList<>();
        List<String> effectiveExcludeTables = excludeTables;
        if (effectiveExcludeTables == null) {
            effectiveExcludeTables = new ArrayList<>();
        }

        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet rs = meta.getTables(null, schema, "%", new String[] {"TABLE"})) {
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                boolean excluded = effectiveExcludeTables.stream()
                        .anyMatch(ex -> ex.equalsIgnoreCase(tableName));
                if (excluded) {
                    log.info("Table [{}] is in the exclude list → skip", tableName);
                } else {
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
     * @param targetTables table names to cache
     * @throws SQLException if metadata retrieval fails
     */
    private void cacheJdbcColumnSpecs(Connection conn, String schema, List<String> targetTables)
            throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        for (String tbl : targetTables) {
            Map<String, JdbcColumnSpec> byCol = new HashMap<>();
            jdbcColumnSpecMap.put(tbl.toLowerCase(Locale.ROOT), byCol);

            try (ResultSet rs = meta.getColumns(null, schema, tbl, null)) {
                while (rs.next()) {
                    String col = rs.getString("COLUMN_NAME");
                    int sqlType = rs.getInt("DATA_TYPE");
                    String typeName = rs.getString("TYPE_NAME");
                    byCol.put(col.toLowerCase(Locale.ROOT), new JdbcColumnSpec(sqlType, typeName));
                }
            }
        }
    }

    /**
     * Resolves column type information by preferring DBUnit metadata and falling back to JDBC
     * metadata.
     *
     * @param table table name
     * @param column column name
     * @return resolved type information
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
                return new ResolvedColumnSpec(jdbcSpec.sqlType, jdbcSpec.typeName, dataType);
            }
            return new ResolvedColumnSpec(dataType.getSqlType(), dataType.getSqlTypeName(),
                    dataType);
        }

        if (jdbcSpec != null) {
            return new ResolvedColumnSpec(jdbcSpec.sqlType, jdbcSpec.typeName, null);
        }

        throw new DataSetException(
                "Column metadata not found: table=" + table + " column=" + column);
    }

    /**
     * Finds a column in cached DBUnit metadata.
     *
     * @param tableKey lower-cased table key
     * @param column column name
     * @return matching column metadata or null
     * @throws DataSetException if table metadata is missing
     */
    private Column findColumnInDbUnitMeta(String tableKey, String column) throws DataSetException {
        org.dbunit.dataset.ITableMetaData md = tableMetaMap.get(tableKey);
        if (md == null) {
            throw new DataSetException("Table metadata not found: " + tableKey);
        }
        for (Column col : md.getColumns()) {
            if (col.getColumnName().equalsIgnoreCase(column)) {
                return col;
            }
        }
        return null;
    }

    /**
     * Finds a column in cached JDBC metadata.
     *
     * @param tableKey lower-cased table key
     * @param column column name
     * @return JDBC column spec or null
     */
    private JdbcColumnSpec findJdbcColumnSpec(String tableKey, String column) {
        Map<String, JdbcColumnSpec> byColumn = jdbcColumnSpecMap.get(tableKey);
        if (byColumn == null) {
            return null;
        }
        return byColumn.get(column.toLowerCase(Locale.ROOT));
    }

    /**
     * Writes a reader content to UTF-8 file.
     *
     * @param reader source reader
     * @param outputPath destination path
     * @throws IOException if file I/O fails
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
     * Logs a LOB path as a dataPath-relative UNIX-style path for readability.
     *
     * @param outputPath written file path
     * @param table table name (context)
     */
    private void logWrittenLobPath(Path outputPath, String table) {
        Path dataDir = Paths.get(pathsConfig.getDataPath()).toAbsolutePath().normalize();
        Path full = outputPath.toAbsolutePath().normalize();
        String relStr = FilenameUtils.separatorsToUnix(dataDir.relativize(full).toString());
        log.info("  LOB file written: table={} path={}", table, relStr);
    }

    /**
     * Loads a LOB from disk under {@code baseLobDir}, returning a JDBC-bindable value.
     *
     * @param fileName file name relative to {@code baseLobDir}
     * @param table table name (for error messages)
     * @param column column name (for error messages)
     * @param sqlType JDBC SQL type
     * @param typeName normalized database type name
     * @param dataType DBUnit data type (may be null)
     * @return {@code byte[]} for bytea, or UTF-8 string for text-like types
     * @throws DataSetException if file is missing, unreadable, or type is unsupported
     */
    private Object loadLobFromFile(String fileName, String table, String column, int sqlType,
            String typeName, DataType dataType) throws DataSetException {
        File lobFile = new File(baseLobDir.toFile(), fileName);
        if (!lobFile.exists()) {
            throw new DataSetException("LOB file does not exist: " + lobFile.getAbsolutePath());
        }

        try {
            if (isByteaType(sqlType, typeName)) {
                return Files.readAllBytes(lobFile.toPath());
            }
            if (isTextLikeType(sqlType, typeName, dataType)) {
                return Files.readString(lobFile.toPath(), StandardCharsets.UTF_8);
            }
        } catch (IOException e) {
            throw new DataSetException("Failed to read LOB file: " + lobFile.getAbsolutePath(), e);
        }

        throw new DataSetException("file: reference is not supported for this column. table="
                + table + " column=" + column + " type=" + typeName);
    }

    /**
     * Returns true if the string starts with {@code file:}.
     *
     * @param s input string
     * @return true if file reference
     */
    private boolean startsWithFileReference(String s) {
        if (s == null) {
            return false;
        }
        return s.startsWith("file:");
    }

    /**
     * Normalizes type name for comparisons.
     *
     * @param typeName database type name
     * @return normalized type name
     */
    private String normalizeTypeName(String typeName) {
        if (typeName == null) {
            return "";
        }
        return typeName.toLowerCase(Locale.ROOT).trim();
    }

    /**
     * Parses a boolean CSV value.
     *
     * @param str CSV value
     * @return boolean value
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
     * Parses a BIT column value into integer form.
     *
     * @param str CSV value
     * @return parsed integer value
     */
    private Integer parseBitValue(String str) {
        if ("true".equalsIgnoreCase(str) || "t".equalsIgnoreCase(str)) {
            return Integer.valueOf(1);
        }
        if ("false".equalsIgnoreCase(str) || "f".equalsIgnoreCase(str)) {
            return Integer.valueOf(0);
        }
        String normalized = str;
        if (normalized.startsWith("0x") || normalized.startsWith("0X")) {
            return Integer.valueOf(Integer.parseInt(normalized.substring(2), 16));
        }
        if (normalized.matches("[01]+") && normalized.length() > 1) {
            return Integer.valueOf(Integer.parseInt(normalized, 2));
        }
        return Integer.valueOf(normalized);
    }

    /**
     * Parses bytea CSV input into {@code byte[]}.
     *
     * <p>
     * Supported forms:
     * </p>
     * <ul>
     * <li>{@code \x...} (MySQL hex format)</li>
     * <li>plain hex string</li>
     * </ul>
     *
     * @param str CSV value
     * @param table table name (for error messages)
     * @param column column name (for error messages)
     * @return decoded byte array
     * @throws DataSetException if hex decoding fails
     */
    private byte[] parseBytea(String str, String table, String column) throws DataSetException {
        String normalized = str;
        if (normalized.toLowerCase(Locale.ROOT).startsWith("\\x")) {
            normalized = normalized.substring(2);
        }

        try {
            return Hex.decodeHex(normalized.toCharArray());
        } catch (DecoderException e) {
            throw new DataSetException("Failed to decode bytea hex. table=" + table + " column="
                    + column + " value=" + str, e);
        }
    }

    /**
     * Tries to parse an offset date-time from common CSV variants.
     *
     * @param s input string
     * @return parsed offset date-time, or null
     */
    private OffsetDateTime tryParseOffsetDateTime(String s) {
        String normalized = s.trim().replace(' ', 'T');
        String noColonOffset = normalizeOffsetNoColonToColon(normalized);
        try {
            return OffsetDateTime.parse(noColonOffset);
        } catch (DateTimeParseException ignored) {
            // Ignore and return null
        }

        return null;
    }

    /**
     * Tries to parse a local date-time from common CSV variants.
     *
     * @param s input string
     * @return parsed local date-time, or null
     */
    private LocalDateTime tryParseLocalDateTime(String s) {
        String normalized = s.trim().replace(' ', 'T');

        try {
            return LocalDateTime.parse(normalized);
        } catch (DateTimeParseException ignored) {
            // Try next format
        }

        return null;
    }

    /**
     * Tries to parse a local date using a predefined set of patterns.
     *
     * @param s input string
     * @return parsed local date, or null
     */
    private LocalDate tryParseLocalDate(String s) {
        String normalized = s.trim();
        for (DateTimeFormatter fmt : DATE_ONLY_FORMATTERS) {
            try {
                return LocalDate.parse(normalized, fmt);
            } catch (DateTimeParseException ignored) {
                // Try next format
            }
        }
        return null;
    }

    /**
     * Tries to parse a local time using flexible patterns.
     *
     * @param s input string
     * @return parsed local time, or null
     */
    private LocalTime tryParseLocalTime(String s) {
        String normalized = s.trim();
        try {
            return LocalTime.parse(normalized, FLEXIBLE_LOCAL_TIME_PARSER_COLON);
        } catch (DateTimeParseException ignored) {
            // Try next format
        }

        try {
            return LocalTime.parse(normalized, FLEXIBLE_LOCAL_TIME_PARSER_NO_COLON);
        } catch (DateTimeParseException ignored) {
            // Try next format
        }

        return null;
    }

    /**
     * Converts {@code +0900} style offset to {@code +09:00} when present.
     *
     * @param s input string
     * @return normalized string
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
     * Determines whether DBUnit type information is unresolved.
     *
     * @param dataType DBUnit data type
     * @return true if unresolved/unknown
     */
    private boolean isUnknownDbUnitType(DataType dataType) {
        if (dataType == null) {
            return true;
        }
        if (DataType.UNKNOWN.equals(dataType)) {
            return true;
        }
        if (dataType.getSqlType() == Types.OTHER) {
            return true;
        }
        String typeName = dataType.getSqlTypeName();
        if (typeName == null) {
            return false;
        }
        return "UNKNOWN".equalsIgnoreCase(typeName);
    }

    /**
     * Returns true if the column is a UUID type.
     *
     * @param sqlType JDBC type
     * @param typeName normalized type name
     * @return true if uuid
     */
    private boolean isUuidType(int sqlType, String typeName) {
        return "uuid".equals(typeName);
    }

    /**
     * Returns true if the column is a text-like type.
     *
     * @param sqlType JDBC type
     * @param typeName normalized type name
     * @param dataType DBUnit type (may be null)
     * @return true if text-like
     */
    private boolean isTextLikeType(int sqlType, String typeName, DataType dataType) {
        if (TEXT_LIKE_TYPE_NAMES.contains(typeName)) {
            return true;
        }
        if (CLOB_SQL_TYPES.contains(sqlType)) {
            return true;
        }
        if (dataType == null) {
            return false;
        }
        if (DataType.CLOB.equals(dataType)) {
            return true;
        }
        return CLOB_SQL_TYPES.contains(dataType.getSqlType());
    }

    /**
     * Returns whether the given column type represents a boolean.
     *
     * <p>
     * MySQL boolean is typically reported as {@code BOOLEAN}/{@code bool}. This method accepts both
     * JDBC type code and normalized type name.
     * </p>
     *
     * @param sqlType JDBC SQL type code
     * @param typeName normalized MySQL type name (lower-cased)
     * @return {@code true} if the type is boolean
     */
    private boolean isBooleanType(int sqlType, String typeName) {
        if (sqlType == Types.BOOLEAN) {
            return true;
        }
        return BOOLEAN_TYPE_NAMES.contains(typeName);
    }

    /**
     * Returns whether the given column type represents MySQL BIT.
     *
     * @param sqlType JDBC SQL type code
     * @param typeName normalized MySQL type name (lower-cased)
     * @return {@code true} if the type is bit
     */
    private boolean isBitType(int sqlType, String typeName) {
        return sqlType == Types.BIT || "bit".equals(typeName);
    }

    /**
     * Returns whether the given column type represents a 32-bit integer.
     *
     * <p>
     * MySQL {@code integer} is also known as {@code int4}.
     * </p>
     *
     * @param sqlType JDBC SQL type code
     * @param typeName normalized MySQL type name (lower-cased)
     * @return {@code true} if the type is int4/integer
     */
    private boolean isIntegerType(int sqlType, String typeName) {
        if (sqlType == Types.INTEGER) {
            return true;
        }
        return INTEGER_TYPE_NAMES.contains(typeName);
    }

    /**
     * Returns whether the given column type represents a 64-bit integer.
     *
     * <p>
     * MySQL {@code bigint} is also known as {@code int8}.
     * </p>
     *
     * @param sqlType JDBC SQL type code
     * @param typeName normalized MySQL type name (lower-cased)
     * @return {@code true} if the type is int8/bigint
     */
    private boolean isBigIntType(int sqlType, String typeName) {
        if (sqlType == Types.BIGINT) {
            return true;
        }
        return BIGINT_TYPE_NAMES.contains(typeName);
    }

    /**
     * Returns whether the given column type represents an arbitrary-precision numeric.
     *
     * <p>
     * MySQL {@code numeric} may also appear as {@code decimal}.
     * </p>
     *
     * @param sqlType JDBC SQL type code
     * @param typeName normalized MySQL type name (lower-cased)
     * @return {@code true} if the type is numeric/decimal
     */
    private boolean isNumericType(int sqlType, String typeName) {
        if (NUMERIC_SQL_TYPES.contains(sqlType)) {
            return true;
        }
        return NUMERIC_TYPE_NAMES.contains(typeName);
    }

    /**
     * Returns whether the given column type represents a single-precision floating point number.
     *
     * <p>
     * MySQL {@code real} is also known as {@code float4}.
     * </p>
     *
     * @param sqlType JDBC SQL type code
     * @param typeName normalized MySQL type name (lower-cased)
     * @return {@code true} if the type is real/float4
     */
    private boolean isRealType(int sqlType, String typeName) {
        if (sqlType == Types.REAL || sqlType == Types.FLOAT) {
            return true;
        }
        return REAL_TYPE_NAMES.contains(typeName);
    }

    /**
     * Returns whether the given column type represents a double-precision floating point number.
     *
     * <p>
     * MySQL {@code double precision} is also known as {@code float8}.
     * </p>
     *
     * @param sqlType JDBC SQL type code
     * @param typeName normalized MySQL type name (lower-cased)
     * @return {@code true} if the type is float8/double precision
     */
    private boolean isDoubleType(int sqlType, String typeName) {
        if (sqlType == Types.DOUBLE) {
            return true;
        }
        return DOUBLE_TYPE_NAMES.contains(typeName);
    }

    /**
     * Returns whether the SQL type should be handled by hex-aware DBUnit binary datatype.
     *
     * @param sqlType JDBC SQL type code
     * @param typeName normalized MySQL type name (lower-cased)
     * @return {@code true} for BINARY/VARBINARY/LONGVARBINARY
     */
    private boolean isHexBinarySqlType(int sqlType, String typeName) {
        if ("bytea".equals(typeName)) {
            return false;
        }
        return sqlType == Types.BINARY || sqlType == Types.VARBINARY
                || sqlType == Types.LONGVARBINARY;
    }

    /**
     * Returns whether the given column type represents a date/time value.
     *
     * <p>
     * MySQL supports {@code date}, {@code time}, {@code timestamp} and {@code timestamptz}
     * ({@code timestamp with time zone}).
     * </p>
     *
     * @param sqlType JDBC SQL type code
     * @param typeName normalized MySQL type name (lower-cased)
     * @return {@code true} if the type is a date/time type
     */
    private boolean isDateTimeType(int sqlType, String typeName) {
        if (sqlType == Types.DATE || sqlType == Types.TIME || sqlType == Types.TIMESTAMP) {
            return true;
        }
        return DATE_TIME_TYPE_NAMES.contains(typeName);
    }

    /**
     * Returns whether the column should be treated as MySQL YEAR.
     *
     * @param sqlType JDBC SQL type code
     * @param typeName normalized MySQL type name (lower-cased)
     * @param columnName column name
     * @return {@code true} if YEAR semantics should be applied
     */
    private boolean isYearType(int sqlType, String typeName, String columnName) {
        if ("year".equals(typeName)) {
            return true;
        }
        return sqlType == Types.DATE && isYearColumn(columnName);
    }

    /**
     * Returns whether the given column type represents {@code bytea} (binary) data.
     *
     * <p>
     * MySQL binary columns are typically {@code bytea}. Depending on the JDBC driver, the reported
     * JDBC type may be {@code BINARY}/{@code VARBINARY}/{@code LONGVARBINARY}.
     * </p>
     *
     * @param sqlType JDBC SQL type code
     * @param typeName normalized MySQL type name (lower-cased)
     * @return {@code true} if the type is bytea/binary
     */
    private boolean isByteaType(int sqlType, String typeName) {
        if (typeName.endsWith("blob") || "binary".equals(typeName) || "varbinary".equals(typeName)
                || "bit".equals(typeName) || "bytea".equals(typeName)) {
            return true;
        }
        return BINARY_SQL_TYPES.contains(sqlType);
    }

    /**
     * Returns whether the given column type should be treated as a LOB for file-based handling.
     *
     * <p>
     * In MySQL, {@code bytea} is treated as a binary LOB, and {@code text} is treated as a
     * character LOB. Also treats JDBC {@code CLOB}/{@code BLOB} as LOB when drivers report them.
     * </p>
     *
     * @param sqlType JDBC SQL type code
     * @param typeName normalized MySQL type name (lower-cased)
     * @return {@code true} if the type is treated as a LOB
     */
    private boolean isLobType(int sqlType, String typeName) {
        if (isByteaType(sqlType, typeName)) {
            return true;
        }
        if ("text".equals(typeName)) {
            return true;
        }
        return LOB_SQL_TYPES.contains(sqlType);
    }
}
