package io.github.yok.flexdblink.db.oracle;

import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.db.DbDialectHandler;
import io.github.yok.flexdblink.db.DbUnitConfigFactory;
import io.github.yok.flexdblink.util.DateTimeFormatSupport;
import io.github.yok.flexdblink.util.LobPathConstants;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Method;
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
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import oracle.sql.INTERVALDS;
import oracle.sql.INTERVALYM;
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
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.csv.CsvDataSet;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.IDataTypeFactory;

/**
 * Oracle-specific implementation of {@link DbDialectHandler}.
 *
 * <p>
 * Provides Oracle-dialect logic to format any JDBC/DBUnit value (numbers, dates/times, INTERVAL,
 * LOB, RAW, etc.) for CSV output and to convert CSV strings back into JDBC-bindable values when
 * loading.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
@Slf4j
public class OracleDialectHandler implements DbDialectHandler {

    // Base path used to store LOB files
    private final Path baseLobDir;
    // Date/time normalization utility
    private final DateTimeFormatSupport dateTimeFormatter;
    // Per-table metadata cache
    private final Map<String, ITableMetaData> tableMetaMap = new HashMap<>();
    // Per-table JDBC metadata cache used as fallback when DBUnit metadata is insufficient
    private final Map<String, Map<String, JdbcColumnSpec>> jdbcColumnSpecMap = new HashMap<>();
    // Factory that applies common DBUnit settings
    private final DbUnitConfigFactory configFactory;

    private final PathsConfig pathsConfig;

    // Flexible parser definitions
    private static final DateTimeFormatter FLEXIBLE_OFFSET_DATETIME_PARSER =
            new DateTimeFormatterBuilder().appendPattern("yyyyMMdd").optionalStart()
                    .appendLiteral('T').optionalEnd().optionalStart().appendLiteral(' ')
                    .optionalEnd().appendPattern("HHmmss").optionalStart()
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd()
                    .appendOffset("+HHmm", "Z").toFormatter(Locale.ENGLISH);

    private static final DateTimeFormatter FLEXIBLE_TIME_PARSER = new DateTimeFormatterBuilder()
            .appendPattern("HH:mm").optionalStart().appendPattern(":ss").optionalEnd()
            .optionalStart().appendLiteral('.')
            .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true).optionalEnd().toFormatter();

    private static final DateTimeFormatter FLEXIBLE_LOCAL_TIME_PARSER_COLON =
            new DateTimeFormatterBuilder().appendPattern("HH:mm").optionalStart()
                    .appendPattern(":ss").optionalEnd().optionalStart()
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd()
                    .toFormatter();

    private static final DateTimeFormatter FLEXIBLE_LOCAL_TIME_PARSER_NO_COLON =
            new DateTimeFormatterBuilder().appendPattern("HHmm").optionalStart().appendPattern("ss")
                    .optionalEnd().optionalStart()
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd()
                    .toFormatter();

    private static final DateTimeFormatter FLEXIBLE_TIMESTAMP_PARSER =
            new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss").optionalStart()
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd()
                    .toFormatter();

    private static final DateTimeFormatter FLEXIBLE_OFFSET_DATETIME_PARSER_COLON =
            new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd").optionalStart()
                    .appendLiteral('T').optionalEnd().optionalStart().appendLiteral(' ')
                    .optionalEnd().appendPattern("HH:mm:ss").optionalStart()
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd()
                    .appendOffset("+HH:MM", "Z").toFormatter();

    // OffsetTime 用（コロン形式）: HH:mm[:ss][.fraction]±HH:MM
    private static final DateTimeFormatter FLEXIBLE_OFFSET_TIME_PARSER_COLON =
            new DateTimeFormatterBuilder().appendPattern("HH:mm").optionalStart()
                    .appendPattern(":ss").optionalEnd().optionalStart()
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd()
                    // ここは "+HH:mm"（大文字MMはNG）
                    .appendOffset("+HH:mm", "Z").toFormatter(Locale.ENGLISH);

    // OffsetTime 用（非コロン形式）: HHmm[ss][.fraction]±HHmm
    private static final DateTimeFormatter FLEXIBLE_OFFSET_TIME_PARSER_NO_COLON =
            new DateTimeFormatterBuilder().appendPattern("HHmm").optionalStart().appendPattern("ss")
                    .optionalEnd().optionalStart()
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd()
                    .appendOffset("+HHmm", "Z").toFormatter(Locale.ENGLISH);

    private static final DateTimeFormatter DATE_LITERAL_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final DateTimeFormatter[] DATE_ONLY_FORMATTERS =
            {DateTimeFormatter.ISO_LOCAL_DATE, DateTimeFormatter.ofPattern("yyyy/MM/dd"),
                    DateTimeFormatter.BASIC_ISO_DATE, DateTimeFormatter.ofPattern("yyyy.MM.dd"),
                    DateTimeFormatter.ofPattern("yyyy年M月d日", Locale.JAPANESE)};

    /**
     * JDBC metadata snapshot for one column.
     */
    private static final class JdbcColumnSpec {
        private final int sqlType;
        private final String sqlTypeName;

        private JdbcColumnSpec(int sqlType, String sqlTypeName) {
            this.sqlType = sqlType;
            this.sqlTypeName = sqlTypeName;
        }
    }

    /**
     * Resolved column type information used during CSV-to-DB conversion.
     */
    private static final class ResolvedColumnSpec {
        private final int sqlType;
        private final String sqlTypeName;
        private final DataType dbUnitDataType;

        private ResolvedColumnSpec(int sqlType, String sqlTypeName, DataType dbUnitDataType) {
            this.sqlType = sqlType;
            this.sqlTypeName = sqlTypeName;
            this.dbUnitDataType = dbUnitDataType;
        }
    }

    /**
     * Returns the SQL type name for a given schema/table/column.
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
                // TYPE_NAME contains the DB-specific SQL type name
                return rs.getString("TYPE_NAME");
            } else {
                // If not found, raise an error
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
     * @return ordered list of primary-key column names (KEY_SEQ order); empty if no PK
     * @throws SQLException on JDBC errors
     */
    @Override
    public List<String> getPrimaryKeyColumns(Connection conn, String schema, String table)
            throws SQLException {
        // Accumulate column names with KEY_SEQ to preserve order
        List<Map.Entry<Short, String>> pkList = new ArrayList<>();

        DatabaseMetaData metaData = conn.getMetaData();

        // Store (COLUMN_NAME, KEY_SEQ) pairs
        try (ResultSet rs = metaData.getPrimaryKeys(null, schema, table)) {
            while (rs.next()) {
                String colName = rs.getString("COLUMN_NAME");
                short keySeq = rs.getShort("KEY_SEQ");
                pkList.add(new SimpleEntry<>(keySeq, colName));
            }
        }

        // Sort by KEY_SEQ and extract column names
        pkList.sort(Map.Entry.comparingByKey());

        List<String> pkColumns =
                pkList.stream().map(Map.Entry::getValue).collect(Collectors.toList());

        return pkColumns;
    }

    /**
     * Constructor. Caches table metadata from a DBUnit {@link IDataSet} while honoring
     * {@link DumpConfig#excludeTables}.
     *
     * @param dbConn DBUnit {@link DatabaseConnection}
     * @param dumpConfig dump.exclude-tables configuration
     * @param dbUnitConfig dbunit.dataTypeFactoryMode/preDirName configuration
     * @param configFactory {@link DbUnitConfigFactory} (for common DBUnit configuration)
     * @param dateTimeFormatter date/time formatter utility
     * @param pathsConfig data-path → load/dump path configuration
     * @throws Exception if metadata retrieval fails
     */
    public OracleDialectHandler(DatabaseConnection dbConn, DumpConfig dumpConfig,
            DbUnitConfig dbUnitConfig, DbUnitConfigFactory configFactory,
            DateTimeFormatSupport dateTimeFormatter, PathsConfig pathsConfig) throws Exception {
        this.configFactory = configFactory;
        this.dateTimeFormatter = dateTimeFormatter;
        this.pathsConfig = pathsConfig;

        // Build LOB directory path
        Path dumpBase = Paths.get(pathsConfig.getDump());
        this.baseLobDir = dumpBase.resolve(LobPathConstants.DIRECTORY_NAME);

        // Determine current schema
        Connection jdbcConn = dbConn.getConnection();
        String schema;
        try {
            schema = jdbcConn.getSchema();
        } catch (SQLException e) {
            schema = jdbcConn.getMetaData().getUserName();
        }

        // Apply exclusion list → determine target tables
        List<String> excludeTables = dumpConfig.getExcludeTables();
        List<String> targetTables = fetchTargetTables(jdbcConn, schema, excludeTables);

        // Cache metadata from the DBUnit dataset
        IDataSet ds = dbConn.createDataSet();
        for (String tbl : targetTables) {
            tableMetaMap.put(tbl.toUpperCase(), ds.getTableMetaData(tbl));
        }
        cacheJdbcColumnSpecs(jdbcConn, schema, targetTables);
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
        for (String table : targetTables) {
            Map<String, JdbcColumnSpec> columnMap = new HashMap<>();
            try (ResultSet rs = meta.getColumns(null, schema, table, "%")) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    int sqlType = rs.getInt("DATA_TYPE");
                    String typeName = rs.getString("TYPE_NAME");
                    columnMap.put(columnName.toUpperCase(Locale.ROOT),
                            new JdbcColumnSpec(sqlType, typeName));
                }
            }
            jdbcColumnSpecMap.put(table.toUpperCase(Locale.ROOT), columnMap);
        }
    }

    /**
     * Applies Oracle-specific session initialization to the JDBC {@link Connection}.
     *
     * <ul>
     * <li>NLS_DATE_FORMAT</li>
     * <li>NLS_TIMESTAMP_FORMAT</li>
     * <li>NLS_NUMERIC_CHARACTERS</li>
     * <li>CURRENT_SCHEMA</li>
     * </ul>
     *
     * @param connection JDBC connection to initialize
     * @throws SQLException if any statement fails during initialization
     */
    @Override
    public void prepareConnection(Connection connection) throws SQLException {
        String dateFormat = "YYYY-MM-DD HH24:MI:SS";
        String timestampFormat = "YYYY-MM-DD HH24:MI:SS.FF";
        String numericChars = ".,";
        String schema = connection.getSchema();
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("ALTER SESSION SET NLS_DATE_FORMAT = '" + dateFormat + "'");
            stmt.execute("ALTER SESSION SET NLS_TIMESTAMP_FORMAT = '" + timestampFormat + "'");
            stmt.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '" + numericChars + "'");
            stmt.execute("ALTER SESSION SET CURRENT_SCHEMA = " + schema);
        }
    }

    /**
     * Converts a CSV string value into a JDBC-bindable object according to the table/column type.
     *
     * @param table table name
     * @param column column name
     * @param csvValue CSV string value
     * @return JDBC-bindable object
     * @throws DataSetException if conversion fails
     */
    @Override
    public Object convertCsvValueToDbType(String table, String column, String csvValue)
            throws DataSetException {
        String str = (csvValue != null ? csvValue.trim() : null);
        if (str == null || str.isEmpty()) {
            return null;
        }
        ResolvedColumnSpec resolved = resolveColumnSpec(table, column);
        int sqlType = resolved.sqlType;
        try {
            switch (sqlType) {
                case Types.DECIMAL:
                case Types.NUMERIC:
                    return new BigDecimal(str);
                case Types.TINYINT:
                case Types.INTEGER:
                case Types.SMALLINT:
                    return Integer.valueOf(str);
                case Types.BIGINT:
                    return Long.valueOf(str);
                case Types.REAL:
                case Types.FLOAT:
                case Types.DOUBLE:
                    return Double.valueOf(str);
                case Types.DATE:
                    return parseDate(str, column);
                case Types.TIME:
                    return parseTime(str, column);
                case Types.TIME_WITH_TIMEZONE:
                    return parseOffsetTime(str, column);
                case Types.TIMESTAMP:
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    // Oracle JDBC proprietary codes for timestamp with time zone / local time zone.
                case -101:
                case -102:
                    return parseTimestamp(str, column);
                case Types.OTHER:
                case Types.JAVA_OBJECT:
                    return parseInterval(sqlType, resolved.sqlTypeName, str);
                default:
                    break;
            }
        } catch (Exception e) {
            throw new DataSetException(String.format(
                    "Date/Time/INTERVAL conversion failed: table=%s column=%s sqlType=%d value=%s",
                    table, column, sqlType, str), e);
        }
        // LOB file reference
        if (str.startsWith("file:")) {
            return loadLobFromFile(str.substring(5), table, column, sqlType,
                    resolved.dbUnitDataType);
        }
        // RAW HEX string
        if ((sqlType == Types.BINARY || sqlType == Types.VARBINARY
                || sqlType == Types.LONGVARBINARY) && isHexString(str)) {
            try {
                return Hex.decodeHex(str.toCharArray());
            } catch (Exception e) {
                throw new DataSetException(String.format(
                        "Failed to parse RAW hex string: column=%s, value=%s", column, str), e);
            }
        }
        return str;
    }

    /**
     * Formats a JDBC/DBUnit value for CSV output.
     *
     * @param columnName column name
     * @param dbValue JDBC object
     * @return string for CSV (null → empty string)
     * @throws SQLException if formatting requires DB access and fails
     */
    @Override
    public String formatDbValueForCsv(String columnName, Object dbValue) throws SQLException {
        return dbValue == null ? "" : dbValue.toString();
    }

    /**
     * Resolves the schema name from a connection entry.
     *
     * @param entry connection entry
     * @return schema name (upper-cased user name)
     */
    @Override
    public String resolveSchema(ConnectionConfig.Entry entry) {
        return entry.getUser().toUpperCase();
    }

    /**
     * Creates a DBUnit {@link DatabaseConnection} with the given JDBC connection and schema.
     *
     * <p>
     * Applies the custom data type factory and common DBUnit configuration.
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
        DatabaseConnection dbConn = new DatabaseConnection(jdbc, schema);
        DatabaseConfig config = dbConn.getConfig();
        configFactory.configure(config, getDataTypeFactory());

        return dbConn;
    }

    /**
     * Writes LOB data to a file.
     *
     * @param table table name
     * @param column column name
     * @param lobData Blob/Clob/byte[]/String
     * @param outFile output path
     * @throws IOException on file I/O errors
     * @throws SQLException on JDBC errors
     */
    @Override
    public void writeLobFile(String table, String column, Object lobData, Path outFile)
            throws IOException, SQLException {
        Files.createDirectories(outFile.getParent(), new FileAttribute<?>[0]);
        if (lobData instanceof byte[]) {
            Files.write(outFile, (byte[]) lobData);
        } else if (lobData instanceof String) {
            Files.writeString(outFile, (String) lobData, StandardCharsets.UTF_8);
        } else if (lobData instanceof Blob) {
            try (InputStream in = ((Blob) lobData).getBinaryStream()) {
                Files.copy(in, outFile, StandardCopyOption.REPLACE_EXISTING);
            }
        } else if (lobData instanceof Clob) {
            try (Reader reader = ((Clob) lobData).getCharacterStream();
                    BufferedWriter writer =
                            Files.newBufferedWriter(outFile, StandardCharsets.UTF_8)) {
                char[] buffer = new char[4096];
                int read;
                while ((read = reader.read(buffer)) != -1) {
                    writer.write(buffer, 0, read);
                }
            }
        } else {
            throw new IllegalArgumentException(
                    "Unsupported LOB type: " + lobData.getClass().getName());
        }
        // Build relative path from dataPath (normalize/separator unification)
        Path dataDir = Paths.get(pathsConfig.getDataPath()).toAbsolutePath().normalize();
        Path full = outFile.toAbsolutePath().normalize();

        String relStr = FilenameUtils.separatorsToUnix(dataDir.relativize(full).toString());

        log.info("  LOB file written: {}", relStr);
    }

    /**
     * Reads a LOB file and converts it into a JDBC-bindable object.
     *
     * @param fileRef file name
     * @param table table name
     * @param column column name
     * @param baseDir base directory where LOB files are stored
     * @return byte[] for BLOB, or String for CLOB
     * @throws IOException if file reading fails
     * @throws DataSetException if metadata is inconsistent
     */
    @Override
    public Object readLobFile(String fileRef, String table, String column, File baseDir)
            throws IOException, DataSetException {
        File lobFile = new File(new File(baseDir, LobPathConstants.DIRECTORY_NAME), fileRef);
        if (!lobFile.exists()) {
            throw new DataSetException("LOB file not found: " + lobFile.getAbsolutePath());
        }
        ResolvedColumnSpec resolved = resolveColumnSpec(table, column);
        if (isBlobSqlType(resolved.sqlType, resolved.dbUnitDataType)) {
            return Files.readAllBytes(lobFile.toPath());
        }
        if (isClobSqlType(resolved.sqlType, resolved.dbUnitDataType)) {
            return Files.readString(lobFile.toPath(), StandardCharsets.UTF_8);
        }
        throw new DataSetException("file: only supported for BLOB/CLOB: " + table + "." + column);
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
     * Formats date/time (including INTERVAL) values as CSV strings.
     *
     * @param column column name
     * @param dbValue JDBC value
     * @param conn JDBC connection (for timezone/session info if needed)
     * @return formatted string
     * @throws SQLException if formatting fails and requires DB access
     */
    @Override
    public String formatDateTimeColumn(String column, Object dbValue, Connection conn)
            throws SQLException {
        if (dbValue == null) {
            return "";
        }
        try {
            if (isOracleTimestampWithTimeZoneValue(dbValue)) {
                return formatOracleTimestampWithTimeZone(dbValue, conn);
            }
            return dateTimeFormatter.formatJdbcDateTime(column, dbValue, conn);
        } catch (Exception e) {
            log.debug("Failed to format DATE/INTERVAL: column={} value={} → fallback to toString()",
                    column, dbValue, e);
            return dbValue.toString();
        }
    }

    /**
     * Returns whether value class is Oracle TIMESTAMPTZ/TIMESTAMPLTZ.
     *
     * @param value JDBC value
     * @return {@code true} when Oracle timezone-aware timestamp class is detected
     */
    private boolean isOracleTimestampWithTimeZoneValue(Object value) {
        String className = value.getClass().getName();
        return "oracle.sql.TIMESTAMPTZ".equals(className)
                || "oracle.jdbc.OracleTIMESTAMPTZ".equals(className)
                || "oracle.sql.TIMESTAMPLTZ".equals(className)
                || "oracle.jdbc.OracleTimestampltz".equals(className);
    }

    /**
     * Formats Oracle TIMESTAMPTZ/TIMESTAMPLTZ value into stable textual representation.
     *
     * @param value Oracle driver-specific timestamp object
     * @param connection JDBC connection passed to {@code stringValue(Connection)}
     * @return normalized timestamp text
     * @throws Exception when reflective access fails
     */
    private String formatOracleTimestampWithTimeZone(Object value, Connection connection)
            throws Exception {
        Method method = value.getClass().getMethod("stringValue", Connection.class);
        String raw = (String) method.invoke(value, connection);
        return normalizeOracleTimestampWithTimeZone(raw);
    }

    /**
     * Normalizes Oracle timezone-aware timestamp text.
     *
     * @param raw raw timestamp text
     * @return normalized timestamp text
     */
    private String normalizeOracleTimestampWithTimeZone(String raw) {
        if (raw == null) {
            return null;
        }
        String trimmed = raw.trim();
        Matcher offsetMatcher = Pattern.compile(
                "^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})(?:\\.\\d+)? ([+-]\\d{2}):(\\d{2})$")
                .matcher(trimmed);
        if (offsetMatcher.matches()) {
            return offsetMatcher.group(1) + " " + offsetMatcher.group(2) + offsetMatcher.group(3);
        }
        Matcher regionMatcher = Pattern.compile(
                "^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})(?:\\.\\d+)? ([A-Za-z_]+/[A-Za-z_]+)$")
                .matcher(trimmed);
        if (regionMatcher.matches()) {
            try {
                LocalDateTime ldt = LocalDateTime.parse(regionMatcher.group(1), DATE_LITERAL_FMT);
                String offset = ldt.atZone(ZoneId.of(regionMatcher.group(2))).getOffset().getId()
                        .replace(":", "");
                return regionMatcher.group(1) + " " + offset;
            } catch (DateTimeParseException ex) {
                log.warn("Failed to resolve region offset in TIMESTAMPTZ: raw='{}'", raw, ex);
                return trimmed;
            }
        }
        return trimmed.replaceAll("\\.0+$", "");
    }

    /**
     * Returns whether dump should treat the SQL type as date/time in Oracle.
     *
     * @param sqlType JDBC SQL type code
     * @param sqlTypeName database-specific SQL type name
     * @return {@code true} for standard and Oracle-specific temporal types
     */
    @Override
    public boolean isDateTimeTypeForDump(int sqlType, String sqlTypeName) {
        if (DbDialectHandler.super.isDateTimeTypeForDump(sqlType, sqlTypeName)) {
            return true;
        }
        if (sqlType == -101 || sqlType == -102 || sqlType == Types.TIMESTAMP_WITH_TIMEZONE) {
            return true;
        }
        if (sqlTypeName == null) {
            return false;
        }
        String normalized = sqlTypeName.toUpperCase(Locale.ROOT);
        return normalized.startsWith("TIMESTAMP WITH TIME ZONE")
                || normalized.startsWith("TIMESTAMP WITH LOCAL TIME ZONE");
    }

    /**
     * Returns whether dump should treat the SQL type as binary in Oracle.
     *
     * @param sqlType JDBC SQL type code
     * @param sqlTypeName database-specific SQL type name
     * @return {@code true} for standard binary types and Oracle RAW/LONG RAW
     */
    @Override
    public boolean isBinaryTypeForDump(int sqlType, String sqlTypeName) {
        if (DbDialectHandler.super.isBinaryTypeForDump(sqlType, sqlTypeName)) {
            return true;
        }
        if (sqlTypeName == null) {
            return false;
        }
        String normalized = sqlTypeName.toUpperCase(Locale.ROOT);
        return "RAW".equals(normalized) || "LONG RAW".equals(normalized);
    }

    /**
     * Returns whether dump should use raw JDBC text for Oracle INTERVAL columns.
     *
     * @param columnName column name
     * @param sqlType JDBC SQL type
     * @param sqlTypeName database-specific SQL type name
     * @return {@code true} when the SQL type is Oracle INTERVAL
     */
    @Override
    public boolean shouldUseRawTemporalValueForDump(String columnName, int sqlType,
            String sqlTypeName) {
        return isIntervalSqlTypeName(sqlTypeName);
    }

    /**
     * Normalizes raw Oracle INTERVAL text for CSV dump output.
     *
     * @param columnName column name
     * @param rawValue raw JDBC string value
     * @return normalized interval text
     */
    @Override
    public String normalizeRawTemporalValueForDump(String columnName, String rawValue) {
        if (rawValue == null) {
            return "";
        }
        if (isDaySecondIntervalColumn(columnName)) {
            return rawValue.replaceAll("\\.0+$", "");
        }
        return rawValue;
    }

    /**
     * Parses a CSV date/time string into a JDBC-bindable object.
     *
     * @param column column name
     * @param csvValue CSV date/time string
     * @return parsed date/time/INTERVAL object
     * @throws Exception on parsing failure
     */
    @Override
    public Object parseDateTimeValue(String column, String csvValue) throws Exception {
        String str = (csvValue != null ? csvValue.trim() : null);
        if (str == null || str.isEmpty()) {
            return null;
        }
        if (str.matches("\\d+-\\d+")) {
            return new INTERVALYM(str);
        }
        if (str.matches("\\d+ \\d{2}:\\d{2}:\\d{2}(\\.\\d+)?")) {
            return new INTERVALDS(str);
        }
        try {
            OffsetDateTime odt = OffsetDateTime.parse(str, FLEXIBLE_OFFSET_DATETIME_PARSER);
            return Timestamp.from(odt.toInstant());
        } catch (DateTimeParseException ex) {
            // Try DATE-only patterns
            for (DateTimeFormatter fmt : DATE_ONLY_FORMATTERS) {
                try {
                    LocalDate ld = LocalDate.parse(str, fmt);
                    return Date.valueOf(ld);
                } catch (DateTimeParseException ignore) {
                    log.debug("LocalDate parse mismatch: fmt={} value={}", fmt, str);
                }
            }
            // TIME
            try {
                LocalTime lt = LocalTime.parse(str, FLEXIBLE_TIME_PARSER);
                return Time.valueOf(lt);
            } catch (DateTimeParseException ignore) {
                log.debug("LocalTime parse failed: {}", str);
            }
            // TIMESTAMP
            LocalDateTime ldt = LocalDateTime.parse(str, DATE_LITERAL_FMT);
            return Timestamp.valueOf(ldt);
        }
    }

    /**
     * Returns whether row comparison should use raw DB string values for Oracle INTERVAL columns.
     *
     * @param sqlTypeName database-specific SQL type name
     * @return {@code true} when SQL type is Oracle INTERVAL
     */
    @Override
    public boolean shouldUseRawValueForComparison(String sqlTypeName) {
        return isIntervalSqlTypeName(sqlTypeName);
    }

    /**
     * Normalizes Oracle INTERVAL values for row comparison.
     *
     * @param columnName column name
     * @param sqlTypeName database-specific SQL type name
     * @param value target value
     * @return normalized value
     */
    @Override
    public String normalizeValueForComparison(String columnName, String sqlTypeName, String value) {
        if (value == null) {
            return null;
        }
        if (!isIntervalSqlTypeName(sqlTypeName)) {
            return value;
        }
        String upper = sqlTypeName.toUpperCase(Locale.ROOT);
        if (upper.contains("YEAR")) {
            return normalizeYearMonthInterval(value);
        }
        return normalizeDaySecondInterval(value);
    }

    /**
     * Returns whether the SQL type name represents Oracle INTERVAL.
     *
     * @param sqlTypeName SQL type name
     * @return {@code true} for INTERVAL types
     */
    private boolean isIntervalSqlTypeName(String sqlTypeName) {
        if (sqlTypeName == null) {
            return false;
        }
        return sqlTypeName.toUpperCase(Locale.ROOT).contains("INTERVAL");
    }

    /**
     * Returns whether the column is treated as INTERVAL DAY TO SECOND fixture column.
     *
     * @param columnName column name
     * @return {@code true} when DS interval naming convention is matched
     */
    private boolean isDaySecondIntervalColumn(String columnName) {
        if (columnName == null) {
            return false;
        }
        return "INTERVAL_DS_COL".equalsIgnoreCase(columnName)
                || "IV_DS_COL".equalsIgnoreCase(columnName);
    }

    /**
     * Normalizes INTERVAL DAY TO SECOND text to {@code +DD HH:MM:SS} style.
     *
     * @param raw raw INTERVAL text
     * @return normalized text
     */
    private String normalizeDaySecondInterval(String raw) {
        Pattern pattern = Pattern.compile("([+-]?)(\\d+)\\s+(\\d+):(\\d+):(\\d+)(?:\\.\\d+)?");
        Matcher matcher = pattern.matcher(raw.trim());
        if (matcher.matches()) {
            String sign = matcher.group(1);
            if (!"-".equals(sign)) {
                sign = "+";
            }
            String days = String.format("%02d", Integer.parseInt(matcher.group(2)));
            String hours = String.format("%02d", Integer.parseInt(matcher.group(3)));
            String minutes = String.format("%02d", Integer.parseInt(matcher.group(4)));
            String seconds = String.format("%02d", Integer.parseInt(matcher.group(5)));
            return sign + days + " " + hours + ":" + minutes + ":" + seconds;
        }
        return raw.trim();
    }

    /**
     * Normalizes INTERVAL YEAR TO MONTH text to {@code +YY-MM} style.
     *
     * @param raw raw INTERVAL text
     * @return normalized text
     */
    private String normalizeYearMonthInterval(String raw) {
        Pattern pattern = Pattern.compile("([+-]?)(\\d+)-(\\d+)");
        Matcher matcher = pattern.matcher(raw.trim());
        if (matcher.matches()) {
            String sign = matcher.group(1);
            if (!"-".equals(sign)) {
                sign = "+";
            }
            String years = String.format("%02d", Integer.parseInt(matcher.group(2)));
            String months = String.format("%02d", Integer.parseInt(matcher.group(3)));
            return sign + years + "-" + months;
        }
        return raw.trim();
    }

    /**
     * Returns SQL to fetch the next sequence value.
     *
     * @param sequenceName sequence name
     * @return SQL
     */
    @Override
    public String getNextSequenceSql(String sequenceName) {
        return String.format("SELECT %s.NEXTVAL FROM DUAL", sequenceName);
    }

    /**
     * Returns the SQL template used to retrieve generated keys after INSERT.
     *
     * @return SQL template
     */
    @Override
    public String getGeneratedKeyRetrievalSql() {
        return " RETURNING %s INTO ?";
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
        return true;
    }

    /**
     * Indicates whether IDENTITY columns are supported.
     *
     * @return {@code false}
     */
    @Override
    public boolean supportsIdentityColumns() {
        return false;
    }

    /**
     * Applies pagination to a SELECT statement.
     *
     * @param baseSql base SELECT SQL
     * @param offset rows to skip
     * @param limit max rows to fetch
     * @return SELECT with dialect-specific pagination
     */
    @Override
    public String applyPagination(String baseSql, int offset, int limit) {
        return String.format("%s OFFSET %d ROWS FETCH NEXT %d ROWS ONLY", baseSql, offset, limit);
    }

    /**
     * Quotes an identifier (table/column name) using double quotes.
     *
     * @param identifier identifier to quote
     * @return quoted identifier
     */
    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    /**
     * Returns the boolean TRUE literal.
     *
     * @return "1"
     */
    @Override
    public String getBooleanTrueLiteral() {
        return "1";
    }

    /**
     * Returns the boolean FALSE literal.
     *
     * @return "0"
     */
    @Override
    public String getBooleanFalseLiteral() {
        return "0";
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
     * Formats a {@link LocalDateTime} as a SQL date/time literal.
     *
     * @param dateTime LocalDateTime value
     * @return SQL using TO_DATE
     */
    @Override
    public String formatDateLiteral(LocalDateTime dateTime) {
        return String.format("TO_DATE('%s','YYYY-MM-DD HH24:MI:SS')",
                dateTime.format(DATE_LITERAL_FMT));
    }

    /**
     * Builds a MERGE (UPSERT) statement.
     *
     * @param table table name
     * @param keyCols key columns
     * @param insertCols INSERT columns
     * @param updateCols UPDATE columns
     * @return MERGE SQL
     */
    @Override
    public String buildUpsertSql(String table, List<String> keyCols, List<String> insertCols,
            List<String> updateCols) {
        String sourceCols = keyCols.stream().map(c -> "? AS " + quoteIdentifier(c))
                .collect(Collectors.joining(", "))
                + (insertCols.isEmpty() ? ""
                        : ", " + insertCols.stream().map(c -> "? AS " + quoteIdentifier(c))
                                .collect(Collectors.joining(", ")));
        String onClause =
                keyCols.stream().map(c -> String.format("t.%1$s = s.%1$s", quoteIdentifier(c)))
                        .collect(Collectors.joining(" AND "));
        String updateSet =
                updateCols.stream().map(c -> String.format("t.%1$s = s.%1$s", quoteIdentifier(c)))
                        .collect(Collectors.joining(", "));
        List<String> allInsertCols = new ArrayList<>(keyCols);
        allInsertCols.addAll(insertCols);
        String insertColsSql =
                allInsertCols.stream().map(this::quoteIdentifier).collect(Collectors.joining(","));
        String insertValuesSql = allInsertCols.stream().map(c -> "s." + quoteIdentifier(c))
                .collect(Collectors.joining(","));
        return String.format(
                "MERGE INTO %s t USING (SELECT %s FROM DUAL) s ON (%s) "
                        + "WHEN MATCHED THEN UPDATE SET %s "
                        + "WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
                quoteIdentifier(table), sourceCols, onClause, updateSet, insertColsSql,
                insertValuesSql);
    }

    /**
     * Returns SQL to create a global temporary table.
     *
     * @param table table name
     * @param columnsAndTypes map of column → type literal
     * @return CREATE GLOBAL TEMPORARY TABLE SQL
     */
    @Override
    public String getCreateTempTableSql(String table, Map<String, String> columnsAndTypes) {
        String colsDefinition = columnsAndTypes.entrySet().stream()
                .map(e -> quoteIdentifier(e.getKey()) + " " + e.getValue())
                .collect(Collectors.joining(", "));
        return String.format("CREATE GLOBAL TEMPORARY TABLE %s (%s) ON COMMIT PRESERVE ROWS",
                quoteIdentifier(table), colsDefinition);
    }

    /**
     * Applies a SELECT ... FOR UPDATE clause.
     *
     * @param baseSql base SELECT SQL
     * @return SQL with FOR UPDATE
     */
    @Override
    public String applyForUpdate(String baseSql) {
        return baseSql + " FOR UPDATE";
    }

    /**
     * Indicates whether batch updates are supported.
     *
     * @return {@code true} (batch updates supported)
     */
    @Override
    public boolean supportsBatchUpdates() {
        return true;
    }

    /**
     * Returns the DBUnit data type factory.
     *
     * @return {@link CustomOracleDataTypeFactory}
     */
    @Override
    public IDataTypeFactory getDataTypeFactory() {
        return new CustomOracleDataTypeFactory();
    }

    /**
     * Checks whether the table contains any NOT NULL LOB columns.
     *
     * @param conn JDBC connection
     * @param schema schema name
     * @param table table name
     * @param lobCols LOB columns
     * @return {@code true} if a NOT NULL LOB column exists
     * @throws SQLException on metadata retrieval errors
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
     * Determines whether the specified table has a primary key.
     *
     * @param conn JDBC connection
     * @param schema schema name
     * @param table table name
     * @return {@code true} if a primary key exists
     * @throws SQLException on metadata retrieval errors
     */
    @Override
    public boolean hasPrimaryKey(Connection conn, String schema, String table) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet rs = meta.getPrimaryKeys(null, schema, table)) {
            return rs.next();
        }
    }

    /**
     * Parses the table CSV within a CSV directory and extracts LOB column definitions.
     *
     * @param csvDirPath CSV base directory
     * @param tableName table name
     * @return array of LOB columns
     * @throws IOException if reading CSV fails
     * @throws DataSetException on metadata inconsistency
     */
    @Override
    public Column[] getLobColumns(Path csvDirPath, String tableName)
            throws IOException, DataSetException {

        log.info("CSVParser class={}", CSVParser.class.getName());
        log.info("CSVParser from={}", CSVParser.class.getResource("CSVParser.class"));
        String implVer = CSVParser.class.getPackage().getImplementationVersion();
        log.info("CSV implVersion={}", implVer);

        // Full path to CSV file
        Path csv = csvDirPath.resolve(tableName + ".csv");

        // Compute a dataPath-relative path (e.g., load/pre/DB1/BINARY_TEST_TABLE.csv)
        Path dataDir = Path.of(pathsConfig.getDataPath()).toAbsolutePath().normalize();
        Path full = csv.toAbsolutePath().normalize();
        Path rel = dataDir.relativize(full);
        // Unify separators to UNIX style
        String relPath = FilenameUtils.separatorsToUnix(rel.toString());

        log.info("  Extracting LOB columns from: {}", relPath);

        if (!Files.exists(csv, LinkOption.NOFOLLOW_LINKS)) {
            log.info("CSV file does not exist: {}", csv);
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
                    if (!lobFlags[i] && record.get(i).startsWith("file:")) {
                        lobFlags[i] = true;
                    }
                }
            }
            log.debug("CSV headers: {}", headers);
        }
        CsvDataSet tmp = new CsvDataSet(csvDirPath.toFile());
        Column[] allCols = tmp.getTable(tableName).getTableMetaData().getColumns();
        Map<String, Integer> headerIndex = new HashMap<>();
        for (int i = 0; i < headers.size(); i++) {
            headerIndex.put(headers.get(i), i);
        }
        List<Column> result = new ArrayList<>();
        for (Column col : allCols) {
            Integer idx = headerIndex.get(col.getColumnName());
            if (idx != null && lobFlags[idx]) {
                int sqlType = col.getDataType().getSqlType();
                if (DataType.BLOB.equals(col.getDataType()) || sqlType == Types.BLOB
                        || DataType.CLOB.equals(col.getDataType()) || sqlType == Types.CLOB) {
                    result.add(col);
                }
            }
        }
        return result.toArray(new Column[0]);
    }

    /**
     * Logs the table definition (column names and types).
     *
     * @param conn JDBC connection
     * @param schema schema name
     * @param table table name
     * @param dbId identifier shown in the log
     * @throws SQLException on metadata retrieval errors
     */
    @Override
    public void logTableDefinition(Connection conn, String schema, String table, String dbId)
            throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet rs = meta.getColumns(null, schema, table, null)) {
            while (rs.next()) {
                String colName = rs.getString("COLUMN_NAME");
                String typeName = rs.getString("TYPE_NAME");
                int columnSize = rs.getInt("COLUMN_SIZE");
                int charOctetLength = rs.getInt("CHAR_OCTET_LENGTH");
                log.debug("[{}] def: tbl={} col={} type={} lenC={} lenB={}", dbId, table, colName,
                        typeName, columnSize, charOctetLength);
            }
        }
    }

    /**
     * Parses a date-only string into {@link java.sql.Date}.
     *
     * <p>
     * Tries multiple permissive patterns in the order defined by {@code DATE_ONLY_FORMATTERS}
     * (e.g., {@code ISO_LOCAL_DATE}, {@code yyyy/MM/dd}, {@code yyyyMMdd}, {@code yyyy.MM.dd}, and
     * a Japanese literal form). Each attempt is logged. If none match, wraps the last
     * {@link DateTimeParseException} in a {@link DataSetException}.
     * </p>
     *
     * @param str raw date text (e.g., {@code "2024-07-01"})
     * @param column column name (for logging context)
     * @return {@link java.sql.Date} at start of day
     * @throws DataSetException if no supported format matches
     */
    private Date parseDate(String str, String column) throws DataSetException {
        DateTimeParseException lastEx = null;
        for (DateTimeFormatter fmt : DATE_ONLY_FORMATTERS) {
            try {
                LocalDate ld = LocalDate.parse(str, fmt);
                log.debug("[DATE] column={} value={} → format={} OK", column, str, fmt);
                return Date.valueOf(ld);
            } catch (DateTimeParseException ex) {
                lastEx = ex;
                log.debug("[DATE] column={} value={} → format={} NG", column, str, fmt, ex);
            }
        }
        log.error("DATE conversion failed: column={} value={} (no format matched)", column, str,
                lastEx);
        throw new DataSetException(
                String.format("Failed to convert DATE: column=%s, value=%s", column, str), lastEx);
    }

    /**
     * Parses a local time string into {@link java.sql.Time}.
     *
     * <p>
     * Accepts {@code HH:mm[:ss[.fraction]]} and {@code HHmm[ss[.fraction]]}. Falls back from the
     * colon-separated parser to the compact parser. On failure, throws a {@link DataSetException}
     * with the root cause.
     * </p>
     *
     * @param str raw time text (e.g., {@code "14:05"}, {@code "140501.123"})
     * @param column column name (for logging context)
     * @return {@link java.sql.Time}
     * @throws DataSetException if the value cannot be parsed as a local time
     */
    private Time parseTime(String str, String column) throws DataSetException {
        try {
            return Time.valueOf(LocalTime.parse(str, FLEXIBLE_LOCAL_TIME_PARSER_COLON));
        } catch (DateTimeParseException ex1) {
            log.debug("[TIME] colon parser failed: column={} value={}", column, str, ex1);
            try {
                return Time.valueOf(LocalTime.parse(str, FLEXIBLE_LOCAL_TIME_PARSER_NO_COLON));
            } catch (DateTimeParseException ex2) {
                log.debug("TIME conversion failed: column={} value={}", column, str, ex2);
                throw new DataSetException(
                        String.format("Failed to convert TIME: column=%s, value=%s", column, str),
                        ex2);
            }
        }
    }

    /**
     * Parses an offset time string into {@link java.time.OffsetTime}.
     *
     * <p>
     * Supports {@code HH:mm[:ss[.fraction]]±HH:MM} and {@code HHmmss[.fraction]±HHmm}. Tries the
     * colon style parser first, then the compact one.
     * </p>
     *
     * @param str raw offset time text (e.g., {@code "10:15:00+09:00"}, {@code "101500+0900"})
     * @param column column name (for logging context)
     * @return {@link OffsetTime}
     * @throws DataSetException if parsing fails
     */
    private OffsetTime parseOffsetTime(String str, String column) throws DataSetException {
        try {
            return OffsetTime.parse(str, FLEXIBLE_OFFSET_TIME_PARSER_COLON);
        } catch (Exception ex1) {
            try {
                return OffsetTime.parse(str, FLEXIBLE_OFFSET_TIME_PARSER_NO_COLON);
            } catch (Exception ex2) {
                log.debug("TIME_WITH_TIMEZONE conversion failed: column={} value={}", column, str,
                        ex2);
                throw new DataSetException(
                        String.format("Failed to convert TIME_WITH_TIMEZONE: column=%s value=%s",
                                column, str),
                        ex2);
            }
        }
    }

    /**
     * Parses a timestamp-like string into {@link java.sql.Timestamp}.
     *
     * <p>
     * Normalization: replaces {@code 'T'} with space and {@code '/'} with {@code '-'}. <br>
     * Tries, inorder:
     * </p>
     * <ol>
     * <li>{@code yyyy-MM-dd HH:mm:ss Z}</li>
     * <li>{@code yyyy-MM-dd HH:mm:ss[.fraction]±HH:MM}</li>
     * <li>date-only patterns (interpreted as start-of-day)</li>
     * <li>{@code yyyy-MM-dd HH:mm:ss[.fraction]}</li>
     * </ol>
     * On total failure, throws {@link DataSetException}.
     *
     * @param str raw timestamp text
     * @param column column name (for logging context)
     * @return {@link Timestamp}
     * @throws DataSetException if no supported pattern matches
     */
    private Timestamp parseTimestamp(String str, String column) throws DataSetException {
        String normalized = str.replace('T', ' ').replace('/', '-').trim();
        try {
            DateTimeFormatter fmtOffset =
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss Z", Locale.ENGLISH);
            OffsetDateTime odt = OffsetDateTime.parse(normalized, fmtOffset);
            return Timestamp.from(odt.toInstant());
        } catch (DateTimeParseException e) {
            log.debug("fmtOffset parse failed: column={} value={}", column, normalized);
        }
        try {
            OffsetDateTime odt =
                    OffsetDateTime.parse(normalized, FLEXIBLE_OFFSET_DATETIME_PARSER_COLON);
            return Timestamp.from(odt.toInstant());
        } catch (DateTimeParseException e) {
            log.debug("FLEXIBLE_OFFSET_DATETIME_PARSER_COLON parse failed: column={} value={}",
                    column, normalized);
        }
        for (DateTimeFormatter fmt : DATE_ONLY_FORMATTERS) {
            try {
                LocalDate ld = LocalDate.parse(normalized, fmt);
                return Timestamp.valueOf(ld.atStartOfDay());
            } catch (DateTimeParseException ignore) {
                log.debug("DATE-only format {} parse failed: column={} value={}", fmt, column,
                        normalized);
            }
        }
        try {
            LocalDateTime ldt = LocalDateTime.parse(normalized, FLEXIBLE_TIMESTAMP_PARSER);
            return Timestamp.valueOf(ldt);
        } catch (DateTimeParseException e) {
            log.debug("FLEXIBLE_TIMESTAMP_PARSER parse failed: column={} value={}", column,
                    normalized);
            throw new DataSetException(
                    String.format("Failed to convert TIMESTAMP: column=%s, value=%s", column, str),
                    e);
        }
    }

    /**
     * Interprets INTERVAL strings as Oracle {@code INTERVALYM} or {@code INTERVALDS} objects.
     *
     * <p>
     * Uses {@code sqlType} and {@code sqlTypeName} to decide: returns {@link INTERVALYM} for YEAR
     * TO MONTH and {@link INTERVALDS} for DAY TO SECOND; otherwise returns the original string.
     * </p>
     *
     * @param sqlType JDBC SQL type code
     * @param sqlTypeName database-specific type name
     * @param str raw interval text
     * @return Oracle INTERVAL object or the original string
     */
    private Object parseInterval(int sqlType, String sqlTypeName, String str) {
        if (sqlTypeName == null) {
            if (sqlType == -103) {
                return new INTERVALYM(str);
            }
            if (sqlType == -104) {
                return new INTERVALDS(str);
            }
            return str;
        }
        if (sqlType == -103 || sqlTypeName.startsWith("INTERVAL YEAR")) {
            return new INTERVALYM(str);
        }
        if (sqlType == -104 || sqlTypeName.startsWith("INTERVAL DAY")) {
            return new INTERVALDS(str);
        }
        return str;
    }

    /**
     * Loads a LOB from disk under the preconfigured base directory, returning a JDBC-bindable
     * value.
     *
     * @param fileName file name relative to {@code baseLobDir}
     * @param table table name (for error messages)
     * @param column column name (for error messages)
     * @param sqlType JDBC SQL type of the target column
     * @param dataType DBUnit data type of the target column
     * @return {@code byte[]} for BLOB or {@link String} for CLOB/NCLOB
     * @throws DataSetException if the file is missing, unreadable, or the type is unsupported
     */
    private Object loadLobFromFile(String fileName, String table, String column, int sqlType,
            DataType dataType) throws DataSetException {
        File lobFile = new File(baseLobDir.toFile(), fileName);
        if (!lobFile.exists()) {
            throw new DataSetException("LOB file does not exist: " + lobFile.getAbsolutePath());
        }
        try {
            if (isBlobSqlType(sqlType, dataType)) {
                return Files.readAllBytes(lobFile.toPath());
            }
            if (isClobSqlType(sqlType, dataType)) {
                return Files.readString(lobFile.toPath(), StandardCharsets.UTF_8);
            }
        } catch (IOException e) {
            throw new DataSetException("Failed to read LOB file: " + lobFile.getAbsolutePath(), e);
        }
        throw new DataSetException(
                String.format("file: reference is supported only for BLOB/CLOB: column=%s, type=%s",
                        column, dataType));
    }

    /**
     * Resolves column type information by preferring DBUnit metadata and falling back to JDBC
     * metadata.
     *
     * @param table table name
     * @param column column name
     * @return resolved type information
     * @throws DataSetException if table/column metadata cannot be resolved
     */
    private ResolvedColumnSpec resolveColumnSpec(String table, String column)
            throws DataSetException {
        String tableKey = table.toUpperCase(Locale.ROOT);
        Column dbUnitColumn = findColumnInDbUnitMeta(tableKey, column);
        JdbcColumnSpec jdbcSpec = findJdbcColumnSpec(tableKey, column);
        if (dbUnitColumn != null) {
            DataType dataType = dbUnitColumn.getDataType();
            if (jdbcSpec != null && isUnknownDbUnitType(dataType)) {
                return new ResolvedColumnSpec(jdbcSpec.sqlType, jdbcSpec.sqlTypeName, dataType);
            }
            return new ResolvedColumnSpec(dataType.getSqlType(), dataType.getSqlTypeName(),
                    dataType);
        }

        if (jdbcSpec != null) {
            return new ResolvedColumnSpec(jdbcSpec.sqlType, jdbcSpec.sqlTypeName, null);
        }
        throw new DataSetException(
                String.format("Column metadata not found: table=%s, column=%s", table, column));
    }

    /**
     * Finds a column in cached DBUnit metadata.
     *
     * @param tableKey upper-cased table key
     * @param column column name
     * @return matching column metadata or {@code null}
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
     * @param tableKey upper-cased table key
     * @param column column name
     * @return JDBC column spec or {@code null}
     */
    private JdbcColumnSpec findJdbcColumnSpec(String tableKey, String column) {
        Map<String, JdbcColumnSpec> columnMap = jdbcColumnSpecMap.get(tableKey);
        if (columnMap == null) {
            return null;
        }
        return columnMap.get(column.toUpperCase(Locale.ROOT));
    }

    /**
     * Determines whether the given type represents a BLOB.
     *
     * @param sqlType JDBC SQL type
     * @param dataType DBUnit data type
     * @return {@code true} if BLOB
     */
    private boolean isBlobSqlType(int sqlType, DataType dataType) {
        if (sqlType == Types.BLOB) {
            return true;
        }
        if (dataType == null) {
            return false;
        }
        if (DataType.BLOB.equals(dataType)) {
            return true;
        }
        return dataType.getSqlType() == Types.BLOB;
    }

    /**
     * Determines whether the given type represents a CLOB/NCLOB.
     *
     * @param sqlType JDBC SQL type
     * @param dataType DBUnit data type
     * @return {@code true} if CLOB/NCLOB
     */
    private boolean isClobSqlType(int sqlType, DataType dataType) {
        if (sqlType == Types.CLOB || sqlType == Types.NCLOB) {
            return true;
        }
        if (dataType == null) {
            return false;
        }
        if (DataType.CLOB.equals(dataType)) {
            return true;
        }
        int dbUnitSqlType = dataType.getSqlType();
        return dbUnitSqlType == Types.CLOB || dbUnitSqlType == Types.NCLOB;
    }

    /**
     * Determines whether DBUnit type information is unresolved.
     *
     * @param dataType DBUnit data type
     * @return {@code true} if unresolved/unknown
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
     * Returns whether the given string consists solely of hexadecimal characters.
     *
     * <p>
     * Used for RAW/VARBINARY columns whose CSV representation is a hex string.
     * </p>
     *
     * @param s candidate string
     * @return {@code true} if non-empty and matches {@code [0-9A-Fa-f]+}; otherwise {@code false}
     */
    private static boolean isHexString(String s) {
        return s != null && !s.isEmpty() && s.matches("[0-9A-Fa-f]+");
    }

    /**
     * Returns the total row count for a table.
     *
     * @param conn database connection
     * @param table table name whose rows are counted
     * @return number of rows in the table
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

}
