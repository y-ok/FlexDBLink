package io.github.yok.flexdblink.junit;

import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.CsvDateTimeFormatProperties;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.db.DbDialectHandler;
import io.github.yok.flexdblink.db.DbDialectHandlerFactory;
import io.github.yok.flexdblink.db.DbUnitConfigFactory;
import io.github.yok.flexdblink.db.LobResolvingTableWrapper;
import io.github.yok.flexdblink.util.CsvUtils;
import io.github.yok.flexdblink.util.DateTimeFormatUtil;
import java.io.File;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Hex;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.csv.CsvDataSet;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * Fluent assertion utility for comparing DB records against expected CSV data.
 *
 * <p>
 * Designed to work with {@link LoadData}. The scenario is automatically resolved from the
 * {@code @LoadData} annotation on the calling test class/method. LOB columns ({@code file:xxx}
 * references in CSV) are resolved via {@link LobResolvingTableWrapper} and compared using the
 * appropriate {@link DbDialectHandler} for each supported database (Oracle, PostgreSQL, MySQL, SQL
 * Server).
 * </p>
 *
 * <h3>Usage</h3>
 *
 * <pre>
 * // setup
 * flexAssert = FlexAssert.globalExcludeColumns("CREATED_AT", "UPDATED_AT").table("USERS")
 *         .excludeColumns("ID");
 *
 * // assertion
 * flexAssert.assertTables("DB1");
 * </pre>
 *
 * @author Yasuharu.Okawauchi
 */
@Slf4j
public class FlexAssert {

    private final Set<String> globalExcludes;
    private final Map<String, Set<String>> tableExcludes;
    private final DialectHandlerResolver dialectHandlerResolver;

    private FlexAssert(Set<String> globalExcludes, Map<String, Set<String>> tableExcludes) {
        this(globalExcludes, tableExcludes,
                (classRoot, entry, conn) -> createDialectHandler(classRoot, entry));
    }

    FlexAssert(Set<String> globalExcludes, Map<String, Set<String>> tableExcludes,
            DialectHandlerResolver dialectHandlerResolver) {
        this.globalExcludes = globalExcludes;
        this.tableExcludes = tableExcludes;
        this.dialectHandlerResolver = dialectHandlerResolver;
    }

    /**
     * Create an instance with global exclude columns.
     *
     * @param columns column names to exclude from all table comparisons
     * @return new instance
     */
    public static FlexAssert globalExcludeColumns(String... columns) {
        Set<String> set = new LinkedHashSet<>();
        Collections.addAll(set, columns);
        return new FlexAssert(set, Collections.emptyMap());
    }

    /**
     * Create an instance with no exclude columns.
     *
     * @return new instance
     */
    public static FlexAssert withDefaults() {
        return new FlexAssert(Collections.emptySet(), Collections.emptyMap());
    }

    /**
     * Start table-specific exclude column configuration.
     *
     * @param tableName table name
     * @return table exclude builder
     */
    public TableExclude table(String tableName) {
        return new TableExclude(this, tableName);
    }

    /**
     * Compare all CSV files under the expected directory against actual DB records.
     *
     * <p>
     * The scenario is resolved from the {@code @LoadData} annotation. Expected CSV directory:
     * {@code {classRoot}/{scenario}/expected/{dbName}/}. LOB references ({@code file:xxx}) in CSV
     * are resolved via {@link LobResolvingTableWrapper}.
     * </p>
     *
     * @param dbName database logical ID
     */
    public void assertTables(String dbName) {
        TestContext ctx = resolveTestContext();
        Path expectedDir = ctx.expectedDir.resolve(dbName);

        if (!Files.isDirectory(expectedDir)) {
            throw new IllegalStateException(
                    "Expected directory not found: " + expectedDir.toAbsolutePath());
        }

        log.info("Start assertion. scenario={}, db={}", ctx.scenario, dbName);
        logExcludeConfig();

        try {
            DataSource ds = resolveDataSource(dbName);
            Connection conn = DataSourceUtils.getConnection(ds);
            ConnectionConfig.Entry entry = buildConnectionEntry(ds, conn, dbName);
            DbDialectHandler dialectHandler =
                    dialectHandlerResolver.resolve(ctx.classRoot, entry, conn);
            String schema = dialectHandler.resolveSchema(entry);

            IDataSet expectedDataSet = new CsvDataSet(expectedDir.toFile());
            String[] tableNames = expectedDataSet.getTableNames();

            int passCount = 0;
            int failCount = 0;
            StringBuilder failures = new StringBuilder();

            for (String tableName : tableNames) {
                try {
                    assertSingleTable(conn, schema, expectedDataSet, tableName,
                            expectedDir.toFile(), dialectHandler);
                    passCount++;
                } catch (AssertionError | Exception e) {
                    failCount++;
                    failures.append("\n[").append(tableName).append("] ").append(e.getMessage());
                }
            }

            if (failCount > 0) {
                throw new AssertionError(
                        failCount + " of " + tableNames.length + " tables failed." + failures);
            }

            log.info("All tables passed. ({} tables)", passCount);

        } catch (AssertionError e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Assertion failed due to unexpected error.", e);
        }
    }

    /**
     * Compare a single table's actual DB records against CSV expected data.
     *
     * <p>
     * The scenario is resolved from the {@code @LoadData} annotation. LOB references
     * ({@code file:xxx}) in CSV are resolved via {@link LobResolvingTableWrapper}.
     * </p>
     *
     * @param dbName database logical ID
     * @param tableName table name
     */
    public void assertTable(String dbName, String tableName) {
        TestContext ctx = resolveTestContext();
        Path expectedDir = ctx.expectedDir.resolve(dbName);

        if (!Files.isDirectory(expectedDir)) {
            throw new IllegalStateException(
                    "Expected directory not found: " + expectedDir.toAbsolutePath());
        }

        log.info("Start assertion. scenario={}, db={}, table={}", ctx.scenario, dbName, tableName);
        logExcludeConfig();

        try {
            DataSource ds = resolveDataSource(dbName);
            Connection conn = DataSourceUtils.getConnection(ds);
            ConnectionConfig.Entry entry = buildConnectionEntry(ds, conn, dbName);
            DbDialectHandler dialectHandler =
                    dialectHandlerResolver.resolve(ctx.classRoot, entry, conn);
            String schema = dialectHandler.resolveSchema(entry);

            IDataSet expectedDataSet = new CsvDataSet(expectedDir.toFile());
            assertSingleTable(conn, schema, expectedDataSet, tableName, expectedDir.toFile(),
                    dialectHandler);

            log.info("{}: OK", tableName);

        } catch (AssertionError e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Assertion failed due to unexpected error.", e);
        }
    }

    /**
     * Compare a single table with exclude columns applied and LOB references resolved.
     *
     * @param conn JDBC connection
     * @param schema schema name
     * @param expectedDataSet expected dataset from CSV
     * @param tableName table name
     * @param expectedBaseDir base directory for LOB file resolution
     * @param dialectHandler dialect handler for LOB reading and type conversion
     * @throws Exception on comparison failure
     */
    private void assertSingleTable(Connection conn, String schema, IDataSet expectedDataSet,
            String tableName, File expectedBaseDir, DbDialectHandler dialectHandler)
            throws Exception {
        Set<String> excludes = resolveExcludeColumns(tableName);
        ITable rawExpectedTable = expectedDataSet.getTable(tableName);
        String[] columnNames = resolveIncludedColumns(rawExpectedTable, excludes);
        ActualTableSnapshot actual = loadActualTable(conn, tableName, columnNames, dialectHandler);
        List<List<String>> expectedRows = loadExpectedRows(rawExpectedTable, tableName, columnNames,
                actual.columnMetadataByName, expectedBaseDir, dialectHandler);
        List<String> pkColumns = CsvUtils.fetchPrimaryKeyColumns(conn, schema, tableName);
        pkColumns.removeIf(pk -> !actual.columnMetadataByName.containsKey(pk));
        List<Integer> sortIndices = CsvUtils.buildSortIndices(columnNames, pkColumns);
        Comparator<List<String>> comparator = CsvUtils.rowComparator(sortIndices);
        expectedRows.sort(comparator);
        actual.rows.sort(comparator);

        log.info("{}: comparing {} columns (excluded={})", tableName, columnNames.length,
                excludes.isEmpty() ? "none" : excludes);
        assertNormalizedRowsEqual(tableName, columnNames, expectedRows, actual.rows);
        log.info("{}: OK ({} rows)", tableName, actual.rows.size());
    }

    /**
     * Resolve exclude columns for the given table (global + table-specific).
     *
     * @param tableName table name
     * @return merged exclude column set
     */
    Set<String> resolveExcludeColumns(String tableName) {
        Set<String> result = new LinkedHashSet<>(globalExcludes);
        Set<String> tableSpecific = tableExcludes.get(tableName);
        if (tableSpecific != null) {
            result.addAll(tableSpecific);
        }
        return result;
    }

    /**
     * Log the current exclude column configuration.
     */
    private void logExcludeConfig() {
        log.info("Global excludes: {}", globalExcludes.isEmpty() ? "none" : globalExcludes);
        if (!tableExcludes.isEmpty()) {
            for (Map.Entry<String, Set<String>> entry : tableExcludes.entrySet()) {
                log.info("Table excludes: {}={}", entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * Build a {@link ConnectionConfig.Entry} from the active JDBC connection metadata.
     *
     * @param ds resolved data source
     * @param conn active JDBC connection
     * @param dbName database logical ID
     * @return connection entry with URL, user, password, and driver class
     */
    private ConnectionConfig.Entry buildConnectionEntry(DataSource ds, Connection conn,
            String dbName) {
        try {
            ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
            entry.setId(dbName);
            if (ds instanceof DriverManagerDataSource) {
                DriverManagerDataSource driverManagerDataSource = (DriverManagerDataSource) ds;
                entry.setUrl(driverManagerDataSource.getUrl());
                entry.setUser(driverManagerDataSource.getUsername());
                entry.setPassword(driverManagerDataSource.getPassword());
            }
            if (entry.getUrl() == null) {
                entry.setUrl(conn.getMetaData().getURL());
            }
            if (entry.getUser() == null) {
                entry.setUser(conn.getMetaData().getUserName());
            }
            return entry;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to build connection entry from JDBC metadata. dbName=" + dbName, e);
        }
    }

    /**
     * Create a {@link DbDialectHandler} using the same factory pattern as
     * {@link LoadDataExtension}.
     *
     * @param classRoot test class resource root
     * @param entry connection entry for dialect detection
     * @return dialect handler
     */
    private static DbDialectHandler createDialectHandler(Path classRoot,
            ConnectionConfig.Entry entry) {
        CsvDateTimeFormatProperties dtProps = new CsvDateTimeFormatProperties();
        dtProps.setDate("yyyy-MM-dd");
        dtProps.setTime("HH:mm:ss");
        dtProps.setDateTimeWithMillis("yyyy-MM-dd HH:mm:ss.SSS");
        dtProps.setDateTime("yyyy-MM-dd HH:mm:ss");

        DumpConfig dumpConfig = new DumpConfig();
        DbUnitConfigFactory configFactory = new DbUnitConfigFactory();
        DateTimeFormatUtil dateTimeUtil = new DateTimeFormatUtil(dtProps);

        Path dumpRoot = Paths.get(System.getProperty("user.dir"), "target", "dbunit", "dump")
                .toAbsolutePath().normalize();

        PathsConfig pathsConfig = new PathsConfig() {
            @Override
            public String getDump() {
                return dumpRoot.toString();
            }
        };
        pathsConfig.setDataPath(classRoot.toAbsolutePath().toString());

        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        DbDialectHandlerFactory factory = new DbDialectHandlerFactory(dbUnitConfig, dumpConfig,
                pathsConfig, dateTimeUtil, configFactory);
        return factory.create(entry);
    }

    /**
     * Resolve test context from the call stack.
     *
     * @return test context containing classRoot, scenario, and expected directory
     */
    private TestContext resolveTestContext() {
        // Use the test class set by LoadDataExtension (handles inheritance correctly)
        Class<?> testClass = LoadDataExtension.CURRENT_TEST_CLASS.get();
        String currentScenario = LoadDataExtension.CURRENT_SCENARIO.get();
        if (testClass != null && currentScenario != null) {
            Path classRoot = resolveClassRoot(testClass);
            Path expectedDir = classRoot.resolve(currentScenario).resolve("expected");
            return new TestContext(testClass, currentScenario, classRoot, expectedDir);
        }

        // Fallback: scan stack trace for direct @LoadData usage
        StackWalker stackWalker =
                StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);
        List<StackFrameInfo> stack = stackWalker
                .walk(frames -> frames.map(frame -> new StackFrameInfo(frame.getDeclaringClass(),
                        frame.getMethodName())).collect(Collectors.toList()));
        for (StackFrameInfo frame : stack) {
            Class<?> clazz = frame.clazz;
            LoadData methodAnn = findMethodAnnotation(clazz, frame.methodName);
            LoadData classAnn = clazz.getAnnotation(LoadData.class);

            LoadData ann = methodAnn != null ? methodAnn : classAnn;
            if (ann == null) {
                continue;
            }

            String scenario = ann.scenario();
            Path classRoot = resolveClassRoot(clazz);
            Path expectedDir = classRoot.resolve(scenario).resolve("expected");
            return new TestContext(clazz, scenario, classRoot, expectedDir);
        }
        throw new IllegalStateException(
                "Cannot resolve test context. Ensure FlexAssert is called from a test method"
                        + " annotated with @LoadData.");
    }

    /**
     * Find {@link LoadData} annotation on a method by name.
     *
     * @param clazz class to search
     * @param methodName method name
     * @return annotation or null
     */
    private LoadData findMethodAnnotation(Class<?> clazz, String methodName) {
        for (Method m : clazz.getDeclaredMethods()) {
            if (m.getName().equals(methodName)) {
                LoadData ann = m.getAnnotation(LoadData.class);
                if (ann != null) {
                    return ann;
                }
            }
        }
        return null;
    }

    /**
     * Resolve the test class resource root directory.
     *
     * @param testClass test class
     * @return class root path
     */
    private Path resolveClassRoot(Class<?> testClass) {
        String packagePath = testClass.getPackage().getName().replace('.', '/');
        String className = testClass.getSimpleName();

        Path classpathRoot = Paths.get("target", "test-classes");
        Path classRoot = classpathRoot.resolve(packagePath).resolve(className);
        if (Files.isDirectory(classRoot)) {
            return classRoot.toAbsolutePath().normalize();
        }

        Path srcRoot = Paths.get("src", "test", "resources");
        classRoot = srcRoot.resolve(packagePath).resolve(className);
        return classRoot.toAbsolutePath().normalize();
    }

    /**
     * Resolve DataSource for the given dbName. First checks the DataSource used by
     * {@link LoadDataExtension} for the current test (same transaction), then falls back to
     * {@link DataSourceRegistry}.
     *
     * @param dbName database logical ID
     * @return DataSource
     */
    private DataSource resolveDataSource(String dbName) {
        DataSource current = LoadDataExtension.getCurrentDataSource(dbName);
        if (current != null) {
            return current;
        }
        return DataSourceRegistry.find(dbName)
                .orElseThrow(() -> new IllegalStateException("Cannot resolve DataSource for dbName="
                        + dbName + ". Register it via DataSourceRegistry.register()."));
    }

    /**
     * Resolve comparison target columns after excludes are applied.
     *
     * @param expectedTable expected CSV table
     * @param excludes exclude column names
     * @return included column names in source order
     * @throws Exception when metadata cannot be read
     */
    private String[] resolveIncludedColumns(ITable expectedTable, Set<String> excludes)
            throws Exception {
        List<String> names = new java.util.ArrayList<>();
        for (Column column : expectedTable.getTableMetaData().getColumns()) {
            if (!excludes.contains(column.getColumnName())) {
                names.add(column.getColumnName());
            }
        }
        return names.toArray(new String[0]);
    }

    /**
     * Load actual DB rows and JDBC metadata normalized to the same text representation as dump CSV.
     *
     * @param conn JDBC connection
     * @param tableName table name
     * @param columnNames included columns
     * @param dialectHandler dialect handler
     * @return actual rows with column metadata
     * @throws Exception when SQL execution fails
     */
    private ActualTableSnapshot loadActualTable(Connection conn, String tableName,
            String[] columnNames, DbDialectHandler dialectHandler) throws Exception {
        String sql = "SELECT * FROM " + tableName;
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            ResultSetMetaData metaData = rs.getMetaData();
            Map<String, Integer> columnIndexByName = new LinkedHashMap<>();
            Map<String, ColumnMetadata> columnMetadataByName = new LinkedHashMap<>();
            for (int index = 1; index <= metaData.getColumnCount(); index++) {
                String name = metaData.getColumnLabel(index).toUpperCase();
                columnIndexByName.put(name, index);
                columnMetadataByName.put(name, new ColumnMetadata(metaData.getColumnType(index)));
            }

            for (String columnName : columnNames) {
                if (!columnIndexByName.containsKey(columnName)) {
                    throw new IllegalStateException("Column not found in actual table. table="
                            + tableName + ", column=" + columnName);
                }
            }

            List<List<String>> rows = new java.util.ArrayList<>();
            while (rs.next()) {
                List<String> row = new java.util.ArrayList<>(columnNames.length);
                for (String columnName : columnNames) {
                    row.add(normalizeComparableValue(
                            CsvUtils.formatColumnValue(rs, columnName, dialectHandler, conn),
                            columnName, columnMetadataByName.get(columnName)));
                }
                rows.add(row);
            }
            return new ActualTableSnapshot(rows, columnMetadataByName);
        }
    }

    /**
     * Load expected CSV rows and normalize values to the same text representation as actual rows.
     *
     * @param expectedTable expected CSV table
     * @param tableName table name
     * @param columnNames included columns
     * @param columnMetadataByName actual JDBC metadata by column
     * @param expectedBaseDir expected base directory
     * @param dialectHandler dialect handler
     * @return normalized expected rows
     * @throws Exception when expected value normalization fails
     */
    private List<List<String>> loadExpectedRows(ITable expectedTable, String tableName,
            String[] columnNames, Map<String, ColumnMetadata> columnMetadataByName,
            File expectedBaseDir, DbDialectHandler dialectHandler) throws Exception {
        List<List<String>> rows = new ArrayList<>();
        for (int rowIndex = 0; rowIndex < expectedTable.getRowCount(); rowIndex++) {
            List<String> row = new ArrayList<>(columnNames.length);
            for (String columnName : columnNames) {
                row.add(normalizeExpectedValue(expectedTable.getValue(rowIndex, columnName),
                        tableName, columnName, columnMetadataByName.get(columnName),
                        expectedBaseDir, dialectHandler));
            }
            rows.add(row);
        }
        return rows;
    }

    /**
     * Normalize a single expected CSV cell for comparison against an actual DB row.
     *
     * @param rawValue raw CSV cell value
     * @param tableName table name
     * @param columnName column name
     * @param columnMetadata actual JDBC metadata
     * @param expectedBaseDir expected base directory
     * @param dialectHandler dialect handler
     * @return normalized comparable text
     * @throws Exception when LOB resolution fails
     */
    private String normalizeExpectedValue(Object rawValue, String tableName, String columnName,
            ColumnMetadata columnMetadata, File expectedBaseDir, DbDialectHandler dialectHandler)
            throws Exception {
        String stringValue = rawValue.toString();
        if (stringValue.isEmpty()) {
            return "";
        }
        if (stringValue.startsWith("file:")) {
            Object resolved = dialectHandler.readLobFile(stringValue.substring("file:".length()),
                    tableName, columnName, expectedBaseDir);
            return normalizeComparableValue(normalizeResolvedLobValue(resolved, columnMetadata),
                    columnName, columnMetadata);
        }
        return normalizeComparableValue(stringValue, columnName, columnMetadata);
    }

    /**
     * Normalize a resolved LOB object into the same string representation as dump output.
     *
     * @param resolved resolved LOB content
     * @param columnMetadata actual JDBC metadata
     * @return normalized comparable text
     */
    private String normalizeResolvedLobValue(Object resolved, ColumnMetadata columnMetadata) {
        if (resolved == null) {
            return "";
        }
        if (resolved instanceof byte[]) {
            return Hex.encodeHexString((byte[]) resolved).toUpperCase();
        }
        return resolved.toString();
    }

    /**
     * Normalizes comparable text so assertion is stable across dialect-specific representations.
     *
     * @param value formatted cell value
     * @param columnName column name
     * @param columnMetadata JDBC metadata for the column
     * @return canonicalized comparable text
     */
    private String normalizeComparableValue(String value, String columnName,
            ColumnMetadata columnMetadata) {
        if (value.isEmpty()) {
            return value;
        }
        if (isNumericType(columnMetadata.sqlType)) {
            if (isTrueLiteral(value)) {
                return "1";
            }
            if (isFalseLiteral(value)) {
                return "0";
            }
            try {
                return new BigDecimal(value).stripTrailingZeros().toPlainString();
            } catch (NumberFormatException ex) {
                return value;
            }
        }
        if (columnMetadata.sqlType == Types.TIMESTAMP && isTimeOnlyLiteral(value)
                && isTimeSemanticColumn(columnName)) {
            return "1970-01-01 " + value;
        }
        if (isTemporalType(columnMetadata.sqlType)) {
            return value.replaceFirst("\\.0+$", "");
        }
        return value;
    }

    /**
     * Returns whether the SQL type should be compared as a numeric value.
     *
     * @param sqlType JDBC SQL type
     * @return {@code true} when the type is numeric
     */
    private boolean isNumericType(int sqlType) {
        return sqlType == Types.DECIMAL || sqlType == Types.NUMERIC || sqlType == Types.TINYINT
                || sqlType == Types.SMALLINT || sqlType == Types.INTEGER || sqlType == Types.BIGINT
                || sqlType == Types.REAL || sqlType == Types.FLOAT || sqlType == Types.DOUBLE;
    }

    /**
     * Returns whether the SQL type should be compared as a temporal value.
     *
     * @param sqlType JDBC SQL type
     * @return {@code true} when the type is temporal
     */
    private boolean isTemporalType(int sqlType) {
        return sqlType == Types.DATE || sqlType == Types.TIME || sqlType == Types.TIME_WITH_TIMEZONE
                || sqlType == Types.TIMESTAMP || sqlType == Types.TIMESTAMP_WITH_TIMEZONE;
    }

    /**
     * Returns whether the literal represents true.
     *
     * @param value candidate text
     * @return {@code true} when the value is a true literal
     */
    private boolean isTrueLiteral(String value) {
        return "true".equalsIgnoreCase(value) || "t".equalsIgnoreCase(value);
    }

    /**
     * Returns whether the literal represents false.
     *
     * @param value candidate text
     * @return {@code true} when the value is a false literal
     */
    private boolean isFalseLiteral(String value) {
        return "false".equalsIgnoreCase(value) || "f".equalsIgnoreCase(value);
    }

    /**
     * Returns whether the text is in {@code HH:mm:ss} time-only format.
     *
     * @param value candidate text
     * @return {@code true} when the text is time-only
     */
    private boolean isTimeOnlyLiteral(String value) {
        return value.matches("\\d{2}:\\d{2}:\\d{2}");
    }

    /**
     * Returns whether the column name semantically represents a time-only value.
     *
     * @param columnName column name
     * @return {@code true} when the column should be compared as time-only
     */
    private boolean isTimeSemanticColumn(String columnName) {
        String upper = columnName.toUpperCase();
        return upper.contains("TIME") && !upper.contains("TIMESTAMP");
    }

    /**
     * Compare normalized expected and actual rows and report the first mismatch.
     *
     * @param tableName table name
     * @param columnNames compared columns
     * @param expectedRows expected rows
     * @param actualRows actual rows
     */
    private void assertNormalizedRowsEqual(String tableName, String[] columnNames,
            List<List<String>> expectedRows, List<List<String>> actualRows) {
        if (expectedRows.size() != actualRows.size()) {
            throw new AssertionError("row count (table=" + tableName + ")expected:<"
                    + expectedRows.size() + "> but was:<" + actualRows.size() + ">");
        }
        for (int rowIndex = 0; rowIndex < expectedRows.size(); rowIndex++) {
            List<String> expectedRow = expectedRows.get(rowIndex);
            List<String> actualRow = actualRows.get(rowIndex);
            for (int columnIndex = 0; columnIndex < columnNames.length; columnIndex++) {
                String expectedValue = expectedRow.get(columnIndex);
                String actualValue = actualRow.get(columnIndex);
                if (!Objects.equals(expectedValue, actualValue)) {
                    throw new AssertionError("value (table=" + tableName + ", row=" + (rowIndex + 1)
                            + ", col=" + columnNames[columnIndex] + ")expected:<" + expectedValue
                            + "> but was:<" + actualValue + ">");
                }
            }
        }
    }

    /**
     * Actual table snapshot used during normalized comparison.
     */
    private static final class ActualTableSnapshot {
        private final List<List<String>> rows;
        private final Map<String, ColumnMetadata> columnMetadataByName;

        ActualTableSnapshot(List<List<String>> rows,
                Map<String, ColumnMetadata> columnMetadataByName) {
            this.rows = rows;
            this.columnMetadataByName = columnMetadataByName;
        }
    }

    /**
     * Minimal JDBC column metadata needed for expected-value normalization.
     */
    private static final class ColumnMetadata {
        private final int sqlType;

        ColumnMetadata(int sqlType) {
            this.sqlType = sqlType;
        }
    }

    /**
     * Internal context resolved from the test call stack.
     */
    static class TestContext {
        final Class<?> testClass;
        final String scenario;
        final Path classRoot;
        final Path expectedDir;

        TestContext(Class<?> testClass, String scenario, Path classRoot, Path expectedDir) {
            this.testClass = testClass;
            this.scenario = scenario;
            this.classRoot = classRoot;
            this.expectedDir = expectedDir;
        }
    }

    /**
     * Stack frame information required for resolving the active {@link LoadData}.
     */
    private static class StackFrameInfo {
        final Class<?> clazz;
        final String methodName;

        StackFrameInfo(Class<?> clazz, String methodName) {
            this.clazz = clazz;
            this.methodName = methodName;
        }
    }

    /**
     * Resolves a dialect handler for comparison.
     */
    @FunctionalInterface
    interface DialectHandlerResolver {

        /**
         * Resolves a dialect handler.
         *
         * @param classRoot class resource root
         * @param entry connection entry
         * @param conn active JDBC connection
         * @return dialect handler
         */
        DbDialectHandler resolve(Path classRoot, ConnectionConfig.Entry entry, Connection conn);
    }

    /**
     * Builder for table-specific exclude column configuration.
     */
    public static class TableExclude {

        private final FlexAssert parent;
        private final String tableName;

        TableExclude(FlexAssert parent, String tableName) {
            this.parent = parent;
            this.tableName = tableName;
        }

        /**
         * Set exclude columns for this table and return to the parent FlexAssert.
         *
         * @param columns column names to exclude
         * @return new FlexAssert instance with this table's excludes added
         */
        public FlexAssert excludeColumns(String... columns) {
            Map<String, Set<String>> merged = new LinkedHashMap<>(parent.tableExcludes);
            Set<String> existing = merged.getOrDefault(tableName, new LinkedHashSet<>());
            Set<String> updated = new LinkedHashSet<>(existing);
            Collections.addAll(updated, columns);
            merged.put(tableName, updated);
            return new FlexAssert(parent.globalExcludes, merged, parent.dialectHandlerResolver);
        }
    }
}
