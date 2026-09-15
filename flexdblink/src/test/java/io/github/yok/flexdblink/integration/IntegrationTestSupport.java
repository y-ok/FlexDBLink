package io.github.yok.flexdblink.integration;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import io.github.yok.flexdblink.Main;
import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.FilePatternConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.core.DataDumper;
import io.github.yok.flexdblink.core.DataLoader;
import io.github.yok.flexdblink.db.DbDialectHandler;
import io.github.yok.flexdblink.db.DbDialectHandlerFactory;
import io.github.yok.flexdblink.parser.DataLoaderFactory;
import io.github.yok.flexdblink.parser.DatasetFiles;
import io.github.yok.flexdblink.util.CsvUtils;
import io.github.yok.flexdblink.util.ErrorHandler;
import io.github.yok.flexdblink.util.TableOrderingFile;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.provider.Arguments;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.JdbcDatabaseContainer;

/**
 * Shared utility class for integration tests across all database types (MySQL, SQL Server,
 * PostgreSQL, Oracle).
 *
 * <p>
 * Consolidates DB setup, runtime assembly, CSV parsing, and assertion utilities that were
 * previously duplicated across four separate Support classes.
 * </p>
 *
 * <h2>Test philosophy</h2>
 * <ul>
 * <li><b>Load test</b>: DB values formatted through the same production code path as dump
 * ({@link CsvUtils#formatColumnValue}) — no test-side normalization</li>
 * <li><b>Dump test</b>: input CSV compared directly with output CSV — {@code file:} references
 * trigger byte-level file comparison</li>
 * <li><b>Principle</b>: if diff does not match, it is a production code bug</li>
 * </ul>
 */
final class IntegrationTestSupport {

    private IntegrationTestSupport() {}

    enum PrimaryKeyCase {
        SINGLE, SINGLE_CHANGED, NONE, COMPOSITE
    }

    enum LoaderRoute {
        TRANSACTIONAL, LEGACY;

        DataLoader create(Runtime runtime) {
            if (this == LEGACY) {
                return runtime.newLoader();
            }
            return new DataLoader(runtime.pathsConfig, runtime.connectionConfig,
                    runtime.dialectFactory, runtime.dbUnitConfig, runtime.dumpConfig);
        }
    }

    enum MalformedFormat {
        CSV("csv", "ID,NAME\n\"unterminated"), JSON("json", "invalid json"), YAML("yaml",
                "- ID: [unterminated"), YML("yml",
                        "- ID: [unterminated"), XML("xml", "<dataset><broken></dataset>");

        private final String extension;
        private final String input;

        MalformedFormat(String extension, String input) {
            this.extension = extension;
            this.input = input;
        }
    }

    enum FailureOperation {
        LOAD, MAIN_LOAD, TRANSACTIONAL_LOAD, LEGACY_LOAD, DUMP, MAIN_DUMP
    }

    static Stream<Arguments> malformedInputCases() {
        return Arrays.stream(LoaderRoute.values()).flatMap(route -> Arrays
                .stream(MalformedFormat.values()).map(format -> Arguments.of(route, format)));
    }

    /**
     * Starts the container and initializes the test schema with Flyway (all migrations).
     *
     * @param container JDBC container
     * @param migrationLocations one or more Flyway migration locations (e.g.
     *        {@code classpath:db/migration/mysql})
     */
    static void prepareDatabase(JdbcDatabaseContainer<?> container, String... migrationLocations) {
        ensureContainerRunning(container);
        migrate(container, migrationLocations);
    }

    /**
     * Starts the container and initializes the schema without FK constraints (up to V2).
     *
     * @param container JDBC container
     * @param migrationLocations one or more Flyway migration locations
     */
    static void prepareDatabaseWithoutFk(JdbcDatabaseContainer<?> container,
            String... migrationLocations) {
        ensureContainerRunning(container);
        migrateWithoutFk(container, migrationLocations);
    }

    /**
     * Assembles the runtime required to run an integration test.
     *
     * <p>
     * All static configuration (driver class, exclude tables, date/time formats, file patterns) is
     * loaded by Spring via {@code @TestPropertySource} from the DB-specific
     * {@code flexdblink-it.yml}. Dynamic values (JDBC URL, user, password) are injected via
     * {@code @DynamicPropertySource} from the Testcontainers container.
     * </p>
     *
     * @param dataPath work directory for the test
     * @param copyLoadFixtures {@code true} to copy load fixtures into the work directory
     * @param dbName database name used for fixture path (e.g. {@code mysql}, {@code oracle})
     * @param pathsConfig Spring-injected paths configuration
     * @param connectionConfig Spring-injected connection configuration
     * @param dbUnitConfig Spring-injected DBUnit configuration
     * @param dumpConfig Spring-injected dump configuration
     * @param filePatternConfig Spring-injected file pattern configuration
     * @param dialectFactory Spring-injected dialect handler factory
     * @return assembled runtime
     * @throws IOException on file I/O failure
     */
    static Runtime prepareRuntime(Path dataPath, boolean copyLoadFixtures, String dbName,
            PathsConfig pathsConfig, ConnectionConfig connectionConfig, DbUnitConfig dbUnitConfig,
            DumpConfig dumpConfig, FilePatternConfig filePatternConfig,
            DbDialectHandlerFactory dialectFactory) throws IOException {
        Files.createDirectories(dataPath);
        if (copyLoadFixtures) {
            copyLoadFixtures(dataPath, dbName);
        }
        pathsConfig.setDataPath(dataPath.toAbsolutePath().toString());
        return new Runtime(dataPath, connectionConfig, dbUnitConfig, dumpConfig, pathsConfig,
                filePatternConfig, dialectFactory);
    }

    /**
     * Executes DataLoader for the specified scenario.
     *
     * @param runtime runtime
     * @param scenario scenario name (e.g. {@code pre})
     */
    static void executeLoad(Runtime runtime, String scenario) {
        DataLoader loader = runtime.newLoader();
        loader.execute(scenario, List.of("db1"));
    }

    static void assertCsvRoundTripPreservesText(Runtime runtime, Connection jdbc) throws Exception {
        String table = createNamedTable(jdbc, "csv_text", "id INTEGER PRIMARY KEY, name VARCHAR(100)");
        List<String> expected = Arrays.asList(null, "", "null", " A ", "A ", " A", "   ", "\tA\r\n", "a\"b", "C:\\temp\\new");
        try (PreparedStatement insert =
                jdbc.prepareStatement("INSERT INTO " + table + " (id, name) VALUES (?, ?)")) {
            for (int i = 0; i < expected.size(); i++) {
                insert.setInt(1, i + 1);
                insert.setString(2, expected.get(i));
                insert.executeUpdate();
            }
        }
        if ("Oracle".equals(jdbc.getMetaData().getDatabaseProductName())) {
            expected.set(1, null);
        }
        Path dumped = executeDump(runtime, "text_round_trip");
        Path initial = Files.createDirectories(runtime.dataPath.resolve("load/pre/db1"));
        Files.copy(dumped.resolve(table + ".csv"), initial.resolve(table + ".csv"));

        executeLoad(runtime, "pre");

        List<String> actual = new ArrayList<>();
        try (Statement sql = jdbc.createStatement();
                ResultSet rows = sql.executeQuery("SELECT name FROM " + table + " ORDER BY id")) {
            while (rows.next()) {
                actual.add(rows.getString(1));
            }
        }
        assertEquals(expected, actual);
    }

    /**
     * Verifies that applying a scenario retains shared, initial-only, and scenario-only rows.
     *
     * @param runtime configured database runtime
     * @param jdbc connection used to prepare and verify committed rows
     * @param keyCase primary key shape used to exercise duplicate matching
     * @throws Exception if setup or loading fails
     */
    static void assertScenarioRetainsSharedRows(Runtime runtime, Connection jdbc,
            PrimaryKeyCase keyCase) throws Exception {
        // Isolate data retention from the separately tested production error-handler defect.
        ErrorHandler.disableExitForCurrentThread();
        try {
            String definition = "id VARCHAR(30) PRIMARY KEY, name VARCHAR(100)";
            String header = "ID,NAME\n";
            String initialRows = "duplicate,shared\npre-only,baseline\n";
            String scenarioRows = "duplicate,shared\nscenario-only,added\n";
            String[] columns = {"id", "name"};
            List<String> expected =
                    List.of("duplicate:shared", "pre-only:baseline", "scenario-only:added");
            if (keyCase == PrimaryKeyCase.SINGLE_CHANGED) {
                scenarioRows = "duplicate,scenario-value\nscenario-only,added\n";
                expected = List.of("duplicate:scenario-value", "pre-only:baseline",
                        "scenario-only:added");
            } else if (keyCase == PrimaryKeyCase.NONE) {
                definition = "id VARCHAR(30), name VARCHAR(100)";
                initialRows += "duplicate,distinct\n";
                expected = List.of("duplicate:distinct", "duplicate:shared", "pre-only:baseline",
                        "scenario-only:added");
            } else if (keyCase == PrimaryKeyCase.COMPOSITE) {
                definition = "id VARCHAR(30), revision INTEGER, name VARCHAR(100), "
                        + "PRIMARY KEY (id, revision)";
                header = "ID,REVISION,NAME\n";
                initialRows = "duplicate,1,shared\nduplicate,2,distinct\npre-only,1,baseline\n";
                scenarioRows = "duplicate,1,shared\nscenario-only,1,added\n";
                columns = new String[] {"id", "revision", "name"};
                expected = List.of("duplicate:1:shared", "duplicate:2:distinct",
                        "pre-only:1:baseline", "scenario-only:1:added");
            }
            String table = createNamedTable(jdbc, "scenario_duplicate", definition);
            Path initial = Files.createDirectories(runtime.dataPath.resolve("load/pre/db1"));
            Path scenario = Files.createDirectories(runtime.dataPath.resolve("load/scenario/db1"));
            // Match unquoted PostgreSQL column names when exercising full-row comparison.
            if (jdbc.getMetaData().storesLowerCaseIdentifiers()) {
                header = header.toLowerCase(Locale.ROOT);
            }
            Files.writeString(initial.resolve(table + ".csv"), header + initialRows);
            Files.writeString(scenario.resolve(table + ".csv"), header + scenarioRows);

            executeLoad(runtime, "scenario");

            assertEquals(expected, readNamedRows(jdbc, table, columns),
                    "Applying a scenario must retain the row shared with the initial dataset.");
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }
    }

    /**
     * Verifies that malformed input is reported so the caller can roll back all selected tables.
     *
     * @param runtime configured database runtime
     * @param jdbc caller-owned connection used for loading and verification
     * @param route external-connection loading implementation
     * @param format malformed input format
     * @throws Exception if setup or transaction completion fails
     */
    static void assertMalformedInputPreservesRows(Runtime runtime, Connection jdbc,
            LoaderRoute route, MalformedFormat format) throws Exception {
        DataLoader loader = route.create(runtime);
        ErrorHandler.disableExitForCurrentThread();
        try (Statement sql = jdbc.createStatement()) {
            String malformedTable = createNamedTable(jdbc, "malformed_input");
            String validTable = createNamedTable(jdbc, "valid_input");
            sql.execute("INSERT INTO " + malformedTable + " VALUES ('original', 'bad-baseline')");
            sql.execute("INSERT INTO " + validTable + " VALUES ('original', 'good-baseline')");
            Files.writeString(runtime.dataPath.resolve(malformedTable + "." + format.extension),
                    format.input);
            Files.writeString(runtime.dataPath.resolve(validTable + ".csv"),
                    "ID,NAME\nnew,replacement\n");
            // Prove the fixture fails in the selected parser before checking loader behavior.
            Exception parseFailure = assertThrows(Exception.class, () -> {
                if (route == LoaderRoute.LEGACY) {
                    TableOrderingFile.ensure(runtime.dataPath.toFile());
                    DataLoaderFactory.create(runtime.dataPath.toFile(), malformedTable);
                } else {
                    new DatasetFiles(runtime.dataPath.toFile()).parse(malformedTable);
                }
            });
            List<Throwable> causes = ExceptionUtils.getThrowableList(parseFailure);
            Class<?> expectedCause = causes.get(causes.size() - 1).getClass();
            jdbc.setAutoCommit(false);

            Exception failure = loadAndCompleteTransaction(loader, runtime, jdbc);

            assertAll(() -> {
                assertNotNull(failure,
                        "Malformed input must notify the caller instead of succeeding.");
                assertTrue(
                        ExceptionUtils.getThrowableList(failure).stream()
                                .anyMatch(expectedCause::isInstance),
                        "The reported failure must preserve the parser exception: " + failure);
            }, () -> assertEquals(List.of("original:bad-baseline"),
                    readNamedRows(jdbc, malformedTable),
                    "A parse failure must not leave the malformed table empty."),
                    () -> assertEquals(List.of("original:good-baseline"),
                            readNamedRows(jdbc, validTable),
                            "A failed load must allow the caller to roll back all selected tables."));
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }
    }

    private static String createNamedTable(Connection jdbc, String name) throws SQLException {
        return createNamedTable(jdbc, name, "id VARCHAR(30) PRIMARY KEY, name VARCHAR(100)");
    }

    static String createNamedTable(Connection jdbc, String name, String definition)
            throws SQLException {
        String table = name;
        // Keep CSV names consistent with Oracle's folding of unquoted identifiers.
        if (jdbc.getMetaData().storesUpperCaseIdentifiers()) {
            table = name.toUpperCase(Locale.ROOT);
        }
        try (Statement sql = jdbc.createStatement()) {
            sql.execute("CREATE TABLE " + table + " (" + definition + ")");
        }
        return table;
    }

    private static Exception loadAndCompleteTransaction(DataLoader loader, Runtime runtime,
            Connection jdbc) throws Exception {
        // Model a caller that commits success and rolls back a reported load failure.
        Exception failure = null;
        // Model a transaction-bound connection whose lifecycle belongs to the caller.
        Connection external = mock(Connection.class, delegatesTo(jdbc));
        doNothing().when(external).close();
        try {
            loader.executeWithConnection(runtime.dataPath.toFile(),
                    runtime.connectionConfig.getConnections().get(0), external);
        } catch (Exception loadFailure) {
            failure = loadFailure;
        }
        if (failure == null) {
            jdbc.commit();
        } else {
            jdbc.rollback();
        }
        return failure;
    }

    private static List<String> readNamedRows(Connection jdbc, String table) throws SQLException {
        return readNamedRows(jdbc, table, "id", "name");
    }

    static List<String> readNamedRows(Connection jdbc, String table, String... columns)
            throws SQLException {
        List<String> rows = new ArrayList<>();
        String selected = String.join(", ", columns);
        try (Statement sql = jdbc.createStatement();
                ResultSet result = sql.executeQuery(
                        "SELECT " + selected + " FROM " + table + " ORDER BY " + selected)) {
            while (result.next()) {
                List<String> values = new ArrayList<>();
                for (String column : columns) {
                    values.add(result.getString(column));
                }
                rows.add(String.join(":", values));
            }
        }
        return rows;
    }

    static void assertProductionFailureIsReported(Runtime runtime, Connection jdbc,
            FailureOperation operation) throws Exception {
        ErrorHandler.restoreExitForCurrentThread();
        try (Statement sql = jdbc.createStatement()) {
            String table = createNamedTable(jdbc, "failed_operation");
            sql.execute("INSERT INTO " + table + " VALUES ('original', 'baseline')");
            Main main =
                    new Main(runtime.pathsConfig, runtime.dbUnitConfig, runtime.connectionConfig,
                            runtime.filePatternConfig, runtime.dumpConfig, runtime.dialectFactory);
            if (operation == FailureOperation.DUMP || operation == FailureOperation.MAIN_DUMP) {
                // A regular file prevents creation of the dump directory without OS permissions.
                Files.writeString(runtime.dataPath.resolve("dump"), "blocked");
                Executable dump = () -> runtime.newDumper().execute("failure", List.of("db1"));
                if (operation == FailureOperation.MAIN_DUMP) {
                    dump = () -> main.run("--dump", "failure", "--target", "db1");
                }
                Executable action = dump;
                assertAll(() -> assertThrows(RuntimeException.class, action,
                        "A real dump I/O failure must reach the caller in production mode."),
                        () -> assertEquals(List.of("original:baseline"),
                                readNamedRows(jdbc, table)));
                return;
            }

            String invalidRows = "ID,NAME\nduplicate,first\nduplicate,second\n";
            Exception failure;
            if (operation == FailureOperation.TRANSACTIONAL_LOAD
                    || operation == FailureOperation.LEGACY_LOAD) {
                Files.writeString(runtime.dataPath.resolve(table + ".csv"), invalidRows);
                jdbc.setAutoCommit(false);
                LoaderRoute route = LoaderRoute.TRANSACTIONAL;
                if (operation == FailureOperation.LEGACY_LOAD) {
                    route = LoaderRoute.LEGACY;
                }
                failure = loadAndCompleteTransaction(route.create(runtime), runtime, jdbc);
            } else {
                Path initial = Files.createDirectories(runtime.dataPath.resolve("load/pre/db1"));
                Files.writeString(initial.resolve(table + ".csv"), invalidRows);
                failure = null;
                try {
                    if (operation == FailureOperation.MAIN_LOAD) {
                        main.run("--load", "pre", "--target", "db1");
                    } else {
                        runtime.newLoader().execute("pre", List.of("db1"));
                    }
                } catch (Exception loadFailure) {
                    failure = loadFailure;
                }
            }
            Exception reported = failure;
            assertAll(() -> {
                assertNotNull(reported,
                        "A real constraint violation must reach the caller in production mode.");
                assertTrue(
                        ExceptionUtils.getThrowableList(reported).stream()
                                .anyMatch(SQLException.class::isInstance),
                        "The reported failure must preserve the SQL exception: " + reported);
            }, () -> assertEquals(List.of("original:baseline"), readNamedRows(jdbc, table),
                    "A failed load must preserve the committed baseline."));
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }
    }

    /**
     * Executes DataDumper for the specified scenario and returns the output directory.
     *
     * @param runtime runtime
     * @param scenario scenario name
     * @return {@code dump/{scenario}/db1} directory
     */
    static Path executeDump(Runtime runtime, String scenario) {
        DataDumper dumper = runtime.newDumper();
        dumper.execute(scenario, List.of("db1"));
        return runtime.dataPath().resolve("dump").resolve(scenario).resolve("db1");
    }

    /**
     * Opens a JDBC connection to the container.
     *
     * @param container JDBC container
     * @return JDBC connection
     * @throws SQLException on JDBC error
     */
    static Connection openConnection(JdbcDatabaseContainer<?> container) throws SQLException {
        return DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(),
                container.getPassword());
    }

    /**
     * Asserts that the input CSV and output CSV have identical content for all rows and columns.
     *
     * <p>
     * For {@code file:} references, also compares the actual file contents byte-by-byte.
     * </p>
     *
     * @param table table name (for assertion messages)
     * @param idColumn name of the ID column
     * @param inputCsv input CSV path
     * @param outputCsv output CSV path
     * @param inputFilesDir directory containing input LOB files
     * @param outputFilesDir directory containing output LOB files
     * @throws Exception on I/O or assertion failure
     */
    static void assertCsvEquals(String table, String idColumn, Path inputCsv, Path outputCsv,
            Path inputFilesDir, Path outputFilesDir) throws Exception {

        Map<String, Map<String, String>> inputRows = readCsvById(inputCsv, idColumn);
        Map<String, Map<String, String>> outputRows = readCsvById(outputCsv, idColumn);

        assertEquals(inputRows.size(), outputRows.size(), table + " row count mismatch: input="
                + inputRows.size() + " output=" + outputRows.size());

        for (Entry<String, Map<String, String>> entry : inputRows.entrySet()) {
            String id = entry.getKey();
            Map<String, String> inRow = entry.getValue();
            Map<String, String> outRow = outputRows.get(id);
            assertNotNull(outRow, table + " ID=" + id + " not found in output CSV");

            for (Entry<String, String> colEntry : inRow.entrySet()) {
                String column = colEntry.getKey();
                String inVal = colEntry.getValue();
                String outVal = outRow.get(column);
                String msg = "Table=" + table + " ID=" + id + " Column=" + column;

                if (inVal != null && inVal.startsWith("file:") && outVal != null
                        && outVal.startsWith("file:")) {
                    Path inFile = inputFilesDir.resolve(inVal.substring("file:".length()));
                    Path outFile = outputFilesDir.resolve(outVal.substring("file:".length()));
                    assertArrayEquals(Files.readAllBytes(inFile), Files.readAllBytes(outFile),
                            msg + " file content mismatch");
                } else if ("".equals(inVal) && outVal != null && outVal.startsWith("file:")) {
                    // Dump materializes an inline empty character LOB as a zero-byte file.
                    Path outFile = outputFilesDir.resolve(outVal.substring("file:".length()));
                    assertEquals(0L, Files.size(outFile), msg + " empty LOB content mismatch");
                } else {
                    assertEquals(inVal, outVal, msg);
                }
            }
        }
    }

    /**
     * Asserts that all rows in the CSV match the corresponding rows in the database.
     *
     * <p>
     * DB values are formatted through the same production code path as the dump
     * ({@link CsvUtils#formatColumnValue}) so that no test-side normalization is needed. For
     * {@code file:} references (LOBs), the file contents are compared with
     * {@code ResultSet.getBytes()}.
     * </p>
     *
     * @param csvPath CSV file path
     * @param tableName database table name
     * @param idColumn name of the ID column
     * @param conn JDBC connection
     * @param filesDir directory containing LOB files referenced by the CSV
     * @param dialectHandler DB dialect handler for value formatting
     * @throws Exception on I/O, SQL, or assertion failure
     */
    static void assertCsvMatchesDb(Path csvPath, String tableName, String idColumn, Connection conn,
            Path filesDir, DbDialectHandler dialectHandler) throws Exception {

        Map<String, Map<String, String>> csvRows = readCsvById(csvPath, idColumn);
        dialectHandler.prepareConnection(conn);

        for (Entry<String, Map<String, String>> entry : csvRows.entrySet()) {
            String id = entry.getKey();
            Map<String, String> csvRow = entry.getValue();

            String sql = "SELECT * FROM " + tableName + " WHERE " + idColumn + " = " + id;

            try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
                assertTrue(rs.next(), tableName + " ID=" + id + " not found in DB");

                for (Entry<String, String> col : csvRow.entrySet()) {
                    String column = col.getKey();
                    String csvVal = col.getValue();
                    String msg = "Table=" + tableName + " ID=" + id + " Column=" + column;

                    if (csvVal != null && csvVal.startsWith("file:")) {
                        String fileRef = csvVal.substring("file:".length());
                        Object expected = dialectHandler.readLobFile(fileRef, tableName, column,
                                filesDir.getParent().toFile());
                        if (expected instanceof byte[]) {
                            byte[] dbBytes = rs.getBytes(column);
                            assertArrayEquals((byte[]) expected, dbBytes, msg);
                        } else {
                            String dbVal = rs.getString(column);
                            assertEquals(expected, dbVal, msg);
                        }
                    } else {
                        String dbVal = CsvUtils.formatColumnValue(rs, column, dialectHandler, conn);
                        assertEquals(csvVal, dbVal, msg);
                    }
                }

                assertFalse(rs.next(), tableName + " ID=" + id + " has multiple rows");
            }
        }
    }

    /**
     * Reads a single row from a CSV file by its ID column value.
     *
     * @param csvPath path to the CSV file
     * @param idColumn name of the ID column
     * @param id ID value to look up
     * @return map of header to value for the matching row
     * @throws IOException on CSV read failure
     */
    static Map<String, String> readCsvRowById(Path csvPath, String idColumn, String id)
            throws IOException {
        Map<String, Map<String, String>> rows = readCsvById(csvPath, idColumn);
        Map<String, String> row = rows.get(id);
        if (row == null) {
            throw new IllegalStateException(
                    "Row not found: " + idColumn + "=" + id + " in " + csvPath);
        }
        return row;
    }

    /**
     * Parses a CSV file into a map keyed by the specified ID column.
     *
     * @param csvPath path to the CSV file
     * @param idColumn name of the ID column
     * @return map of ID value to (header to value)
     * @throws IOException on CSV read failure
     */
    static Map<String, Map<String, String>> readCsvById(Path csvPath, String idColumn)
            throws IOException {
        Map<String, Map<String, String>> rows = new LinkedHashMap<>();
        CSVFormat format = CsvUtils.FORMAT.builder().setHeader().setSkipHeaderRecord(true).get();

        try (CSVParser parser = CSVParser.parse(csvPath, StandardCharsets.UTF_8, format)) {
            for (CSVRecord record : parser) {
                Map<String, String> row = new LinkedHashMap<>();
                for (String header : parser.getHeaderMap().keySet()) {
                    row.put(header, record.get(header));
                }
                rows.put(record.get(idColumn), row);
            }
        }
        return rows;
    }

    /**
     * Resolves a file path within the directory using case-insensitive matching as a fallback.
     *
     * @param dir directory to search in
     * @param fileName file name to resolve
     * @return resolved path
     * @throws IOException if the file is not found
     */
    static Path resolveFileIgnoreCase(Path dir, String fileName) throws IOException {
        Path direct = dir.resolve(fileName);
        if (Files.exists(direct)) {
            return direct;
        }

        try (Stream<Path> stream = Files.list(dir)) {
            return stream.filter(p -> p.getFileName().toString().equalsIgnoreCase(fileName))
                    .findFirst().orElseThrow(
                            () -> new IOException("File not found (case-insensitive): expected="
                                    + fileName + " dir=" + dir.toAbsolutePath()));
        }
    }

    /**
     * Ensures the container is started.
     *
     * @param container JDBC container
     */
    private static void ensureContainerRunning(JdbcDatabaseContainer<?> container) {
        if (container.isRunning()) {
            return;
        }
        try {
            container.start();
        } catch (ContainerLaunchException e) {
            throw new IllegalStateException("Container must be available for integration tests", e);
        }
    }

    /**
     * Runs Flyway clean and migrate against the container.
     *
     * @param container JDBC container
     * @param migrationLocations one or more Flyway migration locations
     */
    private static void migrate(JdbcDatabaseContainer<?> container, String... migrationLocations) {
        Flyway flyway = Flyway
                .configure().cleanDisabled(false).dataSource(container.getJdbcUrl(),
                        container.getUsername(), container.getPassword())
                .locations(migrationLocations).load();
        flyway.clean();
        flyway.migrate();
    }

    /**
     * Runs Flyway migration up to version 2 (without foreign keys).
     *
     * @param container JDBC container
     * @param migrationLocations one or more Flyway migration locations
     */
    private static void migrateWithoutFk(JdbcDatabaseContainer<?> container,
            String... migrationLocations) {
        Flyway flyway = Flyway
                .configure().cleanDisabled(false).dataSource(container.getJdbcUrl(),
                        container.getUsername(), container.getPassword())
                .locations(migrationLocations).target("2").load();
        flyway.clean();
        flyway.migrate();
    }

    /**
     * Copies load fixtures for the specified database into the data path.
     *
     * @param dataPath work directory for the test
     * @param dbName database name (e.g. {@code mysql}, {@code oracle})
     * @throws IOException on copy failure
     */
    static void copyLoadFixtures(Path dataPath, String dbName) throws IOException {
        Path src = Path.of("src", "test", "resources", "integration", dbName, "load");
        Path commonSrc = Path.of("src", "test", "resources", "integration", "common", "load");
        Path dst = dataPath.resolve("load");
        Files.createDirectories(dst);
        copyTreeIfExists(src, dst);
        copyTreeIfExists(commonSrc, dst);
    }

    /**
     * Copies load fixtures for a single scenario into the data path.
     *
     * @param dataPath work directory for the test
     * @param dbName database name (e.g. {@code mysql}, {@code oracle})
     * @param scenario scenario directory name under {@code load}
     * @throws IOException on copy failure
     */
    static void copyLoadScenarioFixtures(Path dataPath, String dbName, String scenario)
            throws IOException {
        copyLoadScenarioFixtures(dataPath, dbName, scenario, scenario);
    }

    /**
     * Copies load fixtures from source scenario into target scenario directory.
     *
     * @param dataPath work directory for the test
     * @param dbName database name (e.g. {@code mysql}, {@code oracle})
     * @param sourceScenario source scenario directory name under {@code load}
     * @param targetScenario target scenario directory name under {@code load}
     * @throws IOException on copy failure
     */
    static void copyLoadScenarioFixtures(Path dataPath, String dbName, String sourceScenario,
            String targetScenario) throws IOException {
        Path src =
                Path.of("src", "test", "resources", "integration", dbName, "load", sourceScenario);
        Path commonSrc = Path.of("src", "test", "resources", "integration", "common", "load",
                sourceScenario);
        Path dst = dataPath.resolve("load").resolve(targetScenario);
        Files.createDirectories(dst);
        copyTreeIfExists(src, dst);
        copyTreeIfExists(commonSrc, dst);
    }

    /**
     * Overlays one load scenario directory onto another in the test work directory.
     *
     * <p>
     * Existing files in the target are replaced only when the same relative path exists in source.
     * Files absent in source remain unchanged in target.
     * </p>
     *
     * @param dataPath work directory for the test
     * @param sourceScenario source scenario name (e.g. {@code pre_crlf})
     * @param targetScenario target scenario name (e.g. {@code pre})
     * @param dbId target DB ID (e.g. {@code db1})
     * @throws IOException on file I/O failure
     */
    static void overlayLoadScenario(Path dataPath, String sourceScenario, String targetScenario,
            String dbId) throws IOException {
        Path source = dataPath.resolve("load").resolve(sourceScenario).resolve(dbId);
        Path target = dataPath.resolve("load").resolve(targetScenario).resolve(dbId);
        if (Files.notExists(source)) {
            throw new IllegalStateException("Source scenario directory not found: " + source);
        }
        Files.createDirectories(target);
        copyTree(source, target);
    }

    /**
     * Resolves the path of table-ordering file under load fixtures.
     *
     * @param dataPath work directory for the test
     * @param scenario load scenario directory name (e.g. {@code pre})
     * @param dbId target DB ID (e.g. {@code db1})
     * @return path to table-ordering file
     */
    static Path resolveTableOrderingPath(Path dataPath, String scenario, String dbId) {
        return dataPath.resolve("load").resolve(scenario).resolve(dbId)
                .resolve(TableOrderingFile.FILE_NAME);
    }

    /**
     * Copies all files from source tree into destination tree.
     *
     * @param src source root
     * @param dst destination root
     * @throws IOException when file copy fails
     */
    private static void copyTree(Path src, Path dst) throws IOException {
        try (var stream = Files.walk(src)) {
            stream.forEach(path -> {
                try {
                    Path rel = src.relativize(path);
                    Path target = dst.resolve(rel);
                    if (Files.isDirectory(path)) {
                        Files.createDirectories(target);
                    } else {
                        Files.createDirectories(target.getParent());
                        Files.copy(path, target, StandardCopyOption.REPLACE_EXISTING);
                    }
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            });
        }
    }

    /**
     * Copies the source tree when it exists; no-op otherwise.
     *
     * @param src source root
     * @param dst destination root
     * @throws IOException when file copy fails
     */
    private static void copyTreeIfExists(Path src, Path dst) throws IOException {
        if (Files.notExists(src)) {
            return;
        }
        copyTree(src, dst);
    }

    /**
     * Holds the full set of configuration objects needed to run an integration test.
     */
    static final class Runtime {

        private final Path dataPath;
        private final ConnectionConfig connectionConfig;
        private final DbUnitConfig dbUnitConfig;
        private final DumpConfig dumpConfig;
        private final PathsConfig pathsConfig;
        private final FilePatternConfig filePatternConfig;
        private final DbDialectHandlerFactory dialectFactory;

        Runtime(Path dataPath, ConnectionConfig connectionConfig, DbUnitConfig dbUnitConfig,
                DumpConfig dumpConfig, PathsConfig pathsConfig, FilePatternConfig filePatternConfig,
                DbDialectHandlerFactory dialectFactory) {
            this.dataPath = dataPath;
            this.connectionConfig = connectionConfig;
            this.dbUnitConfig = dbUnitConfig;
            this.dumpConfig = dumpConfig;
            this.pathsConfig = pathsConfig;
            this.filePatternConfig = filePatternConfig;
            this.dialectFactory = dialectFactory;
        }

        /**
         * Returns the work directory for this runtime.
         *
         * @return work directory
         */
        Path dataPath() {
            return dataPath;
        }

        /**
         * Creates a {@link DbDialectHandler} for this runtime's connection.
         *
         * @return DbDialectHandler
         */
        DbDialectHandler newDialectHandler() {
            return dialectFactory.create(connectionConfig.getConnections().get(0));
        }

        /**
         * Creates a {@link DataLoader}.
         *
         * @return DataLoader
         */
        DataLoader newLoader() {
            return new DataLoader(pathsConfig, connectionConfig, dialectFactory::create,
                    dbUnitConfig, dumpConfig);
        }

        /**
         * Creates a {@link DataDumper}.
         *
         * @return DataDumper
         */
        DataDumper newDumper() {
            return new DataDumper(pathsConfig, connectionConfig, filePatternConfig, dumpConfig,
                    dialectFactory::create);
        }
    }
}
