package io.github.yok.flexdblink.integration;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.FilePatternConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.core.DataDumper;
import io.github.yok.flexdblink.core.DataLoader;
import io.github.yok.flexdblink.db.DbDialectHandler;
import io.github.yok.flexdblink.db.DbDialectHandlerFactory;
import io.github.yok.flexdblink.util.CsvUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.flywaydb.core.Flyway;
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

    /**
     * Starts the container and initializes the test schema with Flyway (all migrations).
     *
     * @param container JDBC container
     * @param migrationLocation Flyway migration location (e.g.
     *        {@code classpath:db/migration/mysql})
     */
    static void prepareDatabase(JdbcDatabaseContainer<?> container, String migrationLocation) {
        ensureContainerRunning(container);
        migrate(container, migrationLocation);
    }

    /**
     * Starts the container and initializes the schema without FK constraints (up to V2).
     *
     * @param container JDBC container
     * @param migrationLocation Flyway migration location
     */
    static void prepareDatabaseWithoutFk(JdbcDatabaseContainer<?> container,
            String migrationLocation) {
        ensureContainerRunning(container);
        migrateWithoutFk(container, migrationLocation);
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

        for (Map.Entry<String, Map<String, String>> entry : inputRows.entrySet()) {
            String id = entry.getKey();
            Map<String, String> inRow = entry.getValue();
            Map<String, String> outRow = outputRows.get(id);
            assertNotNull(outRow, table + " ID=" + id + " not found in output CSV");

            for (Map.Entry<String, String> colEntry : inRow.entrySet()) {
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

        for (Map.Entry<String, Map<String, String>> entry : csvRows.entrySet()) {
            String id = entry.getKey();
            Map<String, String> csvRow = entry.getValue();

            String sql = "SELECT * FROM " + tableName + " WHERE " + idColumn + " = " + id;

            try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
                assertTrue(rs.next(), tableName + " ID=" + id + " not found in DB");

                for (Map.Entry<String, String> col : csvRow.entrySet()) {
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
        CSVFormat format = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true)
                .setIgnoreSurroundingSpaces(false).setTrim(false).get();

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
     * @param migrationLocation Flyway migration location
     */
    private static void migrate(JdbcDatabaseContainer<?> container, String migrationLocation) {
        Flyway flyway = Flyway
                .configure().cleanDisabled(false).dataSource(container.getJdbcUrl(),
                        container.getUsername(), container.getPassword())
                .locations(migrationLocation).load();
        flyway.clean();
        flyway.migrate();
    }

    /**
     * Runs Flyway migration up to version 2 (without foreign keys).
     *
     * @param container JDBC container
     * @param migrationLocation Flyway migration location
     */
    private static void migrateWithoutFk(JdbcDatabaseContainer<?> container,
            String migrationLocation) {
        Flyway flyway = Flyway
                .configure().cleanDisabled(false).dataSource(container.getJdbcUrl(),
                        container.getUsername(), container.getPassword())
                .locations(migrationLocation).target("2").load();
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
        copyTree(src, dst);
        copyTree(commonSrc, dst);
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
