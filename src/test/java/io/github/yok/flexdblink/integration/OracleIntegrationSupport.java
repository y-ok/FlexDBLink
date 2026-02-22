package io.github.yok.flexdblink.integration;

import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.CsvDateTimeFormatProperties;
import io.github.yok.flexdblink.config.DataTypeFactoryMode;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.FilePatternConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.core.DataDumper;
import io.github.yok.flexdblink.core.DataLoader;
import io.github.yok.flexdblink.db.DbDialectHandlerFactory;
import io.github.yok.flexdblink.db.DbUnitConfigFactory;
import io.github.yok.flexdblink.util.DateTimeFormatUtil;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.flywaydb.core.Flyway;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.oracle.OracleContainer;

/**
 * Utility class for Oracle integration tests.
 *
 * <p>
 * Runs Flyway against a Testcontainers-managed Oracle instance and exercises
 * FlexDBLink (DataLoader / DataDumper) in a production-like configuration.
 * </p>
 *
 * <h2>Expected test resource layout</h2>
 *
 * <pre>
 * src/test/resources
 * ├── db/migration/oracle
 * │   ├── V1__create_oracle_integration_test_tables.sql
 * │   └── V2__seed_integration_test_data.sql
 * └── integration/oracle/load
 *     └── pre
 *         └── db1
 *             ├── files
 *             │   └── (LOB files)
 *             ├── IT_TYPED_MAIN.csv
 *             ├── IT_TYPED_AUX.csv
 *             └── table-ordering.txt
 * </pre>
 *
 * <h2>Responsibilities</h2>
 * <ul>
 * <li>DB setup: start Oracle container and run Flyway clean/migrate</li>
 * <li>Runtime setup: assemble Paths/Connection/DbUnit/Dump/Pattern/DateTime/DialectFactory</li>
 * <li>Execution: provide load/dump helpers and CSV utility methods</li>
 * </ul>
 */
final class OracleIntegrationSupport {

    private OracleIntegrationSupport() {}

    /**
     * Starts the Oracle container and initializes the test schema with Flyway (all migrations).
     *
     * <p>
     * Runs Flyway clean → migrate on every call to ensure the DB is in a clean state
     * at the start of each test.
     * </p>
     *
     * @param container Oracle container
     */
    static void prepareDatabase(OracleContainer container) {
        ensureContainerRunning(container);
        migrate(container);
    }

    /**
     * Initializes the Oracle container without FK constraints.
     *
     * <p>
     * Applies Flyway migrations only up to V2, excluding the V3 FK constraint (FK_AUX_MAIN).
     * </p>
     *
     * @param container Oracle container
     */
    static void prepareDatabaseWithoutFk(OracleContainer container) {
        ensureContainerRunning(container);
        migrateWithoutFk(container);
    }

    private static void migrateWithoutFk(OracleContainer container) {
        Flyway flyway = Flyway
                .configure().cleanDisabled(false).dataSource(container.getJdbcUrl(),
                        container.getUsername(), container.getPassword())
                .locations("classpath:db/migration/oracle").target("2").load();
        flyway.clean();
        flyway.migrate();
    }

    /**
     * Assembles the runtime required to run an integration test.
     *
     * <p>
     * When {@code copyLoadFixtures} is {@code true}, the following resources are copied to
     * {@code dataPath/load}:
     * </p>
     *
     * <pre>
     * src/test/resources/integration/oracle/load/** → ${dataPath}/load/**
     * src/test/resources/integration/common/load/** → ${dataPath}/load/**
     * </pre>
     *
     * @param container Oracle container
     * @param dataPath root path for test data (typically a temp directory)
     * @param copyLoadFixtures {@code true} to copy load fixtures into the work directory
     * @return assembled runtime
     * @throws IOException on file I/O failure
     */
    static Runtime prepareRuntime(OracleContainer container, Path dataPath,
            boolean copyLoadFixtures) throws IOException {
        Files.createDirectories(dataPath);
        if (copyLoadFixtures) {
            copyLoadFixtures(dataPath);
        }

        PathsConfig pathsConfig = pathsConfig(dataPath);
        ConnectionConfig connectionConfig = connectionConfig(container);
        DbUnitConfig dbUnitConfig = dbUnitConfig();
        DumpConfig dumpConfig = dumpConfig();
        dumpConfig.setExcludeTables(List.of("FLYWAY_SCHEMA_HISTORY"));
        FilePatternConfig filePatternConfig = filePatternConfig();
        DateTimeFormatUtil dateTimeUtil = dateTimeUtil();
        DbDialectHandlerFactory factory =
                dialectFactory(dbUnitConfig, dumpConfig, pathsConfig, dateTimeUtil);

        return new Runtime(dataPath, connectionConfig, dbUnitConfig, dumpConfig, pathsConfig,
                filePatternConfig, factory);
    }

    /**
     * Executes DataLoader for the specified scenario.
     *
     * <p>
     * The target connection ID is fixed to {@code db1}.
     * </p>
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
     * <p>
     * The output directory is {@code ${dataPath}/dump/${scenario}/db1}.
     * </p>
     *
     * @param runtime runtime
     * @param scenario scenario name
     * @return output directory
     */
    static Path executeDump(Runtime runtime, String scenario) {
        DataDumper dumper = runtime.newDumper();
        dumper.execute(scenario, List.of("db1"));
        return runtime.dataPath().resolve("dump").resolve(scenario).resolve("db1");
    }

    /**
     * Opens a JDBC connection to the Oracle container.
     *
     * @param container Oracle container
     * @return JDBC connection
     * @throws SQLException on JDBC error
     */
    static Connection openConnection(OracleContainer container) throws SQLException {
        return DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(),
                container.getPassword());
    }

    /**
     * Reads a single row from a CSV file by its {@code ID} column value.
     *
     * @param csvPath path to the CSV file
     * @param id ID value to look up
     * @return map of header → value for the matching row
     * @throws IOException on CSV read failure
     */
    static Map<String, String> readCsvRowById(Path csvPath, String id) throws IOException {
        Map<String, Map<String, String>> rows = readCsvById(csvPath, "ID");
        Map<String, String> row = rows.get(id);
        if (row == null) {
            throw new IllegalStateException("Row not found: ID=" + id + " in " + csvPath);
        }
        return row;
    }

    /**
     * Parses a CSV file into a map keyed by the specified ID column.
     *
     * <p>
     * The first row is treated as the header. Insertion order is preserved via
     * {@link LinkedHashMap}.
     * </p>
     *
     * @param csvPath path to the CSV file
     * @param idColumn name of the ID column
     * @return map of idColumn value → (header → value)
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
     * Decodes a hex string into a byte array.
     *
     * @param value hex string
     * @return decoded byte array, or an empty array if {@code value} is null or empty
     */
    static byte[] decodeHex(String value) {
        if (value == null || value.isEmpty()) {
            return new byte[0];
        }
        try {
            return Hex.decodeHex(value);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid hex value: " + value, e);
        }
    }

    /**
     * Trims the value and returns {@code null} if the result is empty.
     *
     * @param value input value
     * @return {@code null} or the trimmed string
     */
    static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        return trimmed;
    }

    /**
     * Trims the value and converts {@code null} to an empty string.
     *
     * @param value input value
     * @return empty string or the trimmed string
     */
    static String trimToEmpty(String value) {
        if (value == null) {
            return "";
        }
        return value.trim();
    }

    /**
     * Initializes the Oracle schema using Flyway (all migrations).
     *
     * <p>
     * The migration location is fixed to {@code classpath:db/migration/oracle}.
     * </p>
     *
     * @param container Oracle container
     */
    static void migrate(OracleContainer container) {
        Flyway flyway = Flyway
                .configure().cleanDisabled(false).dataSource(container.getJdbcUrl(),
                        container.getUsername(), container.getPassword())
                .locations("classpath:db/migration/oracle").load();
        flyway.clean();
        flyway.migrate();
    }

    /**
     * Copies the load fixtures for Oracle tests into {@code dataPath/load}.
     *
     * <pre>
     * src/test/resources/integration/oracle/load/** → ${dataPath}/load/**
     * src/test/resources/integration/common/load/** → ${dataPath}/load/**
     * </pre>
     *
     * @param dataPath root path for test data
     * @throws IOException on copy failure
     */
    static void copyLoadFixtures(Path dataPath) throws IOException {
        Path src = Path.of("src", "test", "resources", "integration", "oracle", "load");
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
     * Creates a {@link PathsConfig} for the given data path.
     *
     * @param dataPath root path for test data
     * @return PathsConfig
     */
    static PathsConfig pathsConfig(Path dataPath) {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(dataPath.toAbsolutePath().toString());
        return pathsConfig;
    }

    /**
     * Creates a {@link ConnectionConfig} for the Oracle container.
     *
     * <p>
     * The connection ID is fixed to {@code db1}.
     * </p>
     *
     * @param container Oracle container
     * @return ConnectionConfig
     */
    static ConnectionConfig connectionConfig(OracleContainer container) {
        ConnectionConfig connectionConfig = new ConnectionConfig();
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        entry.setUrl(container.getJdbcUrl());
        entry.setUser(container.getUsername());
        entry.setPassword(container.getPassword());
        entry.setDriverClass("oracle.jdbc.OracleDriver");
        connectionConfig.setConnections(List.of(entry));
        return connectionConfig;
    }

    /**
     * Creates a {@link DbUnitConfig} configured for Oracle.
     *
     * @return DbUnitConfig
     */
    static DbUnitConfig dbUnitConfig() {
        DbUnitConfig cfg = new DbUnitConfig();
        cfg.setDataTypeFactoryMode(DataTypeFactoryMode.ORACLE);
        cfg.setPreDirName("pre");
        return cfg;
    }

    /**
     * Creates a default {@link DumpConfig}.
     *
     * @return DumpConfig
     */
    static DumpConfig dumpConfig() {
        return new DumpConfig();
    }

    /**
     * Creates a {@link DateTimeFormatUtil} with the standard CSV date/time format.
     *
     * @return DateTimeFormatUtil
     */
    static DateTimeFormatUtil dateTimeUtil() {
        CsvDateTimeFormatProperties props = new CsvDateTimeFormatProperties();
        props.setDate("yyyy-MM-dd");
        props.setTime("HH:mm:ss");
        props.setDateTime("yyyy-MM-dd HH:mm:ss");
        props.setDateTimeWithMillis("yyyy-MM-dd HH:mm:ss.SSS");
        return new DateTimeFormatUtil(props);
    }

    /**
     * Creates a {@link DbDialectHandlerFactory}.
     *
     * @param dbUnitConfig DbUnitConfig
     * @param dumpConfig DumpConfig
     * @param pathsConfig PathsConfig
     * @param dateTimeUtil DateTimeFormatUtil
     * @return DbDialectHandlerFactory
     */
    static DbDialectHandlerFactory dialectFactory(DbUnitConfig dbUnitConfig, DumpConfig dumpConfig,
            PathsConfig pathsConfig, DateTimeFormatUtil dateTimeUtil) {
        return new DbDialectHandlerFactory(dbUnitConfig, dumpConfig, pathsConfig, dateTimeUtil,
                new DbUnitConfigFactory());
    }

    /**
     * Creates a {@link FilePatternConfig} with the default LOB file naming patterns.
     *
     * @return FilePatternConfig
     */
    static FilePatternConfig filePatternConfig() {
        FilePatternConfig filePatternConfig = new FilePatternConfig();
        Map<String, Map<String, String>> map = new HashMap<>();

        Map<String, String> main = new LinkedHashMap<>();
        main.put("CLOB_COL", "main_{LOB_KIND}_{ID}.txt");
        main.put("NCLOB_COL", "main_n_{LOB_KIND}_{ID}.txt");
        main.put("BLOB_COL", "main_{LOB_KIND}_{ID}.bin");
        map.put("IT_TYPED_MAIN", main);

        Map<String, String> aux = new LinkedHashMap<>();
        aux.put("PAYLOAD_CLOB", "aux_{LOB_KIND}_{ID}.txt");
        aux.put("PAYLOAD_BLOB", "aux_{LOB_KIND}_{ID}.bin");
        map.put("IT_TYPED_AUX", aux);

        filePatternConfig.setFilePatterns(map);
        return filePatternConfig;
    }

    /**
     * Ensures the Oracle container is started.
     *
     * @param container Oracle container
     */
    static void ensureContainerRunning(OracleContainer container) {
        try {
            container.start();
        } catch (ContainerLaunchException e) {
            throw new IllegalStateException(
                    "Oracle container must be available for integration tests", e);
        }
    }

    /**
     * Holds the full set of configuration objects needed to run an integration test.
     *
     * <p>
     * Aggregates the dependencies (PathsConfig, ConnectionConfig, etc.) required to
     * create {@link DataLoader} and {@link DataDumper} instances.
     * </p>
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

        Path dataPath() {
            return dataPath;
        }

        /**
         * Creates a {@link DataLoader} for Oracle.
         *
         * @return DataLoader
         */
        DataLoader newLoader() {
            return new DataLoader(pathsConfig, connectionConfig, dialectFactory::create,
                    dbUnitConfig, dumpConfig);
        }

        /**
         * Creates a {@link DataDumper} for Oracle.
         *
         * @return DataDumper
         */
        DataDumper newDumper() {
            return new DataDumper(pathsConfig, connectionConfig, filePatternConfig, dumpConfig,
                    dialectFactory::create);
        }
    }
}
