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
import io.github.yok.flexdblink.util.OracleDateTimeFormatUtil;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.flywaydb.core.Flyway;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.oracle.OracleContainer;

final class OracleIntegrationSupport {

    private OracleIntegrationSupport() {}

    static void prepareDatabase(OracleContainer container) {
        ensureContainerRunning(container);
        migrate(container);
    }

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
        OracleDateTimeFormatUtil dateTimeUtil = dateTimeUtil();
        DbDialectHandlerFactory factory =
                dialectFactory(dbUnitConfig, dumpConfig, pathsConfig, dateTimeUtil);

        return new Runtime(dataPath, connectionConfig, dbUnitConfig, dumpConfig, pathsConfig,
                filePatternConfig, dateTimeUtil, factory);
    }

    static void executeLoad(Runtime runtime, String scenario) {
        DataLoader loader = runtime.newLoader();
        loader.execute(scenario, List.of("db1"));
    }

    static Path executeDump(Runtime runtime, String scenario) {
        DataDumper dumper = runtime.newDumper();
        dumper.execute(scenario, List.of("db1"));
        return runtime.dataPath().resolve("dump").resolve(scenario).resolve("db1");
    }

    static Connection openConnection(OracleContainer container) throws SQLException {
        return DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(),
                container.getPassword());
    }

    static Map<String, String> readCsvRowById(Path csvPath, String id) throws IOException {
        Map<String, Map<String, String>> rows = readCsvById(csvPath, "ID");
        Map<String, String> row = rows.get(id);
        if (row == null) {
            throw new IllegalStateException("Row not found: ID=" + id + " in " + csvPath);
        }
        return row;
    }

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

    static String normalizeOffsetDateTime(String value) {
        String v = trimToEmpty(value);
        v = v.replaceAll("\\s+([+-]\\d{2}:?\\d{2})$", "$1");
        return v.replaceAll("([+-]\\d{2}):(\\d{2})$", "$1$2");
    }

    static String normalizeOffsetDateTimeToInstant(String value) {
        String normalized = normalizeOffsetDateTime(value);
        if (normalized.isEmpty()) {
            return normalized;
        }
        Instant instant = parseToInstant(normalized);
        if (instant != null) {
            return instant.toString();
        }
        return normalized;
    }

    static String normalizeDateOnly(String value) {
        String v = trimToEmpty(value);
        Matcher m = Pattern.compile("^(\\d{4}-\\d{2}-\\d{2})").matcher(v);
        if (m.find()) {
            return m.group(1);
        }
        return v;
    }

    static String normalizeIntervalYm(String raw) {
        if (raw == null) {
            return null;
        }
        String s = raw.replaceAll("[\\s\\u3000]+", "");
        Matcher m = Pattern.compile("^([+-]?)(\\d+)-(\\d+)$").matcher(s);
        if (!m.matches()) {
            return s;
        }
        String sign = m.group(1);
        if (!"-".equals(sign)) {
            sign = "+";
        }
        int years = Integer.parseInt(m.group(2));
        int months = Integer.parseInt(m.group(3));
        return String.format("%s%02d-%02d", sign, years, months);
    }

    static String normalizeIntervalDs(String raw) {
        if (raw == null) {
            return null;
        }
        String s = raw.replaceAll("\\.\\d+$", "").replaceAll("[\\s\\u3000]+", " ").trim();
        Matcher m = Pattern.compile("^([+-]?)(\\d+) (\\d+):(\\d+):(\\d+)$").matcher(s);
        if (!m.matches()) {
            return s;
        }
        String sign = m.group(1);
        if (!"-".equals(sign)) {
            sign = "+";
        }
        int days = Integer.parseInt(m.group(2));
        int hours = Integer.parseInt(m.group(3));
        int minutes = Integer.parseInt(m.group(4));
        int seconds = Integer.parseInt(m.group(5));
        return String.format("%s%02d %02d:%02d:%02d", sign, days, hours, minutes, seconds);
    }

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

    static String trimToEmpty(String value) {
        if (value == null) {
            return "";
        }
        return value.trim();
    }

    private static Instant parseToInstant(String value) {
        OffsetDateTime odt = parseOffsetDateTime(value);
        if (odt != null) {
            return odt.toInstant();
        }

        Matcher utcMatcher =
                Pattern.compile("^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?) UTC$")
                        .matcher(value);
        if (utcMatcher.matches()) {
            LocalDateTime ldt = parseLocalDateTime(utcMatcher.group(1));
            if (ldt != null) {
                return ldt.toInstant(ZoneOffset.UTC);
            }
        }

        Matcher zoneMatcher = Pattern.compile(
                "^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?) ([A-Za-z0-9_./+-]+)$")
                .matcher(value);
        if (zoneMatcher.matches()) {
            LocalDateTime ldt = parseLocalDateTime(zoneMatcher.group(1));
            if (ldt != null) {
                try {
                    return ldt.atZone(ZoneId.of(zoneMatcher.group(2))).toInstant();
                } catch (DateTimeException e) {
                    return null;
                }
            }
        }
        return null;
    }

    private static OffsetDateTime parseOffsetDateTime(String value) {
        String isoLike = value;
        Matcher compactMatcher = Pattern
                .compile("^(\\d{4}-\\d{2}-\\d{2}) (\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?)([+-]\\d{4})$")
                .matcher(value);
        if (compactMatcher.matches()) {
            String offset = compactMatcher.group(3);
            isoLike = compactMatcher.group(1) + "T" + compactMatcher.group(2)
                    + offset.substring(0, 3) + ":" + offset.substring(3);
        } else {
            Matcher colonMatcher = Pattern.compile(
                    "^(\\d{4}-\\d{2}-\\d{2}) (\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?)([+-]\\d{2}:\\d{2})$")
                    .matcher(value);
            if (colonMatcher.matches()) {
                isoLike =
                        colonMatcher.group(1) + "T" + colonMatcher.group(2) + colonMatcher.group(3);
            }
        }
        try {
            return OffsetDateTime.parse(isoLike);
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    private static LocalDateTime parseLocalDateTime(String value) {
        DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                .appendPattern("yyyy-MM-dd HH:mm:ss").optionalStart()
                .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd().toFormatter();
        try {
            return LocalDateTime.parse(value, formatter);
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    static void migrate(OracleContainer container) {
        Flyway flyway = Flyway
                .configure().cleanDisabled(false).dataSource(container.getJdbcUrl(),
                        container.getUsername(), container.getPassword())
                .locations("classpath:db/migration/oracle").load();
        flyway.clean();
        flyway.migrate();
    }

    static void copyLoadFixtures(Path dataPath) throws IOException {
        Path src = Path.of("src", "test", "resources", "integration", "load");
        Path dst = dataPath.resolve("load");
        Files.createDirectories(dst);
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

    static PathsConfig pathsConfig(Path dataPath) {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(dataPath.toAbsolutePath().toString());
        return pathsConfig;
    }

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

    static DbUnitConfig dbUnitConfig() {
        DbUnitConfig cfg = new DbUnitConfig();
        cfg.setDataTypeFactoryMode(DataTypeFactoryMode.ORACLE);
        cfg.setPreDirName("pre");
        cfg.setLobDirName("files");
        return cfg;
    }

    static DumpConfig dumpConfig() {
        return new DumpConfig();
    }

    static OracleDateTimeFormatUtil dateTimeUtil() {
        CsvDateTimeFormatProperties props = new CsvDateTimeFormatProperties();
        props.setDate("yyyy-MM-dd");
        props.setTime("HH:mm:ss");
        props.setDateTime("yyyy-MM-dd HH:mm:ss");
        props.setDateTimeWithMillis("yyyy-MM-dd HH:mm:ss.SSS");
        return new OracleDateTimeFormatUtil(props);
    }

    static DbDialectHandlerFactory dialectFactory(DbUnitConfig dbUnitConfig, DumpConfig dumpConfig,
            PathsConfig pathsConfig, OracleDateTimeFormatUtil dateTimeUtil) {
        return new DbDialectHandlerFactory(dbUnitConfig, dumpConfig, pathsConfig, dateTimeUtil,
                new DbUnitConfigFactory());
    }

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

    static void ensureContainerRunning(OracleContainer container) {
        try {
            container.start();
        } catch (ContainerLaunchException e) {
            throw new IllegalStateException(
                    "Oracle container must be available for integration tests", e);
        }
    }

    static final class Runtime {

        private final Path dataPath;

        private final ConnectionConfig connectionConfig;

        private final DbUnitConfig dbUnitConfig;

        private final DumpConfig dumpConfig;

        private final PathsConfig pathsConfig;

        private final FilePatternConfig filePatternConfig;

        private final OracleDateTimeFormatUtil dateTimeUtil;

        private final DbDialectHandlerFactory dialectFactory;

        Runtime(Path dataPath, ConnectionConfig connectionConfig, DbUnitConfig dbUnitConfig,
                DumpConfig dumpConfig, PathsConfig pathsConfig, FilePatternConfig filePatternConfig,
                OracleDateTimeFormatUtil dateTimeUtil, DbDialectHandlerFactory dialectFactory) {
            this.dataPath = dataPath;
            this.connectionConfig = connectionConfig;
            this.dbUnitConfig = dbUnitConfig;
            this.dumpConfig = dumpConfig;
            this.pathsConfig = pathsConfig;
            this.filePatternConfig = filePatternConfig;
            this.dateTimeUtil = dateTimeUtil;
            this.dialectFactory = dialectFactory;
        }

        Path dataPath() {
            return dataPath;
        }

        DataLoader newLoader() {
            return new DataLoader(pathsConfig, connectionConfig,
                    entry -> entry.getUser().toUpperCase(), dialectFactory::create, dbUnitConfig,
                    dumpConfig);
        }

        DataDumper newDumper() {
            return new DataDumper(pathsConfig, connectionConfig, filePatternConfig, dumpConfig,
                    entry -> entry.getUser().toUpperCase(), dialectFactory::create, dateTimeUtil);
        }
    }
}
