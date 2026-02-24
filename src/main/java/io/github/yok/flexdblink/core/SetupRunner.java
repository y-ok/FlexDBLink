package io.github.yok.flexdblink.core;

import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.db.DbDialectHandler;
import io.github.yok.flexdblink.util.ErrorHandler;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

/**
 * Scans the database schema for LOB columns and generates {@code file-patterns} entries in
 * {@code conf/application.yml}.
 *
 * <p>
 * For each LOB column found, the generated pattern is {@code {column}_{PK}.{ext}} where the
 * extension is resolved using the DB-native type name first (D-method), falling back to the JDBC
 * SQL type.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
@Slf4j
public class SetupRunner {

    /**
     * Extension resolved from the DB-native type name (case-insensitive). Checked before the JDBC
     * type fallback.
     */
    static final Map<String, String> TYPE_NAME_EXTENSIONS;

    static {
        Map<String, String> m = new LinkedHashMap<>();
        // Oracle
        m.put("nclob", "nclob");
        m.put("clob", "clob");
        m.put("blob", "dat");
        // PostgreSQL
        m.put("bytea", "bin");
        // MySQL (BLOB family)
        m.put("tinyblob", "dat");
        m.put("mediumblob", "dat");
        m.put("longblob", "dat");
        // MySQL (TEXT family)
        m.put("tinytext", "clob");
        m.put("mediumtext", "clob");
        m.put("longtext", "clob");
        // SQL Server
        m.put("varbinary", "bin");
        m.put("image", "bin");
        TYPE_NAME_EXTENSIONS = Collections.unmodifiableMap(m);
    }

    /** Fallback extension keyed by JDBC SQL type. */
    static final Map<Integer, String> JDBC_TYPE_EXTENSIONS =
            Map.of(Types.BLOB, "dat", Types.CLOB, "clob", Types.NCLOB, "nclob");

    private final ConnectionConfig connectionConfig;
    private final Function<ConnectionConfig.Entry, DbDialectHandler> dialectHandlerProvider;

    /**
     * Constructs a SetupRunner.
     *
     * @param connectionConfig connection configuration
     * @param dialectHandlerProvider factory function that creates a dialect handler per connection
     */
    public SetupRunner(ConnectionConfig connectionConfig,
            Function<ConnectionConfig.Entry, DbDialectHandler> dialectHandlerProvider) {
        this.connectionConfig = connectionConfig;
        this.dialectHandlerProvider = dialectHandlerProvider;
    }

    /**
     * Iterates over each target DB, detects LOB columns, builds file-pattern entries, and writes
     * them to {@code conf/application.yml}.
     *
     * @param targetDbIds list of DB IDs to process; if empty all connections are used
     */
    public void execute(List<String> targetDbIds) {
        // Merged result across all target DBs: table → (column → pattern)
        Map<String, Map<String, String>> merged = new LinkedHashMap<>();

        for (ConnectionConfig.Entry entry : connectionConfig.getConnections()) {
            String dbId = entry.getId();
            if (targetDbIds != null && !targetDbIds.isEmpty() && !targetDbIds.contains(dbId)) {
                log.info("[{}] Not targeted → skipping", dbId);
                continue;
            }

            try {
                Class.forName(entry.getDriverClass());
            } catch (ClassNotFoundException e) {
                ErrorHandler.errorAndExit("Driver class not found: " + entry.getDriverClass(), e);
                return;
            }

            try (Connection conn = DriverManager.getConnection(entry.getUrl(), entry.getUser(),
                    entry.getPassword())) {

                DbDialectHandler handler = dialectHandlerProvider.apply(entry);
                String schema = handler.resolveSchema(entry);

                List<String> tables = fetchTables(conn, schema);
                for (String table : tables) {
                    Map<String, String> colPatterns = buildPatterns(conn, schema, table, handler);
                    if (!colPatterns.isEmpty()) {
                        merged.merge(table, colPatterns, (existing, incoming) -> {
                            existing.putAll(incoming);
                            return existing;
                        });
                    }
                }

                log.info("[{}] LOB scan completed: {} table(s) with LOB columns", dbId,
                        merged.size());

            } catch (SQLException e) {
                ErrorHandler.errorAndExit("DB error during setup (DB=" + dbId + ")", e);
            }
        }

        if (merged.isEmpty()) {
            log.info("No LOB columns found. file-patterns not updated.");
            return;
        }

        Path configFile = resolveConfigFile();
        if (writeFilePatterns(configFile, merged)) {
            log.info("file-patterns written to {}", configFile.toAbsolutePath());
        }
    }

    /**
     * Returns all non-excluded table names in the given schema.
     *
     * @param conn JDBC connection
     * @param schema schema name (may be null)
     * @return ordered list of table names
     * @throws SQLException on metadata error
     */
    List<String> fetchTables(Connection conn, String schema) throws SQLException {
        Map<String, String> result = new LinkedHashMap<>();
        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet rs = meta.getTables(null, schema, "%", new String[] {"TABLE"})) {
            while (rs.next()) {
                String name = rs.getString("TABLE_NAME");
                result.put(name, name);
            }
        }
        return List.copyOf(result.values());
    }

    /**
     * Scans the columns of a table and builds {@code column → pattern} entries for LOB columns.
     *
     * @param conn JDBC connection
     * @param schema schema name
     * @param table table name
     * @param handler dialect handler (used for PK retrieval)
     * @return map of column name to file pattern; empty if no LOB columns found
     * @throws SQLException on metadata error
     */
    Map<String, String> buildPatterns(Connection conn, String schema, String table,
            DbDialectHandler handler) throws SQLException {
        List<String> pks = handler.getPrimaryKeyColumns(conn, schema, table);
        String pkPlaceholder = buildPkPlaceholder(pks);

        Map<String, String> patterns = new LinkedHashMap<>();
        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet rs = meta.getColumns(null, schema, table, null)) {
            while (rs.next()) {
                String col = rs.getString("COLUMN_NAME");
                int jdbcType = rs.getInt("DATA_TYPE");
                String typeName = rs.getString("TYPE_NAME");
                if (!isLobType(jdbcType, typeName)) {
                    continue;
                }
                String ext = resolveExtension(typeName, jdbcType);
                patterns.put(col, col + "_" + pkPlaceholder + "." + ext);
            }
        }
        return patterns;
    }

    /**
     * Builds the PK placeholder string used in file patterns.
     * <ul>
     * <li>Single PK: {@code {PK_COL}}</li>
     * <li>Composite PK: {@code {COL1}_{COL2}}</li>
     * <li>No PK: {@code {ID}}</li>
     * </ul>
     *
     * @param pks ordered list of primary key column names
     * @return placeholder string
     */
    String buildPkPlaceholder(List<String> pks) {
        if (pks == null || pks.isEmpty()) {
            return "{ID}";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < pks.size(); i++) {
            if (i > 0) {
                sb.append('_');
            }
            sb.append('{').append(pks.get(i)).append('}');
        }
        return sb.toString();
    }

    /**
     * Returns whether the column should be treated as a LOB based on JDBC type or type name.
     *
     * @param jdbcType JDBC SQL type code
     * @param typeName DB-native type name
     * @return true if the column is a LOB
     */
    boolean isLobType(int jdbcType, String typeName) {
        if (JDBC_TYPE_EXTENSIONS.containsKey(jdbcType)) {
            return true;
        }
        return TYPE_NAME_EXTENSIONS.containsKey(typeName == null ? "" : typeName.toLowerCase());
    }

    /**
     * Resolves the file extension using D-method: DB type name first, JDBC type fallback.
     *
     * @param typeName DB-native type name
     * @param jdbcType JDBC SQL type code
     * @return file extension without leading dot
     */
    String resolveExtension(String typeName, int jdbcType) {
        if (typeName != null) {
            String ext = TYPE_NAME_EXTENSIONS.get(typeName.toLowerCase());
            if (ext != null) {
                return ext;
            }
        }
        return JDBC_TYPE_EXTENSIONS.getOrDefault(jdbcType, "bin");
    }

    /**
     * Determines the path of the configuration file to update. Prefers the value of
     * {@code spring.config.additional-location} system property; defaults to
     * {@code conf/application.yml}.
     *
     * @return resolved path to the application config file
     */
    Path resolveConfigFile() {
        String sysProp = System.getProperty("spring.config.additional-location");
        if (sysProp != null) {
            String dir = sysProp.replace("optional:", "").replace("file:", "");
            return Paths.get(dir).resolve("application.yml");
        }
        return Paths.get("conf", "application.yml");
    }

    /**
     * Reads the YAML config file, replaces the {@code file-patterns} key with the generated map,
     * and writes the result back.
     *
     * @param configFile path to application.yml
     * @param patterns generated file-patterns map
     */
    boolean writeFilePatterns(Path configFile, Map<String, Map<String, String>> patterns) {
        if (!Files.exists(configFile)) {
            ErrorHandler.errorAndExit("Config file not found: " + configFile.toAbsolutePath());
            return false;
        }

        Yaml reader = new Yaml();
        Map<String, Object> config;
        try {
            config = reader.load(Files.newInputStream(configFile));
        } catch (IOException e) {
            ErrorHandler.errorAndExit("Failed to read config file: " + configFile, e);
            return false;
        }
        if (config == null) {
            config = new LinkedHashMap<>();
        }

        config.put("file-patterns", patterns);

        DumperOptions opts = new DumperOptions();
        opts.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        opts.setPrettyFlow(true);
        try (FileWriter fw = new FileWriter(configFile.toFile())) {
            new Yaml(opts).dump(config, fw);
        } catch (IOException e) {
            ErrorHandler.errorAndExit("Failed to write config file: " + configFile, e);
            return false;
        }
        return true;
    }
}
