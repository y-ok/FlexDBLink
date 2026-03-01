package io.github.yok.flexdblink.db;

import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DataTypeFactoryMode;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.db.mysql.CustomMySqlDataTypeFactory;
import io.github.yok.flexdblink.db.mysql.MySqlDialectHandler;
import io.github.yok.flexdblink.db.oracle.CustomOracleDataTypeFactory;
import io.github.yok.flexdblink.db.oracle.OracleDialectHandler;
import io.github.yok.flexdblink.db.postgresql.CustomPostgresqlDataTypeFactory;
import io.github.yok.flexdblink.db.postgresql.PostgresqlDialectHandler;
import io.github.yok.flexdblink.db.sqlserver.CustomSqlServerDataTypeFactory;
import io.github.yok.flexdblink.db.sqlserver.SqlServerDialectHandler;
import io.github.yok.flexdblink.util.DateTimeFormatSupport;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Locale;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.datatype.IDataTypeFactory;
import org.springframework.stereotype.Component;

/**
 * Factory class that creates a {@link DbDialectHandler} according to the database type.
 *
 * <p>
 * The selected handler is resolved per {@link ConnectionConfig.Entry} using
 * {@code connections[].driver-class} first and the JDBC URL as a fallback. This factory creates
 * handlers for Oracle, PostgreSQL, MySQL, and SQL Server. In addition, dump-related settings such
 * as {@link DumpConfig#excludeTables} are passed to each handler.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DbDialectHandlerFactory {

    // DBUnit settings used by dialect handlers (e.g., preDirName, confirmBeforeLoad)
    private final DbUnitConfig dbUnitConfig;

    // Dump settings (e.g., exclude-tables)
    private final DumpConfig dumpConfig;

    // Builds load/dump directory paths from data-path
    private final PathsConfig pathsConfig;

    // Date/time formatter utility for CSV handling
    private final DateTimeFormatSupport dateTimeFormatter;

    // Applies common settings to DBUnit's DatabaseConfig
    private final DbUnitConfigFactory configFactory;

    /**
     * Creates a {@link DbDialectHandler} based on the provided connection entry.
     *
     * <p>
     * Supported database types (resolved from {@code driver-class} or JDBC URL):
     * </p>
     * <ul>
     * <li>{@code ORACLE}: instantiate {@link OracleDialectHandler}</li>
     * <li>{@code POSTGRESQL}: instantiate {@link PostgresqlDialectHandler}</li>
     * <li>{@code MYSQL}: instantiate {@link MySqlDialectHandler}</li>
     * <li>{@code SQLSERVER}: instantiate {@link SqlServerDialectHandler}</li>
     * </ul>
     *
     * @param entry connection information (URL, user, password, ID, etc.)
     * @return a configured instance of {@link DbDialectHandler}
     * @throws IllegalStateException if handler creation or initialization fails
     */
    public DbDialectHandler create(ConnectionConfig.Entry entry) {
        try {
            DataTypeFactoryMode mode = resolveMode(entry);
            if (mode == DataTypeFactoryMode.ORACLE) {
                return createOracle(entry);
            }
            if (mode == DataTypeFactoryMode.POSTGRESQL) {
                return createPostgresql(entry);
            }
            if (mode == DataTypeFactoryMode.MYSQL) {
                return createMySql(entry);
            }
            return createSqlServer(entry);

        } catch (SQLException e) {
            log.error("Failed to establish JDBC connection or initialize session", e);
            throw new IllegalStateException("Failed to connect to the database", e);

        } catch (DataSetException e) {
            log.error("Failed to initialize DBUnit dataset/connection", e);
            throw new IllegalStateException("Failed to initialize DBUnit dataset", e);

        } catch (IllegalArgumentException e) {
            log.error("Invalid dialect resolution input", e);
            throw new IllegalStateException(e.getMessage(), e);

        } catch (Exception e) {
            log.error("Unexpected error during DbDialectHandler creation", e);
            throw new IllegalStateException("Failed to create DbDialectHandler", e);
        }
    }

    /**
     * Resolves the database type for a connection entry.
     *
     * <p>
     * Resolution priority is {@code driver-class} first, then JDBC URL.
     * </p>
     *
     * @param entry connection entry
     * @return resolved database type
     * @throws IllegalArgumentException if the database type cannot be determined
     */
    private DataTypeFactoryMode resolveMode(ConnectionConfig.Entry entry) {
        DataTypeFactoryMode fromDriverClass = resolveModeFromDriverClass(entry.getDriverClass());
        if (fromDriverClass != null) {
            return fromDriverClass;
        }

        DataTypeFactoryMode fromUrl = resolveModeFromJdbcUrl(entry.getUrl());
        if (fromUrl != null) {
            return fromUrl;
        }

        String message = "Unsupported database dialect for connection id=" + entry.getId()
                + " (driver-class=" + entry.getDriverClass() + ", url=" + entry.getUrl() + ")";
        throw new IllegalArgumentException(message);
    }

    /**
     * Resolves the database type from a JDBC driver class name.
     *
     * @param driverClass JDBC driver class name
     * @return resolved database type, or {@code null} when not recognized
     */
    private DataTypeFactoryMode resolveModeFromDriverClass(String driverClass) {
        String normalized = normalizeLower(driverClass);
        if (normalized == null) {
            return null;
        }
        if ("oracle.jdbc.oracledriver".equals(normalized)) {
            return DataTypeFactoryMode.ORACLE;
        }
        if ("org.postgresql.driver".equals(normalized)) {
            return DataTypeFactoryMode.POSTGRESQL;
        }
        if ("com.mysql.cj.jdbc.driver".equals(normalized)
                || "com.mysql.jdbc.driver".equals(normalized)) {
            return DataTypeFactoryMode.MYSQL;
        }
        if ("com.microsoft.sqlserver.jdbc.sqlserverdriver".equals(normalized)) {
            return DataTypeFactoryMode.SQLSERVER;
        }
        return null;
    }

    /**
     * Resolves the database type from a JDBC URL.
     *
     * @param jdbcUrl JDBC URL
     * @return resolved database type, or {@code null} when not recognized
     */
    private DataTypeFactoryMode resolveModeFromJdbcUrl(String jdbcUrl) {
        String normalized = normalizeLower(jdbcUrl);
        if (normalized == null) {
            return null;
        }
        if (normalized.startsWith("jdbc:oracle:")) {
            return DataTypeFactoryMode.ORACLE;
        }
        if (normalized.startsWith("jdbc:postgresql:")) {
            return DataTypeFactoryMode.POSTGRESQL;
        }
        if (normalized.startsWith("jdbc:mysql:")) {
            return DataTypeFactoryMode.MYSQL;
        }
        if (normalized.startsWith("jdbc:sqlserver:")) {
            return DataTypeFactoryMode.SQLSERVER;
        }
        return null;
    }

    /**
     * Normalizes a string for case-insensitive comparison.
     *
     * @param value source string
     * @return lower-case value, or {@code null} when input is {@code null} or blank
     */
    private String normalizeLower(String value) {
        if (value == null) {
            return null;
        }
        if (value.isBlank()) {
            return null;
        }
        return value.toLowerCase(Locale.ROOT);
    }

    /**
     * Creates Oracle dialect handler.
     *
     * <p>
     * This method creates a {@link DatabaseConnection} with the schema resolved from the connection
     * entry, instantiates {@link OracleDialectHandler} to cache metadata, applies common DBUnit
     * configuration, and initializes the JDBC session.
     * </p>
     *
     * @param entry connection entry
     * @return handler
     * @throws Exception if creation fails
     */
    private DbDialectHandler createOracle(ConnectionConfig.Entry entry) throws Exception {
        return initHandler(entry, entry.getUser().toUpperCase(), new CustomOracleDataTypeFactory(),
                (dbConn) -> new OracleDialectHandler(dbConn, dumpConfig, dbUnitConfig,
                        configFactory, dateTimeFormatter, pathsConfig));
    }

    /**
     * Creates PostgreSQL dialect handler.
     *
     * @param entry connection entry
     * @return handler
     * @throws Exception if creation fails
     */
    private DbDialectHandler createPostgresql(ConnectionConfig.Entry entry) throws Exception {
        return initHandler(entry, "public", new CustomPostgresqlDataTypeFactory(),
                (dbConn) -> new PostgresqlDialectHandler(dbConn,
                dumpConfig, dbUnitConfig, configFactory, dateTimeFormatter, pathsConfig));
    }

    /**
     * Creates MySQL dialect handler.
     *
     * @param entry connection entry
     * @return handler
     * @throws Exception if creation fails
     */
    private DbDialectHandler createMySql(ConnectionConfig.Entry entry) throws Exception {
        return initHandler(entry, resolveMySqlDatabase(entry.getUrl()),
                new CustomMySqlDataTypeFactory(),
                (dbConn) -> new MySqlDialectHandler(dbConn, dumpConfig, dbUnitConfig, configFactory,
                        dateTimeFormatter, pathsConfig));
    }

    /**
     * Creates SQL Server dialect handler.
     *
     * @param entry connection entry
     * @return handler
     * @throws Exception if creation fails
     */
    private DbDialectHandler createSqlServer(ConnectionConfig.Entry entry) throws Exception {
        return initHandler(entry, resolveSqlServerSchema(entry.getUrl()),
                new CustomSqlServerDataTypeFactory(),
                (dbConn) -> new SqlServerDialectHandler(dbConn, dumpConfig, dbUnitConfig,
                        configFactory, dateTimeFormatter, pathsConfig));
    }

    /**
     * Common initialization sequence for all dialect handlers.
     *
     * <p>
     * Opens a temporary JDBC connection and a DBUnit {@link DatabaseConnection} for metadata
     * caching during handler construction, then closes both before returning. The handler does not
     * retain the connection — callers supply their own connections for data operations.
     * </p>
     *
     * @param entry connection entry
     * @param schema schema name for the DBUnit connection
     * @param dataTypeFactory DBUnit data type factory to apply before metadata caching
     * @param handlerFactory function that creates a handler from a DatabaseConnection
     * @return initialized handler
     * @throws Exception if creation fails
     */
    private DbDialectHandler initHandler(ConnectionConfig.Entry entry, String schema,
            IDataTypeFactory dataTypeFactory,
            HandlerFactory handlerFactory) throws Exception {
        Connection jdbc =
                DriverManager.getConnection(entry.getUrl(), entry.getUser(), entry.getPassword());
        DatabaseConnection dbConn = null;
        try {
            dbConn = new DatabaseConnection(jdbc, schema);
            DatabaseConfig config = dbConn.getConfig();
            configFactory.configure(config, dataTypeFactory);

            DbDialectHandler handler = handlerFactory.create(dbConn);

            configFactory.configure(config, handler.getDataTypeFactory());

            handler.prepareConnection(jdbc);

            return handler;
        } finally {
            if (dbConn != null) {
                dbConn.close();
            } else {
                jdbc.close();
            }
        }
    }

    /**
     * Factory function that creates a {@link DbDialectHandler} from a {@link DatabaseConnection}.
     */
    @FunctionalInterface
    private interface HandlerFactory {

        /**
         * Creates a dialect handler.
         *
         * @param dbConn DBUnit connection used for metadata initialization
         * @return dialect handler
         * @throws Exception if creation fails
         */
        DbDialectHandler create(DatabaseConnection dbConn) throws Exception;
    }

    /**
     * Resolves MySQL database name from JDBC URL.
     *
     * @param jdbcUrl JDBC URL
     * @return database name
     */
    private String resolveMySqlDatabase(String jdbcUrl) {
        if (jdbcUrl == null) {
            return "testdb";
        }
        int slash = jdbcUrl.lastIndexOf('/');
        if (slash < 0 || slash == jdbcUrl.length() - 1) {
            return "testdb";
        }
        String tail = jdbcUrl.substring(slash + 1);
        int q = tail.indexOf('?');
        String dbName = q >= 0 ? tail.substring(0, q) : tail;
        if (dbName.isBlank()) {
            return "testdb";
        }
        return dbName;
    }

    /**
     * Resolves SQL Server schema from JDBC URL.
     *
     * @param jdbcUrl JDBC URL
     * @return schema name
     */
    private String resolveSqlServerSchema(String jdbcUrl) {
        return "dbo";
    }
}
