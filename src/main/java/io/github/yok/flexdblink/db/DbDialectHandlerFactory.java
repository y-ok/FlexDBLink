package io.github.yok.flexdblink.db;

import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DataTypeFactoryMode;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.db.mysql.MySqlDialectHandler;
import io.github.yok.flexdblink.db.oracle.OracleDialectHandler;
import io.github.yok.flexdblink.db.postgresql.PostgresqlDialectHandler;
import io.github.yok.flexdblink.db.sqlserver.SqlServerDialectHandler;
import io.github.yok.flexdblink.util.DateTimeFormatSupport;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.springframework.stereotype.Component;

/**
 * Factory class that creates a {@link DbDialectHandler} according to the database type.
 *
 * <p>
 * The selected handler is based on {@link DbUnitConfig}'s DataTypeFactoryMode. This factory creates
 * handlers for Oracle, PostgreSQL, MySQL, and SQL Server modes. In addition, dump-related settings
 * such as {@link DumpConfig#excludeTables} are passed to each handler.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DbDialectHandlerFactory {

    // DBUnit settings (includes dataTypeFactoryMode, preDirName)
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
     * Supported DataTypeFactoryMode:
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
            DataTypeFactoryMode mode = dbUnitConfig.getDataTypeFactoryMode();
            if (mode == DataTypeFactoryMode.ORACLE) {
                return createOracle(entry);
            }
            if (mode == DataTypeFactoryMode.POSTGRESQL) {
                return createPostgresql(entry);
            }
            if (mode == DataTypeFactoryMode.MYSQL) {
                return createMySql(entry);
            }
            if (mode == DataTypeFactoryMode.SQLSERVER) {
                return createSqlServer(entry);
            }
            String msg = "Unsupported DataTypeFactoryMode: " + mode;
            log.error(msg);
            throw new IllegalArgumentException(msg);

        } catch (SQLException e) {
            log.error("Failed to establish JDBC connection or initialize session", e);
            throw new IllegalStateException("Failed to connect to the database", e);

        } catch (DataSetException e) {
            log.error("Failed to initialize DBUnit dataset/connection", e);
            throw new IllegalStateException("Failed to initialize DBUnit dataset", e);

        } catch (Exception e) {
            log.error("Unexpected error during DbDialectHandler creation", e);
            throw new IllegalStateException("Failed to create DbDialectHandler", e);
        }
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
        Connection jdbc =
                DriverManager.getConnection(entry.getUrl(), entry.getUser(), entry.getPassword());
        DatabaseConnection dbConn = new DatabaseConnection(jdbc, entry.getUser().toUpperCase());

        OracleDialectHandler handler = new OracleDialectHandler(dbConn, dumpConfig, dbUnitConfig,
                configFactory, dateTimeFormatter, pathsConfig);

        DatabaseConfig config = dbConn.getConfig();
        configFactory.configure(config, handler.getDataTypeFactory());

        handler.prepareConnection(jdbc);

        return handler;
    }

    /**
     * Creates PostgreSQL dialect handler.
     *
     * <p>
     * Creates a single {@link DatabaseConnection} and a single {@link PostgresqlDialectHandler}
     * instance, then applies common DBUnit configuration and session initialization.
     * </p>
     *
     * @param entry connection entry
     * @return handler
     * @throws Exception if creation fails
     */
    private DbDialectHandler createPostgresql(ConnectionConfig.Entry entry) throws Exception {
        Connection jdbc =
                DriverManager.getConnection(entry.getUrl(), entry.getUser(), entry.getPassword());

        String schema = "public";
        DatabaseConnection dbConn = new DatabaseConnection(jdbc, schema);

        PostgresqlDialectHandler handler = new PostgresqlDialectHandler(dbConn, dumpConfig,
                dbUnitConfig, configFactory, dateTimeFormatter, pathsConfig);

        DatabaseConfig config = dbConn.getConfig();
        configFactory.configure(config, handler.getDataTypeFactory());

        handler.prepareConnection(jdbc);

        return handler;
    }

    /**
     * Creates MySQL dialect handler.
     *
     * @param entry connection entry
     * @return handler
     * @throws Exception if creation fails
     */
    private DbDialectHandler createMySql(ConnectionConfig.Entry entry) throws Exception {
        Connection jdbc =
                DriverManager.getConnection(entry.getUrl(), entry.getUser(), entry.getPassword());

        String schema = resolveMySqlDatabase(entry.getUrl());
        DatabaseConnection dbConn = new DatabaseConnection(jdbc, schema);

        MySqlDialectHandler handler = new MySqlDialectHandler(dbConn, dumpConfig, dbUnitConfig,
                configFactory, dateTimeFormatter, pathsConfig);

        DatabaseConfig config = dbConn.getConfig();
        configFactory.configure(config, handler.getDataTypeFactory());

        handler.prepareConnection(jdbc);

        return handler;
    }

    /**
     * Creates SQL Server dialect handler.
     *
     * @param entry connection entry
     * @return handler
     * @throws Exception if creation fails
     */
    private DbDialectHandler createSqlServer(ConnectionConfig.Entry entry) throws Exception {
        Connection jdbc =
                DriverManager.getConnection(entry.getUrl(), entry.getUser(), entry.getPassword());

        String schema = resolveSqlServerSchema(entry.getUrl());
        DatabaseConnection dbConn = new DatabaseConnection(jdbc, schema);

        SqlServerDialectHandler handler = new SqlServerDialectHandler(dbConn, dumpConfig,
                dbUnitConfig, configFactory, dateTimeFormatter, pathsConfig);

        DatabaseConfig config = dbConn.getConfig();
        configFactory.configure(config, handler.getDataTypeFactory());

        handler.prepareConnection(jdbc);

        return handler;
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
