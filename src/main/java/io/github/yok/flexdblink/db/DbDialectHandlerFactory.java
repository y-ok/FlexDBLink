package io.github.yok.flexdblink.db;

import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.util.OracleDateTimeFormatUtil;
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
 * The selected handler is based on {@link DbUnitConfig}'s DataTypeFactoryMode. Currently this
 * factory creates an {@link OracleDialectHandler} for the ORACLE mode. In addition, dump-related
 * settings such as {@link DumpConfig#excludeTables} are passed to the handler.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DbDialectHandlerFactory {

    // DBUnit settings (includes dataTypeFactoryMode, lobDirName, preDirName)
    private final DbUnitConfig dbUnitConfig;

    // Dump settings (e.g., exclude-tables)
    private final DumpConfig dumpConfig;

    // Builds load/dump directory paths from data-path
    private final PathsConfig pathsConfig;

    // Date/time formatter utility for CSV handling
    private final OracleDateTimeFormatUtil dateTimeFormatter;

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
     * </ul>
     *
     * @param entry connection information (URL, user, password, ID, etc.)
     * @return a configured instance of {@link DbDialectHandler}
     * @throws IllegalStateException if handler creation or initialization fails
     */
    public DbDialectHandler create(ConnectionConfig.Entry entry) {
        try {
            switch (dbUnitConfig.getDataTypeFactoryMode()) {
                case ORACLE:
                    return createOracle(entry);

                case POSTGRESQL:
                    return createPostgresql(entry);

                case MYSQL:
                    return createMySql(entry);

                default:
                    String msg = "Unsupported DataTypeFactoryMode: "
                            + dbUnitConfig.getDataTypeFactoryMode();
                    log.error(msg);
                    throw new IllegalArgumentException(msg);
            }

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
}
