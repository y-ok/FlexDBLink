package io.github.yok.flexdblink.db.sqlserver;

import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.db.DbUnitConfigFactory;
import io.github.yok.flexdblink.db.mysql.MySqlDialectHandler;
import io.github.yok.flexdblink.util.OracleDateTimeFormatUtil;
import java.sql.Connection;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.datatype.IDataTypeFactory;

/**
 * SQL Server dialect handler.
 *
 * <p>
 * The implementation reuses MySQL-based CSV/LOB conversion logic and overrides SQL dialect-specific
 * behaviors for SQL Server.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
public class SqlServerDialectHandler extends MySqlDialectHandler {

    private static final IDataTypeFactory SQLSERVER_TYPE_FACTORY =
            new CustomSqlServerDataTypeFactory();
    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Constructor.
     *
     * @param dbConn DBUnit connection
     * @param dumpConfig dump configuration
     * @param dbUnitConfig DBUnit configuration
     * @param configFactory DBUnit config factory
     * @param dateTimeFormatter date time format utility
     * @param pathsConfig path configuration
     * @throws Exception if initialization fails
     */
    public SqlServerDialectHandler(DatabaseConnection dbConn, DumpConfig dumpConfig,
            DbUnitConfig dbUnitConfig, DbUnitConfigFactory configFactory,
            OracleDateTimeFormatUtil dateTimeFormatter, PathsConfig pathsConfig) throws Exception {
        super(dbConn, dumpConfig, dbUnitConfig, configFactory, dateTimeFormatter, pathsConfig);
    }

    /**
     * Applies SQL Server session settings.
     *
     * @param connection JDBC connection
     * @throws java.sql.SQLException on SQL errors
     */
    @Override
    public void prepareConnection(Connection connection) throws java.sql.SQLException {
        try (Statement st = connection.createStatement()) {
            st.execute("SET LANGUAGE us_english");
            st.execute("SET DATEFORMAT ymd");
        }
    }

    /**
     * Resolves SQL Server schema name.
     *
     * @param entry connection config entry
     * @return SQL Server default schema
     */
    @Override
    public String resolveSchema(ConnectionConfig.Entry entry) {
        return "dbo";
    }

    /**
     * Applies SQL Server pagination.
     *
     * @param baseSql base SQL
     * @param offset offset rows
     * @param limit fetch rows
     * @return paginated SQL
     */
    @Override
    public String applyPagination(String baseSql, int offset, int limit) {
        return baseSql + " OFFSET " + offset + " ROWS FETCH NEXT " + limit + " ROWS ONLY";
    }

    /**
     * Quotes SQL Server identifier.
     *
     * @param identifier identifier
     * @return quoted identifier
     */
    @Override
    public String quoteIdentifier(String identifier) {
        return "[" + identifier + "]";
    }

    /**
     * Returns SQL Server TRUE literal.
     *
     * @return true literal
     */
    @Override
    public String getBooleanTrueLiteral() {
        return "1";
    }

    /**
     * Returns SQL Server FALSE literal.
     *
     * @return false literal
     */
    @Override
    public String getBooleanFalseLiteral() {
        return "0";
    }

    /**
     * Returns SQL Server current timestamp function.
     *
     * @return current timestamp function
     */
    @Override
    public String getCurrentTimestampFunction() {
        return "SYSDATETIME()";
    }

    /**
     * Formats SQL Server datetime literal.
     *
     * @param dateTime datetime value
     * @return datetime literal
     */
    @Override
    public String formatDateLiteral(LocalDateTime dateTime) {
        return "CAST('" + DATE_TIME_FORMATTER.format(dateTime) + "' AS DATETIME2)";
    }

    /**
     * Builds SQL Server MERGE statement.
     *
     * @param tableName table name
     * @param keyColumns key columns
     * @param insertColumns insert columns
     * @param updateColumns update columns
     * @return merge sql
     */
    @Override
    public String buildUpsertSql(String tableName, List<String> keyColumns,
            List<String> insertColumns, List<String> updateColumns) {
        String target = quoteIdentifier(tableName);
        String sourceAlias = "src";
        String targetAlias = "tgt";
        String usingSelect = insertColumns.stream().map(c -> "? AS " + quoteIdentifier(c))
                .collect(Collectors.joining(", "));
        String onClause =
                keyColumns
                        .stream().map(c -> targetAlias + "." + quoteIdentifier(c) + " = "
                                + sourceAlias + "." + quoteIdentifier(c))
                        .collect(Collectors.joining(" AND "));
        String updateSet =
                updateColumns
                        .stream().map(c -> targetAlias + "." + quoteIdentifier(c) + " = "
                                + sourceAlias + "." + quoteIdentifier(c))
                        .collect(Collectors.joining(", "));
        String insertCols =
                insertColumns.stream().map(this::quoteIdentifier).collect(Collectors.joining(", "));
        String insertVals = insertColumns.stream().map(c -> sourceAlias + "." + quoteIdentifier(c))
                .collect(Collectors.joining(", "));

        return "MERGE INTO " + target + " AS " + targetAlias + " USING (SELECT " + usingSelect
                + ") AS " + sourceAlias + " ON " + onClause + " WHEN MATCHED THEN UPDATE SET "
                + updateSet + " WHEN NOT MATCHED THEN INSERT (" + insertCols + ") VALUES ("
                + insertVals + ");";
    }

    /**
     * Returns SQL for creating SQL Server temporary table.
     *
     * @param tempTableName temporary table name
     * @param columns columns map
     * @return create temp table SQL
     */
    @Override
    public String getCreateTempTableSql(String tempTableName, Map<String, String> columns) {
        String defs = columns.entrySet().stream()
                .map(e -> quoteIdentifier(e.getKey()) + " " + e.getValue())
                .collect(Collectors.joining(", "));
        return "CREATE TABLE #" + tempTableName + " (" + defs + ")";
    }

    /**
     * Applies SQL Server update lock hint.
     *
     * @param baseSql base SQL
     * @return SQL with update lock
     */
    @Override
    public String applyForUpdate(String baseSql) {
        return baseSql + " WITH (UPDLOCK, ROWLOCK)";
    }

    /**
     * Returns SQL Server specific DBUnit data type factory.
     *
     * @return data type factory
     */
    @Override
    public IDataTypeFactory getDataTypeFactory() {
        return SQLSERVER_TYPE_FACTORY;
    }
}
