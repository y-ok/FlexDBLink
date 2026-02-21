package io.github.yok.flexdblink.db;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * SQL grammar and capability operations for each database dialect.
 */
public interface DbDialectSqlOperations {

    /**
     * Returns SQL to fetch next sequence value.
     *
     * @param sequenceName sequence name
     * @return SQL to fetch next value
     */
    String getNextSequenceSql(String sequenceName);

    /**
     * Returns SQL used to retrieve generated key.
     *
     * @return SQL template for generated key retrieval
     */
    String getGeneratedKeyRetrievalSql();

    /**
     * Returns whether JDBC getGeneratedKeys is supported.
     *
     * @return true when supported
     */
    boolean supportsGetGeneratedKeys();

    /**
     * Returns whether sequence is supported.
     *
     * @return true when supported
     */
    boolean supportsSequences();

    /**
     * Returns whether identity column is supported.
     *
     * @return true when supported
     */
    boolean supportsIdentityColumns();

    /**
     * Applies dialect pagination to base SQL.
     *
     * @param baseSql base select SQL
     * @param offset rows to skip
     * @param limit max rows
     * @return paginated SQL
     */
    String applyPagination(String baseSql, int offset, int limit);

    /**
     * Quotes identifier in dialect style.
     *
     * @param identifier identifier
     * @return quoted identifier
     */
    String quoteIdentifier(String identifier);

    /**
     * Returns literal for boolean true.
     *
     * @return boolean true literal
     */
    String getBooleanTrueLiteral();

    /**
     * Returns literal for boolean false.
     *
     * @return boolean false literal
     */
    String getBooleanFalseLiteral();

    /**
     * Returns expression for current timestamp.
     *
     * @return current timestamp expression
     */
    String getCurrentTimestampFunction();

    /**
     * Formats SQL datetime literal.
     *
     * @param dateTime datetime value
     * @return SQL datetime literal
     */
    String formatDateLiteral(LocalDateTime dateTime);

    /**
     * Builds dialect-specific upsert SQL.
     *
     * @param tableName table name
     * @param keyColumns key columns
     * @param insertColumns insert columns
     * @param updateColumns update columns
     * @return upsert SQL
     */
    String buildUpsertSql(String tableName, List<String> keyColumns, List<String> insertColumns,
            List<String> updateColumns);

    /**
     * Returns SQL to create temporary table.
     *
     * @param tempTableName temporary table name
     * @param columns columns and SQL types
     * @return create temporary table SQL
     */
    String getCreateTempTableSql(String tempTableName, Map<String, String> columns);

    /**
     * Applies FOR UPDATE clause in dialect style.
     *
     * @param baseSql base SQL
     * @return SQL with FOR UPDATE semantic
     */
    String applyForUpdate(String baseSql);

    /**
     * Returns whether batch updates are supported.
     *
     * @return true when supported
     */
    boolean supportsBatchUpdates();
}
