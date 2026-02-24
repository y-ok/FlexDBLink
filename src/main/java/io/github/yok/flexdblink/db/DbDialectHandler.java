package io.github.yok.flexdblink.db;

/**
 * Aggregate interface for database-dialect behavior.
 *
 * <p>
 * This interface intentionally composes focused contracts to keep responsibilities separated:
 * connection/session control, metadata access, value conversion, and SQL grammar capabilities.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
public interface DbDialectHandler extends DbDialectConnectionOperations,
        DbDialectMetadataOperations, DbDialectValueOperations {
}
