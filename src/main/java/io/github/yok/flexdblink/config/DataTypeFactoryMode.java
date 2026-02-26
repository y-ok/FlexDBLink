package io.github.yok.flexdblink.config;

/**
 * Enumerates supported database dialect types used internally by DBUnit-related components.
 *
 * <p>
 * Each constant represents a database product for selecting a dialect handler and data type
 * factory.
 * </p>
 *
 * <ul>
 * <li>ORACLE — for Oracle Database</li>
 * <li>POSTGRESQL — for PostgreSQL</li>
 * <li>MYSQL — for MySQL</li>
 * <li>SQLSERVER — for Microsoft SQL Server</li>
 * </ul>
 *
 * @author Yasuharu.Okawauchi
 */
public enum DataTypeFactoryMode {
    // Use the handler and DataTypeFactory for Oracle DB
    ORACLE,
    // Use the handler and DataTypeFactory for PostgreSQL
    POSTGRESQL,
    // Use the handler and DataTypeFactory for MySQL
    MYSQL,
    // Use the handler and DataTypeFactory for Microsoft SQL Server
    SQLSERVER
}
