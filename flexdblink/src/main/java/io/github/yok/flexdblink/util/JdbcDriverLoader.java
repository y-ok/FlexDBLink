package io.github.yok.flexdblink.util;

import lombok.Generated;

/**
 * Utility for optional JDBC driver class loading.
 *
 * <p>
 * When a driver class name is configured, this utility loads it explicitly via
 * {@link Class#forName(String)}. When the value is {@code null} or blank, it does nothing so JDBC 4
 * auto-loading can be used.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
public final class JdbcDriverLoader {

    /**
     * Prevents instantiation.
     */
    @Generated
    private JdbcDriverLoader() {}

    /**
     * Loads the JDBC driver class only when the class name is configured.
     *
     * @param driverClass fully qualified JDBC driver class name, or {@code null}/blank
     * @throws ClassNotFoundException when the specified class cannot be found
     */
    public static void loadIfConfigured(String driverClass) throws ClassNotFoundException {
        if (driverClass == null) {
            return;
        }
        if (driverClass.isBlank()) {
            return;
        }
        Class.forName(driverClass);
    }
}
