package io.github.yok.flexdblink.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

/**
 * Utility class that logs a fatal error and echoes a concise message to {@code System.err}.
 *
 * <p>
 * Intended for CLI tools and batch jobs where a fail-fast handling is desired.
 * </p>
 *
 * <p>
 * <strong>Behavior:</strong>
 * </p>
 * <ul>
 * <li>Logs the error using SLF4J.</li>
 * <li>Writes a concise message to {@code System.err}.</li>
 * <li>Does not terminate the JVM by itself (callers decide how to end the process).</li>
 * <li>In tests, callers can switch behavior to throwing an exception via thread-local flags.</li>
 * </ul>
 *
 * @author Yasuharu.Okawauchi
 */
@Slf4j
public class ErrorHandler {

    private static final ThreadLocal<Boolean> EXIT_DISABLED =
            ThreadLocal.withInitial(() -> Boolean.FALSE);

    /**
     * Switch to "throw exception instead of ending the process" for the current thread (useful for
     * tests).
     */
    public static void disableExitForCurrentThread() {
        EXIT_DISABLED.set(Boolean.TRUE);
    }

    /**
     * Restore normal behavior for the current thread.
     */
    public static void restoreExitForCurrentThread() {
        EXIT_DISABLED.remove();
    }

    /**
     * Logs the given message and root cause at error level and prints a concise message to
     * {@code System.err}.
     *
     * <p>
     * If "exit is disabled" for the current thread, this method throws an exception instead (useful
     * for tests).
     * </p>
     *
     * @param message message to log
     * @param cause root cause
     */
    public static void errorAndExit(String message, Throwable cause) {
        log.error("{}\n{}", message, ExceptionUtils.getStackTrace(cause));
        if (Boolean.TRUE.equals(EXIT_DISABLED.get())) {
            throw new IllegalStateException(message, cause);
        }
        System.err.println("ERROR: " + message + "\n" + cause.getMessage());
    }

    /**
     * Logs the given message at error level and prints a concise message to {@code System.err}.
     *
     * <p>
     * If "exit is disabled" for the current thread, this method throws an exception instead (useful
     * for tests).
     * </p>
     *
     * @param message message to log
     */
    public static void errorAndExit(String message) {
        log.error(message);
        if (Boolean.TRUE.equals(EXIT_DISABLED.get())) {
            throw new IllegalStateException(message);
        }
    }
}
