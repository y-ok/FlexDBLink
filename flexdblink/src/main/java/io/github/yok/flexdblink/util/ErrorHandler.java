package io.github.yok.flexdblink.util;

import lombok.Generated;
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
 * <li>Always throws an exception so callers can roll back or report a failed process.</li>
 * <li>Tests can suppress the additional stderr echo via the existing thread-local flag.</li>
 * </ul>
 *
 * @author Yasuharu.Okawauchi
 */
@Slf4j
public class ErrorHandler {

    private static final ThreadLocal<Boolean> EXIT_DISABLED =
            ThreadLocal.withInitial(() -> Boolean.FALSE);

    /**
     * Default constructor retained only for compatibility.
     */
    @Generated
    public ErrorHandler() {}

    /**
     * Suppresses the additional stderr echo for the current thread. Exception propagation remains
     * enabled. The method name is retained for compatibility with existing callers.
     */
    public static void disableExitForCurrentThread() {
        EXIT_DISABLED.set(Boolean.TRUE);
    }

    /**
     * Restores the normal stderr echo for the current thread. Exceptions are always propagated.
     */
    public static void restoreExitForCurrentThread() {
        EXIT_DISABLED.remove();
    }

    /**
     * Logs the given message and root cause at error level and prints a concise message to
     * {@code System.err}.
     *
     * <p>
     * Always throws after logging. The stderr echo can be suppressed for tests.
     * </p>
     *
     * @param message message to log
     * @param cause root cause
     * @throws IllegalStateException always, preserving the supplied message and cause
     */
    public static void errorAndExit(String message, Throwable cause) {
        log.error("{}\n{}", message, ExceptionUtils.getStackTrace(cause));
        if (!Boolean.TRUE.equals(EXIT_DISABLED.get())) {
            System.err.println("ERROR: " + message + "\n" + cause.getMessage());
        }
        throw new IllegalStateException(message, cause);
    }

    /**
     * Logs the given message at error level and notifies the caller with an exception.
     *
     * @param message message to log
     * @throws IllegalStateException always, preserving the supplied message
     */
    public static void errorAndExit(String message) {
        log.error(message);
        throw new IllegalStateException(message);
    }
}
