package io.github.yok.flexdblink.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

/**
 * Utility class that logs a fatal error, echoes a concise message to {@code System.err}, and then
 * terminates the JVM with {@code System.exit(1)}.
 *
 * <p>
 * Intended for CLI tools and batch jobs where a fail-fast, non-recoverable termination is desired.
 * </p>
 *
 * <p>
 * <strong>Behavior:</strong>
 * </p>
 * <ul>
 * <li>Writes a detailed error (including stack trace when available) to the application log.</li>
 * <li>Prints a short message to {@code System.err} for immediate console feedback.</li>
 * <li>Invokes {@code System.exit(1)} to signal abnormal termination.</li>
 * </ul>
 *
 * @author Yasuharu.Okawauchi
 */
@Slf4j
public class ErrorHandler {

    private static final ThreadLocal<Boolean> EXIT_DISABLED =
            ThreadLocal.withInitial(() -> Boolean.FALSE);

    /**
     * Disable System.exit for the current thread (useful for tests).
     */
    public static void disableExitForCurrentThread() {
        EXIT_DISABLED.set(Boolean.TRUE);
    }

    /**
     * Restore System.exit behavior for the current thread.
     */
    public static void restoreExitForCurrentThread() {
        EXIT_DISABLED.remove();
    }

    /**
     * Logs the given message and root cause at error level, prints a concise message to
     * {@code System.err}, and terminates the JVM with exit code {@code 1}.
     *
     * <p>
     * The full stack trace is included in the application log via SLF4J. The console (stderr)
     * output contains only the message and the cause's message for readability.
     * </p>
     *
     * @param message human-readable error description
     * @param t the underlying throwable that triggered the fatal error
     */
    public static void errorAndExit(String message, Throwable t) {
        log.error("{}\n{}", message, ExceptionUtils.getStackTrace(t));
        if (Boolean.TRUE.equals(EXIT_DISABLED.get())) {
            throw new IllegalStateException(message, t);
        }
        System.err.println("ERROR: " + message + "\n" + t.getMessage());
    }

    /**
     * Logs the given message at error level and terminates the JVM with exit code {@code 1}.
     *
     * <p>
     * Use this overload when no root cause {@link Throwable} is available or relevant.
     * </p>
     *
     * @param message human-readable error description
     */
    public static void errorAndExit(String message) {
        log.error(message);
        if (Boolean.TRUE.equals(EXIT_DISABLED.get())) {
            throw new IllegalStateException(message);
        }
    }
}
