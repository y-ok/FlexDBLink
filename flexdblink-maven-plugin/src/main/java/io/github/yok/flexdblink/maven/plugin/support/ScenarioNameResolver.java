package io.github.yok.flexdblink.maven.plugin.support;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Resolves effective scenario names for load and dump goals.
 *
 * <p>
 * Load and dump use different fallback rules: load treats blank input as "pre only", while dump
 * generates a timestamped scenario directory name. This class centralizes those rules so the Mojos
 * remain thin.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
public class ScenarioNameResolver {

    /**
     * Timestamp format used when dump generates a default scenario name.
     */
    private static final DateTimeFormatter DUMP_TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    /**
     * Resolves the scenario for the load goal.
     *
     * @param rawScenario raw scenario value from plugin parameters
     * @return trimmed scenario, or {@code null} when blank so core executes pre only
     */
    public String resolveForLoad(String rawScenario) {
        if (rawScenario == null) {
            return null;
        }
        String trimmed = rawScenario.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        return trimmed;
    }

    /**
     * Resolves the scenario for the dump goal.
     *
     * @param rawScenario raw scenario value from plugin parameters
     * @param clock clock used for timestamp generation
     * @return trimmed scenario, or a generated timestamp when blank
     */
    public String resolveForDump(String rawScenario, Clock clock) {
        if (rawScenario == null) {
            return generateTimestamp(clock);
        }
        String trimmed = rawScenario.trim();
        if (trimmed.isEmpty()) {
            return generateTimestamp(clock);
        }
        return trimmed;
    }

    /**
     * Generates a timestamp-based scenario folder name.
     *
     * @param clock clock used to obtain the current time
     * @return timestamp string formatted as {@code yyyyMMddHHmmss}
     */
    public String generateTimestamp(Clock clock) {
        return LocalDateTime.now(clock).format(DUMP_TIMESTAMP_FORMATTER);
    }
}
