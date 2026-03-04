package io.github.yok.flexdblink.config;

import com.google.common.collect.ImmutableList;
import java.util.List;

/**
 * Shared default values used across FlexDBLink modules.
 *
 * <p>
 * These constants represent cross-cutting defaults that are referenced by the core runtime,
 * the Maven plugin, and the JUnit extension.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
public final class FlexDbLinkDefaults {

    /**
     * Flyway schema history table name excluded from load/dump processing by default.
     */
    public static final String FLYWAY_SCHEMA_HISTORY_TABLE = "flyway_schema_history";

    /**
     * Default table exclusions shared by CLI, Maven plugin, and JUnit extension.
     */
    public static final List<String> DEFAULT_EXCLUDE_TABLES =
            ImmutableList.of(FLYWAY_SCHEMA_HISTORY_TABLE);

    /**
     * Prevent instantiation of this constants holder.
     */
    private FlexDbLinkDefaults() {
        // Utility class
    }
}
