package io.github.yok.flexdblink.config;

import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration class that holds settings related to dump operations.
 *
 * <p>
 * You can specify the following properties in {@code application.yml} or
 * {@code application.properties}.
 * </p>
 * <ul>
 * <li>{@code dump.excludeTables}: List of table names to exclude from the dump</li>
 * </ul>
 *
 * <p>
 * If not specified, {@value FlexDbLinkDefaults#FLYWAY_SCHEMA_HISTORY_TABLE} is excluded by default.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
@Component
@ConfigurationProperties(prefix = "dump")
@Getter
@Setter
@NoArgsConstructor
public class DumpConfig {

    /**
     * List of table names to exclude during dump processing.
     */
    private List<String> excludeTables = FlexDbLinkDefaults.DEFAULT_EXCLUDE_TABLES;
}
