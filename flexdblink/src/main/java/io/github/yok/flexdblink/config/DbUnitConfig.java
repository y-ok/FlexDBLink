package io.github.yok.flexdblink.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration class that binds the {@code dbunit} section in {@code application.yml}.
 *
 * <p>
 * Centralizes DBUnit-related runtime settings used by {@code DataLoader} and {@code DataDumper},
 * such as the default load scenario directory and confirmation behavior.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
@Component
@ConfigurationProperties(prefix = "dbunit")
@Data
public class DbUnitConfig {

    /**
     * Directory name used for initial data loading.
     */
    private String preDirName = "pre";

    /**
     * When {@code true}, the user is prompted for confirmation before {@code --load} executes.
     * Defaults to {@code false} (no confirmation required).
     */
    private boolean confirmBeforeLoad = false;
}
