package io.github.yok.flexdblink.integration;

import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.CsvDateTimeFormatProperties;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DbUnitConfigProperties;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.FilePatternConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.db.DbDialectHandlerFactory;
import io.github.yok.flexdblink.db.DbUnitConfigFactory;
import io.github.yok.flexdblink.util.DateTimeFormatUtil;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Spring configuration for integration tests.
 *
 * <p>
 * Enables {@code @ConfigurationProperties} binding for all FlexDBLink config classes and imports
 * the component beans needed by {@link IntegrationTestSupport.Runtime}. Unlike the production
 * {@link io.github.yok.flexdblink.Main}, this configuration does not include
 * {@code CommandLineRunner} behavior.
 * </p>
 */
@Configuration
@EnableConfigurationProperties({ConnectionConfig.class, DbUnitConfig.class, DumpConfig.class,
        FilePatternConfig.class, PathsConfig.class, CsvDateTimeFormatProperties.class,
        DbUnitConfigProperties.class})
@Import({DateTimeFormatUtil.class, DbUnitConfigFactory.class, DbDialectHandlerFactory.class})
class IntegrationTestConfig {
}
