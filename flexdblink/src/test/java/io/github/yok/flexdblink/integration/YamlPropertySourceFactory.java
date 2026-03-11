package io.github.yok.flexdblink.integration;

import java.util.Properties;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.io.ClassPathResource;

/**
 * Loads the common integration test YAML as Spring properties via
 * {@code @ContextConfiguration(initializers = ...)}.
 *
 * <p>
 * Spring Boot 2.7 does not support the {@code factory} attribute on {@code @TestPropertySource}.
 * This initializer loads {@code integration/common/flexdblink-it.yml} and injects the properties
 * into the application context environment.
 * </p>
 *
 * <p>
 * DB-specific values ({@code driver-class}, {@code url}, {@code user}, {@code password}) are
 * injected via {@code @DynamicPropertySource} in each test class.
 * </p>
 */
class YamlPropertySourceFactory
        implements ApplicationContextInitializer<ConfigurableApplicationContext> {

    private static final String YAML_LOCATION = "integration/common/flexdblink-it.yml";

    @Override
    public void initialize(ConfigurableApplicationContext ctx) {
        YamlPropertiesFactoryBean yamlFactory = new YamlPropertiesFactoryBean();
        yamlFactory.setResources(new ClassPathResource(YAML_LOCATION));
        Properties props = yamlFactory.getObject();
        ctx.getEnvironment().getPropertySources()
                .addFirst(new PropertiesPropertySource(YAML_LOCATION, props));
    }
}
