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
import javax.sql.DataSource;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.util.StringUtils;

/**
 * Spring configuration for FlexAssert integration tests.
 *
 * <p>
 * Registers test DataSource beans from dynamic properties so that {@code @LoadData} can resolve
 * them via {@code flexdblink.properties}.
 * </p>
 */
@Configuration
@EnableConfigurationProperties({ConnectionConfig.class, DbUnitConfig.class, DumpConfig.class,
        FilePatternConfig.class, PathsConfig.class, CsvDateTimeFormatProperties.class,
        DbUnitConfigProperties.class})
@Import({DateTimeFormatUtil.class, DbUnitConfigFactory.class, DbDialectHandlerFactory.class})
class FlexAssertTestConfig {

    /**
     * DataSource bean named "ds1" for {@code @LoadData(dbNames = {"db1"})} resolution.
     *
     * @param env Spring environment (contains dynamic properties from Testcontainers)
     * @return DataSource
     */
    @Bean("ds1")
    DataSource ds1(Environment env) {
        return createDataSource(env, 0);
    }

    /**
     * DataSource bean named "ds2" for {@code @LoadData(dbNames = {"db2"})} resolution.
     *
     * @param env Spring environment (contains dynamic properties from Testcontainers)
     * @return DataSource
     */
    @Bean("ds2")
    DataSource ds2(Environment env) {
        return createDataSource(env, 1);
    }

    /**
     * DataSource bean named "ds3" for {@code @LoadData(dbNames = {"db3"})} resolution.
     *
     * @param env Spring environment (contains dynamic properties from Testcontainers)
     * @return DataSource
     */
    @Bean("ds3")
    DataSource ds3(Environment env) {
        return createDataSource(env, 2);
    }

    /**
     * DataSource bean named "ds4" for {@code @LoadData(dbNames = {"db4"})} resolution.
     *
     * @param env Spring environment (contains dynamic properties from Testcontainers)
     * @return DataSource
     */
    @Bean("ds4")
    DataSource ds4(Environment env) {
        return createDataSource(env, 3);
    }

    /**
     * Creates a DriverManager-based {@link DataSource} from indexed connection properties.
     *
     * @param env Spring environment
     * @param index connection index in {@code connections[]}
     * @return DataSource
     */
    private DataSource createDataSource(Environment env, int index) {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setUrl(env.getProperty("connections[" + index + "].url"));
        ds.setUsername(env.getProperty("connections[" + index + "].user"));
        ds.setPassword(env.getProperty("connections[" + index + "].password"));
        String driverClass = env.getProperty("connections[" + index + "].driver-class");
        if (StringUtils.hasText(driverClass)) {
            ds.setDriverClassName(driverClass);
        }
        return ds;
    }
}
