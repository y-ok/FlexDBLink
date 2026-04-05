package io.github.yok.flexdblink.integration;

import io.github.yok.flexdblink.junit.LoadData;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.oracle.OracleContainer;

/**
 * FlexAssert integration tests against Oracle.
 */
@Testcontainers
@SpringBootTest(classes = FlexAssertTestConfig.class)
@ContextConfiguration(initializers = YamlPropertySourceFactory.class)
@LoadData(scenario = "flexassert", dbNames = "db1")
class FlexAssertOracleIT extends FlexAssertIntegrationSupport {

    @Container
    private static final OracleContainer ORACLE = createOracle();

    private static OracleContainer createOracle() {
        OracleContainer container = new OracleContainer("gvenzl/oracle-free:slim-faststart");
        container.start();
        System.setProperty("FLEXASSERT_DB_URL", container.getJdbcUrl());
        System.setProperty("FLEXASSERT_DB_USER", container.getUsername());
        System.setProperty("FLEXASSERT_DB_PASSWORD", container.getPassword());
        System.setProperty("FLEXASSERT_DB_DRIVER", "oracle.jdbc.OracleDriver");
        return container;
    }

    @DynamicPropertySource
    static void containerProps(DynamicPropertyRegistry registry) {
        registry.add("connections[0].id", () -> "db1");
        registry.add("connections[0].driver-class", () -> "oracle.jdbc.OracleDriver");
        registry.add("connections[0].url", ORACLE::getJdbcUrl);
        registry.add("connections[0].user", ORACLE::getUsername);
        registry.add("connections[0].password", ORACLE::getPassword);
    }

    @Override
    JdbcDatabaseContainer<?> container() {
        return ORACLE;
    }

    @Override
    String migrationLocation() {
        return "classpath:db/migration_flexassert/oracle";
    }
}
