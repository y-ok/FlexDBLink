package io.github.yok.flexdblink.integration;

import io.github.yok.flexdblink.junit.LoadData;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * FlexAssert integration tests against PostgreSQL.
 */
@Testcontainers
@SpringBootTest(classes = FlexAssertTestConfig.class)
@ContextConfiguration(initializers = YamlPropertySourceFactory.class)
@LoadData(scenario = "flexassert", dbNames = "db1")
class FlexAssertPostgresqlIT extends FlexAssertIntegrationSupport {

    @Container
    private static final PostgreSQLContainer POSTGRES = createPostgres();

    private static PostgreSQLContainer createPostgres() {
        PostgreSQLContainer container = new PostgreSQLContainer("postgres:16-alpine");
        container.withDatabaseName("testdb").withUsername("test").withPassword("test");
        container.start();
        System.setProperty("FLEXASSERT_DB_URL", container.getJdbcUrl());
        System.setProperty("FLEXASSERT_DB_USER", container.getUsername());
        System.setProperty("FLEXASSERT_DB_PASSWORD", container.getPassword());
        System.setProperty("FLEXASSERT_DB_DRIVER", "org.postgresql.Driver");
        return container;
    }

    @DynamicPropertySource
    static void containerProps(DynamicPropertyRegistry registry) {
        registry.add("connections[0].id", () -> "db1");
        registry.add("connections[0].driver-class", () -> "org.postgresql.Driver");
        registry.add("connections[0].url", POSTGRES::getJdbcUrl);
        registry.add("connections[0].user", POSTGRES::getUsername);
        registry.add("connections[0].password", POSTGRES::getPassword);
    }

    @Override
    JdbcDatabaseContainer<?> container() {
        return POSTGRES;
    }

    @Override
    String migrationLocation() {
        return "classpath:db/migration_flexassert/postgresql";
    }
}
