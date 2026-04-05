package io.github.yok.flexdblink.integration;

import io.github.yok.flexdblink.junit.LoadData;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;

/**
 * FlexAssert integration tests against MySQL.
 */
@Testcontainers
@SpringBootTest(classes = FlexAssertTestConfig.class)
@ContextConfiguration(initializers = YamlPropertySourceFactory.class)
@LoadData(scenario = "flexassert", dbNames = "db1")
class FlexAssertMySqlIT extends FlexAssertIntegrationSupport {

    @Container
    private static final MySQLContainer MYSQL = createMySql();

    private static MySQLContainer createMySql() {
        MySQLContainer container = new MySQLContainer("mysql:8.4");
        container.withDatabaseName("testdb").withUsername("test").withPassword("test");
        container.start();
        System.setProperty("FLEXASSERT_DB_URL", container.getJdbcUrl());
        System.setProperty("FLEXASSERT_DB_USER", container.getUsername());
        System.setProperty("FLEXASSERT_DB_PASSWORD", container.getPassword());
        System.setProperty("FLEXASSERT_DB_DRIVER", "com.mysql.cj.jdbc.Driver");
        return container;
    }

    @DynamicPropertySource
    static void containerProps(DynamicPropertyRegistry registry) {
        registry.add("connections[0].id", () -> "db1");
        registry.add("connections[0].driver-class", () -> "com.mysql.cj.jdbc.Driver");
        registry.add("connections[0].url", MYSQL::getJdbcUrl);
        registry.add("connections[0].user", MYSQL::getUsername);
        registry.add("connections[0].password", MYSQL::getPassword);
    }

    @Override
    JdbcDatabaseContainer<?> container() {
        return MYSQL;
    }

    @Override
    String migrationLocation() {
        return "classpath:db/migration_flexassert/mysql";
    }
}
