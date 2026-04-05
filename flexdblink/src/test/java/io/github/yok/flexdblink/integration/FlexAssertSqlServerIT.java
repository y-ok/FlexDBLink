package io.github.yok.flexdblink.integration;

import io.github.yok.flexdblink.junit.LoadData;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mssqlserver.MSSQLServerContainer;

/**
 * FlexAssert integration tests against SQL Server.
 */
@Testcontainers
@SpringBootTest(classes = FlexAssertTestConfig.class)
@ContextConfiguration(initializers = YamlPropertySourceFactory.class)
@LoadData(scenario = "flexassert", dbNames = "db1")
class FlexAssertSqlServerIT extends FlexAssertIntegrationSupport {

    @Container
    private static final MSSQLServerContainer SQLSERVER = createSqlServer();

    private static MSSQLServerContainer createSqlServer() {
        MSSQLServerContainer container =
                new MSSQLServerContainer("mcr.microsoft.com/mssql/server:2019-latest");
        container.acceptLicense();
        container.start();
        System.setProperty("FLEXASSERT_DB_URL", container.getJdbcUrl());
        System.setProperty("FLEXASSERT_DB_USER", container.getUsername());
        System.setProperty("FLEXASSERT_DB_PASSWORD", container.getPassword());
        System.setProperty("FLEXASSERT_DB_DRIVER", "com.microsoft.sqlserver.jdbc.SQLServerDriver");
        return container;
    }

    @DynamicPropertySource
    static void containerProps(DynamicPropertyRegistry registry) {
        registry.add("connections[0].id", () -> "db1");
        registry.add("connections[0].driver-class",
                () -> "com.microsoft.sqlserver.jdbc.SQLServerDriver");
        registry.add("connections[0].url", SQLSERVER::getJdbcUrl);
        registry.add("connections[0].user", SQLSERVER::getUsername);
        registry.add("connections[0].password", SQLSERVER::getPassword);
    }

    @Override
    JdbcDatabaseContainer<?> container() {
        return SQLSERVER;
    }

    @Override
    String migrationLocation() {
        return "classpath:db/migration_flexassert/sqlserver";
    }
}
