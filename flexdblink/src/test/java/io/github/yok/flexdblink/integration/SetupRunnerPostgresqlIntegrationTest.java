package io.github.yok.flexdblink.integration;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.core.SetupRunner;
import io.github.yok.flexdblink.db.DbDialectHandlerFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Integration tests for {@link SetupRunner} against a PostgreSQL container.
 *
 * <p>
 * Verifies that SetupRunner correctly detects PostgreSQL LOB columns (BYTEA) and generates the
 * expected {@code file-patterns} entries in {@code application.yml}.
 * </p>
 *
 * <p>
 * Note: PostgreSQL TEXT columns (CLOB_COL, NCLOB_COL) are NOT detected as LOBs because the JDBC
 * type is {@code Types.VARCHAR} and "text" is not in the type-name extension map. Only BYTEA
 * columns are detected, mapped to extension {@code .bin}.
 * </p>
 */
@Testcontainers
class SetupRunnerPostgresqlIntegrationTest {

    @Container
    private static final PostgreSQLContainer postgres = createPostgres();

    @TempDir
    Path tempDir;

    private static PostgreSQLContainer createPostgres() {
        PostgreSQLContainer container = new PostgreSQLContainer("postgres:16-alpine");
        container.withDatabaseName("testdb").withUsername("test").withPassword("test");
        return container;
    }

    @BeforeEach
    void setup_正常ケース_PostgreSQLコンテナに対してFlywayを実行する_マイグレーションが完了すること() {
        PostgresqlIntegrationSupport.prepareDatabase(postgres);
    }

    @Test
    void execute_正常ケース_BYTEA列を検出してbinパターンが書き込まれること() throws Exception {
        Path configFile = prepareConfigFile();
        SetupRunner runner = buildRunner();

        try {
            runner.execute(List.of("db1"));
        } finally {
            System.clearProperty("spring.config.additional-location");
        }

        String yaml = Files.readString(configFile);

        assertTrue(yaml.contains("file-patterns"), "file-patterns が生成されていません");

        // IT_TYPED_MAIN の BYTEA 列が検出されること（PostgreSQL は小文字で返す）
        assertTrue(yaml.toLowerCase().contains("it_typed_main"), "it_typed_main が含まれていません");
        assertTrue(yaml.toLowerCase().contains("blob_col"), "blob_col が含まれていません");

        // 拡張子が .bin であること
        assertTrue(yaml.toLowerCase().contains(".bin"), "BYTEA 列の拡張子 .bin が含まれていません");

        // PK プレースホルダ {id} が使われていること（PostgreSQL は小文字）
        assertTrue(yaml.toLowerCase().contains("{id}"), "PK プレースホルダ {id} が含まれていません");

        // IT_TYPED_AUX の BYTEA 列が検出されること
        assertTrue(yaml.toLowerCase().contains("it_typed_aux"), "it_typed_aux が含まれていません");
        assertTrue(yaml.toLowerCase().contains("payload_blob"), "payload_blob が含まれていません");
    }

    @Test
    void execute_正常ケース_TEXT列は検出されないこと() throws Exception {
        Path configFile = prepareConfigFile();
        SetupRunner runner = buildRunner();

        try {
            runner.execute(List.of("db1"));
        } finally {
            System.clearProperty("spring.config.additional-location");
        }

        String yaml = Files.readString(configFile);

        // TEXT 型の列は LOB として検出されない
        assertFalse(yaml.toLowerCase().contains("clob_col"),
                "TEXT 型の clob_col が誤って file-patterns に含まれています");
        assertFalse(yaml.toLowerCase().contains("nclob_col"),
                "TEXT 型の nclob_col が誤って file-patterns に含まれています");
        assertFalse(yaml.toLowerCase().contains("payload_clob"),
                "TEXT 型の payload_clob が誤って file-patterns に含まれています");
    }

    @Test
    void execute_正常ケース_target指定外のDBはスキップされfilePatternが書き込まれないこと() throws Exception {
        Path configFile = prepareConfigFile();
        SetupRunner runner = buildRunner();

        try {
            runner.execute(List.of("other_db"));
        } finally {
            System.clearProperty("spring.config.additional-location");
        }

        String yaml = Files.readString(configFile);
        assertFalse(yaml.contains("file-patterns"), "対象外 DB なのに file-patterns が書き込まれています");
    }

    private Path prepareConfigFile() throws Exception {
        Path configFile = tempDir.resolve("application.yml");
        Files.writeString(configFile, "data-path: /tmp\n");
        System.setProperty("spring.config.additional-location",
                "file:" + tempDir.toAbsolutePath() + "/");
        return configFile;
    }

    private SetupRunner buildRunner() {
        ConnectionConfig connectionConfig = PostgresqlIntegrationSupport.connectionConfig(postgres);
        PathsConfig pathsConfig = PostgresqlIntegrationSupport.pathsConfig(tempDir);
        DbDialectHandlerFactory factory = PostgresqlIntegrationSupport.dialectFactory(
                PostgresqlIntegrationSupport.dbUnitConfig(),
                PostgresqlIntegrationSupport.dumpConfig(), pathsConfig,
                PostgresqlIntegrationSupport.dateTimeUtil());
        return new SetupRunner(connectionConfig, factory::create);
    }
}
