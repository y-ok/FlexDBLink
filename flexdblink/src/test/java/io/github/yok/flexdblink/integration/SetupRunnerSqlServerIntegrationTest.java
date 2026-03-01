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
import org.testcontainers.mssqlserver.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Integration tests for {@link SetupRunner} against a SQL Server container.
 *
 * <p>
 * Verifies that SetupRunner correctly detects SQL Server LOB columns and generates the expected
 * {@code file-patterns} entries in {@code application.yml}.
 * </p>
 *
 * <p>
 * Detected LOB columns and their extensions:
 * <ul>
 * <li>VARBINARY / VARBINARY(MAX) → {@code .bin} (via type-name map key "varbinary")</li>
 * </ul>
 * VARCHAR(MAX) / NVARCHAR(MAX) columns are NOT detected because "varchar" and "nvarchar" are not in
 * the type-name extension map, and the JDBC type does not match {@code Types.BLOB/CLOB/NCLOB}.
 * </p>
 */
@Testcontainers
class SetupRunnerSqlServerIntegrationTest {

    @Container
    private static final MSSQLServerContainer sqlserver = createSqlServer();

    @TempDir
    Path tempDir;

    private static MSSQLServerContainer createSqlServer() {
        MSSQLServerContainer container =
                new MSSQLServerContainer("mcr.microsoft.com/mssql/server:2019-latest");
        container.acceptLicense();
        return container;
    }

    @BeforeEach
    void setup_正常ケース_SQLServerコンテナに対してFlywayを実行する_マイグレーションが完了すること() {
        SqlServerIntegrationSupport.prepareDatabase(sqlserver);
    }

    @Test
    void execute_正常ケース_VARBINARY列を検出してbinパターンが書き込まれること() throws Exception {
        Path configFile = prepareConfigFile();
        SetupRunner runner = buildRunner();

        try {
            runner.execute(List.of("db1"));
        } finally {
            System.clearProperty("spring.config.additional-location");
        }

        String yaml = Files.readString(configFile);
        String yamlLower = yaml.toLowerCase();

        assertTrue(yaml.contains("file-patterns"), "file-patterns が生成されていません");
        assertTrue(yamlLower.contains("it_typed_main"), "IT_TYPED_MAIN が含まれていません");

        // VARBINARY(MAX) 列 BLOB_COL が検出されること
        assertTrue(yamlLower.contains("blob_col"), "BLOB_COL が含まれていません");

        // 拡張子が .bin であること
        assertTrue(yamlLower.contains(".bin"), "VARBINARY 列の拡張子 .bin が含まれていません");

        // PK プレースホルダ
        assertTrue(yamlLower.contains("{id}"), "PK プレースホルダ {ID} が含まれていません");

        // IT_TYPED_AUX の VARBINARY(MAX) 列が検出されること
        assertTrue(yamlLower.contains("it_typed_aux"), "IT_TYPED_AUX が含まれていません");
        assertTrue(yamlLower.contains("payload_blob"), "PAYLOAD_BLOB が含まれていません");
    }

    @Test
    void execute_正常ケース_VARCHAR_MAX列とNVARCHAR_MAX列は検出されないこと() throws Exception {
        Path configFile = prepareConfigFile();
        SetupRunner runner = buildRunner();

        try {
            runner.execute(List.of("db1"));
        } finally {
            System.clearProperty("spring.config.additional-location");
        }

        String yaml = Files.readString(configFile);
        String yamlLower = yaml.toLowerCase();

        // VARCHAR(MAX)、NVARCHAR(MAX) 列は LOB として検出されない
        assertFalse(yamlLower.contains("clob_col"),
                "VARCHAR(MAX) 型の CLOB_COL が誤って file-patterns に含まれています");
        assertFalse(yamlLower.contains("nclob_col"),
                "NVARCHAR(MAX) 型の NCLOB_COL が誤って file-patterns に含まれています");
        assertFalse(yamlLower.contains("payload_clob"),
                "VARCHAR(MAX) 型の PAYLOAD_CLOB が誤って file-patterns に含まれています");
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
        ConnectionConfig connectionConfig = SqlServerIntegrationSupport.connectionConfig(sqlserver);
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toAbsolutePath().toString());
        DbDialectHandlerFactory factory = SqlServerIntegrationSupport.dialectFactory(
                SqlServerIntegrationSupport.dbUnitConfig(),
                SqlServerIntegrationSupport.dumpConfig(), pathsConfig,
                SqlServerIntegrationSupport.dateTimeUtil());
        return new SetupRunner(connectionConfig, factory::create);
    }
}
