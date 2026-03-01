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
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Integration tests for {@link SetupRunner} against a MySQL container.
 *
 * <p>
 * Verifies that SetupRunner correctly detects MySQL LOB columns and generates the expected
 * {@code file-patterns} entries in {@code application.yml}.
 * </p>
 *
 * <p>
 * Detected LOB columns and their extensions:
 * <ul>
 * <li>BLOB → {@code .dat} (via {@code Types.BLOB})</li>
 * <li>TINYBLOB / MEDIUMBLOB / LONGBLOB → {@code .dat} (via type-name map)</li>
 * <li>TINYTEXT / MEDIUMTEXT / LONGTEXT → {@code .clob} (via type-name map)</li>
 * </ul>
 * Plain TEXT columns (CLOB_COL, NCLOB_COL) are NOT detected because the JDBC type is
 * {@code Types.LONGVARCHAR} and "text" is not in the type-name extension map.
 * </p>
 */
@Testcontainers
class SetupRunnerMySqlIntegrationTest {

    @Container
    private static final MySQLContainer mysql = createMySql();

    @TempDir
    Path tempDir;

    private static MySQLContainer createMySql() {
        MySQLContainer container = new MySQLContainer("mysql:8.4");
        container.withDatabaseName("testdb").withUsername("test").withPassword("test");
        return container;
    }

    @BeforeEach
    void setup_正常ケース_MySQLコンテナに対してFlywayを実行する_マイグレーションが完了すること() {
        MySqlIntegrationSupport.prepareDatabase(mysql);
    }

    @Test
    void execute_正常ケース_BLOB系列を検出してdatパターンが書き込まれること() throws Exception {
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

        // BLOB 系列 → .dat
        assertTrue(yamlLower.contains("blob_col"), "BLOB_COL が含まれていません");
        assertTrue(yamlLower.contains(".dat"), "BLOB 系列の拡張子 .dat が含まれていません");

        // LONGBLOB → .dat
        assertTrue(yamlLower.contains("longblob_col"), "LONGBLOB_COL が含まれていません");

        // PK プレースホルダ
        assertTrue(yamlLower.contains("{id}"), "PK プレースホルダ {ID} が含まれていません");

        // IT_TYPED_AUX の BLOB 列が検出されること
        assertTrue(yamlLower.contains("it_typed_aux"), "IT_TYPED_AUX が含まれていません");
        assertTrue(yamlLower.contains("payload_blob"), "PAYLOAD_BLOB が含まれていません");
    }

    @Test
    void execute_正常ケース_LONGTEXT系列を検出してclobパターンが書き込まれること() throws Exception {
        Path configFile = prepareConfigFile();
        SetupRunner runner = buildRunner();

        try {
            runner.execute(List.of("db1"));
        } finally {
            System.clearProperty("spring.config.additional-location");
        }

        String yaml = Files.readString(configFile);
        String yamlLower = yaml.toLowerCase();

        // LONGTEXT → .clob
        assertTrue(yamlLower.contains("longtext_col"), "LONGTEXT_COL が含まれていません");
        assertTrue(yamlLower.contains(".clob"), "LONGTEXT 系列の拡張子 .clob が含まれていません");

        // TINYTEXT / MEDIUMTEXT も検出されること
        assertTrue(yamlLower.contains("tinytext_col"), "TINYTEXT_COL が含まれていません");
        assertTrue(yamlLower.contains("mediumtext_col"), "MEDIUMTEXT_COL が含まれていません");

        // IT_TYPED_AUX の LONGTEXT 列 (PAYLOAD_XML) が検出されること
        assertTrue(yamlLower.contains("payload_xml"), "PAYLOAD_XML (LONGTEXT) が含まれていません");
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
        String yamlLower = yaml.toLowerCase();

        // 通常の TEXT 型（CLOB_COL, NCLOB_COL）は LOB として検出されない
        assertFalse(yamlLower.contains("clob_col"), "TEXT 型の CLOB_COL が誤って file-patterns に含まれています");
        assertFalse(yamlLower.contains("nclob_col"),
                "TEXT 型の NCLOB_COL が誤って file-patterns に含まれています");
        // IT_TYPED_AUX の TEXT 型 (PAYLOAD_CLOB) も検出されない
        assertFalse(yamlLower.contains("payload_clob"),
                "TEXT 型の PAYLOAD_CLOB が誤って file-patterns に含まれています");
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
        ConnectionConfig connectionConfig = MySqlIntegrationSupport.connectionConfig(mysql);
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toAbsolutePath().toString());
        DbDialectHandlerFactory factory = MySqlIntegrationSupport.dialectFactory(
                MySqlIntegrationSupport.dbUnitConfig(), MySqlIntegrationSupport.dumpConfig(),
                pathsConfig, MySqlIntegrationSupport.dateTimeUtil());
        return new SetupRunner(connectionConfig, factory::create);
    }
}
