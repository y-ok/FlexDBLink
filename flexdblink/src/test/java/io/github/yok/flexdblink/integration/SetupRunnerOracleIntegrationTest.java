package io.github.yok.flexdblink.integration;

import static org.junit.jupiter.api.Assertions.assertTrue;
import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.core.SetupRunner;
import io.github.yok.flexdblink.db.DbDialectHandlerFactory;
import io.github.yok.flexdblink.util.ErrorHandler;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.oracle.OracleContainer;

/**
 * Integration tests for {@link SetupRunner} against an Oracle container.
 *
 * <p>
 * Verifies that SetupRunner correctly detects Oracle LOB columns (BLOB, CLOB, NCLOB) and generates
 * the expected {@code file-patterns} entries in {@code application.yml}.
 * </p>
 */
@Testcontainers
class SetupRunnerOracleIntegrationTest {

    @Container
    static final OracleContainer ORACLE = new OracleContainer("gvenzl/oracle-free:slim-faststart");

    @TempDir
    Path tempDir;

    @BeforeAll
    static void setup_正常ケース_Oracleコンテナに対してFlywayを実行する_マイグレーションが完了すること() {
        OracleIntegrationSupport.prepareDatabase(ORACLE);
    }

    @Test
    void execute_正常ケース_OracleのLOB列を検出してfilePatternが書き込まれること() throws Exception {
        ErrorHandler.disableExitForCurrentThread();
        Path configFile = prepareConfigFile();
        SetupRunner runner = buildRunner();

        try {
            runner.execute(List.of("db1"));
        } finally {
            System.clearProperty("spring.config.additional-location");
            ErrorHandler.restoreExitForCurrentThread();
        }

        String yaml = Files.readString(configFile);

        assertTrue(yaml.contains("file-patterns"), "file-patterns が生成されていません");

        // IT_TYPED_MAIN の LOB 列が検出されること
        assertTrue(yaml.contains("IT_TYPED_MAIN") || yaml.contains("it_typed_main"),
                "IT_TYPED_MAIN が含まれていません");
        assertTrue(yaml.toLowerCase().contains("clob_col"), "CLOB_COL が含まれていません");
        assertTrue(yaml.toLowerCase().contains("nclob_col"), "NCLOB_COL が含まれていません");
        assertTrue(yaml.toLowerCase().contains("blob_col"), "BLOB_COL が含まれていません");

        // PK プレースホルダ {ID} が使われていること
        assertTrue(yaml.toLowerCase().contains("{id}"), "PK プレースホルダ {ID} が含まれていません");

        // 拡張子が正しいこと (Oracle: CLOB→.clob, NCLOB→.nclob, BLOB→.dat)
        assertTrue(yaml.toLowerCase().contains(".clob"), "CLOB 列の拡張子 .clob が含まれていません");
        assertTrue(yaml.toLowerCase().contains(".nclob"), "NCLOB 列の拡張子 .nclob が含まれていません");
        assertTrue(yaml.toLowerCase().contains(".dat"), "BLOB 列の拡張子 .dat が含まれていません");

        // IT_TYPED_AUX の LOB 列が検出されること
        assertTrue(yaml.contains("IT_TYPED_AUX") || yaml.contains("it_typed_aux"),
                "IT_TYPED_AUX が含まれていません");
        assertTrue(yaml.toLowerCase().contains("payload_clob"), "PAYLOAD_CLOB が含まれていません");
        assertTrue(yaml.toLowerCase().contains("payload_blob"), "PAYLOAD_BLOB が含まれていません");
    }

    @Test
    void execute_正常ケース_target指定外のDBはスキップされfilePatternが書き込まれないこと() throws Exception {
        ErrorHandler.disableExitForCurrentThread();
        Path configFile = prepareConfigFile();
        SetupRunner runner = buildRunner();

        try {
            runner.execute(List.of("other_db"));
        } finally {
            System.clearProperty("spring.config.additional-location");
            ErrorHandler.restoreExitForCurrentThread();
        }

        String yaml = Files.readString(configFile);
        assertTrue(!yaml.contains("file-patterns"), "対象外 DB なのに file-patterns が書き込まれています");
    }

    private Path prepareConfigFile() throws Exception {
        Path configFile = tempDir.resolve("application.yml");
        Files.writeString(configFile, "data-path: /tmp\n");
        System.setProperty("spring.config.additional-location",
                "file:" + tempDir.toAbsolutePath() + "/");
        return configFile;
    }

    private SetupRunner buildRunner() {
        ConnectionConfig connectionConfig = OracleIntegrationSupport.connectionConfig(ORACLE);
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toAbsolutePath().toString());
        DumpConfig dumpConfig = OracleIntegrationSupport.dumpConfig();
        dumpConfig.setExcludeTables(List.of("flyway_schema_history"));
        DbDialectHandlerFactory factory =
                OracleIntegrationSupport.dialectFactory(OracleIntegrationSupport.dbUnitConfig(),
                        dumpConfig, pathsConfig, OracleIntegrationSupport.dateTimeUtil());
        return new SetupRunner(connectionConfig, factory::create);
    }
}
