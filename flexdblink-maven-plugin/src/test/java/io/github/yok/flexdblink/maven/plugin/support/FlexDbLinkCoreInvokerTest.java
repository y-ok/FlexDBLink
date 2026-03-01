package io.github.yok.flexdblink.maven.plugin.support;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.maven.plugin.config.CoreConfigBundle;
import io.github.yok.flexdblink.maven.plugin.config.PluginConfig;
import io.github.yok.flexdblink.util.ErrorHandler;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FlexDbLinkCoreInvokerTest {

    private final CoreConfigAssembler assembler = new CoreConfigAssembler();
    private final FlexDbLinkCoreInvoker target = new FlexDbLinkCoreInvoker();

    @Test
    void load_正常ケース_空接続構成で実行する_例外が送出されないこと(@TempDir Path tempDir) {
        CoreConfigBundle bundle = buildBundle(tempDir, List.of());

        assertDoesNotThrow(() -> target.load(bundle, null, List.of()));
    }

    @Test
    void dump_正常ケース_空接続構成で実行する_例外が送出されないこと(@TempDir Path tempDir) {
        CoreConfigBundle bundle = buildBundle(tempDir, List.of());

        assertDoesNotThrow(() -> target.dump(bundle, "scenarioA", List.of()));
    }

    @Test
    void load_異常ケース_nullBundleで実行する_ErrorHandlerが復元されること() {
        assertThrows(NullPointerException.class, () -> target.load(null, null, List.of()));

        assertDoesNotThrow(() -> ErrorHandler.errorAndExit("restore-check"));
    }

    @Test
    void connections_正常ケース_接続一覧取得を実行する_同一件数の一覧が返ること(@TempDir Path tempDir) {
        ConnectionConfig.Entry entry1 = new ConnectionConfig.Entry();
        entry1.setId("DB1");
        entry1.setUrl("jdbc:h2:mem:db1");
        entry1.setUser("sa");
        ConnectionConfig.Entry entry2 = new ConnectionConfig.Entry();
        entry2.setId("DB2");
        entry2.setUrl("jdbc:h2:mem:db2");
        entry2.setUser("sa");
        CoreConfigBundle bundle = buildBundle(tempDir, List.of(entry1, entry2));

        List<ConnectionConfig.Entry> actual = target.connections(bundle);

        assertEquals(2, actual.size());
    }

    private CoreConfigBundle buildBundle(Path tempDir, List<ConnectionConfig.Entry> entries) {
        PluginConfig pluginConfig = new PluginConfig();
        pluginConfig.setDataPath(tempDir.toString());
        pluginConfig.setDbunit(defaultDbunit());
        pluginConfig.setFilePatterns(List.of());

        ConnectionConfig connectionConfig = new ConnectionConfig();
        connectionConfig.setConnections(entries);
        return assembler.assemble(pluginConfig, connectionConfig);
    }

    private PluginConfig.DbUnit defaultDbunit() {
        PluginConfig.DbUnit dbunit = new PluginConfig.DbUnit();
        dbunit.setPreDirName("pre");

        PluginConfig.Csv csv = new PluginConfig.Csv();
        PluginConfig.Format format = new PluginConfig.Format();
        format.setDate("yyyy-MM-dd");
        format.setTime("HH:mm:ss");
        format.setDateTime("yyyy-MM-dd HH:mm:ss");
        format.setDateTimeWithMillis("yyyy-MM-dd HH:mm:ss.SSS");
        csv.setFormat(format);
        dbunit.setCsv(csv);

        PluginConfig.RuntimeConfig runtimeConfig = new PluginConfig.RuntimeConfig();
        runtimeConfig.setAllowEmptyFields(Boolean.TRUE);
        runtimeConfig.setBatchedStatements(Boolean.TRUE);
        runtimeConfig.setBatchSize(Integer.valueOf(100));
        dbunit.setConfig(runtimeConfig);

        return dbunit;
    }
}
