package io.github.yok.flexdblink.maven.plugin.mojo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.github.yok.flexdblink.maven.plugin.config.PluginConfig;
import io.github.yok.flexdblink.maven.plugin.support.PluginParameterValidator.GoalType;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.settings.Server;
import org.apache.maven.settings.Settings;
import org.apache.maven.settings.crypto.SettingsDecrypter;
import org.apache.maven.settings.crypto.SettingsDecryptionRequest;
import org.apache.maven.settings.crypto.SettingsDecryptionResult;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AbstractFlexDbLinkMojoTest {

    @Test
    void execute_正常ケース_skip有効で実行する_executeGoalが実行されないこと(@TempDir Path tempDir)
            throws Exception {
        TestMojo target = new TestMojo(GoalType.LOAD);
        configureValid(target, tempDir);
        target.skip = true;

        target.execute();

        assertFalse(target.executeGoalCalled);
    }

    @Test
    void execute_正常ケース_有効設定で実行する_executeGoalが実行されること(@TempDir Path tempDir)
            throws Exception {
        TestMojo target = new TestMojo(GoalType.LOAD);
        configureValid(target, tempDir);

        target.execute();

        assertTrue(target.executeGoalCalled);
    }

    @Test
    void execute_異常ケース_executeGoalで例外送出を実行する_MojoExecutionExceptionが再スローされること(
            @TempDir Path tempDir) throws Exception {
        TestMojo target = new TestMojo(GoalType.LOAD);
        configureValid(target, tempDir);
        target.toThrow = new Exception("boom");

        MojoExecutionException ex =
                assertThrows(MojoExecutionException.class, target::execute);

        assertTrue(ex.getMessage().contains("Failed to execute FlexDBLink goal"));
    }

    @Test
    void execute_異常ケース_executeGoalでMojoFailureException送出を実行する_同一例外が再スローされること(
            @TempDir Path tempDir) throws Exception {
        TestMojo target = new TestMojo(GoalType.LOAD);
        configureValid(target, tempDir);
        target.toThrow = new MojoFailureException("failure");

        MojoFailureException ex =
                assertThrows(MojoFailureException.class, target::execute);

        assertEquals("failure", ex.getMessage());
    }

    @Test
    void execute_異常ケース_executeGoalでMojoExecutionException送出を実行する_同一例外が再スローされること(
            @TempDir Path tempDir) throws Exception {
        TestMojo target = new TestMojo(GoalType.LOAD);
        configureValid(target, tempDir);
        target.toThrow = new MojoExecutionException("execution");

        MojoExecutionException ex =
                assertThrows(MojoExecutionException.class, target::execute);

        assertEquals("execution", ex.getMessage());
    }

    @Test
    void execute_異常ケース_必須パラメータ不足で実行する_MojoFailureExceptionが送出されること(
            @TempDir Path tempDir) {
        TestMojo target = new TestMojo(GoalType.LOAD);
        configureValid(target, tempDir);
        target.serverIds = null;

        MojoFailureException ex =
                assertThrows(MojoFailureException.class, target::execute);

        assertTrue(ex.getMessage().contains("serverIds"));
    }

    @Test
    void execute_異常ケース_settingsDecrypter失敗で実行する_MojoExecutionExceptionが送出されること(
            @TempDir Path tempDir) {
        TestMojo target = new TestMojo(GoalType.LOAD);
        configureValid(target, tempDir);

        SettingsDecrypter broken = mock(SettingsDecrypter.class);
        when(broken.decrypt(any(SettingsDecryptionRequest.class)))
                .thenThrow(new RuntimeException("decrypt failed"));
        target.settingsDecrypter = broken;

        MojoExecutionException ex =
                assertThrows(MojoExecutionException.class, target::execute);

        assertTrue(ex.getMessage()
                .contains("Failed to prepare FlexDBLink plugin context"));
    }

    @Test
    void execute_異常ケース_IllegalStateExceptionを送出する_MojoFailureExceptionが送出されること(
            @TempDir Path tempDir) {
        TestMojo target = new TestMojo(GoalType.LOAD);
        configureValid(target, tempDir);

        SettingsDecrypter broken = mock(SettingsDecrypter.class);
        when(broken.decrypt(any(SettingsDecryptionRequest.class)))
                .thenThrow(new IllegalStateException("bad state"));
        target.settingsDecrypter = broken;

        MojoFailureException ex =
                assertThrows(MojoFailureException.class, target::execute);

        assertTrue(ex.getMessage().contains("bad state"));
    }

    @Test
    void buildPluginConfigFromParameters_正常ケース_targetDbIdsをそのまま設定する_未正規化のリストが返ること(
            @TempDir Path tempDir) {
        TestMojo target = new TestMojo(GoalType.DUMP);
        configureValid(target, tempDir);
        target.targetDbIds = Arrays.asList(" DB1 ", "", "   ", null, "DB2");

        PluginConfig actual = target.buildPluginConfigFromParameters();

        assertEquals(Arrays.asList(" DB1 ", "", "   ", null, "DB2"), actual.getTargetDbIds());
    }

    @Test
    void buildPluginConfigFromParameters_正常ケース_targetDbIdsがnullを実行する_nullが返ること(
            @TempDir Path tempDir) {
        TestMojo target = new TestMojo(GoalType.DUMP);
        configureValid(target, tempDir);
        target.targetDbIds = null;

        PluginConfig actual = target.buildPluginConfigFromParameters();

        assertNull(actual.getTargetDbIds());
    }

    @Test
    void buildPluginConfigFromParameters_正常ケース_serverIdsがnullを実行する_nullが返ること(
            @TempDir Path tempDir) {
        TestMojo target = new TestMojo(GoalType.DUMP);
        configureValid(target, tempDir);
        target.serverIds = null;

        PluginConfig actual = target.buildPluginConfigFromParameters();

        assertNull(actual.getServerIds());
    }

    private void configureValid(TestMojo target, Path tempDir) {
        Server server = new Server();
        server.setId("flexdblink-db1");
        server.setUsername("app");
        server.setPassword("pw");
        Xpp3Dom configuration = new Xpp3Dom("configuration");
        addChild(configuration, "dbId", "DB1");
        addChild(configuration, "url", "jdbc:h2:mem:flexdb");
        addChild(configuration, "driverClass", "org.h2.Driver");
        server.setConfiguration(configuration);

        Settings settings = new Settings();
        settings.setServers(List.of(server));
        target.settings = settings;
        target.settingsDecrypter = passThroughDecrypter(server);
        target.serverIds = List.of("flexdblink-db1");
        target.dataPath = tempDir.toString();
        target.filePatterns = List.of();
        target.targetDbIds = List.of("DB1");
        target.scenario = "scenario1";
        target.skip = false;
        target.dbunit = null;
    }

    private SettingsDecrypter passThroughDecrypter(Server server) {
        SettingsDecrypter decrypter = mock(SettingsDecrypter.class);
        SettingsDecryptionResult result = mock(SettingsDecryptionResult.class);
        when(result.getServer()).thenReturn(server);
        when(decrypter.decrypt(any(SettingsDecryptionRequest.class)))
                .thenReturn(result);
        return decrypter;
    }

    private void addChild(Xpp3Dom parent, String name, String value) {
        Xpp3Dom child = new Xpp3Dom(name);
        child.setValue(value);
        parent.addChild(child);
    }

    private static final class TestMojo extends AbstractFlexDbLinkMojo {

        private final GoalType goalType;
        private Exception toThrow;
        private boolean executeGoalCalled;

        private TestMojo(GoalType goalType) {
            this.goalType = goalType;
        }

        @Override
        protected void executeGoal(PluginContext ctx) throws Exception {
            executeGoalCalled = true;
            if (toThrow == null) {
                return;
            }
            throw toThrow;
        }

        @Override
        protected GoalType goalType() {
            return goalType;
        }
    }
}
