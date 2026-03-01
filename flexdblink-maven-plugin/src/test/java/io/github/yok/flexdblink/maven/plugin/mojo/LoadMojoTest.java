package io.github.yok.flexdblink.maven.plugin.mojo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import io.github.yok.flexdblink.maven.plugin.config.PluginConfig;
import io.github.yok.flexdblink.maven.plugin.support.FlexDbLinkCoreInvoker;
import io.github.yok.flexdblink.maven.plugin.support.PluginParameterValidator.GoalType;
import java.util.List;
import org.junit.jupiter.api.Test;

class LoadMojoTest {

    @Test
    void goalType_正常ケース_LOAD型を取得する_LOADが返ること() {
        LoadMojo target = new LoadMojo();

        assertEquals(GoalType.LOAD, target.goalType());
    }

    @Test
    void executeGoal_正常ケース_未指定scenarioを実行する_nullがcoreに渡ること() {
        LoadMojo target = new LoadMojo();
        CapturingInvoker invoker = new CapturingInvoker();
        PluginConfig config = new PluginConfig();
        config.setScenario("   ");
        config.setTargetDbIds(List.of("DB1"));
        AbstractFlexDbLinkMojo.PluginContext ctx =
                new AbstractFlexDbLinkMojo.PluginContext(config, null, invoker);

        target.executeGoal(ctx);

        assertNull(invoker.loadScenario);
        assertEquals(List.of("DB1"), invoker.loadTargetDbIds);
    }

    @Test
    void executeGoal_正常ケース_指定scenarioを実行する_trim済み文字列がcoreに渡ること() {
        LoadMojo target = new LoadMojo();
        CapturingInvoker invoker = new CapturingInvoker();
        PluginConfig config = new PluginConfig();
        config.setScenario("  scenario1  ");
        AbstractFlexDbLinkMojo.PluginContext ctx =
                new AbstractFlexDbLinkMojo.PluginContext(config, null, invoker);

        target.executeGoal(ctx);

        assertEquals("scenario1", invoker.loadScenario);
    }

    private static class CapturingInvoker extends FlexDbLinkCoreInvoker {
        private String loadScenario;
        private List<String> loadTargetDbIds;

        @Override
        public void load(io.github.yok.flexdblink.maven.plugin.config.CoreConfigBundle bundle,
                String scenarioOrNull, List<String> targetDbIds) {
            this.loadScenario = scenarioOrNull;
            this.loadTargetDbIds = targetDbIds;
        }
    }
}
