package io.github.yok.flexdblink.maven.plugin.mojo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import io.github.yok.flexdblink.maven.plugin.config.PluginConfig;
import io.github.yok.flexdblink.maven.plugin.support.FlexDbLinkCoreInvoker;
import io.github.yok.flexdblink.maven.plugin.support.PluginParameterValidator.GoalType;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;

class DumpMojoTest {

    @Test
    void goalType_正常ケース_DUMP型を取得する_DUMPが返ること() {
        DumpMojo target = new DumpMojo();

        assertEquals(GoalType.DUMP, target.goalType());
    }

    @Test
    void currentClock_正常ケース_システムクロックを取得する_非nullのClockが返ること() {
        DumpMojo target = new DumpMojo();

        assertNotNull(target.currentClock());
    }

    @Test
    void executeGoal_正常ケース_未指定scenarioを実行する_タイムスタンプがcoreに渡ること() {
        TestableDumpMojo target = new TestableDumpMojo();
        CapturingInvoker invoker = new CapturingInvoker();
        PluginConfig config = new PluginConfig();
        config.setTargetDbIds(List.of("DB2"));
        AbstractFlexDbLinkMojo.PluginContext ctx =
                new AbstractFlexDbLinkMojo.PluginContext(config, null, invoker);

        target.executeGoal(ctx);

        assertEquals("20260226123456", invoker.dumpScenario);
        assertEquals(List.of("DB2"), invoker.dumpTargetDbIds);
    }

    @Test
    void executeGoal_正常ケース_指定scenarioを実行する_指定文字列がcoreに渡ること() {
        TestableDumpMojo target = new TestableDumpMojo();
        CapturingInvoker invoker = new CapturingInvoker();
        PluginConfig config = new PluginConfig();
        config.setScenario("manual");
        AbstractFlexDbLinkMojo.PluginContext ctx =
                new AbstractFlexDbLinkMojo.PluginContext(config, null, invoker);

        target.executeGoal(ctx);

        assertEquals("manual", invoker.dumpScenario);
    }

    private static class TestableDumpMojo extends DumpMojo {
        @Override
        protected Clock currentClock() {
            return Clock.fixed(Instant.parse("2026-02-26T12:34:56Z"), ZoneOffset.UTC);
        }
    }

    private static class CapturingInvoker extends FlexDbLinkCoreInvoker {
        private String dumpScenario;
        private List<String> dumpTargetDbIds;

        @Override
        public void dump(io.github.yok.flexdblink.maven.plugin.config.CoreConfigBundle bundle,
                String scenario, List<String> targetDbIds) {
            this.dumpScenario = scenario;
            this.dumpTargetDbIds = targetDbIds;
        }
    }
}
