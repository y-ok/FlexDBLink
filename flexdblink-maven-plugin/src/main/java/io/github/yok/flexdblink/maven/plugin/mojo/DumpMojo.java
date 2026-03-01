package io.github.yok.flexdblink.maven.plugin.mojo;

import io.github.yok.flexdblink.maven.plugin.support.PluginParameterValidator.GoalType;
import java.time.Clock;
import org.apache.maven.plugins.annotations.Mojo;

/**
 * Maven goal that executes FlexDBLink dump using existing core classes.
 *
 * <p>
 * This goal guarantees a non-blank scenario name by generating a timestamp when the user does not
 * specify one explicitly.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
@Mojo(name = "dump", threadSafe = false)
public class DumpMojo extends AbstractFlexDbLinkMojo {

    /**
     * Executes the dump goal after resolving the effective scenario name.
     *
     * @param ctx prepared plugin context
     */
    @Override
    protected void executeGoal(PluginContext ctx) {
        String effectiveScenario =
                scenarioNameResolver.resolveForDump(ctx.pluginConfig.getScenario(), currentClock());
        getLog().info("Executing FlexDBLink dump. scenario=" + effectiveScenario);
        ctx.invoker.dump(ctx.coreBundle, effectiveScenario, ctx.pluginConfig.getTargetDbIds());
    }

    /**
     * Returns the goal type handled by this Mojo.
     *
     * @return goal type
     */
    @Override
    protected GoalType goalType() {
        return GoalType.DUMP;
    }

    /**
     * Returns the clock used for timestamp scenario generation.
     *
     * @return current clock
     */
    protected Clock currentClock() {
        return Clock.systemDefaultZone();
    }
}
