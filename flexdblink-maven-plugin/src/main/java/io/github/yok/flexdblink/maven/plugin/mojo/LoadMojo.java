package io.github.yok.flexdblink.maven.plugin.mojo;

import io.github.yok.flexdblink.maven.plugin.support.PluginParameterValidator.GoalType;
import org.apache.maven.plugins.annotations.Mojo;

/**
 * Maven goal that executes FlexDBLink load using existing core classes.
 *
 * <p>
 * This goal resolves a blank scenario to {@code null}, which instructs the core loader to execute
 * only the pre-data phase.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
@Mojo(name = "load", threadSafe = false)
public class LoadMojo extends AbstractFlexDbLinkMojo {

    /**
     * Executes the load goal after resolving the effective scenario name.
     *
     * @param ctx prepared plugin context
     */
    @Override
    protected void executeGoal(PluginContext ctx) {
        String effectiveScenario =
                scenarioNameResolver.resolveForLoad(ctx.pluginConfig.getScenario());
        getLog().info("Executing FlexDBLink load. scenario=" + effectiveScenario);
        ctx.invoker.load(ctx.coreBundle, effectiveScenario, ctx.pluginConfig.getTargetDbIds());
    }

    /**
     * Returns the goal type handled by this Mojo.
     *
     * @return goal type
     */
    @Override
    protected GoalType goalType() {
        return GoalType.LOAD;
    }
}
