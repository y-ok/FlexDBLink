package io.github.yok.flexdblink.maven.plugin.mojo;

import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.maven.plugin.config.CoreConfigBundle;
import io.github.yok.flexdblink.maven.plugin.config.PluginConfig;
import io.github.yok.flexdblink.maven.plugin.support.CoreConfigAssembler;
import io.github.yok.flexdblink.maven.plugin.support.FlexDbLinkCoreInvoker;
import io.github.yok.flexdblink.maven.plugin.support.MaskingLogUtil;
import io.github.yok.flexdblink.maven.plugin.support.PluginParameterValidator;
import io.github.yok.flexdblink.maven.plugin.support.PluginParameterValidator.GoalType;
import io.github.yok.flexdblink.maven.plugin.support.ScenarioNameResolver;
import io.github.yok.flexdblink.maven.plugin.support.SettingsConnectionResolver;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.settings.Settings;
import org.apache.maven.settings.crypto.SettingsDecrypter;

/**
 * Base Mojo that assembles plugin parameters and invokes existing FlexDBLink core classes.
 *
 * <p>
 * This class provides the shared execution flow for both {@code load} and {@code dump} goals:
 * collect Maven parameters, normalize and validate them, resolve connections from
 * {@code settings.xml}, assemble the reusable core configuration bundle, and finally delegate to a
 * goal-specific implementation.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
public abstract class AbstractFlexDbLinkMojo extends AbstractMojo {

    /**
     * Maven settings injected from the current build session.
     */
    @Parameter(defaultValue = "${settings}", readonly = true, required = true)
    protected Settings settings;

    /**
     * Maven component used to decrypt server credentials.
     */
    @Inject
    protected SettingsDecrypter settingsDecrypter;

    /**
     * Server IDs that identify database connections in {@code settings.xml}.
     */
    @Parameter
    protected List<String> serverIds;

    /**
     * Input data directory passed to the FlexDBLink core module.
     */
    @Parameter(property = "flexdblink.dataPath")
    protected String dataPath;

    /**
     * Optional file pattern overrides for file-based columns.
     */
    @Parameter
    protected List<PluginConfig.FilePattern> filePatterns;

    /**
     * Optional subset of database IDs to process.
     */
    @Parameter
    protected List<String> targetDbIds;

    /**
     * Scenario name provided for load or dump execution.
     */
    @Parameter(property = "flexdblink.scenario")
    protected String scenario;

    /**
     * Whether the plugin execution should be skipped.
     */
    @Parameter(property = "flexdblink.skip", defaultValue = "false")
    protected boolean skip;

    /**
     * Nested DBUnit-related plugin parameters.
     */
    @Parameter
    protected PluginConfig.DbUnit dbunit;

    /**
     * Validator for top-level and nested plugin parameters.
     */
    protected final PluginParameterValidator parameterValidator = new PluginParameterValidator();
    /**
     * Resolver that converts Maven servers into connection entries.
     */
    protected final SettingsConnectionResolver settingsConnectionResolver =
            new SettingsConnectionResolver();
    /**
     * Assembler that builds reusable core configuration objects.
     */
    protected final CoreConfigAssembler coreConfigAssembler = new CoreConfigAssembler();
    /**
     * Adapter used to call the existing FlexDBLink core services.
     */
    protected final FlexDbLinkCoreInvoker coreInvoker = new FlexDbLinkCoreInvoker();
    /**
     * Resolver for effective scenario names and default fallbacks.
     */
    protected final ScenarioNameResolver scenarioNameResolver = new ScenarioNameResolver();

    /**
     * Executes the Mojo using the common setup flow and goal-specific behavior.
     *
     * @throws MojoExecutionException when an execution error occurs
     * @throws MojoFailureException when a configuration error occurs
     */
    @Override
    public final void execute() throws MojoExecutionException, MojoFailureException {
        PluginConfig pluginConfig = buildPluginConfigFromParameters();
        if (pluginConfig.isSkip()) {
            getLog().info("FlexDBLink plugin execution is skipped.");
            return;
        }

        PluginContext context = buildContext(pluginConfig);
        try {
            executeGoal(context);
        } catch (MojoFailureException e) {
            throw e;
        } catch (MojoExecutionException e) {
            throw e;
        } catch (Exception e) {
            throw new MojoExecutionException("Failed to execute FlexDBLink goal.", e);
        }
    }

    /**
     * Executes goal-specific behavior after the common context has been prepared.
     *
     * @param ctx prepared plugin context
     * @throws Exception when the goal fails
     */
    protected abstract void executeGoal(PluginContext ctx) throws Exception;

    /**
     * Returns the goal type handled by the concrete Mojo.
     *
     * @return goal type
     */
    protected abstract GoalType goalType();

    /**
     * Builds a plugin configuration object from Mojo parameters.
     *
     * @return plugin configuration
     */
    protected PluginConfig buildPluginConfigFromParameters() {
        PluginConfig config = new PluginConfig();
        config.setServerIds(copyList(this.serverIds));
        config.setDataPath(this.dataPath);
        config.setFilePatterns(this.filePatterns);
        config.setTargetDbIds(this.targetDbIds);
        config.setScenario(this.scenario);
        config.setSkip(this.skip);
        config.setDbunit(this.dbunit);
        return config;
    }

    /**
     * Builds the plugin context from a plugin configuration.
     *
     * @param pluginConfig plugin configuration
     * @return plugin context
     * @throws MojoFailureException when configuration is invalid
     * @throws MojoExecutionException when context assembly fails unexpectedly
     */
    private PluginContext buildContext(PluginConfig pluginConfig)
            throws MojoFailureException, MojoExecutionException {
        try {
            parameterValidator.validateAndNormalize(pluginConfig, goalType());
            ConnectionConfig connectionConfig = settingsConnectionResolver.resolve(settings,
                    settingsDecrypter, pluginConfig.getServerIds());
            CoreConfigBundle bundle = coreConfigAssembler.assemble(pluginConfig, connectionConfig);
            logResolvedConnections(connectionConfig);
            return new PluginContext(pluginConfig, bundle, coreInvoker);
        } catch (IllegalArgumentException e) {
            throw new MojoFailureException(e.getMessage(), e);
        } catch (IllegalStateException e) {
            throw new MojoFailureException(e.getMessage(), e);
        } catch (Exception e) {
            throw new MojoExecutionException("Failed to prepare FlexDBLink plugin context.", e);
        }
    }

    /**
     * Logs resolved connections with masked values.
     *
     * @param connectionConfig resolved connection configuration
     */
    private void logResolvedConnections(ConnectionConfig connectionConfig) {
        for (ConnectionConfig.Entry entry : connectionConfig.getConnections()) {
            getLog().info("Resolved connection: " + MaskingLogUtil.maskConnection(entry));
        }
    }

    /**
     * Creates a shallow copy of a string list.
     *
     * @param source source list
     * @return copied list, or {@code null} when source is {@code null}
     */
    private List<String> copyList(List<String> source) {
        if (source == null) {
            return null;
        }
        return new ArrayList<>(source);
    }

    /**
     * Immutable execution context shared with concrete Mojos.
     *
     * <p>
     * The context keeps the normalized plugin configuration together with the derived core objects
     * needed during the final load or dump invocation.
     * </p>
     *
     * @author Yasuharu.Okawauchi
     */
    protected static final class PluginContext {
        /**
         * Normalized plugin parameters for the current execution.
         */
        final PluginConfig pluginConfig;
        /**
         * Assembled core configuration objects derived from the plugin parameters.
         */
        final CoreConfigBundle coreBundle;
        /**
         * Core invocation adapter used by the concrete Mojo.
         */
        final FlexDbLinkCoreInvoker invoker;

        /**
         * Creates a context instance.
         *
         * @param pluginConfig normalized plugin configuration
         * @param coreBundle assembled core bundle
         * @param invoker core invoker
         */
        PluginContext(PluginConfig pluginConfig, CoreConfigBundle coreBundle,
                FlexDbLinkCoreInvoker invoker) {
            this.pluginConfig = pluginConfig;
            this.coreBundle = coreBundle;
            this.invoker = invoker;
        }
    }
}
