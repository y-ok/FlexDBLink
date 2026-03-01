package io.github.yok.flexdblink.maven.plugin.support;

import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.core.DataDumper;
import io.github.yok.flexdblink.core.DataLoader;
import io.github.yok.flexdblink.maven.plugin.config.CoreConfigBundle;
import io.github.yok.flexdblink.util.ErrorHandler;
import java.util.List;

/**
 * Invokes existing FlexDBLink core classes from the Maven plugin.
 *
 * <p>
 * This adapter creates the existing loader and dumper entry points and temporarily disables
 * {@code ErrorHandler}'s process exit behavior so Maven can surface failures as plugin exceptions
 * instead of terminating the build JVM.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
public class FlexDbLinkCoreInvoker {

    /**
     * Executes the load operation using the existing core loader.
     *
     * @param bundle assembled core configuration bundle
     * @param scenarioOrNull scenario name, or {@code null} to execute pre only
     * @param targetDbIds target DB IDs
     */
    public void load(CoreConfigBundle bundle, String scenarioOrNull, List<String> targetDbIds) {
        ErrorHandler.disableExitForCurrentThread();
        try {
            DataLoader loader = new DataLoader(bundle.getPathsConfig(),
                    bundle.getConnectionConfig(), bundle.getDbDialectHandlerFactory()::create,
                    bundle.getDbUnitConfig(), bundle.getDumpConfig());
            loader.execute(scenarioOrNull, targetDbIds);
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }
    }

    /**
     * Executes the dump operation using the existing core dumper.
     *
     * @param bundle assembled core configuration bundle
     * @param scenario scenario name (not blank)
     * @param targetDbIds target DB IDs
     */
    public void dump(CoreConfigBundle bundle, String scenario, List<String> targetDbIds) {
        ErrorHandler.disableExitForCurrentThread();
        try {
            DataDumper dumper = new DataDumper(bundle.getPathsConfig(),
                    bundle.getConnectionConfig(), bundle.getFilePatternConfig(),
                    bundle.getDumpConfig(), bundle.getDbDialectHandlerFactory()::create);
            dumper.execute(scenario, targetDbIds);
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }
    }

    /**
     * Extracts the connection list from the bundle for convenience in logs/tests.
     *
     * @param bundle assembled core configuration bundle
     * @return connection entries
     */
    public List<ConnectionConfig.Entry> connections(CoreConfigBundle bundle) {
        return bundle.getConnectionConfig().getConnections();
    }
}
