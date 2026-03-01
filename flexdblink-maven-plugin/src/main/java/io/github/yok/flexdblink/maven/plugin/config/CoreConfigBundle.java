package io.github.yok.flexdblink.maven.plugin.config;

import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.FilePatternConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.db.DbDialectHandlerFactory;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Immutable bundle of FlexDBLink core configuration objects prepared by the Maven plugin.
 *
 * <p>
 * This type groups the already-normalized configuration instances that are passed together to
 * {@code DataLoader}, {@code DataDumper}, and the dialect handler factory. It keeps the plugin
 * boundary small by exposing one container instead of multiple constructor arguments throughout the
 * Mojo support layer.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
@Getter
@AllArgsConstructor
public class CoreConfigBundle {

    /**
     * Path configuration forwarded to the core module.
     */
    private final PathsConfig pathsConfig;
    /**
     * Resolved database connection definitions.
     */
    private final ConnectionConfig connectionConfig;
    /**
     * File pattern configuration used for LOB/file exports.
     */
    private final FilePatternConfig filePatternConfig;
    /**
     * Basic DBUnit configuration for load execution.
     */
    private final DbUnitConfig dbUnitConfig;
    /**
     * Dump-specific configuration forwarded to the core module.
     */
    private final DumpConfig dumpConfig;
    /**
     * Dialect handler factory created from the assembled plugin settings.
     */
    private final DbDialectHandlerFactory dbDialectHandlerFactory;
}
