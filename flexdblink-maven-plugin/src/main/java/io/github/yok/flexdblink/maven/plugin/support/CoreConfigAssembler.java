package io.github.yok.flexdblink.maven.plugin.support;

import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.CsvDateTimeFormatProperties;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DbUnitConfigProperties;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.FilePatternConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.db.DbDialectHandlerFactory;
import io.github.yok.flexdblink.db.DbUnitConfigFactory;
import io.github.yok.flexdblink.maven.plugin.config.CoreConfigBundle;
import io.github.yok.flexdblink.maven.plugin.config.PluginConfig;
import io.github.yok.flexdblink.util.DateTimeFormatUtil;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Assembles existing FlexDBLink configuration objects from normalized plugin parameters.
 *
 * <p>
 * This adapter converts the plugin-local {@link PluginConfig} model into the configuration types
 * already used by the FlexDBLink core module. The goal is to reuse core behavior without
 * duplicating configuration rules inside the plugin.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
public class CoreConfigAssembler {

    /**
     * Builds a core configuration bundle for invoking FlexDBLink core classes.
     *
     * @param pluginConfig normalized plugin configuration
     * @param connectionConfig resolved connection configuration
     * @return assembled core configuration bundle
     */
    public CoreConfigBundle assemble(PluginConfig pluginConfig, ConnectionConfig connectionConfig) {
        PathsConfig pathsConfig = buildPathsConfig(pluginConfig.getDataPath());
        FilePatternConfig filePatternConfig =
                buildFilePatternConfig(pluginConfig.getFilePatterns());
        DbUnitConfig dbUnitConfig = buildDbUnitConfig(pluginConfig.getDbunit());
        DumpConfig dumpConfig = buildDumpConfig();
        DbUnitConfigProperties dbUnitConfigProperties =
                buildDbUnitConfigProperties(pluginConfig.getDbunit());
        CsvDateTimeFormatProperties csvProps =
                buildCsvDateTimeFormatProperties(pluginConfig.getDbunit());
        DateTimeFormatUtil dateTimeFormatUtil = new DateTimeFormatUtil(csvProps);
        DbUnitConfigFactory dbUnitConfigFactory = new DbUnitConfigFactory(dbUnitConfigProperties);
        DbDialectHandlerFactory dbDialectHandlerFactory = new DbDialectHandlerFactory(dbUnitConfig,
                dumpConfig, pathsConfig, dateTimeFormatUtil, dbUnitConfigFactory);
        return new CoreConfigBundle(pathsConfig, connectionConfig, filePatternConfig, dbUnitConfig,
                dumpConfig, dbDialectHandlerFactory);
    }

    /**
     * Builds path configuration.
     *
     * @param dataPath data path from plugin configuration
     * @return path configuration
     */
    private PathsConfig buildPathsConfig(String dataPath) {
        PathsConfig config = new PathsConfig();
        config.setDataPath(dataPath);
        return config;
    }

    /**
     * Builds file pattern configuration.
     *
     * @param patterns plugin file pattern entries
     * @return file pattern configuration
     */
    private FilePatternConfig buildFilePatternConfig(List<PluginConfig.FilePattern> patterns) {
        FilePatternConfig config = new FilePatternConfig();
        Map<String, Map<String, String>> mapped = new LinkedHashMap<>();
        Map<String, String> duplicateKeyMap = new LinkedHashMap<>();
        if (patterns != null) {
            for (PluginConfig.FilePattern pattern : patterns) {
                String tableName = pattern.getTableName().trim();
                String columnName = pattern.getColumnName().trim();
                String fileName = pattern.getFilename().trim();

                String normalizedKey = (tableName + "::" + columnName).toLowerCase();
                if (duplicateKeyMap.containsKey(normalizedKey)) {
                    throw new IllegalArgumentException(
                            "Duplicate filePatterns entry: " + tableName + "." + columnName);
                }
                duplicateKeyMap.put(normalizedKey, fileName);

                Map<String, String> tableMap = mapped.get(tableName);
                if (tableMap == null) {
                    tableMap = new LinkedHashMap<>();
                    mapped.put(tableName, tableMap);
                }
                tableMap.put(columnName, fileName);
            }
        }
        config.setFilePatterns(mapped);
        return config;
    }

    /**
     * Builds dbunit basic configuration.
     *
     * @param dbunit plugin dbunit settings
     * @return dbunit configuration
     */
    private DbUnitConfig buildDbUnitConfig(PluginConfig.DbUnit dbunit) {
        DbUnitConfig config = new DbUnitConfig();
        config.setPreDirName(dbunit.getPreDirName());
        return config;
    }

    /**
     * Builds dump configuration.
     *
     * @return dump configuration with current defaults
     */
    private DumpConfig buildDumpConfig() {
        return new DumpConfig();
    }

    /**
     * Builds DBUnit runtime configuration properties.
     *
     * @param dbunit plugin dbunit settings
     * @return DBUnit config properties
     */
    private DbUnitConfigProperties buildDbUnitConfigProperties(PluginConfig.DbUnit dbunit) {
        DbUnitConfigProperties props = new DbUnitConfigProperties();
        PluginConfig.RuntimeConfig runtimeConfig = dbunit.getConfig();
        props.setAllowEmptyFields(runtimeConfig.getAllowEmptyFields().booleanValue());
        props.setBatchedStatements(runtimeConfig.getBatchedStatements().booleanValue());
        props.setBatchSize(runtimeConfig.getBatchSize().intValue());
        return props;
    }

    /**
     * Builds CSV date-time format properties.
     *
     * @param dbunit plugin dbunit settings
     * @return CSV date-time format properties
     */
    private CsvDateTimeFormatProperties buildCsvDateTimeFormatProperties(
            PluginConfig.DbUnit dbunit) {
        CsvDateTimeFormatProperties props = new CsvDateTimeFormatProperties();
        PluginConfig.Format format = dbunit.getCsv().getFormat();
        props.setDate(format.getDate());
        props.setTime(format.getTime());
        props.setDateTime(format.getDateTime());
        props.setDateTimeWithMillis(format.getDateTimeWithMillis());
        return props;
    }
}
