package io.github.yok.flexdblink.maven.plugin.support;

import io.github.yok.flexdblink.maven.plugin.config.PluginConfig;
import java.util.ArrayList;
import java.util.List;

/**
 * Validates plugin parameters and normalizes optional sections before execution.
 *
 * <p>
 * This validator is intentionally used before any core configuration objects are created. It trims
 * user-provided lists, fills defaults in the nested DBUnit section, and rejects incomplete
 * combinations early so the Mojo can report clear configuration failures.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
public class PluginParameterValidator {

    /**
     * Goal types supported by this validator.
     */
    public enum GoalType {
        LOAD, DUMP
    }

    /**
     * Validates plugin parameters and fills default values for optional settings.
     *
     * @param config plugin configuration
     * @param goalType goal type
     */
    public void validateAndNormalize(PluginConfig config, GoalType goalType) {
        validateRequiredTopLevel(config);
        normalizeTargetDbIds(config);
        normalizeDbUnit(config);
        validateFilePatterns(config);
        validateGoalType(config, goalType);
    }

    /**
     * Validates required top-level parameters.
     *
     * @param config plugin configuration
     */
    private void validateRequiredTopLevel(PluginConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("Plugin configuration is required.");
        }
        if (config.getServerIds() == null || config.getServerIds().isEmpty()) {
            throw new IllegalArgumentException("serverIds is required.");
        }
        for (String serverId : config.getServerIds()) {
            if (serverId == null || serverId.trim().isEmpty()) {
                throw new IllegalArgumentException("serverIds must not contain blank values.");
            }
        }
        if (config.getDataPath() == null || config.getDataPath().trim().isEmpty()) {
            throw new IllegalArgumentException("dataPath is required.");
        }
    }

    /**
     * Normalizes the target DB ID list.
     *
     * @param config plugin configuration
     */
    private void normalizeTargetDbIds(PluginConfig config) {
        if (config.getTargetDbIds() == null) {
            return;
        }
        List<String> normalized = new ArrayList<>();
        for (String raw : config.getTargetDbIds()) {
            if (raw == null) {
                continue;
            }
            String trimmed = raw.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            normalized.add(trimmed);
        }
        config.setTargetDbIds(normalized);
    }

    /**
     * Applies defaults to dbunit settings and validates them.
     *
     * @param config plugin configuration
     */
    private void normalizeDbUnit(PluginConfig config) {
        PluginConfig.DbUnit dbunit = config.getDbunit();
        if (dbunit == null) {
            dbunit = new PluginConfig.DbUnit();
            config.setDbunit(dbunit);
        }

        if (dbunit.getPreDirName() == null || dbunit.getPreDirName().trim().isEmpty()) {
            dbunit.setPreDirName("pre");
        } else {
            dbunit.setPreDirName(dbunit.getPreDirName().trim());
        }

        PluginConfig.Csv csv = dbunit.getCsv();
        if (csv == null) {
            csv = new PluginConfig.Csv();
            dbunit.setCsv(csv);
        }
        PluginConfig.Format format = csv.getFormat();
        if (format == null) {
            format = new PluginConfig.Format();
            csv.setFormat(format);
        }

        format.setDate(defaultIfBlank(format.getDate(), "yyyy-MM-dd"));
        format.setTime(defaultIfBlank(format.getTime(), "HH:mm:ss"));
        format.setDateTime(defaultIfBlank(format.getDateTime(), "yyyy-MM-dd HH:mm:ss"));
        format.setDateTimeWithMillis(
                defaultIfBlank(format.getDateTimeWithMillis(), "yyyy-MM-dd HH:mm:ss.SSS"));

        PluginConfig.RuntimeConfig runtimeConfig = dbunit.getConfig();
        if (runtimeConfig == null) {
            runtimeConfig = new PluginConfig.RuntimeConfig();
            dbunit.setConfig(runtimeConfig);
        }
        if (runtimeConfig.getAllowEmptyFields() == null) {
            runtimeConfig.setAllowEmptyFields(Boolean.TRUE);
        }
        if (runtimeConfig.getBatchedStatements() == null) {
            runtimeConfig.setBatchedStatements(Boolean.TRUE);
        }
        if (runtimeConfig.getBatchSize() == null) {
            runtimeConfig.setBatchSize(Integer.valueOf(100));
        }
        if (runtimeConfig.getBatchSize().intValue() <= 0) {
            throw new IllegalArgumentException("dbunit.config.batchSize must be greater than 0.");
        }
    }

    /**
     * Validates file pattern entries.
     *
     * @param config plugin configuration
     */
    private void validateFilePatterns(PluginConfig config) {
        if (config.getFilePatterns() == null) {
            return;
        }
        for (PluginConfig.FilePattern pattern : config.getFilePatterns()) {
            if (pattern == null) {
                throw new IllegalArgumentException("filePatterns must not contain null entries.");
            }
            if (isBlank(pattern.getTableName())) {
                throw new IllegalArgumentException("filePatterns.tableName is required.");
            }
            if (isBlank(pattern.getColumnName())) {
                throw new IllegalArgumentException("filePatterns.columnName is required.");
            }
            if (isBlank(pattern.getFilename())) {
                throw new IllegalArgumentException("filePatterns.filename is required.");
            }
        }
    }

    /**
     * Validates goal type is not null.
     *
     * @param config plugin configuration
     * @param goalType goal type
     */
    private void validateGoalType(PluginConfig config, GoalType goalType) {
        if (goalType == null) {
            throw new IllegalArgumentException("goalType is required.");
        }
    }

    /**
     * Returns the default value when the candidate is blank.
     *
     * @param candidate candidate value
     * @param defaultValue default value
     * @return candidate or default value
     */
    private String defaultIfBlank(String candidate, String defaultValue) {
        if (candidate == null) {
            return defaultValue;
        }
        String trimmed = candidate.trim();
        if (trimmed.isEmpty()) {
            return defaultValue;
        }
        return trimmed;
    }

    /**
     * Returns whether the string is null or blank.
     *
     * @param value target value
     * @return {@code true} when blank
     */
    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
