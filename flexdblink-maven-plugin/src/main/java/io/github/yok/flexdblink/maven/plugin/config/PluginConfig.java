package io.github.yok.flexdblink.maven.plugin.config;

import java.util.List;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Normalized plugin configuration assembled from Maven parameters.
 *
 * <p>
 * This model is internal to the plugin and is used after Maven has bound the Mojo parameters. The
 * validator layer trims user input, fills defaults, and ensures nested sections are always present
 * before the configuration is converted to FlexDBLink core objects.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
@Data
@NoArgsConstructor
public class PluginConfig {

    /**
     * Maven {@code settings.xml} server IDs to resolve as database connections.
     */
    private List<String> serverIds;
    /**
     * Base data directory used by the FlexDBLink core module.
     */
    private String dataPath;
    /**
     * Optional subset of DB IDs to target during execution.
     */
    private List<String> targetDbIds;
    /**
     * Scenario name supplied by the user.
     */
    private String scenario;
    /**
     * Flag that skips Mojo execution when set.
     */
    private boolean skip;
    /**
     * Optional file pattern mappings for file-based columns.
     */
    private List<FilePattern> filePatterns;
    /**
     * Nested DBUnit-related plugin settings.
     */
    private DbUnit dbunit;

    /**
     * File pattern entry defined in the plugin configuration.
     *
     * <p>
     * Each entry maps one table/column pair to the file name pattern used by the core dump logic.
     * </p>
     *
     * @author Yasuharu.Okawauchi
     */
    @Data
    @NoArgsConstructor
    public static class FilePattern {
        /**
         * Table name that owns the target column.
         */
        private String tableName;
        /**
         * Column name associated with the file pattern.
         */
        private String columnName;
        /**
         * File name pattern used for import/export.
         */
        private String filename;
    }

    /**
     * DBUnit-related plugin configuration.
     *
     * <p>
     * This section contains defaults and runtime settings that are converted into the existing core
     * {@code DbUnitConfig} model.
     * </p>
     *
     * @author Yasuharu.Okawauchi
     */
    @Data
    @NoArgsConstructor
    public static class DbUnit {
        /**
         * Directory name used for pre-load datasets.
         */
        private String preDirName;
        /**
         * CSV-related settings for date/time parsing and formatting.
         */
        private Csv csv;
        /**
         * DBUnit runtime behavior flags and limits.
         */
        private RuntimeConfig config;
    }

    /**
     * CSV-related plugin configuration.
     *
     * <p>
     * This wrapper groups CSV-specific settings so that the plugin configuration structure matches
     * the XML nesting used in the Mojo parameters.
     * </p>
     *
     * @author Yasuharu.Okawauchi
     */
    @Data
    @NoArgsConstructor
    public static class Csv {
        /**
         * Date/time format patterns used by CSV processing.
         */
        private Format format;
    }

    /**
     * CSV date-time format settings.
     *
     * <p>
     * These patterns are forwarded to the core formatter utilities and therefore must remain
     * compatible with {@link java.time.format.DateTimeFormatter}.
     * </p>
     *
     * @author Yasuharu.Okawauchi
     */
    @Data
    @NoArgsConstructor
    public static class Format {
        /**
         * Pattern used for date-only values.
         */
        private String date;
        /**
         * Pattern used for time-only values.
         */
        private String time;
        /**
         * Pattern used for timestamp values without milliseconds.
         */
        private String dateTime;
        /**
         * Pattern used for timestamp values with milliseconds.
         */
        private String dateTimeWithMillis;
    }

    /**
     * DBUnit runtime feature settings.
     *
     * <p>
     * The validator applies default values here so downstream assembly code can treat the fields as
     * required.
     * </p>
     *
     * @author Yasuharu.Okawauchi
     */
    @Data
    @NoArgsConstructor
    public static class RuntimeConfig {
        /**
         * Whether empty CSV fields are accepted.
         */
        private Boolean allowEmptyFields;
        /**
         * Whether DBUnit batch statements are enabled.
         */
        private Boolean batchedStatements;
        /**
         * Batch size applied to DBUnit write operations.
         */
        private Integer batchSize;
    }
}
