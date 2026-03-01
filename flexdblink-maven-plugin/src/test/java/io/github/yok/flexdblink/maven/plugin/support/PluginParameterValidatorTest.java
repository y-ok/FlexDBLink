package io.github.yok.flexdblink.maven.plugin.support;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.github.yok.flexdblink.maven.plugin.config.PluginConfig;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class PluginParameterValidatorTest {

    private final PluginParameterValidator target = new PluginParameterValidator();

    @Test
    void validateAndNormalize_正常ケース_未指定dbunitを補完する_既定値が設定されること() {
        PluginConfig config = new PluginConfig();
        config.setServerIds(List.of("db1"));
        config.setDataPath("/tmp/data");

        target.validateAndNormalize(config, PluginParameterValidator.GoalType.LOAD);

        assertEquals("pre", config.getDbunit().getPreDirName());
        assertEquals("yyyy-MM-dd", config.getDbunit().getCsv().getFormat().getDate());
        assertEquals("HH:mm:ss", config.getDbunit().getCsv().getFormat().getTime());
        assertEquals("yyyy-MM-dd HH:mm:ss", config.getDbunit().getCsv().getFormat().getDateTime());
        assertEquals("yyyy-MM-dd HH:mm:ss.SSS",
                config.getDbunit().getCsv().getFormat().getDateTimeWithMillis());
        assertTrue(config.getDbunit().getConfig().getAllowEmptyFields().booleanValue());
        assertTrue(config.getDbunit().getConfig().getBatchedStatements().booleanValue());
        assertEquals(100, config.getDbunit().getConfig().getBatchSize().intValue());
    }

    @Test
    void validateAndNormalize_正常ケース_targetDbIdsを正規化する_空要素が除外されること() {
        PluginConfig config = new PluginConfig();
        config.setServerIds(List.of("db1"));
        config.setDataPath("/tmp/data");
        config.setTargetDbIds(Arrays.asList(" DB1 ", "", "   ", null, "DB2"));

        target.validateAndNormalize(config, PluginParameterValidator.GoalType.LOAD);

        assertEquals(List.of("DB1", "DB2"), config.getTargetDbIds());
    }

    @Test
    void validateAndNormalize_正常ケース_targetDbIdsがnull_nullのままであること() {
        PluginConfig config = new PluginConfig();
        config.setServerIds(List.of("db1"));
        config.setDataPath("/tmp/data");
        config.setTargetDbIds(null);

        target.validateAndNormalize(config, PluginParameterValidator.GoalType.LOAD);

        assertNull(config.getTargetDbIds());
    }

    @Test
    void validateAndNormalize_正常ケース_preDirName空白を補完する_既定値preが設定されること() {
        PluginConfig config = new PluginConfig();
        config.setServerIds(List.of("db1"));
        config.setDataPath("/tmp/data");
        PluginConfig.DbUnit dbunit = new PluginConfig.DbUnit();
        dbunit.setPreDirName("   ");
        config.setDbunit(dbunit);

        target.validateAndNormalize(config, PluginParameterValidator.GoalType.LOAD);

        assertEquals("pre", config.getDbunit().getPreDirName());
    }

    @Test
    void validateAndNormalize_正常ケース_format空文字を補完する_既定値が設定されること() {
        PluginConfig config = new PluginConfig();
        config.setServerIds(List.of("db1"));
        config.setDataPath("/tmp/data");
        PluginConfig.DbUnit dbunit = new PluginConfig.DbUnit();
        PluginConfig.Csv csv = new PluginConfig.Csv();
        PluginConfig.Format format = new PluginConfig.Format();
        format.setDate("");
        format.setTime("");
        format.setDateTime("");
        format.setDateTimeWithMillis("");
        csv.setFormat(format);
        dbunit.setCsv(csv);
        config.setDbunit(dbunit);

        target.validateAndNormalize(config, PluginParameterValidator.GoalType.LOAD);

        assertEquals("yyyy-MM-dd", config.getDbunit().getCsv().getFormat().getDate());
        assertEquals("HH:mm:ss", config.getDbunit().getCsv().getFormat().getTime());
        assertEquals("yyyy-MM-dd HH:mm:ss", config.getDbunit().getCsv().getFormat().getDateTime());
        assertEquals("yyyy-MM-dd HH:mm:ss.SSS",
                config.getDbunit().getCsv().getFormat().getDateTimeWithMillis());
    }

    @Test
    void validateAndNormalize_正常ケース_preDirName指定済みを補完する_trim済み値が設定されること() {
        PluginConfig config = new PluginConfig();
        config.setServerIds(List.of("db1"));
        config.setDataPath("/tmp/data");
        PluginConfig.DbUnit dbunit = new PluginConfig.DbUnit();
        dbunit.setPreDirName("  setup  ");
        config.setDbunit(dbunit);

        target.validateAndNormalize(config, PluginParameterValidator.GoalType.LOAD);

        assertEquals("setup", config.getDbunit().getPreDirName());
    }

    @Test
    void validateAndNormalize_正常ケース_format指定済みを補完する_指定値がtrimされること() {
        PluginConfig config = new PluginConfig();
        config.setServerIds(List.of("db1"));
        config.setDataPath("/tmp/data");
        PluginConfig.DbUnit dbunit = new PluginConfig.DbUnit();
        PluginConfig.Csv csv = new PluginConfig.Csv();
        PluginConfig.Format format = new PluginConfig.Format();
        format.setDate(" yyyy/MM/dd ");
        format.setTime(" HH:mm ");
        format.setDateTime(" yyyy/MM/dd HH:mm ");
        format.setDateTimeWithMillis(" yyyy/MM/dd HH:mm:ss.SSS ");
        csv.setFormat(format);
        dbunit.setCsv(csv);
        config.setDbunit(dbunit);

        target.validateAndNormalize(config, PluginParameterValidator.GoalType.LOAD);

        assertEquals("yyyy/MM/dd", config.getDbunit().getCsv().getFormat().getDate());
        assertEquals("HH:mm", config.getDbunit().getCsv().getFormat().getTime());
        assertEquals("yyyy/MM/dd HH:mm", config.getDbunit().getCsv().getFormat().getDateTime());
        assertEquals("yyyy/MM/dd HH:mm:ss.SSS",
                config.getDbunit().getCsv().getFormat().getDateTimeWithMillis());
    }

    @Test
    void validateAndNormalize_正常ケース_runtimeConfig指定済みを補完する_指定値が維持されること() {
        PluginConfig config = new PluginConfig();
        config.setServerIds(List.of("db1"));
        config.setDataPath("/tmp/data");
        PluginConfig.DbUnit dbunit = new PluginConfig.DbUnit();
        PluginConfig.RuntimeConfig runtimeConfig = new PluginConfig.RuntimeConfig();
        runtimeConfig.setAllowEmptyFields(Boolean.FALSE);
        runtimeConfig.setBatchedStatements(Boolean.FALSE);
        runtimeConfig.setBatchSize(Integer.valueOf(50));
        dbunit.setConfig(runtimeConfig);
        config.setDbunit(dbunit);

        target.validateAndNormalize(config, PluginParameterValidator.GoalType.DUMP);

        assertEquals(Boolean.FALSE, config.getDbunit().getConfig().getAllowEmptyFields());
        assertEquals(Boolean.FALSE, config.getDbunit().getConfig().getBatchedStatements());
        assertEquals(50, config.getDbunit().getConfig().getBatchSize().intValue());
    }

    @Test
    void validateAndNormalize_正常ケース_filePatternsがnull_検証をスキップすること() {
        PluginConfig config = new PluginConfig();
        config.setServerIds(List.of("db1"));
        config.setDataPath("/tmp/data");
        config.setFilePatterns(null);

        target.validateAndNormalize(config, PluginParameterValidator.GoalType.LOAD);

        assertNull(config.getFilePatterns());
    }

    @Test
    void validateAndNormalize_正常ケース_有効filePatternsを検証する_例外が送出されないこと() {
        PluginConfig config = new PluginConfig();
        config.setServerIds(List.of("db1"));
        config.setDataPath("/tmp/data");
        PluginConfig.FilePattern pattern = new PluginConfig.FilePattern();
        pattern.setTableName("employee");
        pattern.setColumnName("photo");
        pattern.setFilename("employee_${ID}.bin");
        config.setFilePatterns(List.of(pattern));

        target.validateAndNormalize(config, PluginParameterValidator.GoalType.DUMP);

        assertEquals(1, config.getFilePatterns().size());
    }

    @Test
    void validateAndNormalize_異常ケース_configがnullを検証する_IllegalArgumentExceptionが送出されること() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> target.validateAndNormalize(null, PluginParameterValidator.GoalType.LOAD));

        assertTrue(ex.getMessage().contains("Plugin configuration is required"));
    }

    @Test
    void validateAndNormalize_異常ケース_serverIdsがnullを検証する_IllegalArgumentExceptionが送出されること() {
        PluginConfig config = new PluginConfig();
        config.setDataPath("/tmp/data");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> target.validateAndNormalize(config, PluginParameterValidator.GoalType.LOAD));

        assertTrue(ex.getMessage().contains("serverIds is required"));
    }

    @Test
    void validateAndNormalize_異常ケース_serverIds空リストを検証する_IllegalArgumentExceptionが送出されること() {
        PluginConfig config = new PluginConfig();
        config.setServerIds(List.of());
        config.setDataPath("/tmp/data");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> target.validateAndNormalize(config, PluginParameterValidator.GoalType.LOAD));

        assertTrue(ex.getMessage().contains("serverIds is required"));
    }

    @Test
    void validateAndNormalize_異常ケース_serverIds空白値を検証する_IllegalArgumentExceptionが送出されること() {
        PluginConfig config = new PluginConfig();
        config.setServerIds(Arrays.asList("db1", "  "));
        config.setDataPath("/tmp/data");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> target.validateAndNormalize(config, PluginParameterValidator.GoalType.LOAD));

        assertTrue(ex.getMessage().contains("serverIds must not contain blank"));
    }

    @Test
    void validateAndNormalize_異常ケース_serverIdsにnull要素を検証する_IllegalArgumentExceptionが送出されること() {
        PluginConfig config = new PluginConfig();
        config.setServerIds(Arrays.asList("db1", null));
        config.setDataPath("/tmp/data");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> target.validateAndNormalize(config, PluginParameterValidator.GoalType.LOAD));

        assertTrue(ex.getMessage().contains("serverIds must not contain blank"));
    }

    @Test
    void validateAndNormalize_異常ケース_dataPath空白を検証する_IllegalArgumentExceptionが送出されること() {
        PluginConfig config = new PluginConfig();
        config.setServerIds(List.of("db1"));
        config.setDataPath("   ");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> target.validateAndNormalize(config, PluginParameterValidator.GoalType.LOAD));

        assertTrue(ex.getMessage().contains("dataPath"));
    }

    @Test
    void validateAndNormalize_異常ケース_batchSize不正を検証する_IllegalArgumentExceptionが送出されること() {
        PluginConfig config = new PluginConfig();
        PluginConfig.DbUnit dbunit = new PluginConfig.DbUnit();
        PluginConfig.Csv csv = new PluginConfig.Csv();
        csv.setFormat(new PluginConfig.Format());
        dbunit.setCsv(csv);
        PluginConfig.RuntimeConfig runtimeConfig = new PluginConfig.RuntimeConfig();
        runtimeConfig.setBatchSize(Integer.valueOf(0));
        dbunit.setConfig(runtimeConfig);
        config.setDbunit(dbunit);
        config.setServerIds(List.of("db1"));
        config.setDataPath("/tmp/data");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> target.validateAndNormalize(config, PluginParameterValidator.GoalType.LOAD));

        assertTrue(ex.getMessage().contains("batchSize"));
    }

    @Test
    void validateAndNormalize_異常ケース_dataPath未指定を検証する_IllegalArgumentExceptionが送出されること() {
        PluginConfig config = new PluginConfig();
        config.setServerIds(List.of("db1"));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> target.validateAndNormalize(config, PluginParameterValidator.GoalType.LOAD));

        assertTrue(ex.getMessage().contains("dataPath"));
    }

    @Test
    void validateAndNormalize_異常ケース_filePatternsにnull要素を検証する_IllegalArgumentExceptionが送出されること() {
        PluginConfig config = new PluginConfig();
        config.setServerIds(List.of("db1"));
        config.setDataPath("/tmp/data");
        config.setFilePatterns(Arrays.asList((PluginConfig.FilePattern) null));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> target.validateAndNormalize(config, PluginParameterValidator.GoalType.LOAD));

        assertTrue(ex.getMessage().contains("filePatterns must not contain null"));
    }

    @Test
    void validateAndNormalize_異常ケース_tableName空白を検証する_IllegalArgumentExceptionが送出されること() {
        PluginConfig config = new PluginConfig();
        config.setServerIds(List.of("db1"));
        config.setDataPath("/tmp/data");
        PluginConfig.FilePattern pattern = new PluginConfig.FilePattern();
        pattern.setTableName("  ");
        config.setFilePatterns(List.of(pattern));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> target.validateAndNormalize(config, PluginParameterValidator.GoalType.LOAD));

        assertTrue(ex.getMessage().contains("filePatterns.tableName"));
    }

    @Test
    void validateAndNormalize_異常ケース_columnName空白を検証する_IllegalArgumentExceptionが送出されること() {
        PluginConfig config = new PluginConfig();
        config.setServerIds(List.of("db1"));
        config.setDataPath("/tmp/data");
        PluginConfig.FilePattern pattern = new PluginConfig.FilePattern();
        pattern.setTableName("employee");
        pattern.setColumnName(null);
        config.setFilePatterns(List.of(pattern));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> target.validateAndNormalize(config, PluginParameterValidator.GoalType.LOAD));

        assertTrue(ex.getMessage().contains("filePatterns.columnName"));
    }

    @Test
    void validateAndNormalize_異常ケース_filePatterns必須項目を検証する_IllegalArgumentExceptionが送出されること() {
        PluginConfig config = new PluginConfig();
        config.setServerIds(List.of("db1"));
        config.setDataPath("/tmp/data");
        PluginConfig.FilePattern pattern = new PluginConfig.FilePattern();
        pattern.setTableName("employee");
        pattern.setColumnName("photo");
        pattern.setFilename(" ");
        config.setFilePatterns(List.of(pattern));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> target.validateAndNormalize(config, PluginParameterValidator.GoalType.DUMP));

        assertTrue(ex.getMessage().contains("filePatterns.filename"));
    }

    @Test
    void validateAndNormalize_異常ケース_goalTypeがnullを検証する_IllegalArgumentExceptionが送出されること() {
        PluginConfig config = new PluginConfig();
        config.setServerIds(List.of("db1"));
        config.setDataPath("/tmp/data");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> target.validateAndNormalize(config, null));

        assertTrue(ex.getMessage().contains("goalType"));
    }
}
