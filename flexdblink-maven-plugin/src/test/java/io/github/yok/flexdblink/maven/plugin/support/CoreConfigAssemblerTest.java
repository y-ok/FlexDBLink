package io.github.yok.flexdblink.maven.plugin.support;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DbUnitConfigProperties;
import io.github.yok.flexdblink.db.DbUnitConfigFactory;
import io.github.yok.flexdblink.maven.plugin.config.CoreConfigBundle;
import io.github.yok.flexdblink.maven.plugin.config.PluginConfig;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class CoreConfigAssemblerTest {

    private final CoreConfigAssembler target = new CoreConfigAssembler();

    @Test
    void assemble_正常ケース_POM設定を変換する_CoreConfigBundleが返ること() throws Exception {
        PluginConfig pluginConfig = new PluginConfig();
        pluginConfig.setDataPath("/tmp/flexdblink-data");
        pluginConfig.setDbunit(dbunit());
        pluginConfig.setFilePatterns(List.of(
                filePattern("employee", "photo", "employee/${ID}_photo.bin")));

        ConnectionConfig connectionConfig = new ConnectionConfig();
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("DB1");
        entry.setUrl("jdbc:postgresql://localhost:5432/app");
        entry.setUser("app");
        connectionConfig.setConnections(List.of(entry));

        CoreConfigBundle bundle = target.assemble(pluginConfig, connectionConfig);

        assertEquals("/tmp/flexdblink-data", bundle.getPathsConfig().getDataPath());
        assertEquals("pre", bundle.getDbUnitConfig().getPreDirName());
        assertTrue(bundle.getFilePatternConfig()
                .getPattern("employee", "photo").isPresent());
        assertEquals("employee/${ID}_photo.bin",
                bundle.getFilePatternConfig()
                        .getPattern("employee", "photo").get());

        DbUnitConfigFactory factory =
                extractDbUnitConfigFactory(bundle.getDbDialectHandlerFactory());
        DbUnitConfigProperties props = extractDbUnitConfigProperties(factory);
        assertTrue(props.isAllowEmptyFields());
        assertTrue(props.isBatchedStatements());
        assertEquals(100, props.getBatchSize());
    }

    @Test
    void assemble_正常ケース_filePatternsがnullを変換する_空のfilePatternConfigが返ること() {
        PluginConfig pluginConfig = new PluginConfig();
        pluginConfig.setDataPath("/tmp/flexdblink-data");
        pluginConfig.setDbunit(dbunit());
        pluginConfig.setFilePatterns(null);

        ConnectionConfig connectionConfig = new ConnectionConfig();
        connectionConfig.setConnections(List.of());

        CoreConfigBundle bundle = target.assemble(pluginConfig, connectionConfig);

        assertFalse(bundle.getFilePatternConfig()
                .getPattern("any", "col").isPresent());
    }

    @Test
    void assemble_正常ケース_filePatterns空リストを変換する_空のfilePatternConfigが返ること() {
        PluginConfig pluginConfig = new PluginConfig();
        pluginConfig.setDataPath("/tmp/flexdblink-data");
        pluginConfig.setDbunit(dbunit());
        pluginConfig.setFilePatterns(Collections.emptyList());

        ConnectionConfig connectionConfig = new ConnectionConfig();
        connectionConfig.setConnections(List.of());

        CoreConfigBundle bundle = target.assemble(pluginConfig, connectionConfig);

        assertFalse(bundle.getFilePatternConfig()
                .getPattern("any", "col").isPresent());
    }

    @Test
    void assemble_正常ケース_同一テーブル異カラムを変換する_両方のパターンが返ること() {
        PluginConfig pluginConfig = new PluginConfig();
        pluginConfig.setDataPath("/tmp/flexdblink-data");
        pluginConfig.setDbunit(dbunit());
        pluginConfig.setFilePatterns(List.of(
                filePattern("employee", "photo", "photo.bin"),
                filePattern("employee", "resume", "resume.pdf")));

        ConnectionConfig connectionConfig = new ConnectionConfig();
        connectionConfig.setConnections(List.of());

        CoreConfigBundle bundle =
                target.assemble(pluginConfig, connectionConfig);

        assertTrue(bundle.getFilePatternConfig()
                .getPattern("employee", "photo").isPresent());
        assertTrue(bundle.getFilePatternConfig()
                .getPattern("employee", "resume").isPresent());
    }

    @Test
    void assemble_異常ケース_filePatterns重複を変換する_IllegalArgumentExceptionが送出されること() {
        PluginConfig pluginConfig = new PluginConfig();
        pluginConfig.setDataPath("/tmp/flexdblink-data");
        pluginConfig.setDbunit(dbunit());
        pluginConfig.setFilePatterns(List.of(
                filePattern("employee", "photo", "a.bin"),
                filePattern("EMPLOYEE", "PHOTO", "b.bin")));

        ConnectionConfig connectionConfig = new ConnectionConfig();
        connectionConfig.setConnections(List.of());

        IllegalArgumentException ex =
                org.junit.jupiter.api.Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> target.assemble(pluginConfig, connectionConfig));

        assertTrue(ex.getMessage().contains("Duplicate filePatterns"));
    }

    private PluginConfig.DbUnit dbunit() {
        PluginConfig.DbUnit dbunit = new PluginConfig.DbUnit();
        dbunit.setPreDirName("pre");
        PluginConfig.Csv csv = new PluginConfig.Csv();
        PluginConfig.Format format = new PluginConfig.Format();
        format.setDate("yyyy-MM-dd");
        format.setTime("HH:mm:ss");
        format.setDateTime("yyyy-MM-dd HH:mm:ss");
        format.setDateTimeWithMillis("yyyy-MM-dd HH:mm:ss.SSS");
        csv.setFormat(format);
        dbunit.setCsv(csv);
        PluginConfig.RuntimeConfig runtimeConfig = new PluginConfig.RuntimeConfig();
        runtimeConfig.setAllowEmptyFields(Boolean.TRUE);
        runtimeConfig.setBatchedStatements(Boolean.TRUE);
        runtimeConfig.setBatchSize(Integer.valueOf(100));
        dbunit.setConfig(runtimeConfig);
        return dbunit;
    }

    private PluginConfig.FilePattern filePattern(String tableName,
            String columnName, String filename) {
        PluginConfig.FilePattern filePattern = new PluginConfig.FilePattern();
        filePattern.setTableName(tableName);
        filePattern.setColumnName(columnName);
        filePattern.setFilename(filename);
        return filePattern;
    }

    private DbUnitConfigProperties extractDbUnitConfigProperties(
            DbUnitConfigFactory factory) throws Exception {
        Field field = DbUnitConfigFactory.class.getDeclaredField("props");
        field.setAccessible(true);
        return (DbUnitConfigProperties) field.get(factory);
    }

    private DbUnitConfigFactory extractDbUnitConfigFactory(
            io.github.yok.flexdblink.db.DbDialectHandlerFactory factory)
            throws Exception {
        Field field = io.github.yok.flexdblink.db.DbDialectHandlerFactory.class
                .getDeclaredField("configFactory");
        field.setAccessible(true);
        return (DbUnitConfigFactory) field.get(factory);
    }
}
