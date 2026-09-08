package io.github.yok.flexdblink.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.CsvDateTimeFormatProperties;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.core.DataLoader;
import io.github.yok.flexdblink.db.DbDialectHandlerFactory;
import io.github.yok.flexdblink.db.DbUnitConfigFactory;
import io.github.yok.flexdblink.db.JdbcMetadataCache;
import io.github.yok.flexdblink.util.DateTimeFormatUtil;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.oracle.OracleContainer;

@Testcontainers
class OracleMetadataCacheIT {
    @Container
    static final OracleContainer ORACLE = new OracleContainer("gvenzl/oracle-free:slim-faststart");

    @TempDir
    Path directory;

    @Test
    void executeWithConnection_正常ケース_接続を変えて十テーブルを再ロードする_列とキーの再利用と独立したデータであること()
            throws Exception {
        String schema = ORACLE.getUsername().toUpperCase();
        Files.createDirectories(directory.resolve("files"));
        Files.writeString(directory.resolve("files/body.txt"), "日本語😀".repeat(100));
        try (Connection jdbc = open(); Statement statement = jdbc.createStatement()) {
            for (int table = 0; table < 10; table++) {
                String sql = "CREATE TABLE MC_" + table + " (ID NUMBER PRIMARY KEY, BODY CLOB";
                if (table > 0) {
                    sql += ", CONSTRAINT MC_FK_" + table + " FOREIGN KEY (ID) REFERENCES MC_"
                            + (table - 1) + " (ID)";
                }
                statement.execute(sql + ")");
                StringBuilder csv = new StringBuilder("ID,BODY\n");
                for (int row = 0; row < 20; row++) {
                    csv.append(row).append(",file:body.txt\n");
                }
                Files.writeString(directory.resolve("MC_" + table + ".csv"), csv);
            }
        }
        PathsConfig paths = new PathsConfig();
        paths.setDataPath(directory.toString());
        DbUnitConfig config = new DbUnitConfig();
        DumpConfig dump = new DumpConfig();
        CsvDateTimeFormatProperties formats = new CsvDateTimeFormatProperties();
        formats.setDate("yyyy-MM-dd");
        formats.setTime("HH:mm:ss");
        formats.setDateTime("yyyy-MM-dd HH:mm:ss");
        formats.setDateTimeWithMillis("yyyy-MM-dd HH:mm:ss.SSS");
        DbDialectHandlerFactory factory = new DbDialectHandlerFactory(config, dump, paths,
                new DateTimeFormatUtil(formats), new DbUnitConfigFactory());
        DataLoader loader = new DataLoader(paths, new ConnectionConfig(), factory, config, dump);
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("oracle");
        entry.setUrl(ORACLE.getJdbcUrl());
        entry.setUser(ORACLE.getUsername());
        JdbcMetadataCache cache = new JdbcMetadataCache();
        for (int load = 0; load < 3; load++) {
            String expected = "Load " + load + " 日本語😀";
            Files.writeString(directory.resolve("files/body.txt"), expected);
            try (Connection jdbc = open(); Statement statement = jdbc.createStatement()) {
                jdbc.setAutoCommit(false);
                DatabaseMetaData metadata = mock(DatabaseMetaData.class,
                        delegatesTo(jdbc.getMetaData()));
                Connection monitored = mock(Connection.class, delegatesTo(jdbc));
                when(monitored.getMetaData()).thenReturn(metadata);
                Connection wrapped = cache.wrap(monitored);
                try (ResultSet keys = wrapped.getMetaData().getPrimaryKeys(null, schema, "MC_0")) {
                    assertTrue(keys.next());
                    assertEquals("ID", keys.getString("COLUMN_NAME"));
                }
                loader.executeWithConnection(directory.toFile(), entry, wrapped);
                for (int table = 0; table < 10; table++) {
                    try (ResultSet rows = statement.executeQuery("SELECT BODY FROM MC_" + table)) {
                        int count = 0;
                        while (rows.next()) {
                            assertEquals(expected, rows.getString(1));
                            count++;
                        }
                        assertEquals(20, count);
                    }
                }
                if (load > 0) {
                    verify(metadata, never()).getColumns(any(), any(), any(), any());
                    verify(metadata, never()).getPrimaryKeys(any(), any(), any());
                    verify(metadata, never()).getImportedKeys(any(), any(), any());
                }
                jdbc.rollback();
                try (ResultSet rows = statement.executeQuery("SELECT COUNT(*) FROM MC_9")) {
                    assertTrue(rows.next());
                    assertEquals(0, rows.getInt(1));
                }
            }
        }
        try (Connection jdbc = open(); Statement statement = jdbc.createStatement()) {
            statement.execute("ALTER TABLE MC_0 ADD EXTRA VARCHAR2(20)");
            cache.clear();
            try (ResultSet columns = cache.wrap(jdbc).getMetaData()
                    .getColumns(null, schema, "MC_0", "EXTRA")) {
                assertTrue(columns.next());
                assertEquals("EXTRA", columns.getString("COLUMN_NAME"));
            }
        }
    }

    private Connection open() throws Exception {
        return DriverManager.getConnection(ORACLE.getJdbcUrl(), ORACLE.getUsername(),
                ORACLE.getPassword());
    }
}
