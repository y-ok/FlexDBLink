package io.github.yok.flexdblink.integration;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.oracle.OracleContainer;

/**
 * Regression tests for resource retention and repeated work in the transactional load path.
 */
@Testcontainers
class LoadDataPerformanceRegressionIT {

    @Container
    private static final OracleContainer ORACLE =
            new OracleContainer("gvenzl/oracle-free:slim-faststart");

    @TempDir
    Path directory;

    private final JdbcMetadataCache metadataCache = new JdbcMetadataCache();
    private List<String> tables;
    private DataLoader loader;
    private ConnectionConfig.Entry entry;

    @BeforeAll
    static void allowTemporaryLobObservation() throws Exception {
        // Grant observation access only inside this test's disposable database.
        try (Connection admin = DriverManager.getConnection(ORACLE.getJdbcUrl(), "sys as sysdba",
                ORACLE.getEnvMap().get("ORACLE_PASSWORD"));
                Statement statement = admin.createStatement()) {
            statement.execute("GRANT SELECT ON SYS.V_$TEMPORARY_LOBS TO " + ORACLE.getUsername());
        }
    }

    @BeforeEach
    void prepareFixture() throws Exception {
        String prefix = "PR_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8)
                .toUpperCase(Locale.ROOT);
        tables = List.of(prefix + "_A", prefix + "_B");
        Files.createDirectories(directory.resolve("files"));
        Files.writeString(directory.resolve("files/body.txt"), "initial");
        try (Connection jdbc = open(); Statement statement = jdbc.createStatement()) {
            for (String table : tables) {
                statement.execute("CREATE TABLE " + table + " (ID NUMBER PRIMARY KEY, BODY CLOB)");
                Files.writeString(directory.resolve(table + ".csv"),
                        "ID,BODY\n1,file:body.txt\n2,file:body.txt\n3,file:body.txt\n");
            }
        }
        PathsConfig paths = new PathsConfig();
        paths.setDataPath(directory.toString());
        CsvDateTimeFormatProperties formats = new CsvDateTimeFormatProperties();
        formats.setDate("yyyy-MM-dd");
        formats.setTime("HH:mm:ss");
        formats.setDateTime("yyyy-MM-dd HH:mm:ss");
        formats.setDateTimeWithMillis("yyyy-MM-dd HH:mm:ss.SSS");
        DbUnitConfig config = new DbUnitConfig();
        DumpConfig dump = new DumpConfig();
        DbDialectHandlerFactory factory = new DbDialectHandlerFactory(config, dump, paths,
                new DateTimeFormatUtil(formats), new DbUnitConfigFactory());
        loader = new DataLoader(paths, new ConnectionConfig(), factory, config, dump);
        entry = new ConnectionConfig.Entry();
        entry.setId("oracle");
        entry.setUrl(ORACLE.getJdbcUrl());
        entry.setUser(ORACLE.getUsername());
    }

    @Test
    void executeWithConnection_正常ケース_大きいCLOBを同じ接続で投入してロールバックする_一時LOB数が開始時と同じであること()
            throws Exception {
        String body = "x".repeat(1_000_000);
        Files.writeString(directory.resolve("files/body.txt"), body);
        try (Connection jdbc = open()) {
            jdbc.setAutoCommit(false);
            long baseline = temporaryLobCount(jdbc);
            List<Long> remaining = new ArrayList<>();
            for (int load = 0; load < 3; load++) {
                try {
                    loader.executeWithConnection(directory.toFile(), entry,
                            metadataCache.wrap(jdbc));
                    assertLoadedBody(jdbc, body);
                } finally {
                    jdbc.rollback();
                }
                assertEmptyTables(jdbc);
                remaining.add(temporaryLobCount(jdbc));
            }
            assertEquals(List.of(baseline, baseline, baseline), remaining,
                    "Temporary CLOBs must be released after each load, including rollback.");
        }
    }

    @Test
    void executeWithConnection_正常ケース_キャッシュを使って再ロードする_テーブルとスキーマの再照会がゼロであること() throws Exception {
        try (Connection jdbc = open()) {
            jdbc.setAutoCommit(false);
            DatabaseMetaData metadata =
                    mock(DatabaseMetaData.class, delegatesTo(jdbc.getMetaData()));
            Connection monitored = mock(Connection.class, delegatesTo(jdbc));
            when(monitored.getMetaData()).thenReturn(metadata);
            try {
                loader.executeWithConnection(directory.toFile(), entry,
                        metadataCache.wrap(monitored));
                assertLoadedBody(jdbc, "initial");
            } finally {
                jdbc.rollback();
            }
            clearInvocations(metadata);
            Files.writeString(directory.resolve("files/body.txt"), "updated");
            try {
                loader.executeWithConnection(directory.toFile(), entry,
                        metadataCache.wrap(monitored));
                assertLoadedBody(jdbc, "updated");
            } finally {
                jdbc.rollback();
            }
            assertAll("A repeated load must not query cached table or schema metadata",
                    () -> verify(metadata, never()).getTables(any(), any(), any(), any()),
                    () -> verify(metadata, never()).getSchemas(),
                    () -> verify(metadata, never()).getSchemas(any(), any()));
        }
    }

    @Test
    void executeWithConnection_正常ケース_現在のスキーマに投入する_冗長な再設定がなく設定SQLが五回未満であること() throws Exception {
        List<String> sessionSql = new ArrayList<>();
        try (Connection jdbc = open()) {
            jdbc.setAutoCommit(false);
            String originalSchema = jdbc.getSchema();
            Connection monitored = mock(Connection.class, delegatesTo(jdbc));
            doAnswer(invocation -> {
                Statement delegate = jdbc.createStatement();
                Statement statement = mock(Statement.class, delegatesTo(delegate));
                doAnswer(execution -> {
                    String sql = execution.getArgument(0);
                    if (sql.startsWith("ALTER SESSION")) {
                        sessionSql.add(sql);
                    }
                    return delegate.execute(sql);
                }).when(statement).execute(anyString());
                return statement;
            }).when(monitored).createStatement();
            try {
                loader.executeWithConnection(directory.toFile(), entry,
                        metadataCache.wrap(monitored));
                assertLoadedBody(jdbc, "initial");
                assertEquals(originalSchema, jdbc.getSchema());
            } finally {
                jdbc.rollback();
            }
        }
        assertAll("Session preparation must omit redundant schema assignment",
                () -> assertFalse(
                        sessionSql.stream().anyMatch(sql -> sql.contains("CURRENT_SCHEMA")),
                        "The current schema must not be assigned to itself: " + sessionSql),
                () -> assertTrue(sessionSql.size() < 5,
                        "Session preparation must execute fewer than five SQL statements: "
                                + sessionSql));
    }

    private Connection open() throws Exception {
        return DriverManager.getConnection(ORACLE.getJdbcUrl(), ORACLE.getUsername(),
                ORACLE.getPassword());
    }

    private long temporaryLobCount(Connection jdbc) throws Exception {
        try (Statement statement = jdbc.createStatement();
                ResultSet rows = statement.executeQuery(
                        "SELECT NVL(SUM(CACHE_LOBS + NOCACHE_LOBS), 0) FROM V$TEMPORARY_LOBS "
                                + "WHERE SID = SYS_CONTEXT('USERENV', 'SID')")) {
            assertTrue(rows.next());
            return rows.getLong(1);
        }
    }

    private void assertLoadedBody(Connection jdbc, String expected) throws Exception {
        try (Statement statement = jdbc.createStatement()) {
            for (String table : tables) {
                try (ResultSet rows = statement.executeQuery("SELECT BODY FROM " + table)) {
                    int count = 0;
                    while (rows.next()) {
                        assertEquals(expected, rows.getString(1));
                        count++;
                    }
                    assertEquals(3, count);
                }
            }
        }
    }

    private void assertEmptyTables(Connection jdbc) throws Exception {
        try (Statement statement = jdbc.createStatement()) {
            for (String table : tables) {
                try (ResultSet rows = statement.executeQuery("SELECT COUNT(*) FROM " + table)) {
                    assertTrue(rows.next());
                    assertEquals(0, rows.getInt(1));
                }
            }
        }
    }
}
