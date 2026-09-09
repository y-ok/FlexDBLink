package io.github.yok.flexdblink.integration;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import io.github.yok.flexdblink.util.ErrorHandler;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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

    @ParameterizedTest
    @ValueSource(ints = {0, 32766, 32767, 1_000_000})
    void executeWithConnection_正常ケース_CLOBを同じ接続で投入してロールバックする_一時LOB数が開始時と同じであること(int length)
            throws Exception {
        String body = "x".repeat(length);
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
    void executeWithConnection_正常ケース_親子テーブルに複数LOBを投入する_UPDATEなしで全内容の一致とロールバックであること()
            throws Exception {
        List<String> bodies = Arrays.asList(null, "", "日本語😀", "あ".repeat(32766),
                "あ".repeat(32767), "日本語😀".repeat(300000));
        String backup = "複製😀".repeat(10000);
        byte[] binary = new byte[512 * 1024];
        Arrays.fill(binary, (byte) 0xa5);
        Files.writeString(directory.resolve("files/backup.txt"), backup);
        Files.write(directory.resolve("files/payload.bin"), binary);
        StringBuilder csv = new StringBuilder("ID,BODY,BACKUP,PAYLOAD,NOTE\n");
        for (int row = 0; row < bodies.size(); row++) {
            csv.append(row).append(',');
            String body = bodies.get(row);
            if (body == null) {
                csv.append("null");
            } else {
                String name = "body" + row + ".txt";
                Files.writeString(directory.resolve("files").resolve(name), body);
                csv.append("file:").append(name);
            }
            csv.append(",file:backup.txt,file:payload.bin,note").append(row).append('\n');
        }
        List<String> preparedSql = new ArrayList<>();
        try (Connection jdbc = open(); Statement statement = jdbc.createStatement()) {
            for (String table : tables) {
                statement.execute("ALTER TABLE " + table
                        + " ADD (BACKUP CLOB, PAYLOAD BLOB, NOTE VARCHAR2(40))");
                statement.execute("INSERT INTO " + table + " (ID, BODY) VALUES (99, 'original')");
                Files.writeString(directory.resolve(table + ".csv"), csv);
            }
            statement.execute("ALTER TABLE " + tables.get(1) + " ADD FOREIGN KEY (ID) REFERENCES "
                    + tables.get(0) + " (ID)");
            jdbc.setAutoCommit(false);
            Connection monitored = mock(Connection.class, delegatesTo(jdbc));
            doAnswer(invocation -> {
                String sql = invocation.getArgument(0);
                preparedSql.add(sql.toUpperCase(Locale.ROOT));
                return jdbc.prepareStatement(sql);
            }).when(monitored).prepareStatement(anyString());
            ErrorHandler.disableExitForCurrentThread();
            try {
                loader.executeWithConnection(directory.toFile(), entry,
                        metadataCache.wrap(monitored));
                for (String table : tables) {
                    try (ResultSet rows = statement.executeQuery(
                            "SELECT ID, BODY, BACKUP, PAYLOAD, NOTE FROM " + table + " ORDER BY ID")) {
                        for (int row = 0; row < bodies.size(); row++) {
                            assertTrue(rows.next());
                            assertEquals(row, rows.getInt(1));
                            assertEquals(bodies.get(row), rows.getString(2));
                            assertEquals(backup, rows.getString(3));
                            assertArrayEquals(binary, rows.getBytes(4));
                            assertEquals("note" + row, rows.getString(5));
                        }
                        assertFalse(rows.next());
                    }
                }
                assertEquals(2, preparedSql.stream().filter(sql -> sql.startsWith("INSERT")).count());
                assertEquals(0, preparedSql.stream().filter(sql -> sql.startsWith("UPDATE")).count());
            } finally {
                ErrorHandler.restoreExitForCurrentThread();
                jdbc.rollback();
            }
            for (String table : tables) {
                try (ResultSet rows = statement.executeQuery("SELECT ID, BODY FROM " + table)) {
                    assertTrue(rows.next());
                    assertEquals(99, rows.getInt(1));
                    assertEquals("original", rows.getString(2));
                    assertFalse(rows.next());
                }
            }
            assertEquals(0, temporaryLobCount(jdbc));
        }
    }

    @Test
    void executeWithConnection_異常ケース_CLOBのバッチ投入で主キーを重複させる_ロールバック後の一時LOB数が開始時と同じであること()
            throws Exception {
        Files.writeString(directory.resolve("files/body.txt"), "x".repeat(1_000_000));
        for (String table : tables) {
            Files.writeString(directory.resolve(table + ".csv"),
                    "ID,BODY\n1,file:body.txt\n1,file:body.txt\n");
        }
        try (Connection jdbc = open()) {
            jdbc.setAutoCommit(false);
            long baseline = temporaryLobCount(jdbc);
            ErrorHandler.disableExitForCurrentThread();
            try {
                IllegalStateException failure = assertThrows(IllegalStateException.class,
                        () -> loader.executeWithConnection(directory.toFile(), entry,
                                metadataCache.wrap(jdbc)));
                SQLException sql =
                        assertInstanceOf(SQLException.class, failure.getCause().getCause());
                assertEquals(1, sql.getErrorCode());
            } finally {
                ErrorHandler.restoreExitForCurrentThread();
                jdbc.rollback();
            }
            assertEmptyTables(jdbc);
            assertEquals(baseline, temporaryLobCount(jdbc));
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
    void executeWithConnection_正常ケース_現在のスキーマに投入する_設定SQLが一回で四設定が反映されスキーマが不変であること() throws Exception {
        List<String> sessionSql = new ArrayList<>();
        try (Connection jdbc = open()) {
            jdbc.setAutoCommit(false);
            String originalSchema = jdbc.getSchema();
            // Start with different values so unchanged defaults cannot satisfy the assertions.
            try (Statement statement = jdbc.createStatement()) {
                statement.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY/MM/DD' "
                        + "NLS_TIMESTAMP_FORMAT = 'YYYY/MM/DD HH24:MI:SS' "
                        + "NLS_NUMERIC_CHARACTERS = ',.' TIME_ZONE = '+00:00'");
            }
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
                assertSessionSettings(jdbc);
                assertEquals(originalSchema, jdbc.getSchema());
            } finally {
                jdbc.rollback();
            }
        }
        assertAll("Session preparation must execute once without redundant schema assignment",
                () -> assertFalse(
                        sessionSql.stream().anyMatch(sql -> sql.contains("CURRENT_SCHEMA")),
                        "The current schema must not be assigned to itself: " + sessionSql),
                () -> assertEquals(1, sessionSql.size(),
                        "Session preparation must execute exactly one SQL statement: "
                                + sessionSql));
    }

    private void assertSessionSettings(Connection jdbc) throws Exception {
        Map<String, String> settings = new HashMap<>();
        try (Statement statement = jdbc.createStatement();
                ResultSet rows = statement
                        .executeQuery("SELECT PARAMETER, VALUE FROM NLS_SESSION_PARAMETERS "
                                + "WHERE PARAMETER IN ('NLS_DATE_FORMAT', 'NLS_TIMESTAMP_FORMAT', "
                                + "'NLS_NUMERIC_CHARACTERS') "
                                + "UNION ALL SELECT 'TIME_ZONE', SESSIONTIMEZONE FROM DUAL")) {
            while (rows.next()) {
                settings.put(rows.getString(1), rows.getString(2));
            }
        }
        assertEquals(Map.of("NLS_DATE_FORMAT", "YYYY-MM-DD HH24:MI:SS", "NLS_TIMESTAMP_FORMAT",
                "YYYY-MM-DD HH24:MI:SS.FF", "NLS_NUMERIC_CHARACTERS", ".,", "TIME_ZONE", "+09:00"),
                settings);
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
