package io.github.yok.flexdblink.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.CsvDateTimeFormatProperties;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.db.DbDialectHandler;
import io.github.yok.flexdblink.db.DbDialectHandlerFactory;
import io.github.yok.flexdblink.db.DbUnitConfigFactory;
import io.github.yok.flexdblink.util.ErrorHandler;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.datatype.DataType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.AdditionalAnswers;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers
class TransactionalDataLoaderTest {
    @Container
    static final PostgreSQLContainer DATABASE = new PostgreSQLContainer("postgres:16-alpine");
    @TempDir
    Path directory;

    private Connection open() throws Exception {
        return DriverManager.getConnection(DATABASE.getJdbcUrl(), DATABASE.getUsername(),
                DATABASE.getPassword());
    }

    private DbDialectHandler dialect(Connection connection) throws Exception {
        DbDialectHandler dialect = mock(DbDialectHandler.class);
        when(dialect.resolveSchema(any())).thenReturn("public");
        when(dialect.createDbUnitConnection(any(), eq("public")))
                .thenReturn(new DatabaseConnection(connection, "public"));
        when(dialect.getLobColumns(any(org.dbunit.dataset.ITable.class))).thenReturn(new Column[0]);
        when(dialect.convertCsvValueToDbType(anyString(), anyString(), anyString()))
                .thenAnswer(call -> call.getArgument(2));
        return dialect;
    }

    private DataLoader loader(DbDialectHandlerFactory factory, DumpConfig config) {
        return new DataLoader(new PathsConfig(), new ConnectionConfig(), factory,
                new DbUnitConfig(), config);
    }

    private ConnectionConfig.Entry entry() {
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("test");
        return entry;
    }

    @Test
    void executeWithConnection_正常ケース_親子データを置換する_削除順が正しくロールバック可能であること() throws Exception {
        try (Connection jdbc = open(); Statement sql = jdbc.createStatement()) {
            sql.execute("CREATE TABLE PARENT(ID INT PRIMARY KEY)");
            sql.execute("CREATE TABLE CHILD(ID INT PRIMARY KEY, PID INT REFERENCES PARENT(ID))");
            sql.execute("INSERT INTO PARENT VALUES(9)");
            sql.execute("INSERT INTO CHILD VALUES(9,9)");
            jdbc.setAutoCommit(false);
            Files.writeString(directory.resolve("PARENT.csv"), "ID\n1\n");
            Files.writeString(directory.resolve("CHILD.csv"), "ID,PID\n2,1\n");
            Files.writeString(directory.resolve("table-ordering.txt"), "untouched");
            DbDialectHandler dialect = dialect(jdbc);
            DbDialectHandlerFactory factory = mock(DbDialectHandlerFactory.class);
            when(factory.create(any(), same(jdbc), anyList())).thenReturn(dialect);
            DataLoader loader = loader(factory, new DumpConfig());
            loader.executeWithConnection(directory.toFile(), entry(), jdbc);
            assertFalse(jdbc.isClosed());
            assertFalse(jdbc.getAutoCommit());
            try (ResultSet rows = sql.executeQuery("SELECT PID FROM CHILD")) {
                assertTrue(rows.next());
                assertEquals(1, rows.getInt(1));
            }
            verify(factory, never()).create(any(ConnectionConfig.Entry.class));
            verify(dialect, never()).countRows(any(), anyString());
            verify(dialect, never()).logTableDefinition(any(), any(), any(), any());
            assertEquals("untouched", Files.readString(directory.resolve("table-ordering.txt")));
            jdbc.rollback();
            try (ResultSet rows = sql.executeQuery("SELECT PID FROM CHILD")) {
                assertTrue(rows.next());
                assertEquals(9, rows.getInt(1));
            }
            Files.writeString(directory.resolve("PARENT.csv"), "ID\n3\n");
            Files.writeString(directory.resolve("CHILD.csv"), "ID,PID\n4,3\n");
            loader.executeWithConnection(directory.toFile(), entry(), jdbc);
            try (ResultSet rows = sql.executeQuery("SELECT PID FROM CHILD")) {
                assertTrue(rows.next());
                assertEquals(3, rows.getInt(1));
            }
            jdbc.rollback();
        }
    }

    @Test
    void executeWithConnection_正常ケース_LOB制約と非CSVを指定する_期待した値であること() throws Exception {
        try (Connection jdbc = open(); Statement sql = jdbc.createStatement()) {
            sql.execute("CREATE TABLE SAMPLE(ID INT PRIMARY KEY, BODY VARCHAR)");
            jdbc.setAutoCommit(false);
            Files.writeString(directory.resolve("SAMPLE.csv"), "ID,BODY\n1,file:body.txt\n");
            DbDialectHandler dialect = dialect(jdbc);
            when(dialect.getLobColumns(any(org.dbunit.dataset.ITable.class)))
                    .thenReturn(new Column[] {new Column("BODY", DataType.VARCHAR)});
            when(dialect.readLobFile(anyString(), anyString(), anyString(), any()))
                    .thenReturn("payload");
            DbDialectHandlerFactory factory = mock(DbDialectHandlerFactory.class);
            when(factory.create(any(), same(jdbc), anyList())).thenReturn(dialect);
            DataLoader loader = loader(factory, new DumpConfig());
            for (boolean required : List.of(false, true)) {
                when(dialect.hasNotNullLobColumn(any(), anyString(), anyString(), any()))
                        .thenReturn(required);
                loader.executeWithConnection(directory.toFile(), entry(), jdbc);
                try (ResultSet rows = sql.executeQuery("SELECT BODY FROM SAMPLE")) {
                    assertTrue(rows.next());
                    assertEquals("payload", rows.getString(1));
                }
                jdbc.rollback();
            }
            Path jsonDir = Files.createDirectory(directory.resolve("json"));
            Files.writeString(jsonDir.resolve("SAMPLE.json"), "[{\"ID\":2,\"BODY\":\"json\"}]");
            loader.executeWithConnection(jsonDir.toFile(), entry(), jdbc);
            try (ResultSet rows = sql.executeQuery("SELECT BODY FROM SAMPLE")) {
                assertTrue(rows.next());
                assertEquals("json", rows.getString(1));
            }
            jdbc.rollback();
        }
    }

    @Test
    void executeWithConnection_正常ケース_空または除外済みを指定する_接続の準備が不要であること() throws Exception {
        DbDialectHandlerFactory factory = mock(DbDialectHandlerFactory.class);
        Connection jdbc = mock(Connection.class);
        loader(factory, new DumpConfig()).executeWithConnection(directory.toFile(), entry(), jdbc);
        Files.writeString(directory.resolve("SKIP.csv"), "ID\n1\n");
        DumpConfig config = new DumpConfig();
        config.setExcludeTables(List.of("skip"));
        loader(factory, config).executeWithConnection(directory.toFile(), entry(), jdbc);
        verifyNoInteractions(factory, jdbc);
    }

    @Test
    void executeWithConnection_異常ケース_初期化が失敗する_例外が通知される結果であること() throws Exception {
        Files.writeString(directory.resolve("A.csv"), "ID\n1\n");
        DbDialectHandlerFactory factory = mock(DbDialectHandlerFactory.class);
        when(factory.create(any(), any(), anyList()))
                .thenThrow(new SQLException("metadata failed"));
        DataLoader loader = loader(factory, new DumpConfig());
        ErrorHandler.disableExitForCurrentThread();
        try {
            assertThrows(IllegalStateException.class, () -> loader
                    .executeWithConnection(directory.toFile(), entry(), mock(Connection.class)));
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }
        assertDoesNotThrow(() -> loader.executeWithConnection(directory.toFile(), entry(),
                mock(Connection.class)));
    }

    @Test
    void executeWithConnection_正常ケース_同一データを両経路でロードする_結果が同一であること() throws Exception {
        try (Connection jdbc = open(); Statement sql = jdbc.createStatement()) {
            for (int table = 0; table < 12; table++) {
                String name = "bench_" + table;
                sql.execute("CREATE TABLE " + name + "(id INT PRIMARY KEY, body VARCHAR)");
                StringBuilder csv = new StringBuilder("id,body\n");
                for (int row = 0; row < 20; row++) {
                    csv.append(row).append(",value").append(row).append('\n');
                }
                Files.writeString(directory.resolve(name + ".csv"), csv);
            }
            for (int table = 0; table < 40; table++) {
                sql.execute("CREATE TABLE unrelated_" + table + "(id INT PRIMARY KEY)");
            }
            PathsConfig paths = new PathsConfig();
            paths.setDataPath(directory.toString());
            DumpConfig dump = new DumpConfig();
            DbUnitConfig config = new DbUnitConfig();
            CsvDateTimeFormatProperties formats = new CsvDateTimeFormatProperties();
            formats.setDate("yyyy-MM-dd");
            formats.setTime("HH:mm:ss");
            formats.setDateTime("yyyy-MM-dd HH:mm:ss");
            formats.setDateTimeWithMillis("yyyy-MM-dd HH:mm:ss.SSS");
            DbDialectHandlerFactory factory = new DbDialectHandlerFactory(config, dump, paths,
                    new io.github.yok.flexdblink.util.DateTimeFormatUtil(formats),
                    new DbUnitConfigFactory());
            DataLoader original =
                    new DataLoader(paths, new ConnectionConfig(), factory::create, config, dump);
            DataLoader optimized =
                    new DataLoader(paths, new ConnectionConfig(), factory, config, dump);
            ConnectionConfig.Entry entry = entry();
            entry.setUrl(DATABASE.getJdbcUrl());
            entry.setUser(DATABASE.getUsername());
            entry.setPassword(DATABASE.getPassword());
            Connection external = mock(Connection.class, AdditionalAnswers.delegatesTo(jdbc));
            doNothing().when(external).close();
            jdbc.setAutoCommit(false);
            long[] oldNanos = new long[3];
            long[] newNanos = new long[3];
            ErrorHandler.disableExitForCurrentThread();
            try {
                for (int sample = -1; sample < 3; sample++) {
                    for (DataLoader selected : List.of(original, optimized)) {
                        long started = System.nanoTime();
                        selected.executeWithConnection(directory.toFile(), entry, external);
                        long elapsed = System.nanoTime() - started;
                        if (sample >= 0) {
                            if (selected == original) {
                                oldNanos[sample] = elapsed;
                            } else {
                                newNanos[sample] = elapsed;
                            }
                        }
                        for (int table = 0; table < 12; table++) {
                            try (ResultSet rows = sql.executeQuery(
                                    "SELECT count(*), min(body), max(body) FROM bench_" + table)) {
                                assertTrue(rows.next());
                                assertEquals(20, rows.getInt(1));
                                assertEquals("value0", rows.getString(2));
                                assertEquals("value9", rows.getString(3));
                            }
                        }
                        jdbc.rollback();
                    }
                }
            } finally {
                ErrorHandler.restoreExitForCurrentThread();
            }
            java.util.Arrays.sort(oldNanos);
            java.util.Arrays.sort(newNanos);
            System.out.printf(
                    "LOAD_BENCHMARK tables=12 rowsPerTable=20 unrelated=40 legacyMedianMs=%.3f optimizedMedianMs=%.3f%n",
                    oldNanos[1] / 1_000_000.0, newNanos[1] / 1_000_000.0);
        }
    }

    @Test
    void executeWithConnection_異常ケース_FK取得失敗と不正ファイルを指定する_正常テーブルはロード済みであること() throws Exception {
        try (Connection jdbc = open(); Statement sql = jdbc.createStatement()) {
            sql.execute("CREATE TABLE bad_input(id INT PRIMARY KEY)");
            sql.execute("CREATE TABLE good_input(id INT PRIMARY KEY)");
            jdbc.setAutoCommit(false);
            Files.writeString(directory.resolve("bad_input.json"), "invalid");
            Files.writeString(directory.resolve("good_input.csv"), "id\n1\n");
            Connection external = mock(Connection.class, AdditionalAnswers.delegatesTo(jdbc));
            DatabaseMetaData meta = mock(DatabaseMetaData.class);
            when(meta.getImportedKeys(any(), any(), any()))
                    .thenThrow(new SQLException("unavailable"));
            doReturn(meta).when(external).getMetaData();
            DbDialectHandler dialect = dialect(jdbc);
            DbDialectHandlerFactory factory = mock(DbDialectHandlerFactory.class);
            when(factory.create(any(), same(external), anyList())).thenReturn(dialect);
            Logger logger = (Logger) LoggerFactory.getLogger(TransactionalDataLoader.class);
            Level previous = logger.getLevel();
            logger.setLevel(Level.DEBUG);
            try {
                loader(factory, new DumpConfig()).executeWithConnection(directory.toFile(), entry(),
                        external);
                verify(dialect).logTableDefinition(external, "public", "good_input", "test");
                try (ResultSet rows = sql.executeQuery("SELECT id FROM good_input")) {
                    assertTrue(rows.next());
                    assertEquals(1, rows.getInt(1));
                }
            } finally {
                logger.setLevel(previous);
                jdbc.rollback();
            }
        }
    }
}
