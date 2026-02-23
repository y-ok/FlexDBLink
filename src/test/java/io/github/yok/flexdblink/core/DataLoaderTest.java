package io.github.yok.flexdblink.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.FilePatternConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.db.DbDialectHandler;
import io.github.yok.flexdblink.parser.DataLoaderFactory;
import io.github.yok.flexdblink.util.ErrorHandler;
import io.github.yok.flexdblink.util.TableDependencyResolver;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.statement.IBatchStatement;
import org.dbunit.database.statement.IStatementFactory;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.operation.DatabaseOperation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

class DataLoaderTest {

    @TempDir
    Path tempDir;

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private void withMockedDatabaseOperations(DataLoader loader, DatabaseOperation cleanInsert,
            DatabaseOperation update, DatabaseOperation insert, DatabaseOperation deleteAll,
            ThrowingRunnable runnable) throws Exception {
        DataLoader.OperationExecutor original = loader.getOperationExecutor();
        DataLoader.OperationExecutor executor = new DataLoader.OperationExecutor() {
            @Override
            public void cleanInsert(org.dbunit.database.IDatabaseConnection connection,
                    IDataSet dataSet) throws Exception {
                cleanInsert.execute(connection, dataSet);
            }

            @Override
            public void deleteAll(IDatabaseConnection connection, IDataSet dataSet)
                    throws Exception {
                deleteAll.execute(connection, dataSet);
            }

            @Override
            public void update(org.dbunit.database.IDatabaseConnection connection, IDataSet dataSet)
                    throws Exception {
                update.execute(connection, dataSet);
            }

            @Override
            public void insert(org.dbunit.database.IDatabaseConnection connection, IDataSet dataSet)
                    throws Exception {
                insert.execute(connection, dataSet);
            }
        };
        loader.setOperationExecutor(executor);
        try {
            runnable.run();
        } finally {
            loader.setOperationExecutor(original);
        }
    }

    @Test
    void operationExecutor_正常ケース_cleanInsertを実行する_デフォルト実装行が通過すること() throws Exception {
        DataLoader loader = new DataLoader(mock(PathsConfig.class), mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class),
                mock(DumpConfig.class));
        DataLoader.OperationExecutor executor = loader.getOperationExecutor();
        IDatabaseConnection connection = mockConnectionForNoOpOperation();

        assertDoesNotThrow(() -> executor.cleanInsert(connection, new DefaultDataSet()));
    }

    @Test
    void operationExecutor_正常ケース_updateを実行する_デフォルト実装行が通過すること() throws Exception {
        DataLoader loader = new DataLoader(mock(PathsConfig.class), mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class),
                mock(DumpConfig.class));
        DataLoader.OperationExecutor executor = loader.getOperationExecutor();
        IDatabaseConnection connection = mockConnectionForNoOpOperation();

        assertDoesNotThrow(() -> executor.update(connection, new DefaultDataSet()));
    }

    @Test
    void operationExecutor_正常ケース_insertを実行する_デフォルト実装行が通過すること() throws Exception {
        DataLoader loader = new DataLoader(mock(PathsConfig.class), mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class),
                mock(DumpConfig.class));
        DataLoader.OperationExecutor executor = loader.getOperationExecutor();
        IDatabaseConnection connection = mockConnectionForNoOpOperation();

        assertDoesNotThrow(() -> executor.insert(connection, new DefaultDataSet()));
    }

    private IDatabaseConnection mockConnectionForNoOpOperation() throws Exception {
        IDatabaseConnection connection = mock(IDatabaseConnection.class);
        DatabaseConfig config = new DatabaseConfig();
        IStatementFactory statementFactory = mock(IStatementFactory.class);
        IBatchStatement batchStatement = mock(IBatchStatement.class);
        config.setProperty(DatabaseConfig.PROPERTY_STATEMENT_FACTORY, statementFactory);

        when(connection.getConfig()).thenReturn(config);
        when(connection.createDataSet()).thenReturn(new DefaultDataSet());
        when(statementFactory.createBatchStatement(connection)).thenReturn(batchStatement);
        return connection;
    }

    @Test
    void createDbUnitConn_正常ケース_方言ハンドラ委譲して接続を返すこと() throws Exception {
        PathsConfig pathsConfig = mock(PathsConfig.class);
        ConnectionConfig connectionConfig = mock(ConnectionConfig.class);
        DbUnitConfig dbUnitConfig = mock(DbUnitConfig.class);
        DumpConfig dumpConfig = mock(DumpConfig.class);

        DataLoader loader = new DataLoader(pathsConfig, connectionConfig,
                e -> mock(DbDialectHandler.class), dbUnitConfig, dumpConfig);

        Connection jdbc = mock(Connection.class);
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);
        when(dialectHandler.resolveSchema(any())).thenReturn("APP");
        DatabaseConnection expected = mock(DatabaseConnection.class);
        when(dialectHandler.createDbUnitConnection(eq(jdbc), eq("APP"))).thenReturn(expected);

        Method method = DataLoader.class.getDeclaredMethod("createDbUnitConn", Connection.class,
                ConnectionConfig.Entry.class, DbDialectHandler.class);
        method.setAccessible(true);
        DatabaseConnection actual =
                (DatabaseConnection) method.invoke(loader, jdbc, entry, dialectHandler);

        verify(dialectHandler).prepareConnection(jdbc);
        verify(dialectHandler).createDbUnitConnection(jdbc, "APP");
        assertSame(expected, actual);
    }

    @Test
    void execute_異常ケース_DBUnit接続初期化失敗時にロールバックする_ロールバックが実行されること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());

        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        dbUnitConfig.setPreDirName("pre");

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:mock");
        entry.setUser("user");
        entry.setPassword("pass");

        ConnectionConfig connectionConfig = new ConnectionConfig();
        connectionConfig.setConnections(List.of(entry));

        DumpConfig dumpConfig = new DumpConfig();
        Connection jdbcConnection = mock(Connection.class);
        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);
        when(dialectHandler.resolveSchema(any())).thenReturn("APP");
        doThrow(new SQLException("prepare failure")).when(dialectHandler)
                .prepareConnection(jdbcConnection);

        DataLoader loader = new DataLoader(pathsConfig, connectionConfig, e -> dialectHandler,
                dbUnitConfig, dumpConfig);

        Path dbDir = tempDir.resolve("load").resolve("pre").resolve("db1");
        Files.createDirectories(dbDir);
        Files.writeString(dbDir.resolve("TBL.csv"), "ID\n1\n", StandardCharsets.UTF_8);
        Files.writeString(dbDir.resolve("table-ordering.txt"), "TBL\n", StandardCharsets.UTF_8);

        ErrorHandler.disableExitForCurrentThread();
        try (MockedStatic<DriverManager> driverManager = mockStatic(DriverManager.class)) {
            driverManager.when(() -> DriverManager.getConnection("jdbc:mock", "user", "pass"))
                    .thenReturn(jdbcConnection);

            Assertions.assertThrows(IllegalStateException.class, () -> loader.execute(null, null));
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }

        verify(jdbcConnection).setAutoCommit(false);
        verify(dialectHandler).prepareConnection(jdbcConnection);
        verify(jdbcConnection).rollback();
        verify(jdbcConnection, never()).commit();
    }

    @Test
    public void execute_異常ケース_シナリオモード_getColumnTypeNameがnull_SQLException分岐を通りロールバックされること()
            throws Exception {

        // --- ディレクトリ作成（pre は作らない → initial はスキップ、scenario だけ実行させる） ---
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());

        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        dbUnitConfig.setPreDirName("pre");

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:mock");
        entry.setUser("user");
        entry.setPassword("pass");

        ConnectionConfig connectionConfig = new ConnectionConfig();
        connectionConfig.setConnections(List.of(entry));

        DumpConfig dumpConfig = new DumpConfig(); // excludeTables は空のまま

        Path scenarioDir = tempDir.resolve("load").resolve("scenario").resolve("db1");
        Files.createDirectories(scenarioDir);
        Files.writeString(scenarioDir.resolve("T1.csv"), "C1\nA\n", StandardCharsets.UTF_8);
        Files.writeString(scenarioDir.resolve("table-ordering.txt"), "T1\n",
                StandardCharsets.UTF_8);

        // --- モック ---
        Connection jdbc = mock(Connection.class);

        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        IDataSet dbDataSet = mock(IDataSet.class);
        ITable originalDbTable = mock(ITable.class);
        when(dbConn.createDataSet()).thenReturn(dbDataSet);
        when(dbDataSet.getTable("T1")).thenReturn(originalDbTable);
        when(originalDbTable.getRowCount()).thenReturn(1);

        ITable base = mock(ITable.class);
        ITableMetaData meta = mock(ITableMetaData.class);
        when(meta.getTableName()).thenReturn("T1");
        when(meta.getColumns()).thenReturn(new Column[] {new Column("C1", DataType.VARCHAR)});
        when(base.getTableMetaData()).thenReturn(meta);
        when(base.getRowCount()).thenReturn(1);

        IDataSet dataSet = mock(IDataSet.class);
        when(dataSet.getTable("T1")).thenReturn(base);

        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);
        when(dialectHandler.resolveSchema(any())).thenReturn("APP");
        when(dialectHandler.createDbUnitConnection(eq(jdbc), eq("APP"))).thenReturn(dbConn);

        // ★ PKなし → detectDuplicates が rowsEqual（全列比較）へ入る
        when(dialectHandler.getPrimaryKeyColumns(eq(jdbc), eq("APP"), eq("T1")))
                .thenReturn(List.of());

        // ★ ここが目的：typeName == null → rowsEqual 内で SQLException を throw
        when(dialectHandler.getColumnTypeName(eq(jdbc), eq("APP"), eq("T1"), eq("C1")))
                .thenReturn(null);

        DataLoader loader = new DataLoader(pathsConfig, connectionConfig, e -> dialectHandler,
                dbUnitConfig, dumpConfig);

        ErrorHandler.disableExitForCurrentThread();
        try (MockedStatic<DriverManager> driverManager = mockStatic(DriverManager.class);
                MockedStatic<DataLoaderFactory> factory = mockStatic(DataLoaderFactory.class)) {

            driverManager.when(() -> DriverManager.getConnection("jdbc:mock", "user", "pass"))
                    .thenReturn(jdbc);

            factory.when(() -> DataLoaderFactory.create(scenarioDir.toFile(), "T1"))
                    .thenReturn(dataSet);

            // public execute() 経由で分岐を踏む
            assertThrows(IllegalStateException.class, () -> loader.execute("scenario", null));
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }

        // rollback される（commit はされない）
        verify(jdbc).setAutoCommit(false);
        verify(jdbc).rollback();
        verify(jdbc, never()).commit();

        // DBUnit 接続は finally で close
        verify(dbConn).close();
    }

    @Test
    void executeWithConnection_異常ケース_引数がnullである_IllegalArgumentExceptionが送出されること()
            throws Exception {
        DataLoader loader = new DataLoader(mock(PathsConfig.class), mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class),
                mock(DumpConfig.class));
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        assertThrows(IllegalArgumentException.class,
                () -> loader.executeWithConnection(null, entry, mock(Connection.class)));
        assertThrows(IllegalArgumentException.class,
                () -> loader.executeWithConnection(tempDir.toFile(), null, mock(Connection.class)));
        assertThrows(IllegalArgumentException.class,
                () -> loader.executeWithConnection(tempDir.toFile(), entry, null));
    }

    @Test
    void executeWithConnection_異常ケース_ディレクトリが存在しない_IllegalStateExceptionが送出されること() throws Exception {
        DataLoader loader = new DataLoader(mock(PathsConfig.class), mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class),
                mock(DumpConfig.class));
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        assertThrows(IllegalStateException.class,
                () -> loader.executeWithConnection(tempDir.resolve("missing").toFile(), entry,
                        mock(Connection.class)));
    }

    @Test
    void deployWithConnection_正常ケース_データセット解決失敗をスキップする_DBUnit接続がクローズされること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(List.of());

        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);
        when(dialectHandler.resolveSchema(any())).thenReturn("APP");
        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        when(dialectHandler.createDbUnitConnection(any(), eq("APP"))).thenReturn(dbConn);

        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class),
                e -> dialectHandler, mock(DbUnitConfig.class), dumpConfig);

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        Connection jdbc = mock(Connection.class);

        Path dir = tempDir.resolve("single");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("T1.csv"), "ID\n1\n", StandardCharsets.UTF_8);

        Method method = DataLoader.class.getDeclaredMethod("deployWithConnection", File.class,
                String.class, ConnectionConfig.Entry.class, Connection.class,
                DbDialectHandler.class, String.class);
        method.setAccessible(true);

        try (MockedStatic<DataLoaderFactory> factory = mockStatic(DataLoaderFactory.class)) {
            factory.when(() -> DataLoaderFactory.create(dir.toFile(), "T1"))
                    .thenThrow(new RuntimeException("skip"));

            method.invoke(loader, dir.toFile(), "db1", entry, jdbc, dialectHandler, "err");
        }

        verify(dialectHandler).createDbUnitConnection(jdbc, "APP");
        verify(dbConn).close();
    }

    @Test
    void deployWithConnection_正常ケース_全テーブルが除外される_データセット解決が呼ばれないこと() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(List.of("t1"));

        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);
        when(dialectHandler.resolveSchema(any())).thenReturn("APP");
        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        when(dialectHandler.createDbUnitConnection(any(), eq("APP"))).thenReturn(dbConn);

        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class),
                e -> dialectHandler, mock(DbUnitConfig.class), dumpConfig);

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        Connection jdbc = mock(Connection.class);

        Path dir = tempDir.resolve("excluded");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("T1.csv"), "ID\n1\n", StandardCharsets.UTF_8);

        Method method = DataLoader.class.getDeclaredMethod("deployWithConnection", File.class,
                String.class, ConnectionConfig.Entry.class, Connection.class,
                DbDialectHandler.class, String.class);
        method.setAccessible(true);

        try (MockedStatic<DataLoaderFactory> factory = mockStatic(DataLoaderFactory.class)) {
            method.invoke(loader, dir.toFile(), "db1", entry, jdbc, dialectHandler, "err");
            factory.verifyNoInteractions();
        }

        verify(dialectHandler, never()).createDbUnitConnection(any(), any());
        verify(dbConn, never()).close();
    }

    @Test
    void deploy_正常ケース_全テーブルが除外される_接続処理が呼ばれないこと() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(List.of("t1"));

        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class), dumpConfig);

        Path dir = tempDir.resolve("all_excluded");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("T1.csv"), "ID\n1\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("table-ordering.txt"), "T1\n", StandardCharsets.UTF_8);

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:dummy");
        entry.setUser("u");
        entry.setPassword("p");

        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);
        when(dialectHandler.resolveSchema(any())).thenReturn("APP");
        Method method = DataLoader.class.getDeclaredMethod("deploy", File.class, String.class,
                boolean.class, ConnectionConfig.Entry.class, DbDialectHandler.class, String.class);
        method.setAccessible(true);

        try (MockedStatic<DriverManager> driverManager = mockStatic(DriverManager.class)) {
            method.invoke(loader, dir.toFile(), "db1", true, entry, dialectHandler, "err");
            driverManager.verifyNoInteractions();
        }

        verify(dialectHandler, never()).prepareConnection(any());
    }

    @Test
    void deploy_正常ケース_データセットファイルが存在しない_接続処理が呼ばれないこと() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(List.of());

        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class), dumpConfig);

        Path dir = tempDir.resolve("no_dataset_files");
        Files.createDirectories(dir);
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:dummy");
        entry.setUser("u");
        entry.setPassword("p");

        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);
        when(dialectHandler.resolveSchema(any())).thenReturn("APP");
        Method method = DataLoader.class.getDeclaredMethod("deploy", File.class, String.class,
                boolean.class, ConnectionConfig.Entry.class, DbDialectHandler.class, String.class);
        method.setAccessible(true);

        try (MockedStatic<DriverManager> driverManager = mockStatic(DriverManager.class)) {
            method.invoke(loader, dir.toFile(), "db1", true, entry, dialectHandler, "err");
            driverManager.verifyNoInteractions();
        }

        verify(dialectHandler, never()).prepareConnection(any());
    }

    @Test
    void execute_正常ケース_targetDB指定で一部のみ処理する_対象外はスキップされること() {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());

        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        dbUnitConfig.setPreDirName("pre");

        ConnectionConfig.Entry db1 = new ConnectionConfig.Entry();
        db1.setId("db1");
        ConnectionConfig.Entry db2 = new ConnectionConfig.Entry();
        db2.setId("db2");
        ConnectionConfig connectionConfig = new ConnectionConfig();
        connectionConfig.setConnections(List.of(db1, db2));

        DataLoader loader = new DataLoader(pathsConfig, connectionConfig,
                e -> mock(DbDialectHandler.class), dbUnitConfig, new DumpConfig());

        // DBごとのディレクトリは作成しない（deploy内で早期スキップ分岐へ）
        loader.execute("scenario1", List.of("db2"));
        assertTrue(true);
    }

    @Test
    void deploy_正常ケース_orderingが空行のみである_テーブルなしでスキップされること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(List.of());

        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class), dumpConfig);

        Path dir = tempDir.resolve("empty_ordering");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("table-ordering.txt"), "\n  \n", StandardCharsets.UTF_8);

        Method method = DataLoader.class.getDeclaredMethod("deploy", File.class, String.class,
                boolean.class, ConnectionConfig.Entry.class, DbDialectHandler.class, String.class);
        method.setAccessible(true);

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:dummy");
        entry.setUser("u");
        entry.setPassword("p");
        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);
        when(dialectHandler.resolveSchema(any())).thenReturn("APP");

        try (MockedStatic<DriverManager> driverManager = mockStatic(DriverManager.class)) {
            method.invoke(loader, dir.toFile(), "db1", true, entry, dialectHandler, "err");
            driverManager.verifyNoInteractions();
        }
    }

    @Test
    void deploy_異常ケース_ドライバクラスが不正である_ErrorHandler経由でIllegalStateExceptionが送出されること()
            throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(List.of());

        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class), dumpConfig);

        Path dir = tempDir.resolve("driver_error");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("TBL.csv"), "ID\n1\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("table-ordering.txt"), "TBL\n", StandardCharsets.UTF_8);

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setDriverClass("no.such.Driver");
        entry.setUrl("jdbc:dummy");
        entry.setUser("u");
        entry.setPassword("p");

        Method method = DataLoader.class.getDeclaredMethod("deploy", File.class, String.class,
                boolean.class, ConnectionConfig.Entry.class, DbDialectHandler.class, String.class);
        method.setAccessible(true);

        ErrorHandler.disableExitForCurrentThread();
        try {
            InvocationTargetException ex = assertThrows(InvocationTargetException.class,
                    () -> method.invoke(loader, dir.toFile(), "db1", true, entry,
                            mock(DbDialectHandler.class), "fatal"));
            assertTrue(ex.getCause() instanceof IllegalStateException);
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }
    }

    @Test
    void logSummary_正常ケース_サマリ情報が存在する_例外なく完了すること() throws Exception {
        DataLoader loader = new DataLoader(mock(PathsConfig.class), mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class),
                mock(DumpConfig.class));

        java.lang.reflect.Field field = DataLoader.class.getDeclaredField("insertSummary");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Map<String, Integer>> summary =
                (Map<String, Map<String, Integer>>) field.get(loader);
        Map<String, Integer> tableMap = new LinkedHashMap<>();
        tableMap.put("T1", 10);
        tableMap.put("LONG_TABLE", 200);
        summary.put("DB1", tableMap);

        Method method = DataLoader.class.getDeclaredMethod("logSummary");
        method.setAccessible(true);
        method.invoke(loader);

        assertEquals(1, summary.size());
    }

    @Test
    void deploy_正常ケース_初期ロードLOBなしを実行する_コミットされサマリが更新されること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(java.util.Arrays.asList("t2", null));
        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class), dumpConfig);

        Path dir = tempDir.resolve("deploy_initial_success");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("table-ordering.txt"), "T1\n\nT2\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("T1.csv"), "ID\n1\n", StandardCharsets.UTF_8);

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:dummy");
        entry.setUser("u");
        entry.setPassword("p");

        Connection jdbc = mock(Connection.class);
        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);
        when(dialectHandler.resolveSchema(any())).thenReturn("APP");
        when(dialectHandler.createDbUnitConnection(jdbc, "APP")).thenReturn(dbConn);
        when(dialectHandler.getLobColumns(dir, "T1")).thenReturn(new Column[0]);
        when(dialectHandler.countRows(jdbc, "T1")).thenReturn(3);

        SimpleDataSetWrapper dataSetWrapper = buildSimpleDataSet("T1", 1, "ID", "1");

        Method method = DataLoader.class.getDeclaredMethod("deploy", File.class, String.class,
                boolean.class, ConnectionConfig.Entry.class, DbDialectHandler.class, String.class);
        method.setAccessible(true);

        DatabaseOperation clean = mock(DatabaseOperation.class);
        DatabaseOperation update = mock(DatabaseOperation.class);
        DatabaseOperation insert = mock(DatabaseOperation.class);
        DatabaseOperation deleteAll = mock(DatabaseOperation.class);
        withMockedDatabaseOperations(loader, clean, update, insert, deleteAll, () -> {
            try (MockedStatic<DriverManager> driverManager = mockStatic(DriverManager.class);
                    MockedStatic<DataLoaderFactory> factory = mockStatic(DataLoaderFactory.class)) {
                driverManager.when(() -> DriverManager.getConnection("jdbc:dummy", "u", "p"))
                        .thenReturn(jdbc);
                factory.when(() -> DataLoaderFactory.create(dir.toFile(), "T1"))
                        .thenReturn(dataSetWrapper.dataSet);

                method.invoke(loader, dir.toFile(), "db1", true, entry, dialectHandler, "fatal");
            }
        });

        verify(clean, times(1)).execute(eq(dbConn), any());
        verify(update, never()).execute(any(), any());
        verify(insert, never()).execute(any(), any());
        verify(jdbc).commit();
        verify(jdbc, never()).rollback();
        verify(dbConn).close();
    }

    @Test
    void deploy_異常ケース_ロールバック失敗が発生する_ErrorHandler経由で例外が再スローされること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(List.of());
        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class), dumpConfig);

        Path dir = tempDir.resolve("deploy_rollback_fail");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("table-ordering.txt"), "T1\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("T1.csv"), "ID\n1\n", StandardCharsets.UTF_8);

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:dummy3");
        entry.setUser("u");
        entry.setPassword("p");

        Connection jdbc = mock(Connection.class);
        doThrow(new SQLException("rb fail")).when(jdbc).rollback();
        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);
        when(dialectHandler.resolveSchema(any())).thenReturn("APP");
        when(dialectHandler.createDbUnitConnection(jdbc, "APP")).thenReturn(dbConn);
        when(dialectHandler.getLobColumns(dir, "T1")).thenReturn(new Column[0]);

        SimpleDataSetWrapper dataSetWrapper = buildSimpleDataSet("T1", 1, "ID", "1");

        Method method = DataLoader.class.getDeclaredMethod("deploy", File.class, String.class,
                boolean.class, ConnectionConfig.Entry.class, DbDialectHandler.class, String.class);
        method.setAccessible(true);

        DatabaseOperation clean = mock(DatabaseOperation.class);
        doThrow(new RuntimeException("boom")).when(clean).execute(any(), any());
        DatabaseOperation update = mock(DatabaseOperation.class);
        DatabaseOperation insert = mock(DatabaseOperation.class);
        DatabaseOperation deleteAll = mock(DatabaseOperation.class);

        ErrorHandler.disableExitForCurrentThread();
        try {
            withMockedDatabaseOperations(loader, clean, update, insert, deleteAll, () -> {
                try (MockedStatic<DriverManager> driverManager = mockStatic(DriverManager.class);
                        MockedStatic<DataLoaderFactory> factory =
                                mockStatic(DataLoaderFactory.class)) {
                    driverManager.when(() -> DriverManager.getConnection("jdbc:dummy3", "u", "p"))
                            .thenReturn(jdbc);
                    factory.when(() -> DataLoaderFactory.create(dir.toFile(), "T1"))
                            .thenReturn(dataSetWrapper.dataSet);

                    InvocationTargetException ex = assertThrows(InvocationTargetException.class,
                            () -> method.invoke(loader, dir.toFile(), "db1", true, entry,
                                    dialectHandler, "fatal"));
                    assertTrue(ex.getCause() instanceof IllegalStateException);
                }
            });
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }

        verify(jdbc).rollback();
        verify(dbConn).close();
    }

    @Test
    void deployWithConnection_正常ケース_初期ロード成功分岐を実行する_サマリ更新されクローズされること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(java.util.Arrays.asList("t2", null));

        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);
        when(dialectHandler.resolveSchema(any())).thenReturn("APP");
        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        when(dialectHandler.createDbUnitConnection(any(), eq("APP"))).thenReturn(dbConn);
        when(dialectHandler.getLobColumns(any(), eq("T1"))).thenReturn(new Column[0]);
        when(dialectHandler.countRows(any(), eq("T1"))).thenReturn(2);

        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class),
                e -> dialectHandler, mock(DbUnitConfig.class), dumpConfig);
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        Connection jdbc = mock(Connection.class);

        Path dir = tempDir.resolve("with_conn_success");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("T1.csv"), "ID\n1\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("T2.csv"), "ID\n2\n", StandardCharsets.UTF_8);

        SimpleDataSetWrapper dataSetWrapper = buildSimpleDataSet("T1", 1, "ID", "1");

        Method method = DataLoader.class.getDeclaredMethod("deployWithConnection", File.class,
                String.class, ConnectionConfig.Entry.class, Connection.class,
                DbDialectHandler.class, String.class);
        method.setAccessible(true);

        DatabaseOperation clean = mock(DatabaseOperation.class);
        DatabaseOperation update = mock(DatabaseOperation.class);
        DatabaseOperation insert = mock(DatabaseOperation.class);
        DatabaseOperation deleteAll = mock(DatabaseOperation.class);
        withMockedDatabaseOperations(loader, clean, update, insert, deleteAll, () -> {
            try (MockedStatic<DataLoaderFactory> factory = mockStatic(DataLoaderFactory.class)) {
                factory.when(() -> DataLoaderFactory.create(dir.toFile(), "T1"))
                        .thenReturn(dataSetWrapper.dataSet);
                method.invoke(loader, dir.toFile(), "db1", entry, jdbc, dialectHandler, "err");
            }
        });

        verify(dialectHandler).logTableDefinition(jdbc, "APP", "T1", "db1");
        verify(clean, times(1)).execute(eq(dbConn), any());
        verify(dbConn).close();
    }

    @Test
    void deploy_正常ケース_データセットファイルなしでorderingが空白のみである_テーブルなしでスキップされること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class), new DumpConfig());

        Path dir = tempDir.resolve("ordering_blank_only");
        Files.createDirectories(dir);
        // No dataset files: ensureTableOrdering deletes the blank ordering and does not recreate it
        Files.writeString(dir.resolve("table-ordering.txt"), "\n \n", StandardCharsets.UTF_8);

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:no-call");
        entry.setUser("u");
        entry.setPassword("p");

        Method method = DataLoader.class.getDeclaredMethod("deploy", File.class, String.class,
                boolean.class, ConnectionConfig.Entry.class, DbDialectHandler.class, String.class);
        method.setAccessible(true);

        try (MockedStatic<DriverManager> driverManager = mockStatic(DriverManager.class)) {
            method.invoke(loader, dir.toFile(), "db1", true, entry, mock(DbDialectHandler.class),
                    "fatal");
            driverManager.verifyNoInteractions();
        }
    }

    @Test
    void deploy_正常ケース_シナリオモード重複ありを実行する_削除後にINSERTが実行されること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class), new DumpConfig());

        Path dir = tempDir.resolve("deploy_scenario");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("table-ordering.txt"), "T1\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("T1.csv"), "C1\nA\n", StandardCharsets.UTF_8);

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:scenario");
        entry.setUser("u");
        entry.setPassword("p");

        Connection jdbc = mock(Connection.class);
        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        IDataSet dbDataSet = mock(IDataSet.class);
        when(dbConn.createDataSet()).thenReturn(dbDataSet);

        ITable original = mock(ITable.class);
        when(dbDataSet.getTable("T1")).thenReturn(original);
        when(original.getRowCount()).thenReturn(1);
        when(original.getValue(0, "C1")).thenReturn("A");

        DbDialectHandler dialect = mock(DbDialectHandler.class);
        when(dialect.resolveSchema(any())).thenReturn("APP");
        when(dialect.createDbUnitConnection(jdbc, "APP")).thenReturn(dbConn);
        when(dialect.getPrimaryKeyColumns(jdbc, "APP", "T1")).thenReturn(List.of());
        when(dialect.getColumnTypeName(jdbc, "APP", "T1", "C1")).thenReturn("VARCHAR2");
        when(dialect.formatDbValueForCsv("C1", "A")).thenReturn("A");
        when(dialect.countRows(jdbc, "T1")).thenReturn(1);
        when(dialect.quoteIdentifier("APP")).thenReturn("\"APP\"");
        when(dialect.quoteIdentifier("T1")).thenReturn("\"T1\"");
        when(dialect.quoteIdentifier("C1")).thenReturn("\"C1\"");

        PreparedStatement ps = mock(PreparedStatement.class);
        when(jdbc.prepareStatement("DELETE FROM \"APP\".\"T1\" WHERE \"C1\" = ?")).thenReturn(ps);
        when(ps.executeUpdate()).thenReturn(1);

        SimpleDataSetWrapper dataSetWrapper = buildSimpleDataSet("T1", 1, "C1", "A");

        Method method = DataLoader.class.getDeclaredMethod("deploy", File.class, String.class,
                boolean.class, ConnectionConfig.Entry.class, DbDialectHandler.class, String.class);
        method.setAccessible(true);

        DatabaseOperation clean = mock(DatabaseOperation.class);
        DatabaseOperation update = mock(DatabaseOperation.class);
        DatabaseOperation insert = mock(DatabaseOperation.class);
        DatabaseOperation deleteAll = mock(DatabaseOperation.class);
        withMockedDatabaseOperations(loader, clean, update, insert, deleteAll, () -> {
            try (MockedStatic<DriverManager> driverManager = mockStatic(DriverManager.class);
                    MockedStatic<DataLoaderFactory> factory = mockStatic(DataLoaderFactory.class)) {
                driverManager.when(() -> DriverManager.getConnection("jdbc:scenario", "u", "p"))
                        .thenReturn(jdbc);
                factory.when(() -> DataLoaderFactory.create(dir.toFile(), "T1"))
                        .thenReturn(dataSetWrapper.dataSet);
                method.invoke(loader, dir.toFile(), "db1", false, entry, dialect, "fatal");
            }
        });
        verify(insert, times(1)).execute(eq(dbConn), any());
    }

    @Test
    void executeWithConnection_正常ケース_有効ディレクトリを指定する_開始と終了まで実行されること() throws Exception {
        PathsConfig paths = new PathsConfig();
        paths.setDataPath(tempDir.toString());
        DbDialectHandler dialect = mock(DbDialectHandler.class);
        when(dialect.resolveSchema(any())).thenReturn("APP");
        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        when(dialect.createDbUnitConnection(any(), eq("APP"))).thenReturn(dbConn);
        DataLoader loader = new DataLoader(paths, mock(ConnectionConfig.class), e -> dialect,
                mock(DbUnitConfig.class), new DumpConfig());
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        Path dir = tempDir.resolve("exec_with_conn_ok");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("T1.csv"), "ID\n1\n", StandardCharsets.UTF_8);
        try (MockedStatic<DataLoaderFactory> factory = mockStatic(DataLoaderFactory.class)) {
            factory.when(() -> DataLoaderFactory.create(dir.toFile(), "T1"))
                    .thenThrow(new RuntimeException("skip"));
            loader.executeWithConnection(dir.toFile(), entry, mock(Connection.class));
        }
        verify(dialect).createDbUnitConnection(any(), eq("APP"));
        verify(dbConn).close();
    }

    @Test
    void deployWithConnection_異常ケース_無効ディレクトリを直接指定する_IllegalStateExceptionが送出されること()
            throws Exception {
        DataLoader loader = new DataLoader(mock(PathsConfig.class), mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class),
                mock(DumpConfig.class));
        Method method = DataLoader.class.getDeclaredMethod("deployWithConnection", File.class,
                String.class, ConnectionConfig.Entry.class, Connection.class,
                DbDialectHandler.class, String.class);
        method.setAccessible(true);
        InvocationTargetException ex = assertThrows(InvocationTargetException.class,
                () -> method.invoke(loader, tempDir.resolve("missing").toFile(), "db1",
                        new ConnectionConfig.Entry(), mock(Connection.class),
                        mock(DbDialectHandler.class), "fatal"));
        assertTrue(ex.getCause() instanceof IllegalStateException);
    }

    @Test
    void deployWithConnection_異常ケース_スキーマ解決で例外が発生しクローズ失敗する_ErrorHandler経由で例外が送出されること()
            throws Exception {
        PathsConfig paths = new PathsConfig();
        paths.setDataPath(tempDir.toString());
        DbDialectHandler dialect = mock(DbDialectHandler.class);
        when(dialect.resolveSchema(any())).thenReturn("APP");
        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        when(dialect.createDbUnitConnection(any(), anyString())).thenReturn(dbConn);
        doThrow(new RuntimeException("close")).when(dbConn).close();
        when(dialect.resolveSchema(any())).thenThrow(new RuntimeException("schema"));
        DataLoader loader = new DataLoader(paths, mock(ConnectionConfig.class), e -> dialect,
                mock(DbUnitConfig.class), new DumpConfig());
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        Path dir = tempDir.resolve("with_conn_err");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("T1.csv"), "ID\n1\n", StandardCharsets.UTF_8);

        Method method = DataLoader.class.getDeclaredMethod("deployWithConnection", File.class,
                String.class, ConnectionConfig.Entry.class, Connection.class,
                DbDialectHandler.class, String.class);
        method.setAccessible(true);

        ErrorHandler.disableExitForCurrentThread();
        try {
            InvocationTargetException ex =
                    assertThrows(InvocationTargetException.class, () -> method.invoke(loader,
                            dir.toFile(), "db1", entry, mock(Connection.class), dialect, "fatal"));
            assertTrue(ex.getCause() instanceof IllegalStateException);
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }
    }

    @Test
    void deploy_正常ケース_初期ロードLOBありNULL許可を実行する_CLEAN_INSERTとUPDATEが実行されること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class), new DumpConfig());

        Path dir = tempDir.resolve("deploy_initial_lob_nullable");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("table-ordering.txt"), "T1\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("T1.csv"), "ID\n1\n", StandardCharsets.UTF_8);

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:lob");
        entry.setUser("u");
        entry.setPassword("p");

        Connection jdbc = mock(Connection.class);
        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        DbDialectHandler dialect = mock(DbDialectHandler.class);
        when(dialect.resolveSchema(any())).thenReturn("APP");
        when(dialect.createDbUnitConnection(jdbc, "APP")).thenReturn(dbConn);
        when(dialect.getLobColumns(any(), eq("T1")))
                .thenReturn(new Column[] {new Column("LOB_COL", DataType.VARCHAR)});
        when(dialect.hasNotNullLobColumn(any(), eq("APP"), eq("T1"), any())).thenReturn(false);
        when(dialect.countRows(jdbc, "T1")).thenReturn(1);

        SimpleDataSetWrapper dataSetWrapper = buildSimpleDataSet("T1", 1, "ID", "1");
        Method method = DataLoader.class.getDeclaredMethod("deploy", File.class, String.class,
                boolean.class, ConnectionConfig.Entry.class, DbDialectHandler.class, String.class);
        method.setAccessible(true);

        DatabaseOperation clean = mock(DatabaseOperation.class);
        DatabaseOperation update = mock(DatabaseOperation.class);
        DatabaseOperation insert = mock(DatabaseOperation.class);
        DatabaseOperation deleteAll = mock(DatabaseOperation.class);
        withMockedDatabaseOperations(loader, clean, update, insert, deleteAll, () -> {
            try (MockedStatic<DriverManager> driverManager = mockStatic(DriverManager.class);
                    MockedStatic<DataLoaderFactory> factory = mockStatic(DataLoaderFactory.class)) {
                driverManager.when(() -> DriverManager.getConnection("jdbc:lob", "u", "p"))
                        .thenReturn(jdbc);
                factory.when(() -> DataLoaderFactory.create(dir.toFile(), "T1"))
                        .thenReturn(dataSetWrapper.dataSet);
                method.invoke(loader, dir.toFile(), "db1", true, entry, dialect, "fatal");
            }
        });
        verify(clean, times(1)).execute(eq(dbConn), any());
        verify(update, times(1)).execute(eq(dbConn), any());
    }

    @Test
    void deploy_正常ケース_シナリオモードPK重複ありを実行する_重複削除が実行されること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class), new DumpConfig());

        Path dir = tempDir.resolve("deploy_scenario_pk");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("table-ordering.txt"), "T1\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("T1.csv"), "ID\n1\n", StandardCharsets.UTF_8);

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:scpk");
        entry.setUser("u");
        entry.setPassword("p");

        Connection jdbc = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        when(jdbc.prepareStatement("DELETE FROM \"APP\".\"T1\" WHERE \"ID\" = ?")).thenReturn(ps);
        when(ps.executeBatch()).thenReturn(new int[] {1});

        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        IDataSet dbDataSet = mock(IDataSet.class);
        ITable original = mock(ITable.class);
        when(dbConn.createDataSet()).thenReturn(dbDataSet);
        when(dbDataSet.getTable("T1")).thenReturn(original);
        when(original.getRowCount()).thenReturn(1);
        when(original.getValue(0, "ID")).thenReturn("1");

        DbDialectHandler dialect = mock(DbDialectHandler.class);
        when(dialect.resolveSchema(any())).thenReturn("APP");
        when(dialect.createDbUnitConnection(jdbc, "APP")).thenReturn(dbConn);
        when(dialect.getPrimaryKeyColumns(jdbc, "APP", "T1")).thenReturn(List.of("ID"));
        when(dialect.convertCsvValueToDbType("T1", "ID", "1")).thenReturn("1");
        when(dialect.countRows(jdbc, "T1")).thenReturn(1);
        when(dialect.quoteIdentifier("APP")).thenReturn("\"APP\"");
        when(dialect.quoteIdentifier("T1")).thenReturn("\"T1\"");
        when(dialect.quoteIdentifier("ID")).thenReturn("\"ID\"");

        SimpleDataSetWrapper dataSetWrapper = buildSimpleDataSet("T1", 1, "ID", "1");
        Method method = DataLoader.class.getDeclaredMethod("deploy", File.class, String.class,
                boolean.class, ConnectionConfig.Entry.class, DbDialectHandler.class, String.class);
        method.setAccessible(true);

        DatabaseOperation clean = mock(DatabaseOperation.class);
        DatabaseOperation update = mock(DatabaseOperation.class);
        DatabaseOperation insert = mock(DatabaseOperation.class);
        DatabaseOperation deleteAll = mock(DatabaseOperation.class);
        withMockedDatabaseOperations(loader, clean, update, insert, deleteAll, () -> {
            try (MockedStatic<DriverManager> driverManager = mockStatic(DriverManager.class);
                    MockedStatic<DataLoaderFactory> factory = mockStatic(DataLoaderFactory.class)) {
                driverManager.when(() -> DriverManager.getConnection("jdbc:scpk", "u", "p"))
                        .thenReturn(jdbc);
                factory.when(() -> DataLoaderFactory.create(dir.toFile(), "T1"))
                        .thenReturn(dataSetWrapper.dataSet);
                method.invoke(loader, dir.toFile(), "db1", false, entry, dialect, "fatal");
            }
        });
        verify(insert, times(1)).execute(eq(dbConn), any());
    }

    @Test
    void deployWithConnection_正常ケース_LOBありNULL許可を実行する_CLEAN_INSERTとUPDATEが実行されること()
            throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);
        when(dialectHandler.resolveSchema(any())).thenReturn("APP");
        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        when(dialectHandler.createDbUnitConnection(any(), eq("APP"))).thenReturn(dbConn);
        when(dialectHandler.getLobColumns(any(), eq("T1")))
                .thenReturn(new Column[] {new Column("LOB_COL", DataType.VARCHAR)});
        when(dialectHandler.hasNotNullLobColumn(any(), eq("APP"), eq("T1"), any()))
                .thenReturn(false);
        when(dialectHandler.countRows(any(), eq("T1"))).thenReturn(2);
        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class),
                e -> dialectHandler, mock(DbUnitConfig.class), new DumpConfig());
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        Path dir = tempDir.resolve("with_conn_lob_nullable");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("T1.csv"), "ID\n1\n", StandardCharsets.UTF_8);
        SimpleDataSetWrapper dataSetWrapper = buildSimpleDataSet("T1", 1, "ID", "1");
        Method method = DataLoader.class.getDeclaredMethod("deployWithConnection", File.class,
                String.class, ConnectionConfig.Entry.class, Connection.class,
                DbDialectHandler.class, String.class);
        method.setAccessible(true);
        DatabaseOperation clean = mock(DatabaseOperation.class);
        DatabaseOperation update = mock(DatabaseOperation.class);
        DatabaseOperation insert = mock(DatabaseOperation.class);
        DatabaseOperation deleteAll = mock(DatabaseOperation.class);
        withMockedDatabaseOperations(loader, clean, update, insert, deleteAll, () -> {
            try (MockedStatic<DataLoaderFactory> factory = mockStatic(DataLoaderFactory.class)) {
                factory.when(() -> DataLoaderFactory.create(dir.toFile(), "T1"))
                        .thenReturn(dataSetWrapper.dataSet);
                method.invoke(loader, dir.toFile(), "db1", entry, mock(Connection.class),
                        dialectHandler, "err");
            }
        });
        verify(clean).execute(eq(dbConn), any());
        verify(update).execute(eq(dbConn), any());
    }

    @Test
    void deploy_正常ケース_初期ロードLOBありNOTNULLを実行する_CLEAN_INSERTのみ実行されること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class), new DumpConfig());
        Path dir = tempDir.resolve("deploy_initial_lob_notnull");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("table-ordering.txt"), "T1\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("T1.csv"), "ID\n1\n", StandardCharsets.UTF_8);
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:lob2");
        entry.setUser("u");
        entry.setPassword("p");
        Connection jdbc = mock(Connection.class);
        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        DbDialectHandler dialect = mock(DbDialectHandler.class);
        when(dialect.resolveSchema(any())).thenReturn("APP");
        when(dialect.createDbUnitConnection(jdbc, "APP")).thenReturn(dbConn);
        when(dialect.getLobColumns(any(), eq("T1")))
                .thenReturn(new Column[] {new Column("LOB_COL", DataType.VARCHAR)});
        when(dialect.hasNotNullLobColumn(any(), eq("APP"), eq("T1"), any())).thenReturn(true);
        when(dialect.countRows(jdbc, "T1")).thenReturn(1);
        SimpleDataSetWrapper dataSetWrapper = buildSimpleDataSet("T1", 1, "ID", "1");
        Method method = DataLoader.class.getDeclaredMethod("deploy", File.class, String.class,
                boolean.class, ConnectionConfig.Entry.class, DbDialectHandler.class, String.class);
        method.setAccessible(true);
        DatabaseOperation clean = mock(DatabaseOperation.class);
        DatabaseOperation update = mock(DatabaseOperation.class);
        DatabaseOperation insert = mock(DatabaseOperation.class);
        DatabaseOperation deleteAll = mock(DatabaseOperation.class);
        withMockedDatabaseOperations(loader, clean, update, insert, deleteAll, () -> {
            try (MockedStatic<DriverManager> driverManager = mockStatic(DriverManager.class);
                    MockedStatic<DataLoaderFactory> factory = mockStatic(DataLoaderFactory.class)) {
                driverManager.when(() -> DriverManager.getConnection("jdbc:lob2", "u", "p"))
                        .thenReturn(jdbc);
                factory.when(() -> DataLoaderFactory.create(dir.toFile(), "T1"))
                        .thenReturn(dataSetWrapper.dataSet);
                method.invoke(loader, dir.toFile(), "db1", true, entry, dialect, "fatal");
            }
        });
        verify(clean, times(1)).execute(eq(dbConn), any());
        verify(update, never()).execute(any(), any());
    }

    @Test
    void deploy_異常ケース_接続取得に失敗する_ErrorHandler経由で例外が送出されること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class), new DumpConfig());
        Path dir = tempDir.resolve("deploy_conn_fail");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("table-ordering.txt"), "T1\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("T1.csv"), "ID\n1\n", StandardCharsets.UTF_8);
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:fail");
        entry.setUser("u");
        entry.setPassword("p");
        Method method = DataLoader.class.getDeclaredMethod("deploy", File.class, String.class,
                boolean.class, ConnectionConfig.Entry.class, DbDialectHandler.class, String.class);
        method.setAccessible(true);
        DbDialectHandler dialect = mock(DbDialectHandler.class);
        when(dialect.resolveSchema(any())).thenReturn("APP");
        ErrorHandler.disableExitForCurrentThread();
        try (MockedStatic<DriverManager> driverManager = mockStatic(DriverManager.class)) {
            driverManager.when(() -> DriverManager.getConnection("jdbc:fail", "u", "p"))
                    .thenThrow(new RuntimeException("conn fail"));
            InvocationTargetException ex =
                    assertThrows(InvocationTargetException.class, () -> method.invoke(loader,
                            dir.toFile(), "db1", true, entry, dialect, "fatal"));
            assertTrue(ex.getCause() instanceof IllegalStateException);
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }
    }

    @Test
    void deploy_正常ケース_シナリオモードPK重複ありで削除経路を通す_重複削除SQLが準備されること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class), new DumpConfig());
        Path dir = tempDir.resolve("deploy_scenario_pk_delete");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("table-ordering.txt"), "T1\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("T1.csv"), "ID\n1\n", StandardCharsets.UTF_8);
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:scpk2");
        entry.setUser("u");
        entry.setPassword("p");
        Connection jdbc = mock(Connection.class);
        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        DbDialectHandler dialect = mock(DbDialectHandler.class);
        when(dialect.resolveSchema(any())).thenReturn("APP");
        when(dialect.createDbUnitConnection(jdbc, "APP")).thenReturn(dbConn);
        when(dialect.getPrimaryKeyColumns(jdbc, "APP", "T1")).thenReturn(List.of("ID"));
        when(dialect.convertCsvValueToDbType("T1", "ID", "1")).thenReturn("1");
        when(dialect.countRows(jdbc, "T1")).thenReturn(1);
        when(dialect.quoteIdentifier("APP")).thenReturn("\"APP\"");
        when(dialect.quoteIdentifier("T1")).thenReturn("\"T1\"");
        when(dialect.quoteIdentifier("ID")).thenReturn("\"ID\"");
        IDataSet dbDataSet = mock(IDataSet.class);
        ITable base = mock(ITable.class);
        ITableMetaData meta = mock(ITableMetaData.class);
        when(meta.getTableName()).thenReturn("T1");
        when(meta.getColumns()).thenReturn(new Column[] {new Column("ID", DataType.VARCHAR)});
        when(base.getTableMetaData()).thenReturn(meta);
        when(base.getRowCount()).thenReturn(1);
        when(base.getValue(0, "ID")).thenReturn("1");
        IDataSet dataSet = mock(IDataSet.class);
        when(dataSet.getTable("T1")).thenReturn(base);
        when(dbConn.createDataSet()).thenReturn(dbDataSet);
        when(dbDataSet.getTable("T1")).thenReturn(base);
        PreparedStatement ps = mock(PreparedStatement.class);
        when(jdbc.prepareStatement("DELETE FROM \"APP\".\"T1\" WHERE \"ID\" = ?")).thenReturn(ps);
        when(ps.executeBatch()).thenReturn(new int[] {1});
        Method method = DataLoader.class.getDeclaredMethod("deploy", File.class, String.class,
                boolean.class, ConnectionConfig.Entry.class, DbDialectHandler.class, String.class);
        method.setAccessible(true);
        DatabaseOperation clean = mock(DatabaseOperation.class);
        DatabaseOperation update = mock(DatabaseOperation.class);
        DatabaseOperation insert = mock(DatabaseOperation.class);
        DatabaseOperation deleteAll = mock(DatabaseOperation.class);
        withMockedDatabaseOperations(loader, clean, update, insert, deleteAll, () -> {
            try (MockedStatic<DriverManager> driverManager = mockStatic(DriverManager.class);
                    MockedStatic<DataLoaderFactory> factory = mockStatic(DataLoaderFactory.class)) {
                driverManager.when(() -> DriverManager.getConnection("jdbc:scpk2", "u", "p"))
                        .thenReturn(jdbc);
                factory.when(() -> DataLoaderFactory.create(dir.toFile(), "T1"))
                        .thenReturn(dataSet);
                method.invoke(loader, dir.toFile(), "db1", false, entry, dialect, "fatal");
            }
        });
        verify(insert, times(1)).execute(eq(dbConn), any());
    }

    @Test
    void deployWithConnection_異常ケース_テーブル処理中例外かつクローズ失敗する_ErrorHandler経由で例外が送出されること()
            throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DbDialectHandler dialect = mock(DbDialectHandler.class);
        when(dialect.resolveSchema(any())).thenReturn("APP");
        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        when(dialect.createDbUnitConnection(any(), eq("APP"))).thenReturn(dbConn);
        doThrow(new RuntimeException("close")).when(dbConn).close();
        doThrow(new RuntimeException("ddl")).when(dialect).logTableDefinition(any(), eq("APP"),
                eq("T1"), eq("db1"));
        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class), e -> dialect,
                mock(DbUnitConfig.class), new DumpConfig());
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        Path dir = tempDir.resolve("with_conn_table_error");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("T1.csv"), "ID\n1\n", StandardCharsets.UTF_8);
        SimpleDataSetWrapper dataSetWrapper = buildSimpleDataSet("T1", 1, "ID", "1");
        Method method = DataLoader.class.getDeclaredMethod("deployWithConnection", File.class,
                String.class, ConnectionConfig.Entry.class, Connection.class,
                DbDialectHandler.class, String.class);
        method.setAccessible(true);
        ErrorHandler.disableExitForCurrentThread();
        try (MockedStatic<DataLoaderFactory> factory = mockStatic(DataLoaderFactory.class)) {
            factory.when(() -> DataLoaderFactory.create(dir.toFile(), "T1"))
                    .thenReturn(dataSetWrapper.dataSet);
            InvocationTargetException ex =
                    assertThrows(InvocationTargetException.class, () -> method.invoke(loader,
                            dir.toFile(), "db1", entry, mock(Connection.class), dialect, "fatal"));
            assertTrue(ex.getCause() instanceof IllegalStateException);
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }
    }

    @Test
    void deployWithConnection_正常ケース_LOBありNOTNULLを実行する_CLEAN_INSERTのみ実行されること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DbDialectHandler dialect = mock(DbDialectHandler.class);
        when(dialect.resolveSchema(any())).thenReturn("APP");
        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        when(dialect.createDbUnitConnection(any(), eq("APP"))).thenReturn(dbConn);
        when(dialect.getLobColumns(any(), eq("T1")))
                .thenReturn(new Column[] {new Column("LOB_COL", DataType.VARCHAR)});
        when(dialect.hasNotNullLobColumn(any(), eq("APP"), eq("T1"), any())).thenReturn(true);
        when(dialect.countRows(any(), eq("T1"))).thenReturn(1);
        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class), e -> dialect,
                mock(DbUnitConfig.class), new DumpConfig());
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        Path dir = tempDir.resolve("with_conn_lob_notnull");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("T1.csv"), "ID\n1\n", StandardCharsets.UTF_8);
        SimpleDataSetWrapper dataSetWrapper = buildSimpleDataSet("T1", 1, "ID", "1");
        Method method = DataLoader.class.getDeclaredMethod("deployWithConnection", File.class,
                String.class, ConnectionConfig.Entry.class, Connection.class,
                DbDialectHandler.class, String.class);
        method.setAccessible(true);
        DatabaseOperation clean = mock(DatabaseOperation.class);
        DatabaseOperation update = mock(DatabaseOperation.class);
        DatabaseOperation insert = mock(DatabaseOperation.class);
        DatabaseOperation deleteAll = mock(DatabaseOperation.class);
        withMockedDatabaseOperations(loader, clean, update, insert, deleteAll, () -> {
            try (MockedStatic<DataLoaderFactory> factory = mockStatic(DataLoaderFactory.class)) {
                factory.when(() -> DataLoaderFactory.create(dir.toFile(), "T1"))
                        .thenReturn(dataSetWrapper.dataSet);
                method.invoke(loader, dir.toFile(), "db1", entry, mock(Connection.class), dialect,
                        "fatal");
            }
        });
        verify(clean, times(1)).execute(eq(dbConn), any());
        verify(update, never()).execute(any(), any());
    }

    @Test
    void deploy_正常ケース_ErrorHandler呼び出し行を通す_エラーハンドラが呼ばれること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class), new DumpConfig());
        Path dir = tempDir.resolve("deploy_errorhandler_line");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("table-ordering.txt"), "T1\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("T1.csv"), "ID\n1\n", StandardCharsets.UTF_8);
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:errorhandler");
        entry.setUser("u");
        entry.setPassword("p");
        Method method = DataLoader.class.getDeclaredMethod("deploy", File.class, String.class,
                boolean.class, ConnectionConfig.Entry.class, DbDialectHandler.class, String.class);
        method.setAccessible(true);
        try (MockedStatic<DriverManager> driverManager = mockStatic(DriverManager.class);
                MockedStatic<ErrorHandler> errorHandler = mockStatic(ErrorHandler.class)) {
            driverManager.when(() -> DriverManager.getConnection("jdbc:errorhandler", "u", "p"))
                    .thenThrow(new RuntimeException("x"));
            errorHandler.when(() -> ErrorHandler.errorAndExit(anyString(), any(Throwable.class)))
                    .thenAnswer(inv -> null);
            method.invoke(loader, dir.toFile(), "db1", true, entry, mock(DbDialectHandler.class),
                    "fatal");
            errorHandler.verify(() -> ErrorHandler.errorAndExit(eq("fatal"), any(Throwable.class)),
                    times(1));
        }
    }

    @Test
    void deployWithConnection_正常ケース_ErrorHandler呼び出し行を通す_エラーハンドラが呼ばれること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DbDialectHandler dialect = mock(DbDialectHandler.class);
        when(dialect.resolveSchema(any())).thenReturn("APP");
        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        when(dialect.createDbUnitConnection(any(), eq("APP"))).thenReturn(dbConn);
        doThrow(new RuntimeException("ddl")).when(dialect).logTableDefinition(any(), eq("APP"),
                eq("T1"), eq("db1"));
        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class), e -> dialect,
                mock(DbUnitConfig.class), new DumpConfig());
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        Path dir = tempDir.resolve("with_conn_errorhandler_line");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("T1.csv"), "ID\n1\n", StandardCharsets.UTF_8);
        SimpleDataSetWrapper dataSetWrapper = buildSimpleDataSet("T1", 1, "ID", "1");
        Method method = DataLoader.class.getDeclaredMethod("deployWithConnection", File.class,
                String.class, ConnectionConfig.Entry.class, Connection.class,
                DbDialectHandler.class, String.class);
        method.setAccessible(true);
        try (MockedStatic<DataLoaderFactory> factory = mockStatic(DataLoaderFactory.class);
                MockedStatic<ErrorHandler> errorHandler = mockStatic(ErrorHandler.class)) {
            factory.when(() -> DataLoaderFactory.create(dir.toFile(), "T1"))
                    .thenReturn(dataSetWrapper.dataSet);
            errorHandler.when(() -> ErrorHandler.errorAndExit(anyString(), any(Throwable.class)))
                    .thenAnswer(inv -> null);
            method.invoke(loader, dir.toFile(), "db1", entry, mock(Connection.class), dialect,
                    "fatal");
            errorHandler.verify(() -> ErrorHandler.errorAndExit(eq("fatal"), any(Throwable.class)),
                    times(1));
        }
    }

    @Test
    void execute_正常ケース_シナリオ空文字とターゲット空を指定する_分岐を通過して終了すること() {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        dbUnitConfig.setPreDirName("pre");
        ConnectionConfig.Entry db1 = new ConnectionConfig.Entry();
        db1.setId("db1");
        ConnectionConfig connectionConfig = new ConnectionConfig();
        connectionConfig.setConnections(List.of(db1));
        DataLoader loader = new DataLoader(pathsConfig, connectionConfig,
                e -> mock(DbDialectHandler.class), dbUnitConfig, new DumpConfig());
        loader.execute("", List.of());
        loader.execute("scenario", null);
        assertTrue(true);
    }

    @Test
    void deployWithConnection_正常ケース_dumpConfigがnullである_例外なく完了すること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DbDialectHandler dialect = mock(DbDialectHandler.class);
        when(dialect.resolveSchema(any())).thenReturn("APP");
        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        when(dialect.createDbUnitConnection(any(), eq("APP"))).thenReturn(dbConn);
        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class), e -> dialect,
                mock(DbUnitConfig.class), null);
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        Path dir = tempDir.resolve("with_conn_null_dump");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("T1.csv"), "ID\n1\n", StandardCharsets.UTF_8);
        try (MockedStatic<DataLoaderFactory> factory = mockStatic(DataLoaderFactory.class)) {
            factory.when(() -> DataLoaderFactory.create(dir.toFile(), "T1"))
                    .thenThrow(new RuntimeException("skip"));
            Method method = DataLoader.class.getDeclaredMethod("deployWithConnection", File.class,
                    String.class, ConnectionConfig.Entry.class, Connection.class,
                    DbDialectHandler.class, String.class);
            method.setAccessible(true);
            method.invoke(loader, dir.toFile(), "db1", entry, mock(Connection.class), dialect,
                    "fatal");
        }
        verify(dbConn).close();
    }

    @Test
    void deploy_正常ケース_dumpConfigの除外テーブルがnullである_除外分岐を通過すること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(null);
        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class), dumpConfig);
        Path dir = tempDir.resolve("deploy_dump_null");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("table-ordering.txt"), "T1\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("T1.csv"), "ID\n1\n", StandardCharsets.UTF_8);
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:dumpnull");
        entry.setUser("u");
        entry.setPassword("p");
        Connection jdbc = mock(Connection.class);
        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        DbDialectHandler dialect = mock(DbDialectHandler.class);
        when(dialect.resolveSchema(any())).thenReturn("APP");
        when(dialect.createDbUnitConnection(jdbc, "APP")).thenReturn(dbConn);
        when(dialect.getLobColumns(any(), eq("T1"))).thenReturn(new Column[0]);
        when(dialect.countRows(jdbc, "T1")).thenReturn(1);
        SimpleDataSetWrapper dataSetWrapper = buildSimpleDataSet("T1", 1, "ID", "1");
        Method method = DataLoader.class.getDeclaredMethod("deploy", File.class, String.class,
                boolean.class, ConnectionConfig.Entry.class, DbDialectHandler.class, String.class);
        method.setAccessible(true);
        DatabaseOperation clean = mock(DatabaseOperation.class);
        DatabaseOperation update = mock(DatabaseOperation.class);
        DatabaseOperation insert = mock(DatabaseOperation.class);
        DatabaseOperation deleteAll = mock(DatabaseOperation.class);
        withMockedDatabaseOperations(loader, clean, update, insert, deleteAll, () -> {
            try (MockedStatic<DriverManager> driverManager = mockStatic(DriverManager.class);
                    MockedStatic<DataLoaderFactory> factory = mockStatic(DataLoaderFactory.class)) {
                driverManager.when(() -> DriverManager.getConnection("jdbc:dumpnull", "u", "p"))
                        .thenReturn(jdbc);
                factory.when(() -> DataLoaderFactory.create(dir.toFile(), "T1"))
                        .thenReturn(dataSetWrapper.dataSet);
                method.invoke(loader, dir.toFile(), "db1", true, entry, dialect, "fatal");
            }
        });
        verify(jdbc).commit();
    }

    @Test
    void deployWithConnection_正常ケース_dumpConfigの除外テーブルがnullである_除外分岐を通過すること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(null);
        DbDialectHandler dialect = mock(DbDialectHandler.class);
        when(dialect.resolveSchema(any())).thenReturn("APP");
        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        when(dialect.createDbUnitConnection(any(), eq("APP"))).thenReturn(dbConn);
        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class), e -> dialect,
                mock(DbUnitConfig.class), dumpConfig);
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        Path dir = tempDir.resolve("with_conn_dump_null");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("T1.csv"), "ID\n1\n", StandardCharsets.UTF_8);
        try (MockedStatic<DataLoaderFactory> factory = mockStatic(DataLoaderFactory.class)) {
            factory.when(() -> DataLoaderFactory.create(dir.toFile(), "T1"))
                    .thenThrow(new RuntimeException("skip"));
            Method method = DataLoader.class.getDeclaredMethod("deployWithConnection", File.class,
                    String.class, ConnectionConfig.Entry.class, Connection.class,
                    DbDialectHandler.class, String.class);
            method.setAccessible(true);
            method.invoke(loader, dir.toFile(), "db1", entry, mock(Connection.class), dialect,
                    "fatal");
        }
        verify(dbConn).close();
    }

    @Test
    void deployWithConnection_正常ケース_listFilesがnullを返す_データセットなしで終了すること() throws Exception {
        class NullListFile extends File {
            private static final long serialVersionUID = 1L;

            NullListFile(String pathname) {
                super(pathname);
            }

            @Override
            public File[] listFiles(java.io.FilenameFilter filter) {
                return null;
            }
        }
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DbDialectHandler dialect = mock(DbDialectHandler.class);
        when(dialect.resolveSchema(any())).thenReturn("APP");
        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class), e -> dialect,
                mock(DbUnitConfig.class), new DumpConfig());
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        File dir = new NullListFile(tempDir.toString());
        Method method = DataLoader.class.getDeclaredMethod("deployWithConnection", File.class,
                String.class, ConnectionConfig.Entry.class, Connection.class,
                DbDialectHandler.class, String.class);
        method.setAccessible(true);
        method.invoke(loader, dir, "db1", entry, mock(Connection.class), dialect, "fatal");
        verify(dialect, never()).createDbUnitConnection(any(), anyString());
    }

    @Test
    void deploy_正常ケース_dumpConfig除外テーブルが空である_除外条件の空分岐を通過すること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(List.of());
        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class), dumpConfig);
        Path dir = tempDir.resolve("deploy_dump_empty");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("table-ordering.txt"), "T1\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("T1.csv"), "ID\n1\n", StandardCharsets.UTF_8);
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:dumpempty");
        entry.setUser("u");
        entry.setPassword("p");
        Connection jdbc = mock(Connection.class);
        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        DbDialectHandler dialect = mock(DbDialectHandler.class);
        when(dialect.resolveSchema(any())).thenReturn("APP");
        when(dialect.createDbUnitConnection(jdbc, "APP")).thenReturn(dbConn);
        when(dialect.getLobColumns(any(), eq("T1"))).thenReturn(new Column[0]);
        when(dialect.countRows(jdbc, "T1")).thenReturn(1);
        SimpleDataSetWrapper dataSetWrapper = buildSimpleDataSet("T1", 1, "ID", "1");
        Method method = DataLoader.class.getDeclaredMethod("deploy", File.class, String.class,
                boolean.class, ConnectionConfig.Entry.class, DbDialectHandler.class, String.class);
        method.setAccessible(true);
        DatabaseOperation clean = mock(DatabaseOperation.class);
        DatabaseOperation update = mock(DatabaseOperation.class);
        DatabaseOperation insert = mock(DatabaseOperation.class);
        DatabaseOperation deleteAll = mock(DatabaseOperation.class);
        withMockedDatabaseOperations(loader, clean, update, insert, deleteAll, () -> {
            try (MockedStatic<DriverManager> driverManager = mockStatic(DriverManager.class);
                    MockedStatic<DataLoaderFactory> factory = mockStatic(DataLoaderFactory.class)) {
                driverManager.when(() -> DriverManager.getConnection("jdbc:dumpempty", "u", "p"))
                        .thenReturn(jdbc);
                factory.when(() -> DataLoaderFactory.create(dir.toFile(), "T1"))
                        .thenReturn(dataSetWrapper.dataSet);
                method.invoke(loader, dir.toFile(), "db1", true, entry, dialect, "fatal");
            }
        });
        verify(jdbc).commit();
    }

    @Test
    void deploy_正常ケース_dumpConfigがnullである_除外条件のnull分岐を通過すること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class), null);
        Path dir = tempDir.resolve("deploy_dump_null");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("table-ordering.txt"), "T1\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("T1.csv"), "ID\n1\n", StandardCharsets.UTF_8);
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:dumpnull");
        entry.setUser("u");
        entry.setPassword("p");
        Connection jdbc = mock(Connection.class);
        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        DbDialectHandler dialect = mock(DbDialectHandler.class);
        when(dialect.resolveSchema(any())).thenReturn("APP");
        when(dialect.createDbUnitConnection(jdbc, "APP")).thenReturn(dbConn);
        when(dialect.getLobColumns(any(), eq("T1"))).thenReturn(new Column[0]);
        when(dialect.countRows(jdbc, "T1")).thenReturn(1);
        SimpleDataSetWrapper dataSetWrapper = buildSimpleDataSet("T1", 1, "ID", "1");
        Method method = DataLoader.class.getDeclaredMethod("deploy", File.class, String.class,
                boolean.class, ConnectionConfig.Entry.class, DbDialectHandler.class, String.class);
        method.setAccessible(true);
        DatabaseOperation clean = mock(DatabaseOperation.class);
        DatabaseOperation update = mock(DatabaseOperation.class);
        DatabaseOperation insert = mock(DatabaseOperation.class);
        DatabaseOperation deleteAll = mock(DatabaseOperation.class);
        withMockedDatabaseOperations(loader, clean, update, insert, deleteAll, () -> {
            try (MockedStatic<DriverManager> driverManager = mockStatic(DriverManager.class);
                    MockedStatic<DataLoaderFactory> factory = mockStatic(DataLoaderFactory.class)) {
                driverManager.when(() -> DriverManager.getConnection("jdbc:dumpnull", "u", "p"))
                        .thenReturn(jdbc);
                factory.when(() -> DataLoaderFactory.create(dir.toFile(), "T1"))
                        .thenReturn(dataSetWrapper.dataSet);
                method.invoke(loader, dir.toFile(), "db1", true, entry, dialect, "fatal");
            }
        });
        verify(jdbc).commit();
    }

    @Test
    void deploy_正常ケース_tableOrderingのテーブル名が空白のみ_トリム後に空となりスキップされること() throws Exception {

        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());

        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(List.of());

        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class), dumpConfig);

        Path dir = tempDir.resolve("deploy_tables_blank_only");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("  .csv"), "ID\n1\n", StandardCharsets.UTF_8);

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:should-not-connect");
        entry.setUser("u");
        entry.setPassword("p");

        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);

        Method method = DataLoader.class.getDeclaredMethod("deploy", File.class, String.class,
                boolean.class, ConnectionConfig.Entry.class, DbDialectHandler.class, String.class);
        method.setAccessible(true);

        try (MockedStatic<DriverManager> driverManager = mockStatic(DriverManager.class)) {
            method.invoke(loader, dir.toFile(), "db1", true, entry, dialectHandler, "fatal");

            // tables が空で return するので、JDBC 接続は発生しない
            driverManager.verifyNoInteractions();
        }

        // こちらも到達しない（接続処理に入らない）
        verifyNoInteractions(dialectHandler);
    }

    @Test
    public void execute_異常ケース_tableOrdering作成に失敗する_ErrorHandlerが呼ばれること() throws Exception {
        PathsConfig pathsConfig = mock(PathsConfig.class);
        Path dataRoot = Files.createDirectories(tempDir.resolve("data"));
        Path dumpRoot = Files.createDirectories(tempDir.resolve("dump"));
        when(pathsConfig.getDataPath()).thenReturn(dataRoot.toString());
        when(pathsConfig.getDump()).thenReturn(dumpRoot.toString());

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:dummy");
        entry.setUser("u");
        entry.setPassword("p");
        ConnectionConfig connectionConfig = new ConnectionConfig();
        connectionConfig.setConnections(List.of(entry));

        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(List.of());
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);

        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);
        when(dialectHandler.resolveSchema(any())).thenReturn("APP");
        DatabaseConnection dbConn = mock(DatabaseConnection.class);

        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);

        ResultSet tableRs = mock(ResultSet.class);
        when(meta.getTables(isNull(), eq("APP"), eq("%"), any(String[].class))).thenReturn(tableRs);
        when(tableRs.next()).thenReturn(true, false);
        when(tableRs.getString("TABLE_NAME")).thenReturn("T1");

        when(dialectHandler.createDbUnitConnection(eq(conn), eq("APP"))).thenReturn(dbConn);

        DataDumper dumper = new DataDumper(pathsConfig, connectionConfig, filePatternConfig,
                dumpConfig, e -> dialectHandler);

        try (MockedStatic<DriverManager> driverManager = mockStatic(DriverManager.class);
                MockedStatic<TableDependencyResolver> resolver =
                        mockStatic(TableDependencyResolver.class);
                MockedStatic<FileUtils> fileUtils = mockStatic(FileUtils.class);
                MockedStatic<ErrorHandler> errorHandler = mockStatic(ErrorHandler.class)) {

            driverManager.when(() -> DriverManager.getConnection("jdbc:dummy", "u", "p"))
                    .thenReturn(conn);

            resolver.when(
                    () -> TableDependencyResolver.resolveLoadOrder(any(), any(), any(), any()))
                    .thenReturn(List.of("T1"));

            IOException ioEx = new IOException("write fail");
            fileUtils.when(() -> FileUtils.writeStringToFile(any(File.class), anyString(),
                    any(Charset.class))).thenThrow(ioEx);

            errorHandler.when(() -> ErrorHandler.errorAndExit(anyString(), any(Throwable.class)))
                    .thenAnswer(inv -> {
                        throw new IllegalStateException("exit");
                    });
            errorHandler.when(() -> ErrorHandler.errorAndExit(anyString())).thenAnswer(inv -> {
                throw new IllegalStateException("exit");
            });

            assertThrows(IllegalStateException.class, () -> dumper.execute("scn", List.of("db1")));
            errorHandler.verify(() -> ErrorHandler.errorAndExit(
                    eq("Failed to create table-ordering.txt"), any(IOException.class)), times(1));
        }
    }

    @Test
    void executeWithConnection_異常ケース_未存在ディレクトリを指定する_存在チェック分岐を通ること() {
        DataLoader loader = new DataLoader(mock(PathsConfig.class), mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class),
                mock(DumpConfig.class));
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        assertThrows(IllegalStateException.class,
                () -> loader.executeWithConnection(tempDir.resolve("missing_again").toFile(), entry,
                        mock(Connection.class)));
    }

    @Test
    void executeWithConnection_異常ケース_存在する通常ファイルを指定する_存在チェック分岐を通ること() throws Exception {
        DataLoader loader = new DataLoader(mock(PathsConfig.class), mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class),
                mock(DumpConfig.class));
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        Path file = tempDir.resolve("existing_file.txt");
        Files.writeString(file, "x", StandardCharsets.UTF_8);
        assertThrows(IllegalStateException.class,
                () -> loader.executeWithConnection(file.toFile(), entry, mock(Connection.class)));
    }

    @Test
    void deployWithConnection_異常ケース_未存在ディレクトリを直接指定する_存在チェック分岐を通ること() throws Exception {
        DataLoader loader = new DataLoader(mock(PathsConfig.class), mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class),
                mock(DumpConfig.class));
        Method method = DataLoader.class.getDeclaredMethod("deployWithConnection", File.class,
                String.class, ConnectionConfig.Entry.class, Connection.class,
                DbDialectHandler.class, String.class);
        method.setAccessible(true);
        InvocationTargetException ex = assertThrows(InvocationTargetException.class,
                () -> method.invoke(loader, tempDir.resolve("missing_again2").toFile(), "db1",
                        new ConnectionConfig.Entry(), mock(Connection.class),
                        mock(DbDialectHandler.class), "fatal"));
        assertTrue(ex.getCause() instanceof IllegalStateException);
    }

    @Test
    void deployWithConnection_異常ケース_存在する通常ファイルを直接指定する_存在チェック分岐を通ること() throws Exception {
        DataLoader loader = new DataLoader(mock(PathsConfig.class), mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class),
                mock(DumpConfig.class));
        Method method = DataLoader.class.getDeclaredMethod("deployWithConnection", File.class,
                String.class, ConnectionConfig.Entry.class, Connection.class,
                DbDialectHandler.class, String.class);
        method.setAccessible(true);
        Path file = tempDir.resolve("existing_file2.txt");
        Files.writeString(file, "x", StandardCharsets.UTF_8);
        InvocationTargetException ex = assertThrows(InvocationTargetException.class,
                () -> method.invoke(loader, file.toFile(), "db1", new ConnectionConfig.Entry(),
                        mock(Connection.class), mock(DbDialectHandler.class), "fatal"));
        assertTrue(ex.getCause() instanceof IllegalStateException);
    }

    private SimpleDataSetWrapper buildSimpleDataSet(String tableName, int rowCount, String colName,
            Object val) throws DataSetException {
        ITable base = mock(ITable.class);
        when(base.getRowCount()).thenReturn(rowCount);
        when(base.getValue(0, colName)).thenReturn(val);
        ITableMetaData meta = mock(ITableMetaData.class);
        when(meta.getTableName()).thenReturn(tableName);
        when(meta.getColumns()).thenReturn(new Column[] {new Column(colName, DataType.VARCHAR)});
        when(base.getTableMetaData()).thenReturn(meta);
        IDataSet dataSet = mock(IDataSet.class);
        when(dataSet.getTable(tableName)).thenReturn(base);
        return new SimpleDataSetWrapper(dataSet);
    }

    private static class SimpleDataSetWrapper {
        private final IDataSet dataSet;

        private SimpleDataSetWrapper(IDataSet dataSet) {
            this.dataSet = dataSet;
        }
    }

    // ─── C0カバレッジ補完ケース ─────────────────────────────────────────────────

    @Test
    void deployWithConnection_正常ケース_FK解決でSQLException_アルファベット順で継続されること() throws Exception {
        // L943: FK解決でSQLExceptionがスローされた場合、警告ログ後にアルファベット順で処理が継続されること
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DbDialectHandler dialect = mock(DbDialectHandler.class);
        when(dialect.resolveSchema(any())).thenReturn("APP");
        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        when(dialect.createDbUnitConnection(any(), eq("APP"))).thenReturn(dbConn);
        when(dialect.getLobColumns(any(), eq("T1"))).thenReturn(new Column[0]);
        when(dialect.countRows(any(), eq("T1"))).thenReturn(1);
        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class), e -> dialect,
                mock(DbUnitConfig.class), new DumpConfig());
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        Path dir = tempDir.resolve("with_conn_fk_sqlex");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("T1.csv"), "ID\n1\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("T2.csv"), "ID\n1\n", StandardCharsets.UTF_8);
        SimpleDataSetWrapper ds1 = buildSimpleDataSet("T1", 1, "ID", "1");
        Method method = DataLoader.class.getDeclaredMethod("deployWithConnection", File.class,
                String.class, ConnectionConfig.Entry.class, Connection.class,
                DbDialectHandler.class, String.class);
        method.setAccessible(true);
        DatabaseOperation clean = mock(DatabaseOperation.class);
        DatabaseOperation update = mock(DatabaseOperation.class);
        DatabaseOperation insert = mock(DatabaseOperation.class);
        DatabaseOperation deleteAll = mock(DatabaseOperation.class);
        withMockedDatabaseOperations(loader, clean, update, insert, deleteAll, () -> {
            try (MockedStatic<DataLoaderFactory> factory = mockStatic(DataLoaderFactory.class);
                    MockedStatic<TableDependencyResolver> resolver =
                            mockStatic(TableDependencyResolver.class)) {
                factory.when(() -> DataLoaderFactory.create(dir.toFile(), "T1"))
                        .thenReturn(ds1.dataSet);
                factory.when(() -> DataLoaderFactory.create(dir.toFile(), "T2"))
                        .thenThrow(new RuntimeException("skip T2"));
                resolver.when(
                        () -> TableDependencyResolver.resolveLoadOrder(any(), any(), any(), any()))
                        .thenThrow(new SQLException("FK resolution failed"));
                method.invoke(loader, dir.toFile(), "db1", entry, mock(Connection.class), dialect,
                        "fatal");
            }
        });
        // FK解決失敗後もアルファベット順フォールバックで T1 がロードされること
        verify(clean).execute(eq(dbConn), any());
    }

    @Test
    void deleteAllInReverseOrder_正常ケース_tablesがnullである_早期リターンしてdbConnを呼ばないこと() throws Exception {
        DataLoader loader = new DataLoader(mock(PathsConfig.class), mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class),
                mock(DumpConfig.class));
        Method method = DataLoader.class.getDeclaredMethod("deleteAllInReverseOrder",
                IDatabaseConnection.class, List.class, String.class);
        method.setAccessible(true);

        IDatabaseConnection dbConn = mock(IDatabaseConnection.class);
        method.invoke(loader, dbConn, null, "db1");

        verify(dbConn, never()).createDataSet(any(String[].class));
    }

    @Test
    void deleteAllInReverseOrder_正常ケース_tablesが1件である_早期リターンしてdbConnを呼ばないこと() throws Exception {
        DataLoader loader = new DataLoader(mock(PathsConfig.class), mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class),
                mock(DumpConfig.class));
        Method method = DataLoader.class.getDeclaredMethod("deleteAllInReverseOrder",
                IDatabaseConnection.class, List.class, String.class);
        method.setAccessible(true);

        IDatabaseConnection dbConn = mock(IDatabaseConnection.class);
        method.invoke(loader, dbConn, List.of("T1"), "db1");

        verify(dbConn, never()).createDataSet(any(String[].class));
    }

}
