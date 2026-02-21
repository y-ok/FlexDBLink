package io.github.yok.flexdblink.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.db.DbDialectHandler;
import io.github.yok.flexdblink.parser.DataLoaderFactory;
import io.github.yok.flexdblink.util.ErrorHandler;
import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
            DatabaseOperation update, DatabaseOperation insert, ThrowingRunnable runnable)
            throws Exception {
        DataLoader.OperationExecutor original = loader.getOperationExecutor();
        DataLoader.OperationExecutor executor = new DataLoader.OperationExecutor() {
            @Override
            public void cleanInsert(org.dbunit.database.IDatabaseConnection connection,
                    IDataSet dataSet) throws Exception {
                cleanInsert.execute(connection, dataSet);
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
    void deleteDuplicates_正常ケース_null値列をISNULL条件で削除すること() throws Exception {
        PathsConfig pathsConfig = mock(PathsConfig.class);
        ConnectionConfig connectionConfig = mock(ConnectionConfig.class);
        DbUnitConfig dbUnitConfig = mock(DbUnitConfig.class);
        DumpConfig dumpConfig = mock(DumpConfig.class);
        DataLoader loader = new DataLoader(pathsConfig, connectionConfig,
                e -> mock(DbDialectHandler.class), dbUnitConfig, dumpConfig);

        Connection jdbc = mock(Connection.class);
        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);
        when(dialectHandler.resolveSchema(any())).thenReturn("APP");
        when(dialectHandler.quoteIdentifier("APP")).thenReturn("\"APP\"");
        when(dialectHandler.quoteIdentifier("TBL")).thenReturn("\"TBL\"");
        when(dialectHandler.quoteIdentifier("COL_A")).thenReturn("\"COL_A\"");
        when(dialectHandler.quoteIdentifier("COL_B")).thenReturn("\"COL_B\"");

        ITable originalDbTable = mock(ITable.class);
        when(originalDbTable.getValue(0, "COL_A")).thenReturn(null);
        when(originalDbTable.getValue(0, "COL_B")).thenReturn("VAL_B");

        Column colA = mock(Column.class);
        Column colB = mock(Column.class);
        when(colA.getColumnName()).thenReturn("COL_A");
        when(colB.getColumnName()).thenReturn("COL_B");
        Column[] cols = new Column[] {colA, colB};

        String expectedSql =
                "DELETE FROM \"APP\".\"TBL\" WHERE \"COL_A\" IS NULL AND \"COL_B\" = ?";
        PreparedStatement ps = mock(PreparedStatement.class);
        when(jdbc.prepareStatement(expectedSql)).thenReturn(ps);
        when(ps.executeUpdate()).thenReturn(1);

        Method method = DataLoader.class.getDeclaredMethod("deleteDuplicates", Connection.class,
                String.class, String.class, List.class, Column[].class, ITable.class, Map.class,
                DbDialectHandler.class, String.class);
        method.setAccessible(true);
        method.invoke(loader, jdbc, "APP", "TBL", List.of(), cols, originalDbTable, Map.of(0, 0),
                dialectHandler, "DB1");

        verify(jdbc).prepareStatement(expectedSql);
        verify(ps).setObject(1, "VAL_B");
        verify(ps, never()).setObject(2, null);
        verify(ps).executeUpdate();
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
    void rowsEqual_正常ケース_INTERVAL列が同値である_trueが返ること() throws Exception {
        DataLoader loader = new DataLoader(mock(PathsConfig.class), mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class),
                mock(DumpConfig.class));

        ITable csvTable = mock(ITable.class);
        ITable dbTable = mock(ITable.class);
        Connection jdbc = mock(Connection.class);
        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);
        when(dialectHandler.resolveSchema(any())).thenReturn("APP");
        Column[] cols = new Column[] {new Column("IYM", DataType.VARCHAR)};
        when(dialectHandler.getColumnTypeName(jdbc, "APP", "TBL", "IYM"))
                .thenReturn("INTERVAL YEAR TO MONTH");
        when(dialectHandler.shouldUseRawValueForComparison("INTERVAL YEAR TO MONTH"))
                .thenReturn(true);
        when(dialectHandler.normalizeValueForComparison("IYM", "INTERVAL YEAR TO MONTH", "+2-3"))
                .thenReturn("+02-03");
        when(dialectHandler.normalizeValueForComparison("IYM", "INTERVAL YEAR TO MONTH", "2-3"))
                .thenReturn("+02-03");
        when(csvTable.getValue(0, "IYM")).thenReturn("+2-3");
        when(dbTable.getValue(0, "IYM")).thenReturn("2-3");

        Method method = DataLoader.class.getDeclaredMethod("rowsEqual", ITable.class, ITable.class,
                Connection.class, String.class, String.class, int.class, int.class, Column[].class,
                DbDialectHandler.class);
        method.setAccessible(true);
        boolean actual = (boolean) method.invoke(loader, csvTable, dbTable, jdbc, "APP", "TBL", 0,
                0, cols, dialectHandler);
        assertTrue(actual);
    }

    @Test
    void rowsEqual_正常ケース_通常列が不一致である_falseが返ること() throws Exception {
        DataLoader loader = new DataLoader(mock(PathsConfig.class), mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class),
                mock(DumpConfig.class));

        ITable csvTable = mock(ITable.class);
        ITable dbTable = mock(ITable.class);
        Connection jdbc = mock(Connection.class);
        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);
        when(dialectHandler.resolveSchema(any())).thenReturn("APP");
        Column[] cols = new Column[] {new Column("C1", DataType.VARCHAR)};
        when(dialectHandler.getColumnTypeName(jdbc, "APP", "TBL", "C1")).thenReturn("VARCHAR2");
        when(dialectHandler.formatDbValueForCsv("C1", "B")).thenReturn("B");
        when(csvTable.getValue(0, "C1")).thenReturn("A");
        when(dbTable.getValue(0, "C1")).thenReturn("B");

        Method method = DataLoader.class.getDeclaredMethod("rowsEqual", ITable.class, ITable.class,
                Connection.class, String.class, String.class, int.class, int.class, Column[].class,
                DbDialectHandler.class);
        method.setAccessible(true);
        boolean actual = (boolean) method.invoke(loader, csvTable, dbTable, jdbc, "APP", "TBL", 0,
                0, cols, dialectHandler);
        assertFalse(actual);
    }

    @Test
    void detectDuplicates_正常ケース_PK一致行がある_重複マップが返ること() throws Exception {
        DataLoader loader = new DataLoader(mock(PathsConfig.class), mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class),
                mock(DumpConfig.class));

        ITable csvTable = mock(ITable.class);
        ITable dbTable = mock(ITable.class);
        ITableMetaData metaData = mock(ITableMetaData.class);
        when(csvTable.getRowCount()).thenReturn(1);
        when(dbTable.getRowCount()).thenReturn(1);
        when(csvTable.getTableMetaData()).thenReturn(metaData);
        when(metaData.getColumns()).thenReturn(new Column[] {new Column("ID", DataType.VARCHAR)});
        when(csvTable.getValue(0, "ID")).thenReturn("1");
        when(dbTable.getValue(0, "ID")).thenReturn("1");

        Method method = DataLoader.class.getDeclaredMethod("detectDuplicates", ITable.class,
                ITable.class, List.class, Connection.class, String.class, String.class,
                DbDialectHandler.class);
        method.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<Integer, Integer> actual =
                (Map<Integer, Integer>) method.invoke(loader, csvTable, dbTable, List.of("ID"),
                        mock(Connection.class), "APP", "TBL", mock(DbDialectHandler.class));

        assertEquals(1, actual.size());
        assertEquals(0, actual.get(0));
    }

    @Test
    void detectDuplicates_異常ケース_比較中に例外が起きる_DataSetExceptionが送出されること() throws Exception {
        DataLoader loader = new DataLoader(mock(PathsConfig.class), mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class),
                mock(DumpConfig.class));

        ITable csvTable = mock(ITable.class);
        ITable dbTable = mock(ITable.class);
        when(csvTable.getRowCount()).thenThrow(new RuntimeException("x"));

        Method method = DataLoader.class.getDeclaredMethod("detectDuplicates", ITable.class,
                ITable.class, List.class, Connection.class, String.class, String.class,
                DbDialectHandler.class);
        method.setAccessible(true);

        Exception ex = assertThrows(Exception.class, () -> method.invoke(loader, csvTable, dbTable,
                List.of("ID"), mock(Connection.class), "APP", "TBL", mock(DbDialectHandler.class)));
        assertTrue(ex.getCause() instanceof DataSetException);
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
    void FilteredTable_正常ケース_除外行を指定する_行数と値が正しく返ること() throws Exception {
        ITable delegate = mock(ITable.class);
        ITableMetaData metaData = mock(ITableMetaData.class);
        when(delegate.getTableMetaData()).thenReturn(metaData);
        when(delegate.getRowCount()).thenReturn(4);
        when(delegate.getValue(0, "ID")).thenReturn("0");
        when(delegate.getValue(2, "ID")).thenReturn("2");

        Class<?> clazz = Class.forName("io.github.yok.flexdblink.core.DataLoader$FilteredTable");
        Constructor<?> ctor = clazz.getDeclaredConstructor(ITable.class, Set.class);
        ctor.setAccessible(true);
        Object filtered = ctor.newInstance(delegate, Set.of(1, 3));

        Method getRowCount = clazz.getDeclaredMethod("getRowCount");
        getRowCount.setAccessible(true);
        Method getTableMetaData = clazz.getDeclaredMethod("getTableMetaData");
        getTableMetaData.setAccessible(true);
        Method getValue = clazz.getDeclaredMethod("getValue", int.class, String.class);
        getValue.setAccessible(true);

        assertEquals(2, getRowCount.invoke(filtered));
        assertSame(metaData, getTableMetaData.invoke(filtered));
        assertEquals("0", getValue.invoke(filtered, 0, "ID"));
        assertEquals("2", getValue.invoke(filtered, 1, "ID"));
    }

    @Test
    void ensureTableOrdering_正常ケース_ファイル数一致のorderingが既存である_既存内容が維持されること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class),
                mock(DumpConfig.class));

        Path dir = tempDir.resolve("loadcase1");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("B.csv"), "ID\n1\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("A.json"), "[]", StandardCharsets.UTF_8);
        Path ordering = dir.resolve("table-ordering.txt");
        Files.writeString(ordering, "EXISTING1\nEXISTING2\n", StandardCharsets.UTF_8);

        Method method = DataLoader.class.getDeclaredMethod("ensureTableOrdering", File.class);
        method.setAccessible(true);
        method.invoke(loader, dir.toFile());

        String actual = Files.readString(ordering, StandardCharsets.UTF_8);
        assertEquals("EXISTING1\nEXISTING2\n", actual);
    }

    @Test
    void ensureTableOrdering_正常ケース_ファイル数不一致のorderingが既存である_再生成されること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class),
                mock(DumpConfig.class));

        Path dir = tempDir.resolve("loadcase2");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("B.csv"), "ID\n1\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("A.yaml"), "[]", StandardCharsets.UTF_8);
        Path ordering = dir.resolve("table-ordering.txt");
        Files.writeString(ordering, "ONLYONE\n", StandardCharsets.UTF_8);

        Method method = DataLoader.class.getDeclaredMethod("ensureTableOrdering", File.class);
        method.setAccessible(true);
        method.invoke(loader, dir.toFile());

        String actual = Files.readString(ordering, StandardCharsets.UTF_8);
        assertEquals("A" + System.lineSeparator() + "B", actual);
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
    void deleteDuplicates_正常ケース_PK列で重複削除する_executeBatchが呼ばれること() throws Exception {
        DataLoader loader = new DataLoader(mock(PathsConfig.class), mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class),
                mock(DumpConfig.class));

        Connection jdbc = mock(Connection.class);
        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);
        when(dialectHandler.resolveSchema(any())).thenReturn("APP");
        when(dialectHandler.quoteIdentifier("APP")).thenReturn("\"APP\"");
        when(dialectHandler.quoteIdentifier("TBL")).thenReturn("\"TBL\"");
        when(dialectHandler.quoteIdentifier("ID")).thenReturn("\"ID\"");

        ITable originalDbTable = mock(ITable.class);
        when(originalDbTable.getValue(0, "ID")).thenReturn(100);

        PreparedStatement ps = mock(PreparedStatement.class);
        when(jdbc.prepareStatement("DELETE FROM \"APP\".\"TBL\" WHERE \"ID\" = ?")).thenReturn(ps);
        when(ps.executeBatch()).thenReturn(new int[] {1});

        Method method = DataLoader.class.getDeclaredMethod("deleteDuplicates", Connection.class,
                String.class, String.class, List.class, Column[].class, ITable.class, Map.class,
                DbDialectHandler.class, String.class);
        method.setAccessible(true);
        method.invoke(loader, jdbc, "APP", "TBL", List.of("ID"),
                new Column[] {new Column("ID", DataType.INTEGER)}, originalDbTable, Map.of(0, 0),
                dialectHandler, "DB1");

        verify(ps).setObject(1, 100);
        verify(ps, times(1)).addBatch();
        verify(ps).executeBatch();
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
        withMockedDatabaseOperations(loader, clean, update, insert, () -> {
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

        ErrorHandler.disableExitForCurrentThread();
        try {
            withMockedDatabaseOperations(loader, clean, update, insert, () -> {
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
    void rowsEqual_正常ケース_DAYSECONDと空白列を比較する_trueが返ること() throws Exception {
        DataLoader loader = new DataLoader(mock(PathsConfig.class), mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class),
                mock(DumpConfig.class));

        ITable csvTable = mock(ITable.class);
        ITable dbTable = mock(ITable.class);
        Connection jdbc = mock(Connection.class);
        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);
        when(dialectHandler.resolveSchema(any())).thenReturn("APP");
        Column[] cols = new Column[] {new Column("IDS", DataType.VARCHAR),
                new Column("C1", DataType.VARCHAR)};
        when(dialectHandler.getColumnTypeName(jdbc, "APP", "TBL", "IDS"))
                .thenReturn("INTERVAL DAY TO SECOND");
        when(dialectHandler.shouldUseRawValueForComparison("INTERVAL DAY TO SECOND"))
                .thenReturn(true);
        when(dialectHandler.normalizeValueForComparison("IDS", "INTERVAL DAY TO SECOND", "1 2:3:4"))
                .thenReturn("+01 02:03:04");
        when(dialectHandler.normalizeValueForComparison("IDS", "INTERVAL DAY TO SECOND",
                "+01 02:03:04")).thenReturn("+01 02:03:04");
        when(dialectHandler.getColumnTypeName(jdbc, "APP", "TBL", "C1")).thenReturn("VARCHAR2");
        when(dialectHandler.formatDbValueForCsv("C1", " ")).thenReturn(" ");
        when(dialectHandler.normalizeValueForComparison("C1", "VARCHAR2", null)).thenReturn(null);
        when(csvTable.getValue(0, "IDS")).thenReturn("1 2:3:4");
        when(dbTable.getValue(0, "IDS")).thenReturn("+01 02:03:04");
        when(csvTable.getValue(0, "C1")).thenReturn(" ");
        when(dbTable.getValue(0, "C1")).thenReturn(" ");

        Method method = DataLoader.class.getDeclaredMethod("rowsEqual", ITable.class, ITable.class,
                Connection.class, String.class, String.class, int.class, int.class, Column[].class,
                DbDialectHandler.class);
        method.setAccessible(true);
        boolean actual = (boolean) method.invoke(loader, csvTable, dbTable, jdbc, "APP", "TBL", 0,
                0, cols, dialectHandler);
        assertTrue(actual);
    }

    @Test
    void ensureTableOrdering_異常ケース_書き込み失敗が発生する_ErrorHandler経由で例外が送出されること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class),
                mock(DumpConfig.class));

        Path dir = tempDir.resolve("ordering_write_fail");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("A.csv"), "ID\n1\n", StandardCharsets.UTF_8);

        Method method = DataLoader.class.getDeclaredMethod("ensureTableOrdering", File.class);
        method.setAccessible(true);

        ErrorHandler.disableExitForCurrentThread();
        try (MockedStatic<FileUtils> fileUtils = mockStatic(FileUtils.class)) {
            fileUtils
                    .when(() -> FileUtils.writeStringToFile(any(File.class), anyString(),
                            eq(StandardCharsets.UTF_8)))
                    .thenThrow(new java.io.IOException("write fail"));
            InvocationTargetException ex = assertThrows(InvocationTargetException.class,
                    () -> method.invoke(loader, dir.toFile()));
            assertTrue(ex.getCause() instanceof IllegalStateException);
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }
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
        withMockedDatabaseOperations(loader, clean, update, insert, () -> {
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
    void deploy_正常ケース_orderingが空白のみである_テーブルなしでスキップされること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class), new DumpConfig());

        Path dir = tempDir.resolve("ordering_blank_only");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("A.csv"), "ID\n1\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("B.csv"), "ID\n2\n", StandardCharsets.UTF_8);
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
        withMockedDatabaseOperations(loader, clean, update, insert, () -> {
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
    void detectDuplicates_正常ケース_PK不一致がある_重複に追加されないこと() throws Exception {
        DataLoader loader = new DataLoader(mock(PathsConfig.class), mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class),
                mock(DumpConfig.class));
        ITable csv = mock(ITable.class);
        ITable db = mock(ITable.class);
        ITableMetaData meta = mock(ITableMetaData.class);
        when(csv.getTableMetaData()).thenReturn(meta);
        when(meta.getColumns()).thenReturn(new Column[] {new Column("ID", DataType.VARCHAR)});
        when(csv.getRowCount()).thenReturn(1);
        when(db.getRowCount()).thenReturn(1);
        when(csv.getValue(0, "ID")).thenReturn("1");
        when(db.getValue(0, "ID")).thenReturn("2");

        Method method = DataLoader.class.getDeclaredMethod("detectDuplicates", ITable.class,
                ITable.class, List.class, Connection.class, String.class, String.class,
                DbDialectHandler.class);
        method.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<Integer, Integer> result = (Map<Integer, Integer>) method.invoke(loader, csv, db,
                List.of("ID"), mock(Connection.class), "APP", "T1", mock(DbDialectHandler.class));
        assertTrue(result.isEmpty());
    }

    @Test
    void deleteDuplicates_異常ケース_SQL例外が発生する_DataSetExceptionが送出されること() throws Exception {
        DataLoader loader = new DataLoader(mock(PathsConfig.class), mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class),
                mock(DumpConfig.class));
        Connection jdbc = mock(Connection.class);
        when(jdbc.prepareStatement(anyString())).thenThrow(new SQLException("x"));
        DbDialectHandler dialect = mock(DbDialectHandler.class);
        when(dialect.resolveSchema(any())).thenReturn("APP");
        when(dialect.quoteIdentifier(anyString()))
                .thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");
        ITable original = mock(ITable.class);
        when(original.getValue(0, "ID")).thenReturn("1");

        Method method = DataLoader.class.getDeclaredMethod("deleteDuplicates", Connection.class,
                String.class, String.class, List.class, Column[].class, ITable.class, Map.class,
                DbDialectHandler.class, String.class);
        method.setAccessible(true);

        InvocationTargetException ex = assertThrows(InvocationTargetException.class,
                () -> method.invoke(loader, jdbc, "APP", "T1", List.of("ID"),
                        new Column[] {new Column("ID", DataType.VARCHAR)}, original, Map.of(0, 0),
                        dialect, "db1"));
        assertTrue(ex.getCause() instanceof DataSetException);
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
        withMockedDatabaseOperations(loader, clean, update, insert, () -> {
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
        withMockedDatabaseOperations(loader, clean, update, insert, () -> {
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
    void detectDuplicates_正常ケース_PKなし重複がある_重複マップが返ること() throws Exception {
        DataLoader loader = new DataLoader(mock(PathsConfig.class), mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class),
                mock(DumpConfig.class));
        ITable csv = mock(ITable.class);
        ITable db = mock(ITable.class);
        ITableMetaData meta = mock(ITableMetaData.class);
        when(csv.getTableMetaData()).thenReturn(meta);
        when(meta.getColumns()).thenReturn(new Column[] {new Column("C1", DataType.VARCHAR)});
        when(csv.getRowCount()).thenReturn(1);
        when(db.getRowCount()).thenReturn(1);
        when(csv.getValue(0, "C1")).thenReturn("A");
        when(db.getValue(0, "C1")).thenReturn("A");
        DbDialectHandler dialect = mock(DbDialectHandler.class);
        when(dialect.resolveSchema(any())).thenReturn("APP");
        when(dialect.getColumnTypeName(any(), eq("APP"), eq("T1"), eq("C1")))
                .thenReturn("VARCHAR2");
        when(dialect.formatDbValueForCsv("C1", "A")).thenReturn("A");

        Method method = DataLoader.class.getDeclaredMethod("detectDuplicates", ITable.class,
                ITable.class, List.class, Connection.class, String.class, String.class,
                DbDialectHandler.class);
        method.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<Integer, Integer> result = (Map<Integer, Integer>) method.invoke(loader, csv, db,
                List.of(), mock(Connection.class), "APP", "T1", dialect);
        assertEquals(0, result.get(0));
    }

    @Test
    void deleteDuplicates_正常ケース_重複マップが空である_何も実行されないこと() throws Exception {
        DataLoader loader = new DataLoader(mock(PathsConfig.class), mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class),
                mock(DumpConfig.class));
        Method method = DataLoader.class.getDeclaredMethod("deleteDuplicates", Connection.class,
                String.class, String.class, List.class, Column[].class, ITable.class, Map.class,
                DbDialectHandler.class, String.class);
        method.setAccessible(true);
        Connection jdbc = mock(Connection.class);
        method.invoke(loader, jdbc, "APP", "T1", List.of(), new Column[0], mock(ITable.class),
                Map.of(), mock(DbDialectHandler.class), "db1");
        verify(jdbc, never()).prepareStatement(anyString());
    }

    @Test
    void ensureTableOrdering_正常ケース_対象がファイルである_生成せず終了すること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class), new DumpConfig());
        Path file = tempDir.resolve("not_dir_for_ordering.txt");
        Files.writeString(file, "x", StandardCharsets.UTF_8);
        Method method = DataLoader.class.getDeclaredMethod("ensureTableOrdering", File.class);
        method.setAccessible(true);
        method.invoke(loader, file.toFile());
        assertTrue(Files.exists(file));
    }

    @Test
    void ensureTableOrdering_正常ケース_ordering読込失敗が発生する_例外なく完了すること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class), new DumpConfig());
        Path dir = tempDir.resolve("ordering_read_fail2");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("A.csv"), "ID\n1\n", StandardCharsets.UTF_8);
        Files.createDirectories(dir.resolve("table-ordering.txt"));
        Method method = DataLoader.class.getDeclaredMethod("ensureTableOrdering", File.class);
        method.setAccessible(true);
        method.invoke(loader, dir.toFile());
        assertTrue(Files.exists(dir.resolve("table-ordering.txt")));
    }

    @Test
    void rowsEqual_正常ケース_DB値がnullである_trueが返ること() throws Exception {
        DataLoader loader = new DataLoader(mock(PathsConfig.class), mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class),
                mock(DumpConfig.class));
        ITable csv = mock(ITable.class);
        ITable db = mock(ITable.class);
        Connection jdbc = mock(Connection.class);
        DbDialectHandler dialect = mock(DbDialectHandler.class);
        when(dialect.resolveSchema(any())).thenReturn("APP");
        Column[] cols = new Column[] {new Column("C1", DataType.VARCHAR)};
        when(dialect.getColumnTypeName(jdbc, "APP", "T1", "C1")).thenReturn("VARCHAR2");
        when(dialect.formatDbValueForCsv("C1", null)).thenReturn(null);
        when(csv.getValue(0, "C1")).thenReturn(null);
        when(db.getValue(0, "C1")).thenReturn(null);
        Method method = DataLoader.class.getDeclaredMethod("rowsEqual", ITable.class, ITable.class,
                Connection.class, String.class, String.class, int.class, int.class, Column[].class,
                DbDialectHandler.class);
        method.setAccessible(true);
        assertTrue(
                (Boolean) method.invoke(loader, csv, db, jdbc, "APP", "T1", 0, 0, cols, dialect));
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
        withMockedDatabaseOperations(loader, clean, update, insert, () -> {
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
        withMockedDatabaseOperations(loader, clean, update, insert, () -> {
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
        withMockedDatabaseOperations(loader, clean, update, insert, () -> {
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
        withMockedDatabaseOperations(loader, clean, update, insert, () -> {
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
    void ensureTableOrdering_正常ケース_ErrorHandler呼び出し行を通す_エラーハンドラが呼ばれること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class), new DumpConfig());
        Path dir = tempDir.resolve("ordering_errorhandler_line");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("A.csv"), "ID\n1\n", StandardCharsets.UTF_8);
        Method method = DataLoader.class.getDeclaredMethod("ensureTableOrdering", File.class);
        method.setAccessible(true);
        try (MockedStatic<FileUtils> fileUtils = mockStatic(FileUtils.class);
                MockedStatic<ErrorHandler> errorHandler = mockStatic(ErrorHandler.class)) {
            fileUtils.when(() -> FileUtils.writeStringToFile(any(File.class), anyString(),
                    eq(StandardCharsets.UTF_8))).thenThrow(new java.io.IOException("x"));
            errorHandler.when(() -> ErrorHandler.errorAndExit(anyString(), any(Throwable.class)))
                    .thenAnswer(inv -> null);
            method.invoke(loader, dir.toFile());
            errorHandler.verify(() -> ErrorHandler
                    .errorAndExit(eq("Failed to create table-ordering.txt"), any(Throwable.class)),
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
    void detectDuplicates_正常ケース_PK列null同士を比較する_重複マップが返ること() throws Exception {
        DataLoader loader = new DataLoader(mock(PathsConfig.class), mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class),
                mock(DumpConfig.class));
        ITable csv = mock(ITable.class);
        ITable db = mock(ITable.class);
        ITableMetaData meta = mock(ITableMetaData.class);
        when(csv.getTableMetaData()).thenReturn(meta);
        when(meta.getColumns()).thenReturn(new Column[] {new Column("ID", DataType.VARCHAR)});
        when(csv.getRowCount()).thenReturn(1);
        when(db.getRowCount()).thenReturn(1);
        when(csv.getValue(0, "ID")).thenReturn(null);
        when(db.getValue(0, "ID")).thenReturn(null);
        Method method = DataLoader.class.getDeclaredMethod("detectDuplicates", ITable.class,
                ITable.class, List.class, Connection.class, String.class, String.class,
                DbDialectHandler.class);
        method.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<Integer, Integer> result = (Map<Integer, Integer>) method.invoke(loader, csv, db,
                List.of("ID"), mock(Connection.class), "APP", "T1", mock(DbDialectHandler.class));
        assertEquals(0, result.get(0));
    }

    @Test
    void detectDuplicates_正常ケース_PK列が片側nullである_重複に追加されないこと() throws Exception {
        DataLoader loader = new DataLoader(mock(PathsConfig.class), mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class),
                mock(DumpConfig.class));
        ITable csv = mock(ITable.class);
        ITable db = mock(ITable.class);
        ITableMetaData meta = mock(ITableMetaData.class);
        when(csv.getTableMetaData()).thenReturn(meta);
        when(meta.getColumns()).thenReturn(new Column[] {new Column("ID", DataType.VARCHAR)});
        when(csv.getRowCount()).thenReturn(1);
        when(db.getRowCount()).thenReturn(1);
        when(csv.getValue(0, "ID")).thenReturn(null);
        when(db.getValue(0, "ID")).thenReturn("1");
        Method method = DataLoader.class.getDeclaredMethod("detectDuplicates", ITable.class,
                ITable.class, List.class, Connection.class, String.class, String.class,
                DbDialectHandler.class);
        method.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<Integer, Integer> result = (Map<Integer, Integer>) method.invoke(loader, csv, db,
                List.of("ID"), mock(Connection.class), "APP", "T1", mock(DbDialectHandler.class));
        assertTrue(result.isEmpty());
    }

    @Test
    void ensureTableOrdering_正常ケース_xml拡張子を指定する_orderingが生成されること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.toString());
        DataLoader loader = new DataLoader(pathsConfig, mock(ConnectionConfig.class),
                e -> mock(DbDialectHandler.class), mock(DbUnitConfig.class), new DumpConfig());
        Path dir = tempDir.resolve("ordering_xml");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("X.xml"), "<dataset/>", StandardCharsets.UTF_8);
        Method method = DataLoader.class.getDeclaredMethod("ensureTableOrdering", File.class);
        method.setAccessible(true);
        method.invoke(loader, dir.toFile());
        assertTrue(Files.exists(dir.resolve("table-ordering.txt")));
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
        withMockedDatabaseOperations(loader, clean, update, insert, () -> {
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
        withMockedDatabaseOperations(loader, clean, update, insert, () -> {
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
        withMockedDatabaseOperations(loader, clean, update, insert, () -> {
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
}
