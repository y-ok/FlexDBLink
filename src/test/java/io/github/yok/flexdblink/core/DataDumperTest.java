package io.github.yok.flexdblink.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.FilePatternConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.db.DbDialectHandler;
import io.github.yok.flexdblink.util.ErrorHandler;
import io.github.yok.flexdblink.util.TableDependencyResolver;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.dbunit.database.DatabaseConnection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class DataDumperTest {

    @TempDir
    Path tempDir;

    @Test
    void fetchTargetTables_正常ケース_excludeTablesがnullでテーブルが複数ある_全件が返ること() throws Exception {
        DataDumper dumper = createDumper();
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);

        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getTables(null, "APP", "%", new String[] {"TABLE"})).thenReturn(rs);
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getString("TABLE_NAME")).thenReturn("T1", "T2");

        Method method = DataDumper.class.getDeclaredMethod("fetchTargetTables", Connection.class,
                String.class, List.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<String> result = (List<String>) method.invoke(dumper, conn, "APP", null);
        assertEquals(List.of("T1", "T2"), result);
    }

    @Test
    void fetchTargetTables_正常ケース_excludeTablesに一致するテーブルがある_除外された一覧が返ること() throws Exception {
        DataDumper dumper = createDumper();
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);

        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getTables(null, "APP", "%", new String[] {"TABLE"})).thenReturn(rs);
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getString("TABLE_NAME")).thenReturn("T1", "T2");

        Method method = DataDumper.class.getDeclaredMethod("fetchTargetTables", Connection.class,
                String.class, List.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<String> result = (List<String>) method.invoke(dumper, conn, "APP", List.of("t1"));
        assertEquals(List.of("T2"), result);
    }

    @Test
    void execute_異常ケース_シナリオが未指定である_ErrorHandlerが呼ばれること() {
        DataDumper dumper = createDumper();
        try (MockedStatic<ErrorHandler> handler = Mockito.mockStatic(ErrorHandler.class)) {
            handler.when(() -> ErrorHandler.errorAndExit(any())).thenAnswer(inv -> {
                throw new IllegalStateException("exit");
            });
            assertThrows(IllegalStateException.class, () -> dumper.execute(null, null));
            handler.verify(
                    () -> ErrorHandler.errorAndExit("Scenario name is required in --dump mode."));
        }
    }

    @Test
    void ensureDirectoryExists_異常ケース_mkdirs失敗するパスを指定する_ErrorHandlerが呼ばれること() throws Exception {
        DataDumper dumper = createDumper();
        Path blockingFile = tempDir.resolve("blocking");
        Files.writeString(blockingFile, "x", StandardCharsets.UTF_8);
        File target = blockingFile.resolve("child").toFile();

        Method method = DataDumper.class.getDeclaredMethod("ensureDirectoryExists", File.class,
                String.class);
        method.setAccessible(true);

        try (MockedStatic<ErrorHandler> handler = Mockito.mockStatic(ErrorHandler.class)) {
            handler.when(() -> ErrorHandler.errorAndExit(any())).thenAnswer(inv -> {
                throw new IllegalStateException("exit");
            });
            assertThrows(Exception.class, () -> method.invoke(dumper, target, "msg"));
            handler.verify(() -> ErrorHandler.errorAndExit("msg: " + target.getAbsolutePath()));
        }
    }

    @Test
    void backupScenarioDirectory_正常ケース_既存シナリオを指定する_バックアップ先へリネームされること() throws Exception {
        DataDumper dumper = createDumper();
        Path dataPath = tempDir.resolve("data");
        Path scenario = dataPath.resolve("scn");
        Files.createDirectories(scenario);
        Files.writeString(scenario.resolve("a.txt"), "x", StandardCharsets.UTF_8);

        Method method = DataDumper.class.getDeclaredMethod("backupScenarioDirectory", Path.class,
                File.class);
        method.setAccessible(true);
        method.invoke(dumper, dataPath, scenario.toFile());

        assertTrue(Files.exists(dataPath));
        assertTrue(Files.notExists(scenario));
        boolean found;
        try (Stream<Path> stream = Files.list(dataPath)) {
            found = stream.anyMatch(p -> p.getFileName().toString().startsWith("scn_"));
        }
        assertTrue(found);
    }

    @Test
    void backupScenarioDirectory_異常ケース_リネームが失敗する_ErrorHandlerが呼ばれること() throws Exception {
        DataDumper dumper = createDumper();
        Method method = DataDumper.class.getDeclaredMethod("backupScenarioDirectory", Path.class,
                File.class);
        method.setAccessible(true);

        class RenameFailFile extends File {
            private static final long serialVersionUID = 1L;

            RenameFailFile(String pathname) {
                super(pathname);
            }

            @Override
            public boolean renameTo(File dest) {
                return false;
            }
        }
        Path dataPath = Files.createDirectories(tempDir.resolve("data_rename_fail"));
        File scenario = new RenameFailFile(dataPath.resolve("scn").toString());
        try (MockedStatic<ErrorHandler> handler = Mockito.mockStatic(ErrorHandler.class)) {
            handler.when(() -> ErrorHandler.errorAndExit(any())).thenAnswer(inv -> null);
            method.invoke(dumper, dataPath, scenario);
            handler.verify(() -> ErrorHandler.errorAndExit(any()));
        }
    }

    @Test
    void prepareDbOutputDirs_正常ケース_ディレクトリ未作成で呼ぶ_DB用とfiles用が作成されること() throws Exception {
        DataDumper dumper = createDumper();
        Method method =
                DataDumper.class.getDeclaredMethod("prepareDbOutputDirs", File.class, String.class);
        method.setAccessible(true);

        File scenarioDir = tempDir.resolve("scenario").toFile();
        File[] dirs = (File[]) method.invoke(dumper, scenarioDir, "db1");

        assertEquals(2, dirs.length);
        assertTrue(dirs[0].exists());
        assertTrue(dirs[1].exists());
    }

    @Test
    void ensureDirectoryExists_正常ケース_既存ディレクトリを指定する_例外なく完了すること() throws Exception {
        DataDumper dumper = createDumper();
        Method method = DataDumper.class.getDeclaredMethod("ensureDirectoryExists", File.class,
                String.class);
        method.setAccessible(true);
        File existing = Files.createDirectories(tempDir.resolve("already_exists")).toFile();
        method.invoke(dumper, existing, "unused");
        assertTrue(existing.exists());
    }

    @Test
    void ensureDirectoryExists_異常ケース_mkdirs失敗時に終了処理を継続する_ErrorHandler呼び出し行が通過すること()
            throws Exception {
        DataDumper dumper = createDumper();
        Path blockingFile = tempDir.resolve("blocking2");
        Files.writeString(blockingFile, "x", StandardCharsets.UTF_8);
        File target = blockingFile.resolve("child").toFile();
        Method method = DataDumper.class.getDeclaredMethod("ensureDirectoryExists", File.class,
                String.class);
        method.setAccessible(true);
        try (MockedStatic<ErrorHandler> handler = Mockito.mockStatic(ErrorHandler.class)) {
            handler.when(() -> ErrorHandler.errorAndExit(any())).thenAnswer(inv -> null);
            method.invoke(dumper, target, "msg");
            handler.verify(() -> ErrorHandler.errorAndExit("msg: " + target.getAbsolutePath()));
        }
    }

    @Test
    void execute_正常ケース_対象DBに含まれない接続を指定する_DriverManager接続が呼ばれないこと() {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.resolve("data1").toString());

        ConnectionConfig config = new ConnectionConfig();
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:dummy");
        entry.setUser("u");
        entry.setPassword("p");
        config.setConnections(List.of(entry));

        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(List.of());

        DataDumper dumper = new DataDumper(pathsConfig, config, new FilePatternConfig(), dumpConfig,
                e -> createDialectHandlerMock());

        try (MockedStatic<DriverManager> driverManager = Mockito.mockStatic(DriverManager.class)) {
            dumper.execute("scenario", List.of("db2"));
            driverManager.verifyNoInteractions();
        }
    }

    @Test
    void execute_正常ケース_対象テーブルが0件である_DBUnit接続がクローズされること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.resolve("data2").toString());
        Files.createDirectories(tempDir.resolve("data2"));

        ConnectionConfig config = new ConnectionConfig();
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:dummy");
        entry.setUser("u");
        entry.setPassword("p");
        config.setConnections(List.of(entry));

        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(null);

        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        DbDialectHandler dialectHandler = createDialectHandlerMock();
        DatabaseConnection dbConn = mock(DatabaseConnection.class);

        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getTables(null, "APP", "%", new String[] {"TABLE"})).thenReturn(rs);
        when(rs.next()).thenReturn(false);
        when(dialectHandler.createDbUnitConnection(conn, "APP")).thenReturn(dbConn);

        DataDumper dumper = new DataDumper(pathsConfig, config, new FilePatternConfig(), dumpConfig,
                e -> dialectHandler);

        try (MockedStatic<DriverManager> driverManager = Mockito.mockStatic(DriverManager.class)) {
            driverManager.when(() -> DriverManager.getConnection("jdbc:dummy", "u", "p"))
                    .thenReturn(conn);
            dumper.execute("scenario", List.of("db1"));
        }

        verify(dialectHandler).createDbUnitConnection(conn, "APP");
        verify(dbConn).close();
    }

    @Test
    void execute_正常ケース_既存シナリオをバックアップして1テーブルを出力する_出力CSVが生成されること() throws Exception {
        Path dataRoot = Files.createDirectories(tempDir.resolve("data_exec_ok"));
        Path dumpRoot = Files.createDirectories(dataRoot.resolve("dump"));
        Path scenarioExisting = Files.createDirectories(dumpRoot.resolve("scn_ok"));
        Files.writeString(scenarioExisting.resolve("old.txt"), "x", StandardCharsets.UTF_8);

        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(dataRoot.toString());

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:execok");
        entry.setUser("u");
        entry.setPassword("p");
        ConnectionConfig config = new ConnectionConfig();
        config.setConnections(List.of(entry));

        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(List.of());

        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        when(filePatternConfig.getPatternsForTable("T1")).thenReturn(Collections.emptyMap());

        Connection conn = mock(Connection.class);
        when(conn.getSchema()).thenReturn("APP");
        when(conn.getCatalog()).thenReturn(null);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet tableRs = mock(ResultSet.class);
        when(meta.getTables(null, "APP", "%", new String[] {"TABLE"})).thenReturn(tableRs);
        when(tableRs.next()).thenReturn(true, false);
        when(tableRs.getString("TABLE_NAME")).thenReturn("T1");
        ResultSet pkRsForExport = mock(ResultSet.class);
        ResultSet pkRsForDump = mock(ResultSet.class);
        when(meta.getPrimaryKeys(any(), eq("APP"), eq("T1"))).thenReturn(pkRsForExport,
                pkRsForDump);
        when(pkRsForExport.next()).thenReturn(false);
        when(pkRsForDump.next()).thenReturn(false);

        Statement stmtExport = mock(Statement.class);
        Statement stmtDump = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmtExport, stmtDump);

        // CsvTableExporter: single SELECT * query (headers + data from same ResultSet)
        ResultSet rsExport = mock(ResultSet.class);
        when(stmtExport.executeQuery("SELECT * FROM \"T1\"")).thenReturn(rsExport);
        ResultSetMetaData mdExport = mock(ResultSetMetaData.class);
        when(rsExport.getMetaData()).thenReturn(mdExport);
        when(mdExport.getColumnCount()).thenReturn(1);
        when(mdExport.getColumnLabel(1)).thenReturn("ID");
        when(mdExport.getColumnType(1)).thenReturn(Types.VARCHAR);
        when(mdExport.getColumnTypeName(1)).thenReturn("VARCHAR2");
        when(rsExport.next()).thenReturn(true, false);
        when(rsExport.getObject(1)).thenReturn("1");

        // LobFileExporter: SELECT * query
        ResultSet rsDump = mock(ResultSet.class);
        when(stmtDump.executeQuery("SELECT * FROM \"T1\"")).thenReturn(rsDump);
        ResultSetMetaData mdDump = mock(ResultSetMetaData.class);
        when(rsDump.getMetaData()).thenReturn(mdDump);
        when(mdDump.getColumnCount()).thenReturn(1);
        when(mdDump.getColumnLabel(1)).thenReturn("ID");
        when(mdDump.getColumnType(1)).thenReturn(Types.VARCHAR);
        when(rsDump.next()).thenReturn(true, false);
        when(rsDump.getString(1)).thenReturn("1");

        DbDialectHandler dialect = createDialectHandlerMock();
        when(dialect.quoteIdentifier(any())).thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");
        when(dialect.createDbUnitConnection(eq(conn), eq("APP")))
                .thenReturn(mock(DatabaseConnection.class));

        DataDumper dumper =
                new DataDumper(pathsConfig, config, filePatternConfig, dumpConfig, e -> dialect);
        try (MockedStatic<DriverManager> driverManager = Mockito.mockStatic(DriverManager.class)) {
            driverManager.when(() -> DriverManager.getConnection("jdbc:execok", "u", "p"))
                    .thenReturn(conn);
            dumper.execute("scn_ok", List.of("db1"));
        }

        Path outCsv = dumpRoot.resolve("scn_ok").resolve("db1").resolve("T1.csv");
        assertTrue(Files.exists(outCsv));
    }

    @Test
    void execute_異常ケース_ドライバ読込に失敗する_ErrorHandlerが呼ばれること() {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.resolve("data_exec_fail").toString());
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        entry.setDriverClass("no.such.Driver");
        entry.setUrl("jdbc:never");
        entry.setUser("u");
        entry.setPassword("p");
        ConnectionConfig config = new ConnectionConfig();
        config.setConnections(List.of(entry));
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(List.of());
        DataDumper dumper = new DataDumper(pathsConfig, config, new FilePatternConfig(), dumpConfig,
                e -> createDialectHandlerMock());
        try (MockedStatic<ErrorHandler> handler = Mockito.mockStatic(ErrorHandler.class)) {
            handler.when(() -> ErrorHandler.errorAndExit(any(), any())).thenAnswer(inv -> {
                throw new IllegalStateException("exit");
            });
            assertThrows(IllegalStateException.class,
                    () -> dumper.execute("scn_fail", List.of("db1")));
            handler.verify(() -> ErrorHandler.errorAndExit(eq("Dump failed (DB=db1)"), any()));
        }
    }

    @Test
    void execute_異常ケース_シナリオ未指定でも終了処理を継続する_必須チェック行が通過すること() {
        DataDumper dumper = createDumper();
        try (MockedStatic<ErrorHandler> handler = Mockito.mockStatic(ErrorHandler.class)) {
            handler.when(() -> ErrorHandler.errorAndExit(any())).thenAnswer(inv -> null);
            assertThrows(NullPointerException.class, () -> dumper.execute(null, null));
            handler.verify(
                    () -> ErrorHandler.errorAndExit("Scenario name is required in --dump mode."));
        }
    }

    @Test
    void execute_異常ケース_シナリオが空文字である_必須チェック行が通過すること() {
        DataDumper dumper = createDumper();
        try (MockedStatic<ErrorHandler> handler =
                org.mockito.Mockito.mockStatic(ErrorHandler.class)) {
            handler.when(() -> ErrorHandler.errorAndExit(any())).thenAnswer(inv -> null);
            assertThrows(NullPointerException.class, () -> dumper.execute("", null));
            handler.verify(
                    () -> ErrorHandler.errorAndExit("Scenario name is required in --dump mode."));
        }
    }

    @Test
    void execute_正常ケース_対象DBリストが空である_全DBが処理されること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        Path dataRoot = Files.createDirectories(tempDir.resolve("data_exec_empty_target"));
        pathsConfig.setDataPath(dataRoot.toString());
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:emptytarget");
        entry.setUser("u");
        entry.setPassword("p");
        ConnectionConfig config = new ConnectionConfig();
        config.setConnections(List.of(entry));
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(List.of());
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        DbDialectHandler dialect = createDialectHandlerMock();
        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getTables(null, "APP", "%", new String[] {"TABLE"})).thenReturn(rs);
        when(rs.next()).thenReturn(false);
        when(dialect.createDbUnitConnection(conn, "APP")).thenReturn(dbConn);
        DataDumper dumper = new DataDumper(pathsConfig, config, new FilePatternConfig(), dumpConfig,
                e -> dialect);
        try (MockedStatic<DriverManager> driverManager = Mockito.mockStatic(DriverManager.class)) {
            driverManager.when(() -> DriverManager.getConnection("jdbc:emptytarget", "u", "p"))
                    .thenReturn(conn);
            dumper.execute("scenario_empty_target", List.of());
        }
        verify(dbConn).close();
    }

    @Test
    void execute_正常ケース_対象DBリストがnullである_全DBが処理されること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        Path dataRoot = Files.createDirectories(tempDir.resolve("data_exec_null_target"));
        pathsConfig.setDataPath(dataRoot.toString());
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:nulltarget");
        entry.setUser("u");
        entry.setPassword("p");
        ConnectionConfig config = new ConnectionConfig();
        config.setConnections(List.of(entry));
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(List.of());
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        DbDialectHandler dialect = createDialectHandlerMock();
        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getTables(null, "APP", "%", new String[] {"TABLE"})).thenReturn(rs);
        when(rs.next()).thenReturn(false);
        when(dialect.createDbUnitConnection(conn, "APP")).thenReturn(dbConn);
        DataDumper dumper = new DataDumper(pathsConfig, config, new FilePatternConfig(), dumpConfig,
                e -> dialect);
        try (MockedStatic<DriverManager> driverManager = Mockito.mockStatic(DriverManager.class)) {
            driverManager.when(() -> DriverManager.getConnection("jdbc:nulltarget", "u", "p"))
                    .thenReturn(conn);
            dumper.execute("scenario_null_target", null);
        }
        verify(dbConn).close();
    }

    @Test
    void execute_異常ケース_接続クローズで例外が発生する_ErrorHandlerが呼ばれること() throws Exception {
        PathsConfig pathsConfig = new PathsConfig();
        Path dataRoot = Files.createDirectories(tempDir.resolve("data_exec_close_fail"));
        pathsConfig.setDataPath(dataRoot.toString());
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:closefail");
        entry.setUser("u");
        entry.setPassword("p");
        ConnectionConfig config = new ConnectionConfig();
        config.setConnections(List.of(entry));
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(List.of());
        Connection conn = mock(Connection.class);
        doThrow(new java.sql.SQLException("close failed")).when(conn).close();
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        DbDialectHandler dialect = createDialectHandlerMock();
        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getTables(null, "APP", "%", new String[] {"TABLE"})).thenReturn(rs);
        when(rs.next()).thenReturn(false);
        when(dialect.createDbUnitConnection(conn, "APP")).thenReturn(dbConn);
        DataDumper dumper = new DataDumper(pathsConfig, config, new FilePatternConfig(), dumpConfig,
                e -> dialect);
        try (MockedStatic<DriverManager> driverManager = Mockito.mockStatic(DriverManager.class);
                MockedStatic<ErrorHandler> handler = Mockito.mockStatic(ErrorHandler.class)) {
            driverManager.when(() -> DriverManager.getConnection("jdbc:closefail", "u", "p"))
                    .thenReturn(conn);
            handler.when(() -> ErrorHandler.errorAndExit(any(), any())).thenAnswer(inv -> null);
            dumper.execute("scenario_close_fail", List.of("db1"));
            handler.verify(() -> ErrorHandler.errorAndExit(eq("Dump failed (DB=db1)"), any()));
        }
    }

    @Test
    void execute_異常ケース_ダンプ後の接続クローズで例外が発生する_ErrorHandlerが呼ばれること() throws Exception {
        Path dataRoot = Files.createDirectories(tempDir.resolve("data_exec_close_fail_after_dump"));
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(dataRoot.toString());
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:closefail2");
        entry.setUser("u");
        entry.setPassword("p");
        ConnectionConfig config = new ConnectionConfig();
        config.setConnections(List.of(entry));
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(List.of());
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        when(filePatternConfig.getPatternsForTable("T1")).thenReturn(Collections.emptyMap());
        Connection conn = mock(Connection.class);
        doThrow(new java.sql.SQLException("close failed")).when(conn).close();
        when(conn.getSchema()).thenReturn("APP");
        when(conn.getCatalog()).thenReturn(null);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet tableRs = mock(ResultSet.class);
        when(meta.getTables(null, "APP", "%", new String[] {"TABLE"})).thenReturn(tableRs);
        when(tableRs.next()).thenReturn(true, false);
        when(tableRs.getString("TABLE_NAME")).thenReturn("T1");
        ResultSet pkRsForExport = mock(ResultSet.class);
        ResultSet pkRsForDump = mock(ResultSet.class);
        when(meta.getPrimaryKeys(any(), eq("APP"), eq("T1"))).thenReturn(pkRsForExport,
                pkRsForDump);
        when(pkRsForExport.next()).thenReturn(false);
        when(pkRsForDump.next()).thenReturn(false);
        Statement stmtExport = mock(Statement.class);
        Statement stmtDump = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmtExport, stmtDump);
        // CsvTableExporter: single SELECT * query
        ResultSet rsExport = mock(ResultSet.class);
        when(stmtExport.executeQuery("SELECT * FROM \"T1\"")).thenReturn(rsExport);
        ResultSetMetaData mdExport = mock(ResultSetMetaData.class);
        when(rsExport.getMetaData()).thenReturn(mdExport);
        when(mdExport.getColumnCount()).thenReturn(1);
        when(mdExport.getColumnLabel(1)).thenReturn("ID");
        when(mdExport.getColumnType(1)).thenReturn(Types.VARCHAR);
        when(mdExport.getColumnTypeName(1)).thenReturn("VARCHAR2");
        when(rsExport.next()).thenReturn(true, false);
        when(rsExport.getObject(1)).thenReturn("1");
        // LobFileExporter: SELECT * query
        ResultSet rsDump = mock(ResultSet.class);
        when(stmtDump.executeQuery("SELECT * FROM \"T1\"")).thenReturn(rsDump);
        ResultSetMetaData mdDump = mock(ResultSetMetaData.class);
        when(rsDump.getMetaData()).thenReturn(mdDump);
        when(mdDump.getColumnCount()).thenReturn(1);
        when(mdDump.getColumnLabel(1)).thenReturn("ID");
        when(mdDump.getColumnType(1)).thenReturn(Types.VARCHAR);
        when(rsDump.next()).thenReturn(true, false);
        when(rsDump.getString(1)).thenReturn("1");
        DbDialectHandler dialect = createDialectHandlerMock();
        when(dialect.quoteIdentifier(any())).thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");
        when(dialect.createDbUnitConnection(eq(conn), eq("APP")))
                .thenReturn(mock(org.dbunit.database.DatabaseConnection.class));
        DataDumper dumper =
                new DataDumper(pathsConfig, config, filePatternConfig, dumpConfig, e -> dialect);
        try (MockedStatic<DriverManager> driverManager =
                org.mockito.Mockito.mockStatic(DriverManager.class);
                MockedStatic<ErrorHandler> handler =
                        org.mockito.Mockito.mockStatic(ErrorHandler.class)) {
            driverManager.when(() -> DriverManager.getConnection("jdbc:closefail2", "u", "p"))
                    .thenReturn(conn);
            handler.when(() -> ErrorHandler.errorAndExit(any(), any())).thenAnswer(inv -> null);
            dumper.execute("scenario_close_fail_after_dump", List.of("db1"));
            handler.verify(() -> ErrorHandler.errorAndExit(eq("Dump failed (DB=db1)"), any()));
        }
    }

    @Test
    void execute_異常ケース_ドライバ読込失敗時に終了処理を継続する_失敗通知行が通過すること() {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(tempDir.resolve("data_exec_fail2").toString());
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        entry.setDriverClass("no.such.Driver");
        entry.setUrl("jdbc:never");
        entry.setUser("u");
        entry.setPassword("p");
        ConnectionConfig config = new ConnectionConfig();
        config.setConnections(List.of(entry));
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(List.of());
        DataDumper dumper = new DataDumper(pathsConfig, config, new FilePatternConfig(), dumpConfig,
                e -> createDialectHandlerMock());
        try (MockedStatic<ErrorHandler> handler =
                org.mockito.Mockito.mockStatic(ErrorHandler.class)) {
            handler.when(() -> ErrorHandler.errorAndExit(any(), any())).thenAnswer(inv -> null);
            dumper.execute("scn_fail2", List.of("db1"));
            handler.verify(() -> ErrorHandler.errorAndExit(eq("Dump failed (DB=db1)"), any()));
        }
    }

    @Test
    public void execute_異常ケース_tableOrdering書き込みに失敗する_ErrorHandlerから例外が送出されること() throws Exception {

        PathsConfig pathsConfig = mock(PathsConfig.class);
        ConnectionConfig connectionConfig = mock(ConnectionConfig.class);
        DumpConfig dumpConfig = mock(DumpConfig.class);
        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);

        // execute() の先頭で使用される
        Path dumpBaseDir = tempDir.resolve("dump_base");
        when(pathsConfig.getDump()).thenReturn(dumpBaseDir.toString());
        when(pathsConfig.getDataPath()).thenReturn(tempDir.toString());
        when(dumpConfig.getExcludeTables()).thenReturn(List.of());

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:dummy");
        entry.setUser("u");
        entry.setPassword("p");
        when(connectionConfig.getConnections()).thenReturn(List.of(entry));

        DbDialectHandler dialectHandler = createDialectHandlerMock();
        when(dialectHandler.resolveSchema(any())).thenReturn("APP");
        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        when(dialectHandler.createDbUnitConnection(any(), eq("APP"))).thenReturn(dbConn);

        DataDumper dumper = new DataDumper(pathsConfig, connectionConfig, filePatternConfig,
                dumpConfig, e -> dialectHandler);

        // fetchTargetTables 用のメタデータを最低限スタブ
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet tableRs = mock(ResultSet.class);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getTables(any(), eq("APP"), any(), any())).thenReturn(tableRs);
        when(tableRs.next()).thenReturn(true, false);
        when(tableRs.getString("TABLE_NAME")).thenReturn("T1");

        // FK解決は本筋ではないので固定値で返す
        IOException io = new IOException("write fail");

        ErrorHandler.disableExitForCurrentThread();
        try (MockedStatic<DriverManager> driverManager = Mockito.mockStatic(DriverManager.class);
                MockedStatic<TableDependencyResolver> resolver =
                        Mockito.mockStatic(TableDependencyResolver.class);
                MockedStatic<FileUtils> fileUtils = Mockito.mockStatic(FileUtils.class)) {

            driverManager.when(() -> DriverManager.getConnection("jdbc:dummy", "u", "p"))
                    .thenReturn(conn);

            resolver.when(
                    () -> TableDependencyResolver.resolveLoadOrder(any(), any(), any(), any()))
                    .thenAnswer(inv -> inv.getArgument(3));

            fileUtils.when(() -> FileUtils.writeStringToFile(any(File.class), anyString(),
                    eq(StandardCharsets.UTF_8))).thenThrow(io);

            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> dumper.execute("scn", List.of("db1")));

            assertEquals("Dump failed (DB=db1)", ex.getMessage());
            assertTrue(ex.getCause() instanceof IllegalStateException);
            assertEquals("Failed to create table-ordering.txt", ex.getCause().getMessage());
            assertSame(io, ex.getCause().getCause());

            fileUtils.verify(() -> FileUtils.writeStringToFile(any(File.class), anyString(),
                    eq(StandardCharsets.UTF_8)));

        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }
    }

    @Test
    void execute_正常ケース_FK解決でSQLException_元の順序でダンプが継続されること() throws Exception {
        Path dataRoot = Files.createDirectories(tempDir.resolve("data_exec_fkfail"));
        Path dumpRoot = Files.createDirectories(dataRoot.resolve("dump"));

        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(dataRoot.toString());

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        entry.setDriverClass("java.lang.String");
        entry.setUrl("jdbc:fkfail");
        entry.setUser("u");
        entry.setPassword("p");
        ConnectionConfig config = new ConnectionConfig();
        config.setConnections(List.of(entry));

        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(List.of());

        FilePatternConfig filePatternConfig = mock(FilePatternConfig.class);
        when(filePatternConfig.getPatternsForTable("T1")).thenReturn(Collections.emptyMap());

        Connection conn = mock(Connection.class);
        when(conn.getSchema()).thenReturn("APP");
        when(conn.getCatalog()).thenReturn(null);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);

        ResultSet tableRs = mock(ResultSet.class);
        when(meta.getTables(null, "APP", "%", new String[] {"TABLE"})).thenReturn(tableRs);
        when(tableRs.next()).thenReturn(true, false);
        when(tableRs.getString("TABLE_NAME")).thenReturn("T1");

        ResultSet pkRsForExport = mock(ResultSet.class);
        ResultSet pkRsForDump = mock(ResultSet.class);
        when(meta.getPrimaryKeys(any(), eq("APP"), eq("T1"))).thenReturn(pkRsForExport,
                pkRsForDump);
        when(pkRsForExport.next()).thenReturn(false);
        when(pkRsForDump.next()).thenReturn(false);

        Statement stmtExport = mock(Statement.class);
        Statement stmtDump = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmtExport, stmtDump);

        // CsvTableExporter: single SELECT * query (headers + data from same ResultSet)
        ResultSet rsExport = mock(ResultSet.class);
        when(stmtExport.executeQuery("SELECT * FROM \"T1\"")).thenReturn(rsExport);
        ResultSetMetaData mdExport = mock(ResultSetMetaData.class);
        when(rsExport.getMetaData()).thenReturn(mdExport);
        when(mdExport.getColumnCount()).thenReturn(1);
        when(mdExport.getColumnLabel(1)).thenReturn("ID");
        when(mdExport.getColumnType(1)).thenReturn(Types.VARCHAR);
        when(mdExport.getColumnTypeName(1)).thenReturn("VARCHAR2");
        when(rsExport.next()).thenReturn(true, false);
        when(rsExport.getObject(1)).thenReturn("1");

        // LobFileExporter: SELECT * query
        ResultSet rsDump = mock(ResultSet.class);
        when(stmtDump.executeQuery("SELECT * FROM \"T1\"")).thenReturn(rsDump);
        ResultSetMetaData mdDump = mock(ResultSetMetaData.class);
        when(rsDump.getMetaData()).thenReturn(mdDump);
        when(mdDump.getColumnCount()).thenReturn(1);
        when(mdDump.getColumnLabel(1)).thenReturn("ID");
        when(mdDump.getColumnType(1)).thenReturn(Types.VARCHAR);
        when(rsDump.next()).thenReturn(true, false);
        when(rsDump.getString(1)).thenReturn("1");

        DbDialectHandler dialect = createDialectHandlerMock();
        when(dialect.quoteIdentifier(any())).thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");
        when(dialect.createDbUnitConnection(eq(conn), eq("APP")))
                .thenReturn(mock(org.dbunit.database.DatabaseConnection.class));

        DataDumper dumper =
                new DataDumper(pathsConfig, config, filePatternConfig, dumpConfig, e -> dialect);
        // thenThrow に渡すオブジェクトはモックコンテキスト外で生成する（UnfinishedStubbingException を回避）
        SQLException fkException = new SQLException("FK resolution failed");
        try (MockedStatic<DriverManager> driverManager =
                org.mockito.Mockito.mockStatic(DriverManager.class);
                MockedStatic<TableDependencyResolver> resolver =
                        org.mockito.Mockito.mockStatic(TableDependencyResolver.class)) {
            driverManager.when(() -> DriverManager.getConnection("jdbc:fkfail", "u", "p"))
                    .thenReturn(conn);
            // FK解決で SQLException をスロー → warn ログ後に元の順序（T1）でダンプが継続されること
            resolver.when(() -> TableDependencyResolver.resolveLoadOrder(eq(conn), isNull(),
                    eq("APP"), eq(List.of("T1")))).thenThrow(fkException);
            dumper.execute("scn_fkfail", List.of("db1"));
        }

        // FK解決失敗後も元の順序でダンプが完了し、CSVが生成されること
        Path outCsv = dumpRoot.resolve("scn_fkfail").resolve("db1").resolve("T1.csv");
        assertTrue(Files.exists(outCsv));
    }

    @Test
    void logTableSummary_正常ケース_複数テーブル件数を指定する_例外なく完了すること() throws Exception {
        DataDumper dumper = createDumper();
        Method method =
                DataDumper.class.getDeclaredMethod("logTableSummary", String.class, Map.class);
        method.setAccessible(true);

        Map<String, Integer> counts = new LinkedHashMap<>();
        counts.put("T1", 1);
        counts.put("LONG_TABLE", 12);

        method.invoke(dumper, "DB1", counts);
        assertEquals(2, counts.size());
    }

    private DataDumper createDumper() {
        return createDumper(mock(FilePatternConfig.class));
    }

    private DbDialectHandler createDialectHandlerMock() {
        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);
        when(dialectHandler.quoteIdentifier(any()))
                .thenAnswer(invocation -> "\"" + invocation.getArgument(0) + "\"");
        when(dialectHandler.resolveSchema(any())).thenReturn("APP");
        try {
            when(dialectHandler.formatDbValueForCsv(nullable(String.class), any()))
                    .thenAnswer(invocation -> {
                        Object value = invocation.getArgument(1);
                        if (value == null) {
                            return "";
                        }
                        return value.toString();
                    });
            when(dialectHandler.formatDateTimeColumn(nullable(String.class), any(), any()))
                    .thenAnswer(invocation -> {
                        Object value = invocation.getArgument(1);
                        if (value == null) {
                            return "";
                        }
                        return value.toString();
                    });
            when(dialectHandler.isDateTimeTypeForDump(anyInt(), nullable(String.class)))
                    .thenAnswer(invocation -> {
                        int sqlType = invocation.getArgument(0);
                        String typeName = invocation.getArgument(1);
                        if (sqlType == Types.DATE || sqlType == Types.TIME
                                || sqlType == Types.TIMESTAMP || sqlType == -101 || sqlType == -102
                                || sqlType == Types.TIMESTAMP_WITH_TIMEZONE) {
                            return true;
                        }
                        if (typeName == null) {
                            return false;
                        }
                        String normalized = typeName.toUpperCase(Locale.ROOT);
                        return normalized.contains("TIMESTAMP") || normalized.contains("DATE")
                                || normalized.contains("TIME");
                    });
            when(dialectHandler.isBinaryTypeForDump(anyInt(), nullable(String.class)))
                    .thenAnswer(invocation -> {
                        int sqlType = invocation.getArgument(0);
                        String typeName = invocation.getArgument(1);
                        if (sqlType == Types.BINARY || sqlType == Types.VARBINARY
                                || sqlType == Types.LONGVARBINARY) {
                            return true;
                        }
                        if (typeName == null) {
                            return false;
                        }
                        String normalized = typeName.toUpperCase(Locale.ROOT);
                        return "RAW".equals(normalized) || "LONG RAW".equals(normalized);
                    });
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to stub formatDbValueForCsv", e);
        }
        return dialectHandler;
    }

    private DataDumper createDumper(FilePatternConfig filePatternConfig) {
        PathsConfig pathsConfig = mock(PathsConfig.class);
        ConnectionConfig connectionConfig = mock(ConnectionConfig.class);
        DumpConfig dumpConfig = mock(DumpConfig.class);
        when(dumpConfig.getExcludeTables()).thenReturn(Collections.emptyList());

        return new DataDumper(pathsConfig, connectionConfig, filePatternConfig, dumpConfig,
                e -> createDialectHandlerMock());
    }
}
