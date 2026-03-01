package io.github.yok.flexdblink.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.db.DbDialectHandler;
import io.github.yok.flexdblink.util.ErrorHandler;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Unit tests for {@link SetupRunner}.
 */
class SetupRunnerTest {

    @TempDir
    Path tempDir;

    private ConnectionConfig connectionConfig;
    private DbDialectHandler dialectHandler;
    private Connection conn;
    private DatabaseMetaData meta;

    @BeforeEach
    void setUp() throws Exception {
        dialectHandler = mock(DbDialectHandler.class);
        conn = mock(Connection.class);
        meta = mock(DatabaseMetaData.class);

        when(conn.getMetaData()).thenReturn(meta);
        when(dialectHandler.resolveSchema(any())).thenReturn("SCHEMA");

        connectionConfig = new ConnectionConfig();
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("DB1");
        entry.setUrl("jdbc:postgresql://localhost/test");
        entry.setUser("sa");
        entry.setPassword("");
        entry.setDriverClass("org.postgresql.Driver");
        connectionConfig.setConnections(List.of(entry));
    }

    @Test
    void typeNameExtensions_正常ケース_Oracle系型が正しくマッピングされること() {
        assertEquals("nclob", SetupRunner.TYPE_NAME_EXTENSIONS.get("nclob"));
        assertEquals("clob", SetupRunner.TYPE_NAME_EXTENSIONS.get("clob"));
        assertEquals("dat", SetupRunner.TYPE_NAME_EXTENSIONS.get("blob"));
    }

    @Test
    void typeNameExtensions_正常ケース_PostgreSQL系型が正しくマッピングされること() {
        assertEquals("bin", SetupRunner.TYPE_NAME_EXTENSIONS.get("bytea"));
    }

    @Test
    void typeNameExtensions_正常ケース_MySQL_BLOB系型が正しくマッピングされること() {
        assertEquals("dat", SetupRunner.TYPE_NAME_EXTENSIONS.get("tinyblob"));
        assertEquals("dat", SetupRunner.TYPE_NAME_EXTENSIONS.get("mediumblob"));
        assertEquals("dat", SetupRunner.TYPE_NAME_EXTENSIONS.get("longblob"));
    }

    @Test
    void typeNameExtensions_正常ケース_MySQL_TEXT系型が正しくマッピングされること() {
        assertEquals("clob", SetupRunner.TYPE_NAME_EXTENSIONS.get("tinytext"));
        assertEquals("clob", SetupRunner.TYPE_NAME_EXTENSIONS.get("mediumtext"));
        assertEquals("clob", SetupRunner.TYPE_NAME_EXTENSIONS.get("longtext"));
    }

    @Test
    void typeNameExtensions_正常ケース_SQLServer系型が正しくマッピングされること() {
        assertEquals("bin", SetupRunner.TYPE_NAME_EXTENSIONS.get("varbinary"));
        assertEquals("bin", SetupRunner.TYPE_NAME_EXTENSIONS.get("image"));
    }

    @Test
    void resolveExtension_正常ケース_型名優先でblobがdatになること() {
        SetupRunner runner = newRunner();
        assertEquals("dat", runner.resolveExtension("blob", Types.BLOB));
    }

    @Test
    void resolveExtension_正常ケース_型名が大文字でもマッチすること() {
        SetupRunner runner = newRunner();
        assertEquals("dat", runner.resolveExtension("BLOB", Types.BLOB));
    }

    @Test
    void resolveExtension_正常ケース_型名が未知でJDBC型のデフォルトが使われること() {
        SetupRunner runner = newRunner();
        assertEquals("dat", runner.resolveExtension("unknown_type", Types.BLOB));
        assertEquals("clob", runner.resolveExtension("unknown_type", Types.CLOB));
        assertEquals("nclob", runner.resolveExtension("unknown_type", Types.NCLOB));
    }

    @Test
    void resolveExtension_正常ケース_型名nullのときJDBC型フォールバックが使われること() {
        SetupRunner runner = newRunner();
        assertEquals("clob", runner.resolveExtension(null, Types.CLOB));
    }

    @Test
    void resolveExtension_正常ケース_型名も未知JDBC型も未知のときbinが返ること() {
        SetupRunner runner = newRunner();
        assertEquals("bin", runner.resolveExtension("completely_unknown", Types.OTHER));
    }

    @Test
    void isLobType_正常ケース_JDBC_BLOBはtrueになること() {
        SetupRunner runner = newRunner();
        assertTrue(runner.isLobType(Types.BLOB, "blob"));
    }

    @Test
    void isLobType_正常ケース_JDBC_CLOBはtrueになること() {
        SetupRunner runner = newRunner();
        assertTrue(runner.isLobType(Types.CLOB, "clob"));
    }

    @Test
    void isLobType_正常ケース_JDBC_NCLOBはtrueになること() {
        SetupRunner runner = newRunner();
        assertTrue(runner.isLobType(Types.NCLOB, "nclob"));
    }

    @Test
    void isLobType_正常ケース_型名byteaはtrueになること() {
        SetupRunner runner = newRunner();
        assertTrue(runner.isLobType(Types.OTHER, "bytea"));
    }

    @Test
    void isLobType_正常ケース_型名varbinaryはtrueになること() {
        SetupRunner runner = newRunner();
        assertTrue(runner.isLobType(Types.VARBINARY, "varbinary"));
    }

    @Test
    void isLobType_正常ケース_VARCHARはfalseになること() {
        SetupRunner runner = newRunner();
        assertFalse(runner.isLobType(Types.VARCHAR, "VARCHAR2"));
    }

    @Test
    void isLobType_正常ケース_型名nullで未知JDBC型はfalseになること() {
        SetupRunner runner = newRunner();
        assertFalse(runner.isLobType(Types.OTHER, null));
    }

    @Test
    void buildPkPlaceholder_正常ケース_PK1列のとき中括弧付きカラム名になること() {
        SetupRunner runner = newRunner();
        assertEquals("{ID}", runner.buildPkPlaceholder(List.of("ID")));
    }

    @Test
    void buildPkPlaceholder_正常ケース_複合PKのとき各列をアンダースコアで連結すること() {
        SetupRunner runner = newRunner();
        assertEquals("{COL1}_{COL2}", runner.buildPkPlaceholder(List.of("COL1", "COL2")));
    }

    @Test
    void buildPkPlaceholder_正常ケース_PKなしのときIDプレースホルダになること() {
        SetupRunner runner = newRunner();
        assertEquals("{ID}", runner.buildPkPlaceholder(Collections.emptyList()));
    }

    @Test
    void buildPkPlaceholder_正常ケース_PKがnullのときIDプレースホルダになること() {
        SetupRunner runner = newRunner();
        assertEquals("{ID}", runner.buildPkPlaceholder(null));
    }

    @Test
    void buildPatterns_正常ケース_BLOB列のパターンが生成されること() throws Exception {
        when(dialectHandler.getPrimaryKeyColumns(conn, "SCHEMA", "T1")).thenReturn(List.of("ID"));

        ResultSet colRs = buildColumnResultSet(new String[] {"FILE_DATA"}, new int[] {Types.BLOB},
                new String[] {"blob"});
        when(meta.getColumns(isNull(), anyString(), anyString(), isNull())).thenReturn(colRs);

        SetupRunner runner = newRunner();
        Map<String, String> patterns = runner.buildPatterns(conn, "SCHEMA", "T1", dialectHandler);

        assertEquals(1, patterns.size());
        assertEquals("FILE_DATA_{ID}.dat", patterns.get("FILE_DATA"));
    }

    @Test
    void buildPatterns_正常ケース_CLOB列のパターンが生成されること() throws Exception {
        when(dialectHandler.getPrimaryKeyColumns(conn, "SCHEMA", "T1")).thenReturn(List.of("ID"));

        ResultSet colRs = buildColumnResultSet(new String[] {"NOTE"}, new int[] {Types.CLOB},
                new String[] {"clob"});
        when(meta.getColumns(isNull(), anyString(), anyString(), isNull())).thenReturn(colRs);

        SetupRunner runner = newRunner();
        Map<String, String> patterns = runner.buildPatterns(conn, "SCHEMA", "T1", dialectHandler);

        assertEquals("NOTE_{ID}.clob", patterns.get("NOTE"));
    }

    @Test
    void buildPatterns_正常ケース_NCLOB列のパターンが生成されること() throws Exception {
        when(dialectHandler.getPrimaryKeyColumns(conn, "SCHEMA", "T1")).thenReturn(List.of("ID"));

        ResultSet colRs = buildColumnResultSet(new String[] {"NDATA"}, new int[] {Types.NCLOB},
                new String[] {"nclob"});
        when(meta.getColumns(isNull(), anyString(), anyString(), isNull())).thenReturn(colRs);

        SetupRunner runner = newRunner();
        Map<String, String> patterns = runner.buildPatterns(conn, "SCHEMA", "T1", dialectHandler);

        assertEquals("NDATA_{ID}.nclob", patterns.get("NDATA"));
    }

    @Test
    void buildPatterns_正常ケース_LOB列がない場合は空マップが返ること() throws Exception {
        when(dialectHandler.getPrimaryKeyColumns(conn, "SCHEMA", "T1")).thenReturn(List.of("ID"));

        ResultSet colRs = buildColumnResultSet(new String[] {"NAME"}, new int[] {Types.VARCHAR},
                new String[] {"VARCHAR2"});
        when(meta.getColumns(isNull(), anyString(), anyString(), isNull())).thenReturn(colRs);

        SetupRunner runner = newRunner();
        Map<String, String> patterns = runner.buildPatterns(conn, "SCHEMA", "T1", dialectHandler);

        assertTrue(patterns.isEmpty());
    }

    @Test
    void buildPatterns_正常ケース_複合PKのとき連結プレースホルダが使われること() throws Exception {
        when(dialectHandler.getPrimaryKeyColumns(conn, "SCHEMA", "T1"))
                .thenReturn(List.of("PK1", "PK2"));

        ResultSet colRs = buildColumnResultSet(new String[] {"DATA"}, new int[] {Types.BLOB},
                new String[] {"blob"});
        when(meta.getColumns(isNull(), anyString(), anyString(), isNull())).thenReturn(colRs);

        SetupRunner runner = newRunner();
        Map<String, String> patterns = runner.buildPatterns(conn, "SCHEMA", "T1", dialectHandler);

        assertEquals("DATA_{PK1}_{PK2}.dat", patterns.get("DATA"));
    }

    @Test
    void buildPatterns_正常ケース_PKなしのときIDプレースホルダが使われること() throws Exception {
        when(dialectHandler.getPrimaryKeyColumns(conn, "SCHEMA", "T1"))
                .thenReturn(Collections.emptyList());

        ResultSet colRs = buildColumnResultSet(new String[] {"DATA"}, new int[] {Types.BLOB},
                new String[] {"blob"});
        when(meta.getColumns(isNull(), anyString(), anyString(), isNull())).thenReturn(colRs);

        SetupRunner runner = newRunner();
        Map<String, String> patterns = runner.buildPatterns(conn, "SCHEMA", "T1", dialectHandler);

        assertEquals("DATA_{ID}.dat", patterns.get("DATA"));
    }

    @Test
    void resolveConfigFile_正常ケース_システムプロパティなしのときconfのapplicationymlを返すこと() {
        System.clearProperty("spring.config.additional-location");
        SetupRunner runner = newRunner();
        Path result = runner.resolveConfigFile();
        assertEquals(Path.of("conf", "application.yml"), result);
    }

    @Test
    void resolveConfigFile_正常ケース_システムプロパティfileプレフィックスのとき正しいパスが返ること() {
        System.setProperty("spring.config.additional-location", "file:myconf/");
        try {
            SetupRunner runner = newRunner();
            Path result = runner.resolveConfigFile();
            assertEquals(Path.of("myconf/").resolve("application.yml"), result);
        } finally {
            System.clearProperty("spring.config.additional-location");
        }
    }

    @Test
    void resolveConfigFile_正常ケース_optionalfileプレフィックスのとき正しいパスが返ること() {
        System.setProperty("spring.config.additional-location", "optional:file:myconf/");
        try {
            SetupRunner runner = newRunner();
            Path result = runner.resolveConfigFile();
            assertEquals(Path.of("myconf/").resolve("application.yml"), result);
        } finally {
            System.clearProperty("spring.config.additional-location");
        }
    }

    @Test
    void writeFilePatterns_正常ケース_filePatternセクションが書き込まれること() throws Exception {
        Path configFile = tempDir.resolve("application.yml");
        Files.writeString(configFile, "data-path: /tmp\nconnections:\n  - id: DB1\n");

        Map<String, Map<String, String>> patterns = Map.of("T1", Map.of("COL1", "COL1_{ID}.dat"));

        SetupRunner runner = newRunner();
        assertTrue(runner.writeFilePatterns(configFile, patterns));

        String written = Files.readString(configFile);
        assertTrue(written.contains("file-patterns"));
        assertTrue(written.contains("T1"));
        assertTrue(written.contains("COL1_{ID}.dat"));
    }

    @Test
    void writeFilePatterns_異常ケース_ファイルが存在しないときErrorHandlerが呼ばれること() {
        Path missing = tempDir.resolve("missing.yml");

        try (MockedStatic<ErrorHandler> mocked = mockStatic(ErrorHandler.class)) {
            mocked.when(() -> ErrorHandler.errorAndExit(anyString())).thenAnswer(inv -> null);

            SetupRunner runner = newRunner();
            assertFalse(runner.writeFilePatterns(missing, Map.of()));

            mocked.verify(() -> ErrorHandler.errorAndExit(anyString()));
        }
    }

    @Test
    void writeFilePatterns_異常ケース_ファイル読込エラーのときErrorHandlerが呼ばれること() throws Exception {
        Path configFile = tempDir.resolve("application.yml");
        Files.writeString(configFile, "data-path: /tmp\n");
        try (MockedStatic<Files> fm = mockStatic(Files.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<ErrorHandler> eh = mockStatic(ErrorHandler.class)) {
            fm.when(() -> Files.newInputStream(configFile)).thenThrow(new IOException("read-fail"));
            eh.when(() -> ErrorHandler.errorAndExit(anyString(), any(Throwable.class)))
                    .thenAnswer(inv -> null);

            SetupRunner runner = newRunner();
            assertFalse(runner.writeFilePatterns(configFile, Map.of()));

            eh.verify(() -> ErrorHandler.errorAndExit(anyString(), any(IOException.class)));
        }
    }

    @Test
    void writeFilePatterns_異常ケース_ファイル書込エラーのときErrorHandlerが呼ばれること() throws Exception {
        Path configFile = tempDir.resolve("application.yml");
        Files.writeString(configFile, "data-path: /tmp\n");
        configFile.toFile().setReadOnly();
        try (MockedStatic<ErrorHandler> eh = mockStatic(ErrorHandler.class)) {
            eh.when(() -> ErrorHandler.errorAndExit(anyString(), any(Throwable.class)))
                    .thenAnswer(inv -> null);

            SetupRunner runner = newRunner();
            assertFalse(runner.writeFilePatterns(configFile,
                    Map.of("T1", Map.of("COL", "COL_{ID}.dat"))));

            eh.verify(() -> ErrorHandler.errorAndExit(anyString(), any(IOException.class)));
        } finally {
            configFile.toFile().setWritable(true);
        }
    }

    @Test
    void execute_正常ケース_target指定外のDBはスキップされること() throws Exception {
        SetupRunner runner = newRunner();

        // DB1 はターゲット外に指定
        try (MockedStatic<DriverManager> dm = mockStatic(DriverManager.class)) {
            runner.execute(List.of("DB2"));
            // DriverManager.getConnection が呼ばれないこと（接続試行なし）
            dm.verifyNoInteractions();
        }
    }

    @Test
    void execute_異常ケース_ドライバクラスが存在しないときErrorHandlerが呼ばれること() {
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("DB1");
        entry.setDriverClass("com.example.NonExistentDriver");
        entry.setUrl("jdbc:nonexistent://localhost/db");
        connectionConfig.setConnections(List.of(entry));

        try (MockedStatic<ErrorHandler> mocked = mockStatic(ErrorHandler.class)) {
            mocked.when(() -> ErrorHandler.errorAndExit(anyString(), any(Throwable.class)))
                    .thenAnswer(inv -> null);

            SetupRunner runner = newRunner();
            runner.execute(List.of("DB1"));

            mocked.verify(() -> ErrorHandler.errorAndExit(anyString(), any(Throwable.class)));
        }
    }

    @Test
    void execute_正常ケース_LOB列なしのときfile_patternsが更新されないこと() throws Exception {
        // conf/application.yml を tempDir に作成
        Path configFile = tempDir.resolve("application.yml");
        Files.writeString(configFile, "data-path: /tmp\n");
        System.setProperty("spring.config.additional-location",
                "file:" + tempDir.toAbsolutePath() + "/");

        try (MockedStatic<DriverManager> dm = mockStatic(DriverManager.class)) {
            dm.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
                    .thenReturn(conn);

            // テーブルなし
            ResultSet tableRs = emptyResultSet();
            when(meta.getTables(isNull(), anyString(), anyString(), any(String[].class)))
                    .thenReturn(tableRs);

            SetupRunner runner = newRunner();
            runner.execute(List.of("DB1"));

            // LOB 列なし → ファイル内容は変わらない
            String content = Files.readString(configFile);
            assertFalse(content.contains("file-patterns"));
        } finally {
            System.clearProperty("spring.config.additional-location");
        }
    }

    @Test
    void execute_正常ケース_LOB列ありのときfile_patternsが書き込まれること() throws Exception {
        Path configFile = tempDir.resolve("application.yml");
        Files.writeString(configFile, "data-path: /tmp\n");
        System.setProperty("spring.config.additional-location",
                "file:" + tempDir.toAbsolutePath() + "/");

        try (MockedStatic<DriverManager> dm = mockStatic(DriverManager.class)) {
            dm.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
                    .thenReturn(conn);

            // TABLE_A を返す
            ResultSet tableRs = buildTableResultSet(new String[] {"TABLE_A"});
            when(meta.getTables(isNull(), anyString(), anyString(), any(String[].class)))
                    .thenReturn(tableRs);

            // TABLE_A に BLOB 列 DATA
            when(dialectHandler.getPrimaryKeyColumns(conn, "SCHEMA", "TABLE_A"))
                    .thenReturn(List.of("ID"));
            ResultSet colRs = buildColumnResultSet(new String[] {"DATA"}, new int[] {Types.BLOB},
                    new String[] {"blob"});
            when(meta.getColumns(isNull(), anyString(), anyString(), isNull())).thenReturn(colRs);

            SetupRunner runner = newRunner();
            runner.execute(List.of("DB1"));

            String written = Files.readString(configFile);
            assertTrue(written.contains("file-patterns"));
            assertTrue(written.contains("TABLE_A"));
            assertTrue(written.contains("DATA_{ID}.dat"));
        } finally {
            System.clearProperty("spring.config.additional-location");
        }
    }

    @Test
    void execute_異常ケース_SQL接続エラーのときErrorHandlerが呼ばれること() throws Exception {
        Path configFile = tempDir.resolve("application.yml");
        Files.writeString(configFile, "data-path: /tmp\n");
        System.setProperty("spring.config.additional-location",
                "file:" + tempDir.toAbsolutePath() + "/");
        // new SQLException() のコンストラクタが DriverManager.getLogWriter() を呼ぶため、
        // mockStatic(DriverManager) ブロックの外で事前に生成する
        SQLException connectionError = new SQLException("connection failed");
        try (MockedStatic<DriverManager> dm = mockStatic(DriverManager.class);
                MockedStatic<ErrorHandler> eh = mockStatic(ErrorHandler.class)) {
            dm.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
                    .thenThrow(connectionError);
            eh.when(() -> ErrorHandler.errorAndExit(anyString(), any(Throwable.class)))
                    .thenAnswer(inv -> null);

            SetupRunner runner = newRunner();
            runner.execute(List.of("DB1"));

            eh.verify(() -> ErrorHandler.errorAndExit(anyString(), any(SQLException.class)));
        } finally {
            System.clearProperty("spring.config.additional-location");
        }
    }

    @Test
    void execute_正常ケース_targetDbIdsがnullのとき全DBが処理されること() throws Exception {
        Path configFile = tempDir.resolve("application.yml");
        Files.writeString(configFile, "data-path: /tmp\n");
        System.setProperty("spring.config.additional-location",
                "file:" + tempDir.toAbsolutePath() + "/");
        try (MockedStatic<DriverManager> dm = mockStatic(DriverManager.class)) {
            dm.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
                    .thenReturn(conn);

            ResultSet tableRs = emptyResultSet();
            when(meta.getTables(isNull(), anyString(), anyString(), any(String[].class)))
                    .thenReturn(tableRs);

            SetupRunner runner = newRunner();
            runner.execute(null);
        } finally {
            System.clearProperty("spring.config.additional-location");
        }
    }

    @Test
    void execute_正常ケース_targetDbIdsが空リストのとき全DBが処理されること() throws Exception {
        Path configFile = tempDir.resolve("application.yml");
        Files.writeString(configFile, "data-path: /tmp\n");
        System.setProperty("spring.config.additional-location",
                "file:" + tempDir.toAbsolutePath() + "/");
        try (MockedStatic<DriverManager> dm = mockStatic(DriverManager.class)) {
            dm.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
                    .thenReturn(conn);

            ResultSet tableRs = emptyResultSet();
            when(meta.getTables(isNull(), anyString(), anyString(), any(String[].class)))
                    .thenReturn(tableRs);

            SetupRunner runner = newRunner();
            runner.execute(Collections.emptyList());
        } finally {
            System.clearProperty("spring.config.additional-location");
        }
    }

    @Test
    void execute_正常ケース_複数接続が処理されること() throws Exception {
        Path configFile = tempDir.resolve("application.yml");
        Files.writeString(configFile, "data-path: /tmp\n");
        System.setProperty("spring.config.additional-location",
                "file:" + tempDir.toAbsolutePath() + "/");

        ConnectionConfig.Entry entry2 = new ConnectionConfig.Entry();
        entry2.setId("DB2");
        entry2.setUrl("jdbc:postgresql://localhost/test2");
        entry2.setUser("sa2");
        entry2.setPassword("");
        entry2.setDriverClass("org.postgresql.Driver");
        connectionConfig.setConnections(List.of(connectionConfig.getConnections().get(0), entry2));

        try (MockedStatic<DriverManager> dm = mockStatic(DriverManager.class)) {
            dm.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
                    .thenReturn(conn);

            ResultSet tableRs = emptyResultSet();
            when(meta.getTables(isNull(), anyString(), anyString(), any(String[].class)))
                    .thenReturn(tableRs);

            SetupRunner runner = newRunner();
            runner.execute(List.of("DB1", "DB2"));
        } finally {
            System.clearProperty("spring.config.additional-location");
        }
    }

    @Test
    void execute_正常ケース_複数接続で同一テーブルのLOB列がマージされること() throws Exception {
        Path configFile = tempDir.resolve("application.yml");
        Files.writeString(configFile, "data-path: /tmp\n");
        System.setProperty("spring.config.additional-location",
                "file:" + tempDir.toAbsolutePath() + "/");

        ConnectionConfig.Entry entry2 = new ConnectionConfig.Entry();
        entry2.setId("DB2");
        entry2.setUrl("jdbc:postgresql://localhost/test2");
        entry2.setUser("sa2");
        entry2.setPassword("");
        entry2.setDriverClass("org.postgresql.Driver");
        connectionConfig.setConnections(List.of(connectionConfig.getConnections().get(0), entry2));

        try (MockedStatic<DriverManager> dm = mockStatic(DriverManager.class)) {
            dm.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
                    .thenReturn(conn);

            // DB1 と DB2 が同一テーブル TABLE_A を返す（別 ResultSet）
            ResultSet tableRs1 = buildTableResultSet(new String[] {"TABLE_A"});
            ResultSet tableRs2 = buildTableResultSet(new String[] {"TABLE_A"});
            when(meta.getTables(isNull(), anyString(), anyString(), any(String[].class)))
                    .thenReturn(tableRs1, tableRs2);

            when(dialectHandler.getPrimaryKeyColumns(conn, "SCHEMA", "TABLE_A"))
                    .thenReturn(List.of("ID"));

            // DB1: BLOB 列, DB2: CLOB 列 → マージラムダが呼ばれること
            ResultSet colRs1 = buildColumnResultSet(new String[] {"DATA"}, new int[] {Types.BLOB},
                    new String[] {"blob"});
            ResultSet colRs2 = buildColumnResultSet(new String[] {"NOTES"}, new int[] {Types.CLOB},
                    new String[] {"clob"});
            when(meta.getColumns(isNull(), anyString(), anyString(), isNull())).thenReturn(colRs1,
                    colRs2);

            SetupRunner runner = newRunner();
            runner.execute(List.of("DB1", "DB2"));

            String written = Files.readString(configFile);
            assertTrue(written.contains("file-patterns"));
            assertTrue(written.contains("TABLE_A"));
        } finally {
            System.clearProperty("spring.config.additional-location");
        }
    }

    @Test
    void execute_正常ケース_writeFilePatternsがfalseを返すときlogが出力されないこと() throws Exception {
        // configFile が存在しないディレクトリを指定 → writeFilePatterns が false を返す
        Path noDir = tempDir.resolve("nodir");
        System.setProperty("spring.config.additional-location",
                "file:" + noDir.toAbsolutePath() + "/");

        try (MockedStatic<DriverManager> dm = mockStatic(DriverManager.class);
                MockedStatic<ErrorHandler> eh = mockStatic(ErrorHandler.class)) {
            dm.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
                    .thenReturn(conn);

            ResultSet tableRs = buildTableResultSet(new String[] {"TABLE_A"});
            when(meta.getTables(isNull(), anyString(), anyString(), any(String[].class)))
                    .thenReturn(tableRs);
            when(dialectHandler.getPrimaryKeyColumns(conn, "SCHEMA", "TABLE_A"))
                    .thenReturn(List.of("ID"));
            ResultSet colRs = buildColumnResultSet(new String[] {"DATA"}, new int[] {Types.BLOB},
                    new String[] {"blob"});
            when(meta.getColumns(isNull(), anyString(), anyString(), isNull())).thenReturn(colRs);

            eh.when(() -> ErrorHandler.errorAndExit(anyString())).thenAnswer(inv -> null);

            SetupRunner runner = newRunner();
            runner.execute(List.of("DB1"));

            // writeFilePatterns が false を返したため ErrorHandler が呼ばれること
            eh.verify(() -> ErrorHandler.errorAndExit(anyString()));
        } finally {
            System.clearProperty("spring.config.additional-location");
        }
    }

    @Test
    void writeFilePatterns_正常ケース_YAMLが空のときconfigがnullでも書き込まれること() throws Exception {
        Path configFile = tempDir.resolve("application.yml");
        Files.writeString(configFile, "");

        Map<String, Map<String, String>> patterns = Map.of("T1", Map.of("COL1", "COL1_{ID}.dat"));

        SetupRunner runner = newRunner();
        assertTrue(runner.writeFilePatterns(configFile, patterns));

        String written = Files.readString(configFile);
        assertTrue(written.contains("file-patterns"));
        assertTrue(written.contains("T1"));
    }

    private SetupRunner newRunner() {
        return new SetupRunner(connectionConfig, entry -> dialectHandler);
    }

    private ResultSet buildTableResultSet(String[] tableNames) throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        int[] idx = {0};
        when(rs.next()).thenAnswer(inv -> {
            if (idx[0] < tableNames.length) {
                idx[0]++;
                return true;
            }
            return false;
        });
        when(rs.getString("TABLE_NAME")).thenAnswer(inv -> tableNames[idx[0] - 1]);
        return rs;
    }

    private ResultSet buildColumnResultSet(String[] colNames, int[] jdbcTypes, String[] typeNames)
            throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        int[] idx = {0};
        when(rs.next()).thenAnswer(inv -> {
            if (idx[0] < colNames.length) {
                idx[0]++;
                return true;
            }
            return false;
        });
        when(rs.getString("COLUMN_NAME")).thenAnswer(inv -> colNames[idx[0] - 1]);
        when(rs.getInt("DATA_TYPE")).thenAnswer(inv -> jdbcTypes[idx[0] - 1]);
        when(rs.getString("TYPE_NAME")).thenAnswer(inv -> typeNames[idx[0] - 1]);
        return rs;
    }

    private ResultSet emptyResultSet() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(false);
        return rs;
    }
}
