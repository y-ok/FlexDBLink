package io.github.yok.flexdblink;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.FilePatternConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.core.DataDumper;
import io.github.yok.flexdblink.core.DataLoader;
import io.github.yok.flexdblink.core.SetupRunner;
import io.github.yok.flexdblink.db.DbDialectHandler;
import io.github.yok.flexdblink.db.DbDialectHandlerFactory;
import io.github.yok.flexdblink.db.DbUnitConfigFactory;
import io.github.yok.flexdblink.util.DateTimeFormatSupport;
import io.github.yok.flexdblink.util.ErrorHandler;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.springframework.boot.SpringApplication;

/**
 * Unit tests for {@link Main}.
 */
class MainTest {

    @TempDir
    Path tempDir;

    private PathsConfig pathsConfig;
    private DbUnitConfig dbUnitConfig;
    private ConnectionConfig connectionConfig;
    private FilePatternConfig filePatternConfig;
    private DumpConfig dumpConfig;
    private TestDbDialectHandlerFactory dialectFactory;

    private Main main;

    @BeforeEach
    void setup() {
        pathsConfig = mock(PathsConfig.class);
        dbUnitConfig = mock(DbUnitConfig.class);

        connectionConfig = new ConnectionConfig();
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        entry.setUser("user1");
        connectionConfig.setConnections(Collections.singletonList(entry));

        filePatternConfig = mock(FilePatternConfig.class);
        dumpConfig = mock(DumpConfig.class);
        dialectFactory = new TestDbDialectHandlerFactory();
        dialectFactory.setHandlerToReturn(mock(DbDialectHandler.class));
        when(dbUnitConfig.getPreDirName()).thenReturn("preScenario");

        main = new Main(pathsConfig, dbUnitConfig, connectionConfig, filePatternConfig, dumpConfig,
                dialectFactory);
    }

    @Test
    void main_正常ケース_SpringApplicationが起動されること() {
        try (MockedConstruction<SpringApplication> mocked =
                mockConstruction(SpringApplication.class, (mock, ctx) -> {
                    // run(String...) をスタブ
                    when(mock.run(any(String[].class))).thenReturn(null);

                    // コンストラクタ引数を検証
                    Object arg0 = ctx.arguments().get(0);
                    assertTrue(arg0 instanceof Class<?>[]);
                    Class<?>[] sources = (Class<?>[]) arg0;
                    assertEquals(1, sources.length);
                    assertEquals(Main.class, sources[0]);
                })) {

            Main.main(new String[] {"--load", "myscenario"});

            SpringApplication app = mocked.constructed().get(0);

            // addCommandLineProperties(false) が呼ばれたこと
            verify(app).setAddCommandLineProperties(false);

            // run(String...) が呼ばれたこと
            verify(app).run(eq("--load"), eq("myscenario"));
        }
    }

    @Test
    void run_異常ケース_ダンプのシナリオを指定しない_シナリオ必須の例外通知であること() {
        IllegalStateException failure =
                assertThrows(IllegalStateException.class, () -> main.run("--dump"));
        assertEquals("Scenario name is required in dump mode.", failure.getMessage());
    }

    @Test
    void run_正常ケース_引数なしはデフォルトでloadが実行されること() {
        try (MockedConstruction<DataLoader> mocked = mockConstruction(DataLoader.class,
                (mock, ctx) -> doNothing().when(mock).execute(anyString(), anyList()))) {

            main.run();

            DataLoader loader = mocked.constructed().get(0);
            verify(loader).execute(eq("preScenario"), eq(List.of("db1")));
        }
    }

    @Test
    void run_正常ケース_loadシナリオ指定が実行されること() {
        try (MockedConstruction<DataLoader> mocked = mockConstruction(DataLoader.class)) {
            main.run("--load", "myscenario");

            DataLoader loader = mocked.constructed().get(0);
            verify(loader).execute(eq("myscenario"), eq(List.of("db1")));
        }
    }

    @Test
    void run_正常ケース_loadオプションのみ指定する_シナリオnullで実行されること() {
        try (MockedConstruction<DataLoader> mocked = mockConstruction(DataLoader.class)) {
            main.run("--load");

            DataLoader loader = mocked.constructed().get(0);
            verify(loader).execute(eq(null), eq(List.of("db1")));
        }
    }

    @ParameterizedTest(name = "{0} / {1} / targetFirst={2}")
    @CsvSource({"--load,--target,false", "--load,-t,false", "-l,--target,false", "-l,-t,false",
            "--load,--target,true", "--load,-t,true", "-l,--target,true", "-l,-t,true"})
    void run_正常ケース_シナリオを省略して対象DBを指定する_指定DBだけがロード対象であること(String loadOption, String targetOption,
            boolean targetFirst) {
        ConnectionConfig.Entry first = new ConnectionConfig.Entry();
        first.setId("DB1");
        ConnectionConfig.Entry second = new ConnectionConfig.Entry();
        second.setId("DB2");
        connectionConfig.setConnections(List.of(first, second));
        try (MockedConstruction<DataLoader> construction = mockConstruction(DataLoader.class)) {
            if (targetFirst) {
                main.run(targetOption, "DB2", loadOption);
            } else {
                main.run(loadOption, targetOption, "DB2");
            }

            ArgumentCaptor<List<String>> targets = ArgumentCaptor.captor();
            ArgumentCaptor<String> scenario = ArgumentCaptor.forClass(String.class);
            verify(construction.constructed().get(0)).execute(scenario.capture(),
                    targets.capture());
            assertAll(() -> assertEquals(List.of("DB2"), targets.getValue(),
                    "Omitting the scenario must not expand the load to unselected databases."),
                    () -> assertNull(scenario.getValue(),
                            "An omitted scenario must use the loader's configured initial dataset."));
        }
    }

    @Test
    void run_異常ケース_通常設定でロードが失敗する_呼び出し元への例外通知であること() {
        // Keep the real ErrorHandler in its production mode; no test-only exception flag.
        ErrorHandler.restoreExitForCurrentThread();
        IllegalStateException cause = new IllegalStateException("load failed");
        try (MockedConstruction<DataLoader> construction = mockConstruction(DataLoader.class,
                (loader, context) -> doThrow(cause).when(loader).execute(anyString(), anyList()))) {
            RuntimeException failure =
                    assertThrows(RuntimeException.class, () -> main.run("--load", "pre"),
                            "A failed load must not be reported to the caller as successful.");
            assertSame(cause,
                    org.apache.commons.lang3.exception.ExceptionUtils.getRootCause(failure));
            assertEquals(1, construction.constructed().size());
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"--load", "--dump", "--setup"})
    void main_異常ケース_通常設定でDB接続を失敗させる_プロセス終了コードが非ゼロであること(String mode) throws Exception {
        Path config = tempDir.resolve("failure.properties");
        Files.writeString(config, "data-path=" + tempDir.toAbsolutePath() + "\n"
                + "connections[0].id=DB1\n" + "connections[0].driver-class=org.postgresql.Driver\n"
                + "connections[0].url=jdbc:postgresql://127.0.0.1:1/unavailable?connectTimeout=1\n"
                + "connections[0].user=test\nconnections[0].password=\n");
        Path testClasses =
                Path.of(MainTest.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        // Exclude test configurations so the child starts the actual production application.
        String classpath = Arrays
                .stream(System.getProperty("surefire.test.class.path").split(File.pathSeparator))
                .filter(entry -> !Path.of(entry).equals(testClasses))
                .collect(Collectors.joining(File.pathSeparator));
        List<String> command = new ArrayList<>(
                List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                        "-Dspring.config.additional-location=" + config.toUri(), "-cp", classpath,
                        Main.class.getName(), mode));
        if (!"--setup".equals(mode)) {
            command.add("pre");
        }
        command.addAll(List.of("--target", "DB1"));
        Path output = tempDir.resolve("process.log");
        Process process = new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(output.toFile()).start();
        try {
            assertTrue(process.waitFor(30, TimeUnit.SECONDS), "CLI process exceeded its timeout.");
            String log = Files.readString(output);
            assertTrue(log.contains("Application started. Args:"),
                    "The child must reach Main.run before failing: " + log);
            assertTrue(log.contains("org.postgresql.util.PSQLException"),
                    "The child must reproduce the intended DB connection failure: " + log);
            assertNotEquals(0, process.exitValue(),
                    "A failed CLI operation must return a nonzero process exit code.\n" + log);
        } finally {
            process.destroyForcibly();
        }
    }

    @Test
    void run_正常ケース_dumpシナリオ指定が実行されること() {
        try (MockedConstruction<DataDumper> mocked = mockConstruction(DataDumper.class)) {
            main.run("--dump", "myscenario");

            DataDumper dumper = mocked.constructed().get(0);
            verify(dumper).execute(eq("myscenario"), eq(List.of("db1")));
        }
    }

    @Test
    void run_正常ケース_target指定が優先されること() {
        try (MockedConstruction<DataLoader> mocked = mockConstruction(DataLoader.class)) {
            main.run("--load", "myscenario", "--target", "dbX,dbY");

            DataLoader loader = mocked.constructed().get(0);
            verify(loader).execute(eq("myscenario"), eq(List.of("dbX", "dbY")));
        }
    }

    @Test
    void run_正常ケース_targetオプションのみを指定する_全DBが対象で実行されること() {
        try (MockedConstruction<DataLoader> mocked = mockConstruction(DataLoader.class)) {
            main.run("--load", "myscenario", "--target");

            DataLoader loader = mocked.constructed().get(0);
            verify(loader).execute(eq("myscenario"), eq(List.of("db1")));
        }
    }

    @Test
    void run_正常ケース_短縮オプション指定でloadが実行されること() {
        try (MockedConstruction<DataLoader> mocked = mockConstruction(DataLoader.class)) {
            main.run("-l", "myscenario", "-t", "dbA,dbB");
            DataLoader loader = mocked.constructed().get(0);
            verify(loader).execute(eq("myscenario"), eq(List.of("dbA", "dbB")));
        }
    }

    @Test
    void run_正常ケース_未知の引数はwarnされても処理継続すること() {
        try (MockedConstruction<DataLoader> mocked = mockConstruction(DataLoader.class)) {
            main.run("--unknown", "xxx");

            DataLoader loader = mocked.constructed().get(0);
            verify(loader).execute(eq("preScenario"), eq(List.of("db1")));
        }
    }

    @Test
    void run_正常ケース_方言プロバイダがFactoryへ委譲する_ハンドラが返ること() {
        try (MockedConstruction<DataLoader> mocked =
                mockConstruction(DataLoader.class, (loader, context) -> {
                    DbDialectHandlerFactory dialectProvider =
                            (DbDialectHandlerFactory) context.arguments().get(2);
                    ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
                    entry.setId("dbX");
                    DbDialectHandler handler = mock(DbDialectHandler.class);
                    dialectFactory.setHandlerToReturn(handler);
                    assertEquals(handler, dialectProvider.apply(entry));
                    assertEquals(entry, dialectFactory.getLastEntry());
                })) {

            main.run("--load", "myscenario");
            DataLoader loader = mocked.constructed().get(0);
            verify(loader).execute(eq("myscenario"), eq(List.of("db1")));
        }
    }

    @Test
    void run_正常ケース_方言プロバイダにnullユーザーEntryを渡す_例外なくハンドラが返ること() {
        try (MockedConstruction<DataLoader> mocked =
                mockConstruction(DataLoader.class, (loader, context) -> {
                    DbDialectHandlerFactory dialectProvider =
                            (DbDialectHandlerFactory) context.arguments().get(2);
                    ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
                    entry.setId("db1");
                    entry.setUser(null);
                    DbDialectHandler handler = mock(DbDialectHandler.class);
                    dialectFactory.setHandlerToReturn(handler);
                    assertEquals(handler, dialectProvider.apply(entry));
                    assertEquals(entry, dialectFactory.getLastEntry());
                })) {
            main.run("--load", "myscenario");
            DataLoader loader = mocked.constructed().get(0);
            verify(loader).execute(eq("myscenario"), eq(List.of("db1")));
        }
    }

    @Test
    void run_異常ケース_DataLoader実行時に例外が発生する_原因を保持した例外通知であること() {
        RuntimeException cause = new RuntimeException("boom");
        try (MockedConstruction<DataLoader> mocked = mockConstruction(DataLoader.class,
                (loader, context) -> doThrow(cause).when(loader).execute(anyString(), anyList()))) {
            IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> main.run("--load", "myscenario"));
            assertEquals("Fatal error: boom", failure.getMessage());
            assertSame(cause, failure.getCause());
            verify(mocked.constructed().get(0)).execute(eq("myscenario"), eq(List.of("db1")));
        }
    }

    @Test
    void run_異常ケース_dumpモードでシナリオ未指定を実行する_IllegalStateExceptionが送出されること() {
        ErrorHandler.disableExitForCurrentThread();
        try {
            Main sut = new Main(mock(PathsConfig.class), mock(DbUnitConfig.class),
                    mock(ConnectionConfig.class), mock(FilePatternConfig.class),
                    mock(DumpConfig.class), new TestDbDialectHandlerFactory());
            assertThrows(IllegalStateException.class, () -> sut.run("--dump"));
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }
    }

    private static class TestDbDialectHandlerFactory extends DbDialectHandlerFactory {

        private DbDialectHandler handlerToReturn;

        private ConnectionConfig.Entry lastEntry;

        TestDbDialectHandlerFactory() {
            super(new DbUnitConfig(), new DumpConfig(), new PathsConfig(),
                    mock(DateTimeFormatSupport.class), new DbUnitConfigFactory());
        }

        void setHandlerToReturn(DbDialectHandler handlerToReturn) {
            this.handlerToReturn = handlerToReturn;
        }

        ConnectionConfig.Entry getLastEntry() {
            return lastEntry;
        }

        @Override
        public DbDialectHandler create(ConnectionConfig.Entry entry) {
            lastEntry = entry;
            return handlerToReturn;
        }
    }

    @Test
    void run_異常ケース_load実行で例外が発生する_呼び出し元への例外通知であること() {
        ErrorHandler.disableExitForCurrentThread();
        try (MockedConstruction<DataLoader> mocked = mockConstruction(DataLoader.class,
                (loader, context) -> doThrow(new RuntimeException("boom2")).when(loader)
                        .execute(anyString(), anyList()))) {
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> main.run("--load", "myscenario"));
            assertTrue(ex.getMessage().contains("Fatal error: boom2"));
            DataLoader loader = mocked.constructed().get(0);
            verify(loader).execute(eq("myscenario"), eq(List.of("db1")));
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }
    }

    @Test
    void run_異常ケース_dump実行で例外が発生する_呼び出し元への例外通知であること() {
        ErrorHandler.disableExitForCurrentThread();
        try (MockedConstruction<DataDumper> mocked = mockConstruction(DataDumper.class,
                (dumper, context) -> doThrow(new RuntimeException("dumpBoom")).when(dumper)
                        .execute(anyString(), anyList()))) {
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> main.run("--dump", "myscenario"));
            assertTrue(ex.getMessage().contains("Fatal error: dumpBoom"));
            DataDumper dumper = mocked.constructed().get(0);
            verify(dumper).execute(eq("myscenario"), eq(List.of("db1")));
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }
    }

    @Test
    void run_異常ケース_通常設定でダンプが失敗する_呼び出し元への例外通知であること() {
        ErrorHandler.restoreExitForCurrentThread();
        try (MockedConstruction<DataDumper> mocked = mockConstruction(DataDumper.class,
                (dumper, context) -> doThrow(new RuntimeException("dump-fail")).when(dumper)
                        .execute(any(), anyList()))) {
            assertThrows(RuntimeException.class, () -> main.run("--dump", "scenario"));
            DataDumper dumper = mocked.constructed().get(0);
            verify(dumper).execute(eq("scenario"), eq(List.of("db1")));
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }
    }

    @Test
    void run_異常ケース_ダンプのシナリオを省略して対象DBを指定する_シナリオ必須の例外通知であること() {
        ErrorHandler.restoreExitForCurrentThread();
        try (MockedConstruction<DataDumper> construction = mockConstruction(DataDumper.class)) {
            RuntimeException failure = assertThrows(RuntimeException.class,
                    () -> main.run("--dump", "--target", "DB2"));
            assertEquals("Scenario name is required in dump mode.", failure.getMessage());
            assertTrue(construction.constructed().isEmpty());
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }
    }

    @Test
    void run_異常ケース_dumpモードで空文字シナリオを指定する_IllegalStateExceptionが送出されること() {
        ErrorHandler.disableExitForCurrentThread();
        try {
            IllegalStateException ex =
                    assertThrows(IllegalStateException.class, () -> main.run("--dump", ""));
            assertEquals("Scenario name is required in dump mode.", ex.getMessage());
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }
    }

    @Test
    void run_正常ケース_setupモードでSetupRunnerが実行されること() {
        try (MockedConstruction<SetupRunner> mocked = mockConstruction(SetupRunner.class,
                (runner, ctx) -> doNothing().when(runner).execute(anyList()))) {

            main.run("--setup");

            SetupRunner runner = mocked.constructed().get(0);
            verify(runner).execute(eq(List.of("db1")));
        }
    }

    @Test
    void run_正常ケース_setup短縮オプションでSetupRunnerが実行されること() {
        try (MockedConstruction<SetupRunner> mocked = mockConstruction(SetupRunner.class,
                (runner, ctx) -> doNothing().when(runner).execute(anyList()))) {

            main.run("-s", "--target", "dbA");

            SetupRunner runner = mocked.constructed().get(0);
            verify(runner).execute(eq(List.of("dbA")));
        }
    }

    @Test
    void run_正常ケース_confirmBeforeLoadfalseのとき確認なしでloadが実行されること() {
        when(dbUnitConfig.isConfirmBeforeLoad()).thenReturn(false);

        try (MockedConstruction<DataLoader> mocked = mockConstruction(DataLoader.class)) {
            main.run("--load", "myscenario");

            DataLoader loader = mocked.constructed().get(0);
            verify(loader).execute(eq("myscenario"), eq(List.of("db1")));
        }
    }

    @Test
    void run_正常ケース_confirmBeforeLoadtrueでy入力のときloadが実行されること() throws Exception {
        when(dbUnitConfig.isConfirmBeforeLoad()).thenReturn(true);

        InputStream originalIn = System.in;
        System.setIn(new ByteArrayInputStream("y\n".getBytes(StandardCharsets.UTF_8)));
        try (MockedConstruction<DataLoader> mocked = mockConstruction(DataLoader.class)) {
            main.run("--load", "myscenario");

            DataLoader loader = mocked.constructed().get(0);
            verify(loader).execute(eq("myscenario"), eq(List.of("db1")));
        } finally {
            System.setIn(originalIn);
        }
    }

    @Test
    void run_正常ケース_confirmBeforeLoadtrueでyes入力のときloadが実行されること() throws Exception {
        when(dbUnitConfig.isConfirmBeforeLoad()).thenReturn(true);

        InputStream originalIn = System.in;
        System.setIn(new ByteArrayInputStream("yes\n".getBytes(StandardCharsets.UTF_8)));
        try (MockedConstruction<DataLoader> mocked = mockConstruction(DataLoader.class)) {
            main.run("--load", "myscenario");

            DataLoader loader = mocked.constructed().get(0);
            verify(loader).execute(eq("myscenario"), eq(List.of("db1")));
        } finally {
            System.setIn(originalIn);
        }
    }

    @Test
    void run_正常ケース_confirmBeforeLoadtrueでn入力のときloadがキャンセルされること() throws Exception {
        when(dbUnitConfig.isConfirmBeforeLoad()).thenReturn(true);

        InputStream originalIn = System.in;
        System.setIn(new ByteArrayInputStream("n\n".getBytes(StandardCharsets.UTF_8)));
        try (MockedConstruction<DataLoader> mocked = mockConstruction(DataLoader.class)) {
            main.run("--load", "myscenario");

            assertTrue(mocked.constructed().isEmpty());
        } finally {
            System.setIn(originalIn);
        }
    }

    @Test
    void run_正常ケース_confirmBeforeLoadtrueでEnterのみ入力のときloadがキャンセルされること() throws Exception {
        when(dbUnitConfig.isConfirmBeforeLoad()).thenReturn(true);

        InputStream originalIn = System.in;
        System.setIn(new ByteArrayInputStream("\n".getBytes(StandardCharsets.UTF_8)));
        try (MockedConstruction<DataLoader> mocked = mockConstruction(DataLoader.class)) {
            main.run("--load", "myscenario");

            assertTrue(mocked.constructed().isEmpty());
        } finally {
            System.setIn(originalIn);
        }
    }

    @Test
    void main_正常ケース_setDefaultPropertiesが呼ばれること() {
        try (MockedConstruction<SpringApplication> mocked =
                mockConstruction(SpringApplication.class, (mock, ctx) -> {
                    when(mock.run(any(String[].class))).thenReturn(null);
                })) {

            Main.main(new String[] {"--load", "myscenario"});

            SpringApplication app = mocked.constructed().get(0);
            verify(app).setDefaultProperties(anyMap());
            verify(app).setAddCommandLineProperties(false);
            verify(app).run(eq("--load"), eq("myscenario"));
        }
    }

    @Test
    void run_異常ケース_confirmBeforeLoad時にIOExceptionが発生する_ErrorHandlerが呼ばれること() throws Exception {
        when(dbUnitConfig.isConfirmBeforeLoad()).thenReturn(true);

        InputStream originalIn = System.in;
        System.setIn(new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException("io-fail");
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                throw new IOException("io-fail");
            }
        });
        try (MockedStatic<ErrorHandler> eh = mockStatic(ErrorHandler.class)) {
            eh.when(() -> ErrorHandler.errorAndExit(anyString(), any(Throwable.class)))
                    .thenAnswer(inv -> null);
            main.run("--load", "myscenario");
            eh.verify(() -> ErrorHandler.errorAndExit(eq("Failed to read user input."),
                    any(IOException.class)));
        } finally {
            System.setIn(originalIn);
        }
    }

    @Test
    void run_異常ケース_setup実行で例外が発生する_呼び出し元への例外通知であること() {
        ErrorHandler.disableExitForCurrentThread();
        try (MockedConstruction<SetupRunner> mocked = mockConstruction(SetupRunner.class,
                (runner, ctx) -> doThrow(new RuntimeException("setupBoom")).when(runner)
                        .execute(anyList()))) {
            IllegalStateException ex =
                    assertThrows(IllegalStateException.class, () -> main.run("--setup"));
            assertTrue(ex.getMessage().contains("Fatal error: setupBoom"));
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }
    }

}
