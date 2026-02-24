package io.github.yok.flexdblink;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import io.github.yok.flexdblink.util.ErrorHandler;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.springframework.boot.SpringApplication;

/**
 * Unit tests for {@link Main}.
 */
class MainTest {

    private PathsConfig pathsConfig;
    private DbUnitConfig dbUnitConfig;
    private ConnectionConfig connectionConfig;
    private FilePatternConfig filePatternConfig;
    private DumpConfig dumpConfig;
    private DbDialectHandlerFactory dialectFactory;

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
        dialectFactory = mock(DbDialectHandlerFactory.class);

        when(dialectFactory.create(any())).thenReturn(mock(DbDialectHandler.class));
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
    void run_異常ケース_シナリオ未指定時にErrorHandlerが呼ばれること() {
        Main sut = new Main(mock(PathsConfig.class), mock(DbUnitConfig.class),
                mock(ConnectionConfig.class), mock(FilePatternConfig.class), mock(DumpConfig.class),
                mock(DbDialectHandlerFactory.class));

        try (MockedStatic<ErrorHandler> mocked = mockStatic(ErrorHandler.class)) {
            mocked.when(() -> ErrorHandler.errorAndExit(anyString())).thenAnswer(inv -> {
                throw new IllegalStateException("exit");
            });

            // dumpモードでシナリオ省略 → 1引数版が呼ばれる
            assertThrows(IllegalStateException.class, () -> sut.run("--dump"));

            mocked.verify(
                    () -> ErrorHandler.errorAndExit(eq("Scenario name is required in dump mode.")));
        }
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
                    @SuppressWarnings("unchecked")
                    Function<ConnectionConfig.Entry, DbDialectHandler> dialectProvider =
                            (Function<ConnectionConfig.Entry, DbDialectHandler>) context.arguments()
                                    .get(2);
                    ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
                    entry.setId("dbX");
                    DbDialectHandler handler = mock(DbDialectHandler.class);
                    when(dialectFactory.create(entry)).thenReturn(handler);
                    assertEquals(handler, dialectProvider.apply(entry));
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
                    @SuppressWarnings("unchecked")
                    Function<ConnectionConfig.Entry, DbDialectHandler> dialectProvider =
                            (Function<ConnectionConfig.Entry, DbDialectHandler>) context.arguments()
                                    .get(2);
                    ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
                    entry.setId("db1");
                    entry.setUser(null);
                    DbDialectHandler handler = mock(DbDialectHandler.class);
                    when(dialectFactory.create(entry)).thenReturn(handler);
                    assertEquals(handler, dialectProvider.apply(entry));
                })) {
            main.run("--load", "myscenario");
            DataLoader loader = mocked.constructed().get(0);
            verify(loader).execute(eq("myscenario"), eq(List.of("db1")));
        }
    }

    @Test
    void run_異常ケース_DataLoader実行時に例外が発生する_ErrorHandlerが呼ばれること() {
        try (MockedConstruction<DataLoader> mocked = mockConstruction(DataLoader.class,
                (loader, context) -> doThrow(new RuntimeException("boom")).when(loader)
                        .execute(anyString(), anyList()));
                MockedStatic<ErrorHandler> eh = mockStatic(ErrorHandler.class)) {

            eh.when(() -> ErrorHandler.errorAndExit(anyString(), any(Throwable.class)))
                    .thenAnswer(inv -> {
                        throw new IllegalStateException("exit");
                    });

            assertThrows(IllegalStateException.class, () -> main.run("--load", "myscenario"));

            DataLoader loader = mocked.constructed().get(0);
            verify(loader).execute(eq("myscenario"), eq(List.of("db1")));
            eh.verify(() -> ErrorHandler.errorAndExit(anyString(), any(Throwable.class)));
        }
    }

    @Test
    void run_異常ケース_dumpモードでシナリオ未指定を実行する_IllegalStateExceptionが送出されること() {
        ErrorHandler.disableExitForCurrentThread();
        try {
            Main sut = new Main(mock(PathsConfig.class), mock(DbUnitConfig.class),
                    mock(ConnectionConfig.class), mock(FilePatternConfig.class),
                    mock(DumpConfig.class), mock(DbDialectHandlerFactory.class));
            assertThrows(IllegalStateException.class, () -> sut.run("--dump"));
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }
    }

    @Test
    void run_異常ケース_load実行で例外が発生する_ErrorHandlerからIllegalStateExceptionが送出されること() {
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
    void run_異常ケース_dump実行で例外が発生する_ErrorHandlerからIllegalStateExceptionが送出されること() {
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
    void run_異常ケース_dumpモードでシナリオ未指定かつダンプ処理で例外が発生する_例外を再送出せず処理終了すること() {
        try (MockedConstruction<DataDumper> mocked = mockConstruction(DataDumper.class,
                (dumper, context) -> doThrow(new RuntimeException("dump-fail")).when(dumper)
                        .execute(any(), anyList()))) {
            assertDoesNotThrow(() -> main.run("--dump"));
            DataDumper dumper = mocked.constructed().get(0);
            verify(dumper).execute(eq(null), eq(List.of("db1")));
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
    void run_異常ケース_confirmBeforeLoad時にIOExceptionが発生する_ErrorHandlerが呼ばれること()
            throws Exception {
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
    void run_異常ケース_setup実行で例外が発生する_ErrorHandlerからIllegalStateExceptionが送出されること() {
        ErrorHandler.disableExitForCurrentThread();
        try (MockedConstruction<SetupRunner> mocked = mockConstruction(SetupRunner.class,
                (runner, ctx) -> doThrow(new RuntimeException("setupBoom")).when(runner)
                        .execute(anyList()))) {
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> main.run("--setup"));
            assertTrue(ex.getMessage().contains("Fatal error: setupBoom"));
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }
    }

}
