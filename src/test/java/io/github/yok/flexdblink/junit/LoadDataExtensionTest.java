package io.github.yok.flexdblink.junit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.core.DataLoader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import javax.sql.DataSource;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import org.springframework.transaction.support.TransactionSynchronizationManager;

class LoadDataExtensionTest {

    // テスト対象
    private LoadDataExtension target;

    // ダミーの「テストクラス」
    @LoadData(scenario = {"CSV"}, dbNames = {"bbb"})
    static class DummyTargetTest {
        @LoadData(scenario = {"METHOD_ONLY"}, dbNames = {"bbb"})
        void methodHasScenario() {}

        @LoadData // scenario 未指定（空配列） -> 自動検出分岐を通す
        void methodScenarioAutoDetect() {}
    }

    @LoadData(scenario = {""})
    static class BlankScenarioClass {
    }

    static class BlankScenarioMethodClass {
        @LoadData(scenario = {""})
        void blankScenarioMethod() {}
    }

    // テストで使用するパス情報（ターゲットに合わせて両系統を作成）
    private Path classpathRoot; // target/test-classes
    @TempDir
    Path tempDir;
    private Path resourcesRoot; // temp test resources
    private Path dummyClassClassPathDir; // target/test-classes/<pkg>/<ClassName>
    private Path dummyClassResourcesDir; // temp test resources/<pkg>/<ClassName>

    // 後始末用
    private final List<Path> createdPaths = new ArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        System.clearProperty("spring.profiles.active");
        target = new LoadDataExtension();

        classpathRoot = Paths.get("target", "test-classes").toAbsolutePath().normalize();
        resourcesRoot = tempDir.resolve("test-resources").toAbsolutePath().normalize();

        String pkgPath = DummyTargetTest.class.getPackage().getName().replace('.', '/');
        String clsName = DummyTargetTest.class.getSimpleName();

        dummyClassClassPathDir = classpathRoot.resolve(pkgPath).resolve(clsName);
        dummyClassResourcesDir = resourcesRoot.resolve(pkgPath).resolve(clsName);

        // 両方の系統の「クラスルート」を作成（beforeAll と loadScenario の双方が参照）
        mkDirs(dummyClassClassPathDir);
        mkDirs(dummyClassResourcesDir);

        // シナリオ配下のディレクトリは各テストで必要に応じて作成する

        // application.properties をクラスパス直下に生成（プロファイル無し）
        writeToFile(classpathRoot.resolve("application.properties"),
                "spring.datasource.bbb.url=jdbc:h2:mem:test\n"
                        + "spring.datasource.bbb.username=sa\n"
                        + "spring.datasource.bbb.password=\n"
                        + "spring.datasource.bbb.driver-class-name=org.h2.Driver\n");
    }

    @AfterEach
    void tearDown() {
        System.clearProperty("spring.profiles.active");
        // TransactionSynchronizationManager にバインドしたものがあれば解除
        try {
            Map<Object, Object> map = TransactionSynchronizationManager.getResourceMap();
            for (Object key : new ArrayList<>(map.keySet())) {
                TransactionSynchronizationManager.unbindResourceIfPossible(key);
            }
        } catch (Exception ignore) {
        }

        // 作成ファイルは残しても良いが、念のため順次削除（存在していれば）
        createdPaths.sort(Comparator.reverseOrder());
        for (Path p : createdPaths) {
            try {
                if (Files.exists(p)) {
                    Files.walk(p).sorted(Comparator.reverseOrder()).forEach(q -> {
                        try {
                            Files.deleteIfExists(q);
                        } catch (IOException ignore) {
                        }
                    });
                }
            } catch (IOException ignore) {
            }
        }
    }

    @Test
    void beforeAll_正常ケース_クラスルートとプロパティが読み込まれること() throws Exception {
        // Arrange
        ExtensionContext ctx = mockContextForClass(DummyTargetTest.class);

        // application.properties は target/test-classes に配置済み

        // Act
        target.beforeAll(ctx);

        // Assert：TestResourceContext が設定されていること
        TestResourceContext trc = getTrc(target);
        assertNotNull(trc);
        assertTrue(trc.getClassRoot().toString().endsWith(DummyTargetTest.class.getSimpleName()));
        Properties appProps = trc.getAppProps();
        assertNotNull(appProps);
        assertEquals("jdbc:h2:mem:test", appProps.getProperty("spring.datasource.bbb.url"));
    }

    @Test
    void beforeAll_異常ケース_クラスリソースが存在しない場合に例外がスローされること() {
        // Arrange: リソースディレクトリを作らない別クラス
        class NoResourceClass {
        }
        ExtensionContext ctx = mockContextForClass(NoResourceClass.class);

        // Act & Assert
        Exception ex = assertThrows(IllegalStateException.class, () -> target.beforeAll(ctx));
        assertTrue(ex.getMessage().contains("Test resource folder not found"));
    }

    @Test
    void beforeTestExecution_正常ケース_クラスアノテーションのシナリオ存在時にロードが行われること() throws Exception {
        // Arrange: クラスレベルアノテーション（@LoadData(scenario={"CSV"}, dbNames={"bbb"})）
        ExtensionContext ctx = mockContextForClass(DummyTargetTest.class);

        // CSV シナリオは作らない → スキップされる
        target.beforeAll(ctx);
        assertDoesNotThrow(() -> target.beforeTestExecution(ctx));
    }

    @Test
    void beforeTestExecution_正常ケース_メソッドアノテーションのシナリオ不存在時にスキップされること() throws Exception {
        // Arrange
        DataSource ds = dummyDataSource();
        TransactionSynchronizationManager.bindResource(ds, new Object());

        // 対象メソッド（@LoadData(scenario={"METHOD_ONLY"})）
        Method dummyMethod = DummyTargetTest.class.getDeclaredMethod("methodHasScenario");

        // ExtensionContext をモックし、ジェネリクス問題を避けるため doReturn(..).when(..) を使用
        ExtensionContext ctx = mock(ExtensionContext.class);
        doReturn(DummyTargetTest.class).when(ctx).getRequiredTestClass();
        doReturn(Optional.of(dummyMethod)).when(ctx).getTestMethod();

        // クラスパス側（target/test-classes）には "METHOD_ONLY" を作成していないため、
        // beforeTestExecution 内の分岐で「not found → スキップ」となるのが期待動作
        target.beforeAll(ctx);

        // Act & Assert
        assertDoesNotThrow(() -> target.beforeTestExecution(ctx));
    }

    @Test
    public void beforeTestExecution_正常ケース_TempDir上のシナリオ存在時_DataLoaderが実行されること(@TempDir Path tempDir)
            throws Exception {

        // Arrange
        ExtensionContext ctx = mockContextForClass(DummyTargetTest.class);

        // TempDir 配下に、extension が期待するレイアウトを作る
        // trc.baseInputDir("CSV") -> <classRoot>/CSV/input
        // multi-DB なので input/bbb が必要
        Path scenarioInput = tempDir.resolve("CSV").resolve("input").resolve("bbb");
        Files.createDirectories(scenarioInput);

        // ★ trc を TempDir を見る TestResourceContext に差し替える
        // ここでは接続情報は buildEntryFromProps("bbb") が読めれば良い
        Properties props = new Properties();
        props.setProperty("spring.datasource.bbb.url", "jdbc:h2:mem:test");
        props.setProperty("spring.datasource.bbb.username", "sa");
        props.setProperty("spring.datasource.bbb.password", "");
        props.setProperty("spring.datasource.bbb.driver-class-name", "org.h2.Driver");

        TestResourceContext trc = newTrc(tempDir, props);
        var trcField = LoadDataExtension.class.getDeclaredField("trc");
        trcField.setAccessible(true);
        trcField.set(target, trc);

        ApplicationContext applicationContext = mock(ApplicationContext.class);

        // DataSource / Connection / MetaData（resolveComponentsForDbId のメタデータ照合用）
        DataSource ds = mock(DataSource.class);
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);

        when(ds.getConnection()).thenReturn(conn);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getURL()).thenReturn("jdbc:h2:mem:test");
        when(meta.getUserName()).thenReturn("sa");

        PlatformTransactionManager tm = new DataSourceTransactionManager(ds);

        when(applicationContext.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"transactionManager"});
        when(applicationContext.getBean("transactionManager", PlatformTransactionManager.class))
                .thenReturn(tm);

        when(applicationContext.getBeanNamesForType(DataSource.class))
                .thenReturn(new String[] {"dataSource"});
        when(applicationContext.getBean("dataSource", DataSource.class)).thenReturn(ds);

        when(applicationContext.getBeansOfType(TransactionInterceptor.class))
                .thenReturn(Collections.emptyMap());

        // Act / Assert
        try (MockedStatic<SpringExtension> spring = Mockito.mockStatic(SpringExtension.class);
                MockedStatic<DataSourceUtils> dsUtils = Mockito.mockStatic(DataSourceUtils.class);
                MockedStatic<TransactionSynchronizationManager> txSync =
                        Mockito.mockStatic(TransactionSynchronizationManager.class);
                MockedConstruction<DataLoader> loaders =
                        Mockito.mockConstruction(DataLoader.class, (loader, context2) -> {
                            doNothing().when(loader).executeWithConnection(Mockito.any(),
                                    Mockito.any(), Mockito.any());
                        })) {

            spring.when(() -> SpringExtension.getApplicationContext(ctx))
                    .thenReturn(applicationContext);
            dsUtils.when(() -> DataSourceUtils.getConnection(ds)).thenReturn(conn);

            // 既存TXあり扱いにして beginTestTx を回避
            txSync.when(TransactionSynchronizationManager::isActualTransactionActive)
                    .thenReturn(true);
            txSync.when(() -> TransactionSynchronizationManager.hasResource(ds)).thenReturn(true);

            assertDoesNotThrow(() -> target.beforeTestExecution(ctx));

            assertEquals(1, loaders.constructed().size());
            verify(loaders.constructed().get(0), times(1)).executeWithConnection(Mockito.any(),
                    Mockito.any(), Mockito.any());
        }
    }

    @Test
    void beforeTestExecution_異常ケース_Spring管理DataSource未バインドでエラーとなること() throws Exception {
        // Arrange: DataSource をバインドしない
        ExtensionContext ctx = mockContextForClass(DummyTargetTest.class);

        // シナリオ未検出ならスキップされることを確認
        target.beforeAll(ctx);
        assertDoesNotThrow(() -> target.beforeTestExecution(ctx));
    }

    @Test
    public void afterTestExecution_正常ケース_開始済みTXはrollbackされ_TxInterceptorのデフォルトTMは復元されること()
            throws Exception {

        // Arrange: ExtensionContext + Store を「Map バックの疑似 Store」で用意
        ExtensionContext context = mock(ExtensionContext.class);
        Store store = mock(Store.class);
        when(context.getStore(Mockito.any(Namespace.class))).thenReturn(store);

        Map<Object, Object> storeMap = new HashMap<>();

        // Store#get(key, type) / remove(key) を Map で再現
        when(store.get(Mockito.any(), Mockito.any())).thenAnswer(invocation -> {
            Object key = invocation.getArgument(0);
            return storeMap.get(key);
        });
        Mockito.doAnswer(invocation -> {
            Object key = invocation.getArgument(0);
            storeMap.remove(key);
            return null;
        }).when(store).remove(Mockito.any());

        // ===== (1) rollbackAllIfBegan 用の TX_RECORDS を仕込む =====
        Class<?> txRecordListClass =
                Class.forName("io.github.yok.flexdblink.junit.LoadDataExtension$TxRecordList");
        Constructor<?> txRecordListCtor = txRecordListClass.getDeclaredConstructor();
        txRecordListCtor.setAccessible(true);
        Object txRecordList = txRecordListCtor.newInstance();

        Class<?> txRecordClass =
                Class.forName("io.github.yok.flexdblink.junit.LoadDataExtension$TxRecord");
        Constructor<?> txRecordCtor = txRecordClass.getDeclaredConstructor(String.class,
                PlatformTransactionManager.class, TransactionStatus.class);
        txRecordCtor.setAccessible(true);

        PlatformTransactionManager tm1 = mock(PlatformTransactionManager.class);
        PlatformTransactionManager tm2 = mock(PlatformTransactionManager.class);
        TransactionStatus st1 = mock(TransactionStatus.class);
        TransactionStatus st2 = mock(TransactionStatus.class);

        Object r1 = txRecordCtor.newInstance("db1", tm1, st1);
        Object r2 = txRecordCtor.newInstance("db2", tm2, st2);

        @SuppressWarnings("unchecked")
        List<Object> txRecords = (List<Object>) txRecordList;
        txRecords.add(r1);
        txRecords.add(r2);

        // STORE_KEY_TX は "TX_RECORDS"
        storeMap.put("TX_RECORDS", txRecordList);

        // ===== (2) restoreTxInterceptorDefaultManager 用の TXI_DEFAULT_SWITCH を仕込む =====
        Class<?> recClass = Class.forName(
                "io.github.yok.flexdblink.junit.LoadDataExtension$TxInterceptorSwitchRecord");
        Constructor<?> recCtor = recClass.getDeclaredConstructor();
        recCtor.setAccessible(true);
        Object rec = recCtor.newInstance();

        java.lang.reflect.Field touchedField = recClass.getDeclaredField("touched");
        touchedField.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<String, Boolean> touched = (Map<String, Boolean>) touchedField.get(rec);
        touched.put("txInterceptor", Boolean.TRUE);

        // STORE_KEY_TXI_DEFAULT は "TXI_DEFAULT_SWITCH"
        storeMap.put("TXI_DEFAULT_SWITCH", rec);

        // ApplicationContext と TransactionInterceptor（復元対象）を用意
        ApplicationContext applicationContext = mock(ApplicationContext.class);

        TransactionInterceptor interceptor = new TransactionInterceptor();
        // 事前に「切り替え済み」状態を再現（beanName が設定されている状態）
        interceptor.setTransactionManagerBeanName("tmA");

        Map<String, TransactionInterceptor> interceptors = new HashMap<>();
        interceptors.put("txInterceptor", interceptor);
        when(applicationContext.getBeansOfType(TransactionInterceptor.class))
                .thenReturn(interceptors);

        // Act
        try (MockedStatic<SpringExtension> spring = Mockito.mockStatic(SpringExtension.class)) {
            spring.when(() -> SpringExtension.getApplicationContext(context))
                    .thenReturn(applicationContext);

            target.afterTestExecution(context);
        }

        // Assert: rollback が 2 件分呼ばれる
        verify(tm1, times(1)).rollback(st1);
        verify(tm2, times(1)).rollback(st2);

        // Assert: STORE_KEY_TX が消えている
        assertEquals(null, storeMap.get("TX_RECORDS"));

        // Assert: TxInterceptor のデフォルト TM 設定が復元されている（beanName が null）
        assertEquals(null, readTxManagerBeanName(interceptor));

        // Assert: STORE_KEY_TXI_DEFAULT が消えている
        assertEquals(null, storeMap.get("TXI_DEFAULT_SWITCH"));
    }

    @Test
    void wrapConnectionNoClose_正常ケース_close呼び出しが無視されること() throws Exception {
        // Arrange: Connection モックを生成
        Connection original = mock(Connection.class);
        when(original.getAutoCommit()).thenReturn(true); // 委譲確認用

        // private メソッド呼び出し
        Method m = LoadDataExtension.class.getDeclaredMethod("wrapConnectionNoClose",
                Connection.class);
        m.setAccessible(true);
        Connection proxy = (Connection) m.invoke(target, original);

        // Act: close() は無視されるべき
        proxy.close();

        // 他のメソッドは委譲されるべき
        boolean autoCommit = proxy.getAutoCommit();

        // Assert
        verify(original, never()).close();
        verify(original, times(1)).getAutoCommit();
        assertTrue(autoCommit);
    }

    @Test
    void maybeGetSpringManagedDataSource_正常ケース_バインド済みのDataSourceが返却されること() throws Exception {
        // Arrange
        DataSource ds = dummyDataSource();
        TransactionSynchronizationManager.bindResource(ds, new Object());

        TestResourceContext trc = newTrc(dummyClassResourcesDir, new Properties());
        Optional<DataSource> opt = trc.springManagedDataSource();

        // Assert
        assertTrue(opt.isPresent());
        assertSame(ds, opt.get());
    }

    @Test
    void maybeGetSpringManagedDataSource_異常ケース_未バインドの場合は空で返ること() throws Exception {
        TestResourceContext trc = newTrc(dummyClassResourcesDir, new Properties());
        Optional<DataSource> opt = trc.springManagedDataSource();
        assertTrue(opt.isEmpty());
    }

    @Test
    void buildEntryFromProps_正常ケース_db指定ありで接続情報が解決されること() throws Exception {
        Properties p = new Properties();
        p.setProperty("spring.datasource.bbb.url", "jdbc:h2:mem:x");
        p.setProperty("spring.datasource.bbb.username", "sa");
        p.setProperty("spring.datasource.bbb.password", "");
        p.setProperty("spring.datasource.bbb.driver-class-name", "org.h2.Driver");
        TestResourceContext trc = newTrc(dummyClassResourcesDir, p);
        ConnectionConfig.Entry entry = trc.buildEntryFromProps("bbb");

        // Assert
        assertEquals("bbb", entry.getId());
        assertEquals("jdbc:h2:mem:x", entry.getUrl());
        assertEquals("sa", entry.getUser());
        assertEquals("org.h2.Driver", entry.getDriverClass());
    }

    @Test
    void buildEntryFromProps_正常ケース_db指定なしでデフォルト接続情報が解決されること() throws Exception {
        Properties p = new Properties();
        p.setProperty("spring.datasource.url", "jdbc:h2:mem:default");
        p.setProperty("spring.datasource.username", "sa");
        TestResourceContext trc = newTrc(dummyClassResourcesDir, p);
        ConnectionConfig.Entry entry = trc.buildEntryFromProps(null);

        // Assert
        assertEquals("default", entry.getId());
        assertEquals("jdbc:h2:mem:default", entry.getUrl());
        assertEquals("sa", entry.getUser());
    }

    @Test
    void resolveDatasetDir_正常ケース_シナリオありの完全パスが構築されること() throws Exception {
        TestResourceContext trc = newTrc(dummyClassResourcesDir, new Properties());
        Path resolved = trc.inputDir("CSV", "bbb");
        assertTrue(resolved.toString()
                .endsWith(DummyTargetTest.class.getSimpleName() + "/CSV/input/bbb"));
    }

    @Test
    void resolveDatasetDir_正常ケース_シナリオなしの完全パスが構築されること() throws Exception {
        TestResourceContext trc = newTrc(dummyClassResourcesDir, new Properties());
        Path resolved = trc.inputDir("", "bbb");
        assertTrue(
                resolved.toString().endsWith(DummyTargetTest.class.getSimpleName() + "/input/bbb"));
    }

    @Test
    void resolveBaseForDbDetection_正常ケース_シナリオありのベースパスが構築されること() throws Exception {
        TestResourceContext trc = newTrc(dummyClassResourcesDir, new Properties());
        Path base = trc.baseInputDir("CSV");
        assertTrue(base.toString().endsWith(DummyTargetTest.class.getSimpleName() + "/CSV/input"));
    }

    @Test
    void resolveBaseForDbDetection_正常ケース_シナリオなしのベースパスが構築されること() throws Exception {
        TestResourceContext trc = newTrc(dummyClassResourcesDir, new Properties());
        Path base = trc.baseInputDir(null);
        assertTrue(base.toString().endsWith(DummyTargetTest.class.getSimpleName() + "/input"));
    }

    @Test
    void detectDbNames_正常ケース_input配下のDBフォルダが検出されること() throws Exception {
        Path base = dummyClassResourcesDir.resolve("AUTO_SCENARIO").resolve("input");
        mkDirs(base.resolve("bbb"));
        TestResourceContext trc = newTrc(dummyClassResourcesDir, new Properties());
        List<String> names = trc.detectDbIds(base);

        // Assert
        assertTrue(names.contains("bbb"));
    }

    @Test
    void expectedDir_正常ケース_シナリオありのexpectedパスが構築されること() throws Exception {
        TestResourceContext trc = newTrc(dummyClassResourcesDir, new Properties());
        Path expected = trc.expectedDir("CSV", "bbb");
        assertTrue(expected.toString()
                .endsWith(DummyTargetTest.class.getSimpleName() + "/CSV/expected/bbb"));
    }

    @Test
    void baseExpectedDir_正常ケース_シナリオなしのexpectedベースパスが構築されること() throws Exception {
        TestResourceContext trc = newTrc(dummyClassResourcesDir, new Properties());
        Path expected = trc.baseExpectedDir("");
        assertTrue(
                expected.toString().endsWith(DummyTargetTest.class.getSimpleName() + "/expected"));
    }

    @Test
    void listDirectories_正常ケース_親がディレクトリでない場合_空リストが返ること() throws Exception {
        TestResourceContext trc = newTrc(dummyClassResourcesDir, new Properties());
        List<String> dirs = trc.listDirectories(dummyClassResourcesDir.resolve("not-exists"));
        assertTrue(dirs.isEmpty());
    }

    @Test
    void springManagedConnection_正常ケース_datasource未指定の場合_空が返ること() throws Exception {
        TestResourceContext trc = newTrc(dummyClassResourcesDir, new Properties());
        Optional<Connection> conn = trc.springManagedConnection(Optional.empty());
        assertTrue(conn.isEmpty());
    }

    @Test
    void springManagedConnection_正常ケース_datasource指定の場合_接続が返ること() throws Exception {
        DataSource ds = dummyDataSource();
        Connection c = mock(Connection.class);
        when(ds.getConnection()).thenReturn(c);
        TestResourceContext trc = newTrc(dummyClassResourcesDir, new Properties());
        Optional<Connection> conn = trc.springManagedConnection(Optional.of(ds));
        assertTrue(conn.isPresent());
    }

    @Test
    void testResourceContextPrivateMethods_正常ケース_profile解決と列挙変換を行う_期待値が返ること() throws Exception {
        Class<?> cls = TestResourceContext.class;

        Method extractProfile = cls.getDeclaredMethod("extractProfile", String.class);
        extractProfile.setAccessible(true);
        assertEquals("dev", extractProfile.invoke(null, "application-dev.properties"));
        assertEquals("qa", extractProfile.invoke(null, "application-qa.yml"));
        assertEquals("prod", extractProfile.invoke(null, "application-prod.yaml"));
        assertEquals("", extractProfile.invoke(null, "other.properties"));

        Method resolveActiveProfiles =
                cls.getDeclaredMethod("resolveActiveProfiles", Properties.class);
        resolveActiveProfiles.setAccessible(true);
        Properties p = new Properties();
        p.setProperty("spring.profiles.active", "a,b;c");
        @SuppressWarnings("unchecked")
        List<String> actives = (List<String>) resolveActiveProfiles.invoke(null, p);
        assertEquals(List.of("a", "b", "c"), actives);

        System.setProperty("spring.profiles.active", "x;y");
        try {
            @SuppressWarnings("unchecked")
            List<String> sysActives = (List<String>) resolveActiveProfiles.invoke(null, p);
            assertEquals(List.of("x", "y"), sysActives);
        } finally {
            System.clearProperty("spring.profiles.active");
        }

        Method enumToList = cls.getDeclaredMethod("enumToList", Enumeration.class);
        enumToList.setAccessible(true);
        Enumeration<URL> e =
                Collections.enumeration(List.of(new URL("file:/u1"), new URL("file:/u2")));
        @SuppressWarnings("unchecked")
        List<URL> list = (List<URL>) enumToList.invoke(null, e);
        assertEquals("file:/u1", list.get(0).toString());
        assertEquals("file:/u2", list.get(1).toString());
    }

    @Test
    void innerClasses_正常ケース_private内部クラスを生成する_フィールド値が保持されること() throws Exception {
        PlatformTransactionManager tm = mock(PlatformTransactionManager.class);
        TransactionStatus status = mock(TransactionStatus.class);
        DataSource ds = dummyDataSource();

        Class<?> txRecordClass =
                Class.forName("io.github.yok.flexdblink.junit.LoadDataExtension$TxRecord");
        Constructor<?> txCtor = txRecordClass.getDeclaredConstructor(String.class,
                PlatformTransactionManager.class, TransactionStatus.class);
        txCtor.setAccessible(true);
        Object txRecord = txCtor.newInstance("db1", tm, status);
        assertEquals("db1", readField(txRecordClass, txRecord, "key"));
        assertSame(tm, readField(txRecordClass, txRecord, "tm"));
        assertSame(status, readField(txRecordClass, txRecord, "status"));

        Class<?> componentsClass =
                Class.forName("io.github.yok.flexdblink.junit.LoadDataExtension$Components");
        Constructor<?> compCtor = componentsClass.getDeclaredConstructor(DataSource.class,
                PlatformTransactionManager.class);
        compCtor.setAccessible(true);
        Object components = compCtor.newInstance(ds, tm);
        assertSame(ds, readField(componentsClass, components, "dataSource"));
        assertSame(tm, readField(componentsClass, components, "txManager"));

        Class<?> tmWithDsClass =
                Class.forName("io.github.yok.flexdblink.junit.LoadDataExtension$TmWithDs");
        Constructor<?> tmWithDsCtor = tmWithDsClass.getDeclaredConstructor(String.class,
                String.class, PlatformTransactionManager.class, DataSource.class);
        tmWithDsCtor.setAccessible(true);
        Object tmWithDs = tmWithDsCtor.newInstance("tm1", "ds1", tm, ds);
        assertEquals("tm1", readField(tmWithDsClass, tmWithDs, "tmName"));
        assertEquals("ds1", readField(tmWithDsClass, tmWithDs, "dsName"));
        assertSame(tm, readField(tmWithDsClass, tmWithDs, "tm"));
        assertSame(ds, readField(tmWithDsClass, tmWithDs, "ds"));

        Class<?> namedDsClass =
                Class.forName("io.github.yok.flexdblink.junit.LoadDataExtension$NamedDs");
        Constructor<?> namedDsCtor =
                namedDsClass.getDeclaredConstructor(String.class, DataSource.class);
        namedDsCtor.setAccessible(true);
        Object namedDs = namedDsCtor.newInstance("ds2", ds);
        assertEquals("ds2", readField(namedDsClass, namedDs, "name"));
        assertSame(ds, readField(namedDsClass, namedDs, "ds"));

        Class<?> probeMetaClass =
                Class.forName("io.github.yok.flexdblink.junit.LoadDataExtension$ProbeMeta");
        Constructor<?> probeCtor =
                probeMetaClass.getDeclaredConstructor(String.class, String.class);
        probeCtor.setAccessible(true);
        Object probe = probeCtor.newInstance("jdbc:h2:mem:x", "sa");
        assertEquals("jdbc:h2:mem:x", readField(probeMetaClass, probe, "url"));
        assertEquals("sa", readField(probeMetaClass, probe, "user"));

        Class<?> txSwitchClass = Class.forName(
                "io.github.yok.flexdblink.junit.LoadDataExtension$TxInterceptorSwitchRecord");
        Constructor<?> txSwitchCtor = txSwitchClass.getDeclaredConstructor();
        txSwitchCtor.setAccessible(true);
        Object txSwitch = txSwitchCtor.newInstance();
        Object touched = readField(txSwitchClass, txSwitch, "touched");
        assertTrue(touched instanceof Map);
    }

    @Test
    void helperMethods_正常ケース_URL比較と正規化を実行する_期待結果が返ること() throws Exception {
        Method simplifyUrl = LoadDataExtension.class.getDeclaredMethod("simplifyUrl", String.class);
        simplifyUrl.setAccessible(true);
        assertEquals("", simplifyUrl.invoke(target, new Object[] {null}));
        assertEquals("jdbc:h2:mem:test", simplifyUrl.invoke(target, "JDBC:H2:MEM:TEST?a=1;b=2"));

        Method equalsIgnoreCaseSafe = LoadDataExtension.class
                .getDeclaredMethod("equalsIgnoreCaseSafe", String.class, String.class);
        equalsIgnoreCaseSafe.setAccessible(true);
        assertEquals(false, equalsIgnoreCaseSafe.invoke(target, "A", null));
        assertEquals(true, equalsIgnoreCaseSafe.invoke(target, "AbC", "aBc"));

        Method urlRoughMatch = LoadDataExtension.class.getDeclaredMethod("urlRoughMatch",
                String.class, String.class);
        urlRoughMatch.setAccessible(true);
        assertEquals(true,
                urlRoughMatch.invoke(target, "jdbc:h2:mem:t1", "jdbc:h2:mem:t1;MODE=Oracle"));
        assertEquals(false, urlRoughMatch.invoke(target, "", "jdbc:h2:mem:t1"));
    }

    @Test
    void probeDataSourceMeta_正常ケース_メタデータ取得可否を判定する_成功時はProbeMeta失敗時はnullが返ること() throws Exception {
        Method probeDataSourceMeta =
                LoadDataExtension.class.getDeclaredMethod("probeDataSourceMeta", DataSource.class);
        probeDataSourceMeta.setAccessible(true);

        Connection okConn = mock(Connection.class);
        DatabaseMetaData md = mock(DatabaseMetaData.class);
        when(okConn.getMetaData()).thenReturn(md);
        when(md.getURL()).thenReturn("jdbc:h2:mem:ok");
        when(md.getUserName()).thenReturn("sa");
        DataSource okDs = mock(DataSource.class);
        when(okDs.getConnection()).thenReturn(okConn);
        Object meta = probeDataSourceMeta.invoke(target, okDs);
        assertNotNull(meta);
        assertEquals("jdbc:h2:mem:ok", readField(meta.getClass(), meta, "url"));
        assertEquals("sa", readField(meta.getClass(), meta, "user"));

        DataSource ngDs = mock(DataSource.class);
        when(ngDs.getConnection()).thenThrow(new RuntimeException("x"));
        Object nullMeta = probeDataSourceMeta.invoke(target, ngDs);
        assertEquals(null, nullMeta);
    }

    @Test
    void resolveTxAndDsSingle_異常ケース_必須Beanが無い場合に例外化する_IllegalStateExceptionが送出されること()
            throws Exception {
        ApplicationContext ac = mock(ApplicationContext.class);
        when(ac.getBean("transactionManager", PlatformTransactionManager.class))
                .thenThrow(new RuntimeException("missing"));
        when(ac.getBean("dataSource", DataSource.class)).thenThrow(new RuntimeException("missing"));

        Method resolveTxManagerSingle = LoadDataExtension.class
                .getDeclaredMethod("resolveTxManagerSingle", ApplicationContext.class);
        resolveTxManagerSingle.setAccessible(true);
        assertThrows(Exception.class, () -> resolveTxManagerSingle.invoke(target, ac));

        Method resolveDataSourceSingle = LoadDataExtension.class
                .getDeclaredMethod("resolveDataSourceSingle", ApplicationContext.class);
        resolveDataSourceSingle.setAccessible(true);
        assertThrows(Exception.class, () -> resolveDataSourceSingle.invoke(target, ac));
    }

    @Test
    void resolveTxManagerByDataSource_正常異常ケース_TM候補数を判定する_単一は返却し複数ゼロは例外であること() throws Exception {
        Method resolveTxManagerByDataSource =
                LoadDataExtension.class.getDeclaredMethod("resolveTxManagerByDataSource",
                        ApplicationContext.class, String.class, DataSource.class);
        resolveTxManagerByDataSource.setAccessible(true);

        DataSource ds = mock(DataSource.class);
        ApplicationContext single = mock(ApplicationContext.class);
        DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);
        when(single.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tm1"});
        when(single.getBean("tm1", PlatformTransactionManager.class)).thenReturn(tm);
        Object resolved = resolveTxManagerByDataSource.invoke(target, single, "db1", ds);
        assertSame(tm, resolved);

        ApplicationContext none = mock(ApplicationContext.class);
        when(none.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tm1"});
        when(none.getBean("tm1", PlatformTransactionManager.class))
                .thenReturn(new DataSourceTransactionManager(mock(DataSource.class)));
        assertThrows(Exception.class,
                () -> resolveTxManagerByDataSource.invoke(target, none, "db1", ds));

        ApplicationContext multiple = mock(ApplicationContext.class);
        when(multiple.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tm1", "tm2"});
        when(multiple.getBean("tm1", PlatformTransactionManager.class))
                .thenReturn(new DataSourceTransactionManager(ds));
        when(multiple.getBean("tm2", PlatformTransactionManager.class))
                .thenReturn(new DataSourceTransactionManager(ds));
        assertThrows(Exception.class,
                () -> resolveTxManagerByDataSource.invoke(target, multiple, "db1", ds));
    }

    @Test
    void txInterceptorSwitch_正常ケース_デフォルトTMを切替して復元する_設定が更新されること() throws Exception {
        // Arrange
        ExtensionContext context = mock(ExtensionContext.class);
        Store store = mock(Store.class);
        when(context.getStore(Mockito.any(Namespace.class))).thenReturn(store);
        Map<Object, Object> storeMap = new HashMap<>();
        when(store.getOrComputeIfAbsent(Mockito.any(), Mockito.<Function<Object, Object>>any(),
                Mockito.any())).thenAnswer(invocation -> {
                    Object key = invocation.getArgument(0);
                    Function<Object, Object> creator = invocation.getArgument(1);
                    if (!storeMap.containsKey(key)) {
                        storeMap.put(key, creator.apply(key));
                    }
                    return storeMap.get(key);
                });
        when(store.get(Mockito.any(), Mockito.any()))
                .thenAnswer(invocation -> storeMap.get(invocation.getArgument(0)));
        Mockito.doAnswer(invocation -> {
            storeMap.remove(invocation.getArgument(0));
            return null;
        }).when(store).remove(Mockito.any());

        ApplicationContext applicationContext = mock(ApplicationContext.class);
        PlatformTransactionManager transactionManager = mock(PlatformTransactionManager.class);
        when(applicationContext.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tmA"});
        when(applicationContext.getBean("tmA", PlatformTransactionManager.class))
                .thenReturn(transactionManager);

        TransactionInterceptor interceptor = new TransactionInterceptor();
        Map<String, TransactionInterceptor> interceptors = new HashMap<>();
        interceptors.put("txInterceptor", interceptor);
        when(applicationContext.getBeansOfType(TransactionInterceptor.class))
                .thenReturn(interceptors);

        Method setTxInterceptorDefaultManager = LoadDataExtension.class.getDeclaredMethod(
                "setTxInterceptorDefaultManager", ExtensionContext.class, ApplicationContext.class,
                PlatformTransactionManager.class, String.class);
        setTxInterceptorDefaultManager.setAccessible(true);

        Method restoreTxInterceptorDefaultManager = LoadDataExtension.class
                .getDeclaredMethod("restoreTxInterceptorDefaultManager", ExtensionContext.class);
        restoreTxInterceptorDefaultManager.setAccessible(true);

        // Act
        setTxInterceptorDefaultManager.invoke(target, context, applicationContext,
                transactionManager, "single");
        assertEquals("tmA", readTxManagerBeanName(interceptor));

        try (MockedStatic<SpringExtension> mocked = Mockito.mockStatic(SpringExtension.class)) {
            mocked.when(() -> SpringExtension.getApplicationContext(context))
                    .thenReturn(applicationContext);
            restoreTxInterceptorDefaultManager.invoke(target, context);
        }

        // Assert
        assertEquals(null, readTxManagerBeanName(interceptor));
    }

    @Test
    void rollbackAllIfBegan_正常ケース_開始済みトランザクションを巻き戻す_rollbackが実行されること() throws Exception {
        // Arrange
        ExtensionContext context = mock(ExtensionContext.class);
        Store store = mock(Store.class);
        when(context.getStore(Mockito.any(Namespace.class))).thenReturn(store);

        Class<?> txRecordListClass =
                Class.forName("io.github.yok.flexdblink.junit.LoadDataExtension$TxRecordList");
        Constructor<?> txRecordListConstructor = txRecordListClass.getDeclaredConstructor();
        txRecordListConstructor.setAccessible(true);
        Object txRecordList = txRecordListConstructor.newInstance();

        PlatformTransactionManager transactionManager = mock(PlatformTransactionManager.class);
        TransactionStatus transactionStatus = mock(TransactionStatus.class);
        Class<?> txRecordClass =
                Class.forName("io.github.yok.flexdblink.junit.LoadDataExtension$TxRecord");
        Constructor<?> txRecordConstructor = txRecordClass.getDeclaredConstructor(String.class,
                PlatformTransactionManager.class, TransactionStatus.class);
        txRecordConstructor.setAccessible(true);
        Object txRecord =
                txRecordConstructor.newInstance("db1", transactionManager, transactionStatus);
        @SuppressWarnings("unchecked")
        List<Object> castedList = (List<Object>) txRecordList;
        castedList.add(txRecord);

        doReturn(txRecordList).when(store).get("TX_RECORDS", txRecordListClass);

        Method rollbackAllIfBegan = LoadDataExtension.class.getDeclaredMethod("rollbackAllIfBegan",
                ExtensionContext.class);
        rollbackAllIfBegan.setAccessible(true);

        // Act
        rollbackAllIfBegan.invoke(target, context);

        // Assert
        verify(transactionManager, times(1)).rollback(transactionStatus);
    }

    @Test
    void resolveComponentsForDbId_正常ケース_TMメタデータ一致時にコンポーネントを解決する_DSとTMが返ること() throws Exception {
        // Arrange
        Properties props = new Properties();
        props.setProperty("spring.datasource.db1.url", "jdbc:h2:mem:match");
        props.setProperty("spring.datasource.db1.username", "sa");
        TestResourceContext trc = newTrc(dummyClassResourcesDir, props);
        var trcField = LoadDataExtension.class.getDeclaredField("trc");
        trcField.setAccessible(true);
        trcField.set(target, trc);

        DataSource ds = mock(DataSource.class);
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(ds.getConnection()).thenReturn(conn);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getURL()).thenReturn("jdbc:h2:mem:match");
        when(meta.getUserName()).thenReturn("sa");

        DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);
        ApplicationContext ac = mock(ApplicationContext.class);
        when(ac.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tm1"});
        when(ac.getBean("tm1", PlatformTransactionManager.class)).thenReturn(tm);
        when(ac.getBeanNamesForType(DataSource.class)).thenReturn(new String[] {"ds1"});
        when(ac.getBean("ds1", DataSource.class)).thenReturn(ds);

        Method resolveComponentsForDbId = LoadDataExtension.class.getDeclaredMethod(
                "resolveComponentsForDbId", ApplicationContext.class, String.class);
        resolveComponentsForDbId.setAccessible(true);

        // Act
        Object components = resolveComponentsForDbId.invoke(target, ac, "db1");

        // Assert
        Class<?> componentsClass =
                Class.forName("io.github.yok.flexdblink.junit.LoadDataExtension$Components");
        assertSame(ds, readField(componentsClass, components, "dataSource"));
        assertSame(tm, readField(componentsClass, components, "txManager"));
    }

    @Test
    void listDbIdsFromFolder_正常ケース_入力配下のディレクトリ名を抽出する_DB名セットが返ること() throws Exception {
        // Arrange
        Path base = dummyClassResourcesDir.resolve("LIST_DBIDS").resolve("input");
        mkDirs(base.resolve("db1"));
        mkDirs(base.resolve("db2"));
        writeToFile(base.resolve("ignore.txt"), "x");

        Method listDbIdsFromFolder =
                LoadDataExtension.class.getDeclaredMethod("listDbIdsFromFolder", Path.class);
        listDbIdsFromFolder.setAccessible(true);

        // Act
        @SuppressWarnings("unchecked")
        Set<String> dbIds = (Set<String>) listDbIdsFromFolder.invoke(target, base);

        // Assert
        assertEquals(Set.of("db1", "db2"), dbIds);
    }

    @Test
    void findBeanNameByInstance_正常ケース_参照一致するBean名を検索する_Bean名が返ること() throws Exception {
        // Arrange
        ApplicationContext applicationContext = mock(ApplicationContext.class);
        DataSource ds1 = mock(DataSource.class);
        DataSource ds2 = mock(DataSource.class);
        when(applicationContext.getBeanNamesForType(DataSource.class))
                .thenReturn(new String[] {"dataSource1", "dataSource2"});
        when(applicationContext.getBean("dataSource1", DataSource.class)).thenReturn(ds1);
        when(applicationContext.getBean("dataSource2", DataSource.class)).thenReturn(ds2);

        Method findBeanNameByInstance = LoadDataExtension.class.getDeclaredMethod(
                "findBeanNameByInstance", ApplicationContext.class, Class.class, Object.class);
        findBeanNameByInstance.setAccessible(true);

        // Act
        Object resolved =
                findBeanNameByInstance.invoke(target, applicationContext, DataSource.class, ds2);

        // Assert
        assertEquals("dataSource2", resolved);
    }

    @Test
    void loadScenarioParticipating_正常ケース_singleDBでシナリオ読込する_DataLoader実行まで到達すること() throws Exception {
        // Arrange
        Path scenarioInput = dummyClassResourcesDir.resolve("S1").resolve("input");
        mkDirs(scenarioInput);

        Properties props = new Properties();
        props.setProperty("spring.datasource.url", "jdbc:h2:mem:s1");
        props.setProperty("spring.datasource.username", "sa");
        TestResourceContext trc = newTrc(dummyClassResourcesDir, props);
        var trcField = LoadDataExtension.class.getDeclaredField("trc");
        trcField.setAccessible(true);
        trcField.set(target, trc);

        ExtensionContext context = mock(ExtensionContext.class);
        Store store = mock(Store.class);
        when(context.getStore(Mockito.any(Namespace.class))).thenReturn(store);
        Map<Object, Object> storeMap = new HashMap<>();
        when(store.getOrComputeIfAbsent(Mockito.any(), Mockito.<Function<Object, Object>>any(),
                Mockito.any())).thenAnswer(invocation -> {
                    Object key = invocation.getArgument(0);
                    Function<Object, Object> creator = invocation.getArgument(1);
                    if (!storeMap.containsKey(key)) {
                        storeMap.put(key, creator.apply(key));
                    }
                    return storeMap.get(key);
                });
        when(store.get(Mockito.any(), Mockito.any()))
                .thenAnswer(invocation -> storeMap.get(invocation.getArgument(0)));
        Mockito.doAnswer(invocation -> {
            storeMap.remove(invocation.getArgument(0));
            return null;
        }).when(store).remove(Mockito.any());

        ApplicationContext applicationContext = mock(ApplicationContext.class);
        DataSource ds = mock(DataSource.class);
        PlatformTransactionManager tm = mock(PlatformTransactionManager.class);
        TransactionStatus status = mock(TransactionStatus.class);
        Connection conn = mock(Connection.class);

        when(applicationContext.getBean("transactionManager", PlatformTransactionManager.class))
                .thenReturn(tm);
        when(applicationContext.getBean("dataSource", DataSource.class)).thenReturn(ds);
        when(applicationContext.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"transactionManager"});
        when(applicationContext.getBeansOfType(TransactionInterceptor.class))
                .thenReturn(Collections.emptyMap());
        when(tm.getTransaction(Mockito.any())).thenReturn(status);

        Method loadScenarioParticipating = LoadDataExtension.class.getDeclaredMethod(
                "loadScenarioParticipating", ExtensionContext.class, String.class, String[].class);
        loadScenarioParticipating.setAccessible(true);

        // Act / Assert
        try (MockedStatic<SpringExtension> spring = Mockito.mockStatic(SpringExtension.class);
                MockedStatic<DataSourceUtils> dsUtils = Mockito.mockStatic(DataSourceUtils.class);
                MockedConstruction<DataLoader> loaders = Mockito.mockConstruction(DataLoader.class,
                        (loader, context2) -> doNothing().when(loader).executeWithConnection(
                                Mockito.any(), Mockito.any(), Mockito.any()))) {
            spring.when(() -> SpringExtension.getApplicationContext(context))
                    .thenReturn(applicationContext);
            dsUtils.when(() -> DataSourceUtils.getConnection(ds)).thenReturn(conn);

            assertDoesNotThrow(
                    () -> loadScenarioParticipating.invoke(target, context, "S1", new String[0]));

            DataLoader loader = loaders.constructed().get(0);
            verify(loader, times(1)).executeWithConnection(Mockito.any(), Mockito.any(),
                    Mockito.any());
        }
    }

    @Test
    void loadScenarioParticipating_異常ケース_multiDBで指定フォルダ不足の場合_IllegalStateExceptionが送出されること()
            throws Exception {
        Path baseInput = dummyClassResourcesDir.resolve("S_MULTI_MISSING").resolve("input");
        mkDirs(baseInput.resolve("db1"));

        Properties props = new Properties();
        props.setProperty("spring.datasource.db1.url", "jdbc:h2:mem:db1");
        props.setProperty("spring.datasource.db1.username", "sa");
        TestResourceContext trc = newTrc(dummyClassResourcesDir, props);
        var trcField = LoadDataExtension.class.getDeclaredField("trc");
        trcField.setAccessible(true);
        trcField.set(target, trc);

        ExtensionContext context = mock(ExtensionContext.class);
        when(context.getStore(Mockito.any(Namespace.class))).thenReturn(mock(Store.class));

        Method loadScenarioParticipating = LoadDataExtension.class.getDeclaredMethod(
                "loadScenarioParticipating", ExtensionContext.class, String.class, String[].class);
        loadScenarioParticipating.setAccessible(true);

        try (MockedStatic<SpringExtension> spring = Mockito.mockStatic(SpringExtension.class)) {
            spring.when(() -> SpringExtension.getApplicationContext(context))
                    .thenReturn(mock(ApplicationContext.class));
            assertThrows(Exception.class, () -> loadScenarioParticipating.invoke(target, context,
                    "S_MULTI_MISSING", new String[] {"db1", "db2"}));
        }
    }

    @Test
    void loadScenarioParticipating_異常ケース_multiDBで既存TXにDS未バインドの場合_IllegalStateExceptionが送出されること()
            throws Exception {
        Path baseInput = dummyClassResourcesDir.resolve("S_MULTI_ACTIVE").resolve("input");
        mkDirs(baseInput.resolve("db1"));

        Properties props = new Properties();
        props.setProperty("spring.datasource.db1.url", "jdbc:h2:mem:match");
        props.setProperty("spring.datasource.db1.username", "sa");
        TestResourceContext trc = newTrc(dummyClassResourcesDir, props);
        var trcField = LoadDataExtension.class.getDeclaredField("trc");
        trcField.setAccessible(true);
        trcField.set(target, trc);

        DataSource ds = mock(DataSource.class);
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(ds.getConnection()).thenReturn(conn);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getURL()).thenReturn("jdbc:h2:mem:match");
        when(meta.getUserName()).thenReturn("sa");

        DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);
        ApplicationContext ac = mock(ApplicationContext.class);
        when(ac.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tm1"});
        when(ac.getBean("tm1", PlatformTransactionManager.class)).thenReturn(tm);
        when(ac.getBeanNamesForType(DataSource.class)).thenReturn(new String[] {"ds1"});
        when(ac.getBean("ds1", DataSource.class)).thenReturn(ds);
        when(ac.getBeansOfType(TransactionInterceptor.class)).thenReturn(Collections.emptyMap());

        ExtensionContext context = mock(ExtensionContext.class);
        when(context.getStore(Mockito.any(Namespace.class))).thenReturn(mock(Store.class));

        Method loadScenarioParticipating = LoadDataExtension.class.getDeclaredMethod(
                "loadScenarioParticipating", ExtensionContext.class, String.class, String[].class);
        loadScenarioParticipating.setAccessible(true);

        try (MockedStatic<SpringExtension> spring = Mockito.mockStatic(SpringExtension.class);
                MockedStatic<TransactionSynchronizationManager> txSync =
                        Mockito.mockStatic(TransactionSynchronizationManager.class)) {
            spring.when(() -> SpringExtension.getApplicationContext(context)).thenReturn(ac);
            txSync.when(TransactionSynchronizationManager::isActualTransactionActive)
                    .thenReturn(true);
            txSync.when(() -> TransactionSynchronizationManager.hasResource(ds)).thenReturn(false);
            assertThrows(Exception.class, () -> loadScenarioParticipating.invoke(target, context,
                    "S_MULTI_ACTIVE", new String[] {"db1"}));
        }
    }

    @Test
    void pickPrimary_正常ケース_primaryが1件のみ存在する_primary候補が返ること() throws Exception {
        DataSource ds1 = mock(DataSource.class);
        DataSource ds2 = mock(DataSource.class);
        Class<?> namedDsClass =
                Class.forName("io.github.yok.flexdblink.junit.LoadDataExtension$NamedDs");
        Constructor<?> ctor = namedDsClass.getDeclaredConstructor(String.class, DataSource.class);
        ctor.setAccessible(true);
        Object n1 = ctor.newInstance("ds1", ds1);
        Object n2 = ctor.newInstance("ds2", ds2);

        ConfigurableApplicationContext ac = mock(ConfigurableApplicationContext.class);
        ConfigurableListableBeanFactory bf = mock(ConfigurableListableBeanFactory.class);
        when(ac.getBeanFactory()).thenReturn(bf);
        when(bf.containsBeanDefinition("ds1")).thenReturn(true);
        when(bf.containsBeanDefinition("ds2")).thenReturn(true);
        BeanDefinition bd1 = mock(BeanDefinition.class);
        BeanDefinition bd2 = mock(BeanDefinition.class);
        when(bf.getBeanDefinition("ds1")).thenReturn(bd1);
        when(bf.getBeanDefinition("ds2")).thenReturn(bd2);
        when(bd1.isPrimary()).thenReturn(false);
        when(bd2.isPrimary()).thenReturn(true);

        Method pickPrimary = LoadDataExtension.class.getDeclaredMethod("pickPrimary",
                ApplicationContext.class, List.class);
        pickPrimary.setAccessible(true);
        Object picked = pickPrimary.invoke(target, ac, List.of(n1, n2));
        assertEquals("ds2", readField(namedDsClass, picked, "name"));
    }

    @Test
    void pickSingleReferencedByTm_正常ケース_TM参照が1件のみの場合_該当候補が返ること() throws Exception {
        DataSource ds1 = mock(DataSource.class);
        DataSource ds2 = mock(DataSource.class);
        DataSource ds3 = mock(DataSource.class);

        Class<?> namedDsClass =
                Class.forName("io.github.yok.flexdblink.junit.LoadDataExtension$NamedDs");
        Constructor<?> ctor = namedDsClass.getDeclaredConstructor(String.class, DataSource.class);
        ctor.setAccessible(true);
        Object n1 = ctor.newInstance("ds1", ds1);
        Object n2 = ctor.newInstance("ds2", ds2);

        ApplicationContext ac = mock(ApplicationContext.class);
        when(ac.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tm1"});
        when(ac.getBean("tm1", PlatformTransactionManager.class))
                .thenReturn(new DataSourceTransactionManager(ds2));

        Method pickSingleReferencedByTm = LoadDataExtension.class.getDeclaredMethod(
                "pickSingleReferencedByTm", ApplicationContext.class, List.class);
        pickSingleReferencedByTm.setAccessible(true);
        Object picked = pickSingleReferencedByTm.invoke(target, ac, List.of(n1, n2));
        assertEquals("ds2", readField(namedDsClass, picked, "name"));

        when(ac.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tm1", "tm2"});
        when(ac.getBean("tm2", PlatformTransactionManager.class))
                .thenReturn(new DataSourceTransactionManager(ds1));
        Object ambiguous = pickSingleReferencedByTm.invoke(target, ac, List.of(n1, n2));
        assertEquals(null, ambiguous);

        when(ac.getBean("tm2", PlatformTransactionManager.class))
                .thenReturn(new DataSourceTransactionManager(ds3));
        Object none = pickSingleReferencedByTm.invoke(target, ac, List.of(n1));
        assertEquals(null, none);
    }

    @Test
    void loadScenarioParticipating_正常ケース_multiDBで既存TXなしの場合_DBごとにロード実行されること() throws Exception {
        Path baseInput = dummyClassResourcesDir.resolve("S_MULTI_OK").resolve("input");
        mkDirs(baseInput.resolve("db1"));
        mkDirs(baseInput.resolve("db2"));
        mkDirs(baseInput.resolve("extra_db"));

        Properties props = new Properties();
        props.setProperty("spring.datasource.db1.url", "jdbc:h2:mem:db1");
        props.setProperty("spring.datasource.db1.username", "sa");
        props.setProperty("spring.datasource.db2.url", "jdbc:h2:mem:db2");
        props.setProperty("spring.datasource.db2.username", "sa2");
        TestResourceContext trc = newTrc(dummyClassResourcesDir, props);
        var trcField = LoadDataExtension.class.getDeclaredField("trc");
        trcField.setAccessible(true);
        trcField.set(target, trc);

        DataSource ds1 = mock(DataSource.class);
        DataSource ds2 = mock(DataSource.class);
        Connection metaConn1 = mock(Connection.class);
        Connection metaConn2 = mock(Connection.class);
        DatabaseMetaData meta1 = mock(DatabaseMetaData.class);
        DatabaseMetaData meta2 = mock(DatabaseMetaData.class);
        when(ds1.getConnection()).thenReturn(metaConn1);
        when(ds2.getConnection()).thenReturn(metaConn2);
        when(metaConn1.getMetaData()).thenReturn(meta1);
        when(metaConn2.getMetaData()).thenReturn(meta2);
        when(meta1.getURL()).thenReturn("jdbc:h2:mem:db1");
        when(meta1.getUserName()).thenReturn("sa");
        when(meta2.getURL()).thenReturn("jdbc:h2:mem:db2");
        when(meta2.getUserName()).thenReturn("sa2");

        DataSourceTransactionManager tm1 = mock(DataSourceTransactionManager.class);
        DataSourceTransactionManager tm2 = mock(DataSourceTransactionManager.class);
        TransactionStatus status1 = mock(TransactionStatus.class);
        TransactionStatus status2 = mock(TransactionStatus.class);
        when(tm1.getDataSource()).thenReturn(ds1);
        when(tm2.getDataSource()).thenReturn(ds2);
        when(tm1.getTransaction(Mockito.any())).thenReturn(status1);
        when(tm2.getTransaction(Mockito.any())).thenReturn(status2);

        ApplicationContext ac = mock(ApplicationContext.class);
        when(ac.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tm1", "tm2"});
        when(ac.getBean("tm1", PlatformTransactionManager.class)).thenReturn(tm1);
        when(ac.getBean("tm2", PlatformTransactionManager.class)).thenReturn(tm2);
        when(ac.getBeanNamesForType(DataSource.class)).thenReturn(new String[] {"ds1", "ds2"});
        when(ac.getBean("ds1", DataSource.class)).thenReturn(ds1);
        when(ac.getBean("ds2", DataSource.class)).thenReturn(ds2);

        ExtensionContext context = mock(ExtensionContext.class);
        Store store = mock(Store.class);
        when(context.getStore(Mockito.any(Namespace.class))).thenReturn(store);
        Map<Object, Object> storeMap = new HashMap<>();
        when(store.getOrComputeIfAbsent(Mockito.any(), Mockito.<Function<Object, Object>>any(),
                Mockito.any())).thenAnswer(invocation -> {
                    Object key = invocation.getArgument(0);
                    Function<Object, Object> creator = invocation.getArgument(1);
                    if (!storeMap.containsKey(key)) {
                        storeMap.put(key, creator.apply(key));
                    }
                    return storeMap.get(key);
                });
        when(store.get(Mockito.any(), Mockito.any()))
                .thenAnswer(invocation -> storeMap.get(invocation.getArgument(0)));
        Mockito.doAnswer(invocation -> {
            storeMap.remove(invocation.getArgument(0));
            return null;
        }).when(store).remove(Mockito.any());

        Connection loadConn1 = mock(Connection.class);
        Connection loadConn2 = mock(Connection.class);

        Method loadScenarioParticipating = LoadDataExtension.class.getDeclaredMethod(
                "loadScenarioParticipating", ExtensionContext.class, String.class, String[].class);
        loadScenarioParticipating.setAccessible(true);

        try (MockedStatic<SpringExtension> spring = Mockito.mockStatic(SpringExtension.class);
                MockedStatic<DataSourceUtils> dsUtils = Mockito.mockStatic(DataSourceUtils.class);
                MockedConstruction<DataLoader> loaders = Mockito.mockConstruction(DataLoader.class,
                        (loader, context2) -> doNothing().when(loader).executeWithConnection(
                                Mockito.any(), Mockito.any(), Mockito.any()))) {
            spring.when(() -> SpringExtension.getApplicationContext(context)).thenReturn(ac);
            dsUtils.when(() -> DataSourceUtils.getConnection(ds1)).thenReturn(loadConn1);
            dsUtils.when(() -> DataSourceUtils.getConnection(ds2)).thenReturn(loadConn2);

            assertDoesNotThrow(() -> loadScenarioParticipating.invoke(target, context, "S_MULTI_OK",
                    new String[] {"db1", "db2"}));

            DataLoader loader = loaders.constructed().get(0);
            verify(loader, times(2)).executeWithConnection(Mockito.any(), Mockito.any(),
                    Mockito.any());
            verify(tm1).getTransaction(Mockito.any());
            verify(tm2).getTransaction(Mockito.any());
        }
    }

    @Test
    void beforeTestExecution_正常ケース_trc未初期化時にbeforeAllを実行する_trcが設定されること() throws Exception {
        // Arrange
        var trcField = LoadDataExtension.class.getDeclaredField("trc");
        trcField.setAccessible(true);
        trcField.set(target, null);

        ExtensionContext context = mockContextForClass(DummyTargetTest.class);

        // Act
        assertDoesNotThrow(() -> target.beforeTestExecution(context));

        // Assert
        assertNotNull(getTrc(target));
    }

    @Test
    void beforeTestExecution_正常ケース_メソッドアノテーションのシナリオ存在時にロードする_DataLoaderが実行されること() throws Exception {
        // Arrange
        Path inputDir =
                dummyClassClassPathDir.resolve("METHOD_ONLY").resolve("input").resolve("bbb");
        mkDirs(inputDir);

        Method testMethod = DummyTargetTest.class.getDeclaredMethod("methodHasScenario");
        ExtensionContext context = mock(ExtensionContext.class);
        doReturn(DummyTargetTest.class).when(context).getRequiredTestClass();
        doReturn(Optional.of(testMethod)).when(context).getTestMethod();

        Map<Object, Object> storeMap = new HashMap<>();
        Store store = mockStore(context, storeMap);

        ApplicationContext applicationContext = mock(ApplicationContext.class);
        DataSource ds = mock(DataSource.class);
        DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(ds.getConnection()).thenReturn(conn);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getURL()).thenReturn("jdbc:h2:mem:test");
        when(meta.getUserName()).thenReturn("sa");

        when(applicationContext.getBean("transactionManager", PlatformTransactionManager.class))
                .thenReturn(tm);
        when(applicationContext.getBean("dataSource", DataSource.class)).thenReturn(ds);
        when(applicationContext.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"transactionManager"});
        when(applicationContext.getBeanNamesForType(DataSource.class))
                .thenReturn(new String[] {"ds1"});
        when(applicationContext.getBean("ds1", DataSource.class)).thenReturn(ds);
        when(applicationContext.getBean("transactionManager", PlatformTransactionManager.class))
                .thenReturn(tm);
        when(applicationContext.getBeansOfType(TransactionInterceptor.class))
                .thenReturn(Collections.emptyMap());

        target.beforeAll(context);

        // Act / Assert
        try (MockedStatic<SpringExtension> spring = Mockito.mockStatic(SpringExtension.class);
                MockedStatic<DataSourceUtils> dsUtils = Mockito.mockStatic(DataSourceUtils.class);
                MockedConstruction<DataLoader> loaders = Mockito.mockConstruction(DataLoader.class,
                        (loader, context2) -> doNothing().when(loader).executeWithConnection(
                                Mockito.any(), Mockito.any(), Mockito.any()))) {
            spring.when(() -> SpringExtension.getApplicationContext(context))
                    .thenReturn(applicationContext);
            dsUtils.when(() -> DataSourceUtils.getConnection(ds)).thenReturn(conn);

            assertDoesNotThrow(() -> target.beforeTestExecution(context));

            DataLoader loader = loaders.constructed().get(0);
            verify(loader, times(1)).executeWithConnection(Mockito.any(), Mockito.any(),
                    Mockito.any());
            assertNotNull(store);
        }
    }

    @Test
    void beforeTestExecution_異常ケース_メソッド処理で例外発生時にRuntimeExceptionへ変換する_RuntimeExceptionが送出されること()
            throws Exception {
        // Arrange
        Path scenarioBase = dummyClassClassPathDir.resolve("METHOD_ONLY");
        mkDirs(scenarioBase);

        Method testMethod = DummyTargetTest.class.getDeclaredMethod("methodHasScenario");
        ExtensionContext context = mock(ExtensionContext.class);
        doReturn(DummyTargetTest.class).when(context).getRequiredTestClass();
        doReturn(Optional.of(testMethod)).when(context).getTestMethod();

        target.beforeAll(context);

        // Act / Assert
        try (MockedStatic<SpringExtension> spring = Mockito.mockStatic(SpringExtension.class)) {
            spring.when(() -> SpringExtension.getApplicationContext(context))
                    .thenReturn(mock(ApplicationContext.class));

            RuntimeException ex =
                    assertThrows(RuntimeException.class, () -> target.beforeTestExecution(context));
            assertNotNull(ex.getCause());
        }
    }

    @Test
    void loadScenarioParticipating_正常ケース_PathsConfigGetterとSchemaResolverを通過する_設定値と例外が期待通りであること()
            throws Exception {
        // Arrange
        Path scenarioInput = dummyClassResourcesDir.resolve("S_PATHS").resolve("input");
        mkDirs(scenarioInput);

        Properties props = new Properties();
        props.setProperty("spring.datasource.url", "jdbc:h2:mem:paths");
        props.setProperty("spring.datasource.username", "sa");
        TestResourceContext trc = newTrc(dummyClassResourcesDir, props);
        var trcField = LoadDataExtension.class.getDeclaredField("trc");
        trcField.setAccessible(true);
        trcField.set(target, trc);

        ExtensionContext context = mock(ExtensionContext.class);
        Store store = mockStore(context, new HashMap<>());

        ApplicationContext applicationContext = mock(ApplicationContext.class);
        DataSource ds = mock(DataSource.class);
        PlatformTransactionManager tm = mock(PlatformTransactionManager.class);
        TransactionStatus status = mock(TransactionStatus.class);
        Connection conn = mock(Connection.class);

        when(applicationContext.getBean("transactionManager", PlatformTransactionManager.class))
                .thenReturn(tm);
        when(applicationContext.getBean("dataSource", DataSource.class)).thenReturn(ds);
        when(applicationContext.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"transactionManager"});
        when(applicationContext.getBeansOfType(TransactionInterceptor.class))
                .thenReturn(Collections.emptyMap());
        when(tm.getTransaction(Mockito.any())).thenReturn(status);

        Method loadScenarioParticipating = LoadDataExtension.class.getDeclaredMethod(
                "loadScenarioParticipating", ExtensionContext.class, String.class, String[].class);
        loadScenarioParticipating.setAccessible(true);

        // Act / Assert
        try (MockedStatic<SpringExtension> spring = Mockito.mockStatic(SpringExtension.class);
                MockedStatic<DataSourceUtils> dsUtils = Mockito.mockStatic(DataSourceUtils.class);
                MockedConstruction<DataLoader> loaders =
                        Mockito.mockConstruction(DataLoader.class, (loader, context2) -> {
                            PathsConfig pathsConfig = (PathsConfig) context2.arguments().get(0);
                            assertTrue(pathsConfig.getLoad().contains("DummyTargetTest"));
                            assertTrue(pathsConfig.getDataPath().contains("DummyTargetTest"));
                            assertTrue(pathsConfig.getDump().contains("target/dbunit/dump"));

                            @SuppressWarnings("unchecked")
                            Function<ConnectionConfig.Entry, String> schemaResolver =
                                    (Function<ConnectionConfig.Entry, String>) context2.arguments()
                                            .get(2);
                            ConnectionConfig.Entry okEntry = new ConnectionConfig.Entry();
                            okEntry.setUser("scott");
                            assertEquals("SCOTT", schemaResolver.apply(okEntry));

                            ConnectionConfig.Entry ngEntry = new ConnectionConfig.Entry();
                            ngEntry.setUser(null);
                            assertThrows(IllegalStateException.class,
                                    () -> schemaResolver.apply(ngEntry));

                            doNothing().when(loader).executeWithConnection(Mockito.any(),
                                    Mockito.any(), Mockito.any());
                        })) {
            spring.when(() -> SpringExtension.getApplicationContext(context))
                    .thenReturn(applicationContext);
            dsUtils.when(() -> DataSourceUtils.getConnection(ds)).thenReturn(conn);

            assertDoesNotThrow(() -> loadScenarioParticipating.invoke(target, context, "S_PATHS",
                    new String[0]));

            DataLoader loader = loaders.constructed().get(0);
            verify(loader).executeWithConnection(Mockito.any(), Mockito.any(), Mockito.any());
            assertNotNull(store);
        }
    }

    @Test
    void loadScenarioParticipating_異常ケース_singleDBで既存TXにDS未バインドの場合でも処理継続する_DataLoaderが実行されること()
            throws Exception {
        // Arrange
        Path scenarioInput = dummyClassResourcesDir.resolve("S_SINGLE_ACTIVE").resolve("input");
        mkDirs(scenarioInput);

        Properties props = new Properties();
        props.setProperty("spring.datasource.url", "jdbc:h2:mem:single_active");
        props.setProperty("spring.datasource.username", "sa");
        TestResourceContext trc = newTrc(dummyClassResourcesDir, props);
        var trcField = LoadDataExtension.class.getDeclaredField("trc");
        trcField.setAccessible(true);
        trcField.set(target, trc);

        ExtensionContext context = mock(ExtensionContext.class);
        mockStore(context, new HashMap<>());

        ApplicationContext applicationContext = mock(ApplicationContext.class);
        DataSource ds = mock(DataSource.class);
        PlatformTransactionManager tm = mock(PlatformTransactionManager.class);
        Connection conn = mock(Connection.class);

        when(applicationContext.getBean("transactionManager", PlatformTransactionManager.class))
                .thenReturn(tm);
        when(applicationContext.getBean("dataSource", DataSource.class)).thenReturn(ds);
        when(applicationContext.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"transactionManager"});
        when(applicationContext.getBeansOfType(TransactionInterceptor.class))
                .thenReturn(Collections.emptyMap());

        Method loadScenarioParticipating = LoadDataExtension.class.getDeclaredMethod(
                "loadScenarioParticipating", ExtensionContext.class, String.class, String[].class);
        loadScenarioParticipating.setAccessible(true);

        // Act / Assert
        try (MockedStatic<SpringExtension> spring = Mockito.mockStatic(SpringExtension.class);
                MockedStatic<DataSourceUtils> dsUtils = Mockito.mockStatic(DataSourceUtils.class);
                MockedStatic<TransactionSynchronizationManager> txSync =
                        Mockito.mockStatic(TransactionSynchronizationManager.class);
                MockedConstruction<DataLoader> loaders = Mockito.mockConstruction(DataLoader.class,
                        (loader, context2) -> doNothing().when(loader).executeWithConnection(
                                Mockito.any(), Mockito.any(), Mockito.any()))) {
            spring.when(() -> SpringExtension.getApplicationContext(context))
                    .thenReturn(applicationContext);
            dsUtils.when(() -> DataSourceUtils.getConnection(ds)).thenReturn(conn);
            txSync.when(TransactionSynchronizationManager::isActualTransactionActive)
                    .thenReturn(true);
            txSync.when(() -> TransactionSynchronizationManager.hasResource(ds)).thenReturn(false);

            assertDoesNotThrow(() -> loadScenarioParticipating.invoke(target, context,
                    "S_SINGLE_ACTIVE", new String[0]));

            DataLoader loader = loaders.constructed().get(0);
            verify(loader).executeWithConnection(Mockito.any(), Mockito.any(), Mockito.any());
            verify(tm, never()).getTransaction(Mockito.any());
        }
    }

    @Test
    void loadScenarioParticipating_異常ケース_singleDBで入力ディレクトリが存在しない_IllegalStateExceptionが送出されること()
            throws Exception {
        // Arrange
        Properties props = new Properties();
        props.setProperty("spring.datasource.url", "jdbc:h2:mem:missing");
        props.setProperty("spring.datasource.username", "sa");
        TestResourceContext trc = newTrc(dummyClassResourcesDir, props);
        var trcField = LoadDataExtension.class.getDeclaredField("trc");
        trcField.setAccessible(true);
        trcField.set(target, trc);

        ExtensionContext context = mock(ExtensionContext.class);
        mockStore(context, new HashMap<>());

        ApplicationContext applicationContext = mock(ApplicationContext.class);
        DataSource ds = mock(DataSource.class);
        PlatformTransactionManager tm = mock(PlatformTransactionManager.class);
        TransactionStatus status = mock(TransactionStatus.class);
        when(applicationContext.getBean("transactionManager", PlatformTransactionManager.class))
                .thenReturn(tm);
        when(applicationContext.getBean("dataSource", DataSource.class)).thenReturn(ds);
        when(applicationContext.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"transactionManager"});
        when(applicationContext.getBeansOfType(TransactionInterceptor.class))
                .thenReturn(Collections.emptyMap());
        when(tm.getTransaction(Mockito.any())).thenReturn(status);

        Method loadScenarioParticipating = LoadDataExtension.class.getDeclaredMethod(
                "loadScenarioParticipating", ExtensionContext.class, String.class, String[].class);
        loadScenarioParticipating.setAccessible(true);

        // Act / Assert
        try (MockedStatic<SpringExtension> spring = Mockito.mockStatic(SpringExtension.class)) {
            spring.when(() -> SpringExtension.getApplicationContext(context))
                    .thenReturn(applicationContext);
            Exception ex = assertThrows(Exception.class, () -> loadScenarioParticipating
                    .invoke(target, context, "NOT_EXISTS_SINGLE", new String[0]));
            assertTrue(ex.getCause() instanceof IllegalStateException);
        }
    }

    @Test
    void loadScenarioParticipating_異常ケース_multiDBでdatasetDir解決先が存在しない_IllegalStateExceptionが送出されること()
            throws Exception {
        // Arrange
        Path listedBase = dummyClassResourcesDir.resolve("S_MULTI_LISTED").resolve("input");
        mkDirs(listedBase.resolve("db1"));

        TestResourceContext mockTrc = mock(TestResourceContext.class);
        when(mockTrc.getClassRoot()).thenReturn(dummyClassResourcesDir);
        when(mockTrc.baseInputDir("S_MULTI_LISTED")).thenReturn(listedBase);
        when(mockTrc.inputDir("S_MULTI_LISTED", "db1")).thenReturn(dummyClassResourcesDir
                .resolve("S_MULTI_NOT_FOUND").resolve("input").resolve("db1"));

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        entry.setUrl("jdbc:h2:mem:db1");
        entry.setUser("sa");
        when(mockTrc.buildEntryFromProps("db1")).thenReturn(entry);

        var trcField = LoadDataExtension.class.getDeclaredField("trc");
        trcField.setAccessible(true);
        trcField.set(target, mockTrc);

        DataSource ds = mock(DataSource.class);
        Connection metaConn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(ds.getConnection()).thenReturn(metaConn);
        when(metaConn.getMetaData()).thenReturn(meta);
        when(meta.getURL()).thenReturn("jdbc:h2:mem:db1");
        when(meta.getUserName()).thenReturn("sa");
        DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);

        ApplicationContext ac = mock(ApplicationContext.class);
        when(ac.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tm1"});
        when(ac.getBean("tm1", PlatformTransactionManager.class)).thenReturn(tm);
        when(ac.getBeanNamesForType(DataSource.class)).thenReturn(new String[] {"ds1"});
        when(ac.getBean("ds1", DataSource.class)).thenReturn(ds);
        when(ac.getBeansOfType(TransactionInterceptor.class)).thenReturn(Collections.emptyMap());

        ExtensionContext context = mock(ExtensionContext.class);
        mockStore(context, new HashMap<>());

        Method loadScenarioParticipating = LoadDataExtension.class.getDeclaredMethod(
                "loadScenarioParticipating", ExtensionContext.class, String.class, String[].class);
        loadScenarioParticipating.setAccessible(true);

        // Act / Assert
        try (MockedStatic<SpringExtension> spring = Mockito.mockStatic(SpringExtension.class);
                MockedStatic<TransactionSynchronizationManager> txSync =
                        Mockito.mockStatic(TransactionSynchronizationManager.class)) {
            spring.when(() -> SpringExtension.getApplicationContext(context)).thenReturn(ac);
            txSync.when(TransactionSynchronizationManager::isActualTransactionActive)
                    .thenReturn(false);
            Exception ex = assertThrows(Exception.class, () -> loadScenarioParticipating
                    .invoke(target, context, "S_MULTI_LISTED", new String[] {"db1"}));
            assertTrue(ex.getCause() instanceof IllegalStateException);
        }
    }

    @Test
    void resolveScenarios_正常ケース_アノテーション未指定時にディレクトリ一覧を返す_シナリオ一覧が返ること() throws Exception {
        // Arrange
        mkDirs(dummyClassResourcesDir.resolve("AUTO_A"));
        mkDirs(dummyClassResourcesDir.resolve("AUTO_B"));
        Properties props = new Properties();
        TestResourceContext trc = newTrc(dummyClassResourcesDir, props);
        var trcField = LoadDataExtension.class.getDeclaredField("trc");
        trcField.setAccessible(true);
        trcField.set(target, trc);

        Method resolveScenarios =
                LoadDataExtension.class.getDeclaredMethod("resolveScenarios", LoadData.class);
        resolveScenarios.setAccessible(true);
        LoadData ann = DummyTargetTest.class.getDeclaredMethod("methodScenarioAutoDetect")
                .getAnnotation(LoadData.class);

        // Act
        @SuppressWarnings("unchecked")
        List<String> scenarios = (List<String>) resolveScenarios.invoke(target, ann);

        // Assert
        assertTrue(scenarios.contains("AUTO_A"));
        assertTrue(scenarios.contains("AUTO_B"));
    }

    @Test
    void setTxInterceptorDefaultManager_異常ケース_TM名を解決できない場合は切替をスキップする_ストアへ記録されないこと()
            throws Exception {
        // Arrange
        ExtensionContext context = mock(ExtensionContext.class);
        Map<Object, Object> storeMap = new HashMap<>();
        mockStore(context, storeMap);

        ApplicationContext ac = mock(ApplicationContext.class);
        PlatformTransactionManager picked = mock(PlatformTransactionManager.class);
        PlatformTransactionManager other = mock(PlatformTransactionManager.class);
        when(ac.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tm1"});
        when(ac.getBean("tm1", PlatformTransactionManager.class)).thenReturn(other);

        Method set = LoadDataExtension.class.getDeclaredMethod("setTxInterceptorDefaultManager",
                ExtensionContext.class, ApplicationContext.class, PlatformTransactionManager.class,
                String.class);
        set.setAccessible(true);

        // Act
        set.invoke(target, context, ac, picked, "K1");

        // Assert
        assertEquals(null, storeMap.get("TXI_DEFAULT_SWITCH"));
    }

    @Test
    void setTxInterceptorDefaultManager_異常ケース_interceptor切替で例外時は継続する_他インターセプタが記録されること()
            throws Exception {
        // Arrange
        ExtensionContext context = mock(ExtensionContext.class);
        Map<Object, Object> storeMap = new HashMap<>();
        mockStore(context, storeMap);

        ApplicationContext ac = mock(ApplicationContext.class);
        PlatformTransactionManager picked = mock(PlatformTransactionManager.class);
        when(ac.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tm1"});
        when(ac.getBean("tm1", PlatformTransactionManager.class)).thenReturn(picked);

        TransactionInterceptor ok = new TransactionInterceptor();
        TransactionInterceptor ng = mock(TransactionInterceptor.class);
        Mockito.doThrow(new RuntimeException("switch error")).when(ng)
                .setTransactionManagerBeanName(Mockito.anyString());

        Map<String, TransactionInterceptor> tis = new HashMap<>();
        tis.put("ok", ok);
        tis.put("ng", ng);
        when(ac.getBeansOfType(TransactionInterceptor.class)).thenReturn(tis);

        Method set = LoadDataExtension.class.getDeclaredMethod("setTxInterceptorDefaultManager",
                ExtensionContext.class, ApplicationContext.class, PlatformTransactionManager.class,
                String.class);
        set.setAccessible(true);

        // Act
        set.invoke(target, context, ac, picked, "K2");

        // Assert
        assertNotNull(storeMap.get("TXI_DEFAULT_SWITCH"));
    }

    @Test
    void restoreTxInterceptorDefaultManager_異常ケース_復元対象なしや復元失敗を許容する_例外とならないこと() throws Exception {
        // Arrange
        ExtensionContext context = mock(ExtensionContext.class);
        Map<Object, Object> storeMap = new HashMap<>();
        mockStore(context, storeMap);

        Method restore = LoadDataExtension.class
                .getDeclaredMethod("restoreTxInterceptorDefaultManager", ExtensionContext.class);
        restore.setAccessible(true);

        // rec == null 分岐
        assertDoesNotThrow(() -> restore.invoke(target, context));

        // rec 有り、存在しない interceptor 名を含める分岐
        Class<?> recClass = Class.forName(
                "io.github.yok.flexdblink.junit.LoadDataExtension$TxInterceptorSwitchRecord");
        Constructor<?> recCtor = recClass.getDeclaredConstructor();
        recCtor.setAccessible(true);
        Object rec = recCtor.newInstance();
        var touchedField = recClass.getDeclaredField("touched");
        touchedField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Boolean> touched = (Map<String, Boolean>) touchedField.get(rec);
        touched.put("missing", Boolean.TRUE);
        touched.put("ng", Boolean.TRUE);
        storeMap.put("TXI_DEFAULT_SWITCH", rec);

        ApplicationContext ac = mock(ApplicationContext.class);
        TransactionInterceptor ng = mock(TransactionInterceptor.class);
        Mockito.doThrow(new RuntimeException("restore error")).when(ng)
                .setTransactionManagerBeanName(null);
        Map<String, TransactionInterceptor> tis = new HashMap<>();
        tis.put("ng", ng);
        when(ac.getBeansOfType(TransactionInterceptor.class)).thenReturn(tis);

        try (MockedStatic<SpringExtension> spring = Mockito.mockStatic(SpringExtension.class)) {
            spring.when(() -> SpringExtension.getApplicationContext(context)).thenReturn(ac);
            assertDoesNotThrow(() -> restore.invoke(target, context));
        }
    }

    @Test
    void rollbackAllIfBegan_異常ケース_rollback例外時も継続して削除する_ストアキーが削除されること() throws Exception {
        // Arrange
        ExtensionContext context = mock(ExtensionContext.class);
        Map<Object, Object> storeMap = new HashMap<>();
        mockStore(context, storeMap);

        Class<?> txRecordListClass =
                Class.forName("io.github.yok.flexdblink.junit.LoadDataExtension$TxRecordList");
        Constructor<?> txRecordListCtor = txRecordListClass.getDeclaredConstructor();
        txRecordListCtor.setAccessible(true);
        Object txRecordList = txRecordListCtor.newInstance();

        Class<?> txRecordClass =
                Class.forName("io.github.yok.flexdblink.junit.LoadDataExtension$TxRecord");
        Constructor<?> txRecordCtor = txRecordClass.getDeclaredConstructor(String.class,
                PlatformTransactionManager.class, TransactionStatus.class);
        txRecordCtor.setAccessible(true);

        PlatformTransactionManager tm = mock(PlatformTransactionManager.class);
        Mockito.doThrow(new RuntimeException("rollback error")).when(tm).rollback(Mockito.any());
        TransactionStatus status = mock(TransactionStatus.class);
        Object record = txRecordCtor.newInstance("db1", tm, status);
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) txRecordList;
        list.add(record);
        storeMap.put("TX_RECORDS", txRecordList);

        Method rollback = LoadDataExtension.class.getDeclaredMethod("rollbackAllIfBegan",
                ExtensionContext.class);
        rollback.setAccessible(true);

        // Act
        assertDoesNotThrow(() -> rollback.invoke(target, context));

        // Assert
        assertEquals(null, storeMap.get("TX_RECORDS"));
    }

    @Test
    void listDbIdsFromFolder_異常ケース_ディレクトリ判定や走査失敗時を処理する_空集合または例外が返ること() throws Exception {
        // Arrange
        Method listDbIdsFromFolder =
                LoadDataExtension.class.getDeclaredMethod("listDbIdsFromFolder", Path.class);
        listDbIdsFromFolder.setAccessible(true);

        // 非ディレクトリ分岐
        @SuppressWarnings("unchecked")
        Set<String> empty = (Set<String>) listDbIdsFromFolder.invoke(target,
                dummyClassResourcesDir.resolve("not-directory-file.txt"));
        assertEquals(Collections.emptySet(), empty);

        // scan 失敗分岐
        Path fakeDir = dummyClassResourcesDir.resolve("fake-dir");
        try (MockedStatic<Files> files =
                Mockito.mockStatic(Files.class, Mockito.CALLS_REAL_METHODS)) {
            files.when(() -> Files.isDirectory(fakeDir)).thenReturn(true);
            files.when(() -> Files.list(fakeDir)).thenThrow(new RuntimeException("scan failure"));

            Exception ex = assertThrows(Exception.class,
                    () -> listDbIdsFromFolder.invoke(target, fakeDir));
            assertNotNull(ex.getCause());
        }
    }

    @Test
    void findBeanNameByInstance_異常ケース_一致するBeanが存在しない場合_unknownが返ること() throws Exception {
        // Arrange
        ApplicationContext applicationContext = mock(ApplicationContext.class);
        DataSource ds1 = mock(DataSource.class);
        DataSource ds2 = mock(DataSource.class);
        DataSource targetDs = mock(DataSource.class);
        when(applicationContext.getBeanNamesForType(DataSource.class))
                .thenReturn(new String[] {"dataSource1", "dataSource2"});
        when(applicationContext.getBean("dataSource1", DataSource.class)).thenReturn(ds1);
        when(applicationContext.getBean("dataSource2", DataSource.class)).thenReturn(ds2);

        Method findBeanNameByInstance = LoadDataExtension.class.getDeclaredMethod(
                "findBeanNameByInstance", ApplicationContext.class, Class.class, Object.class);
        findBeanNameByInstance.setAccessible(true);

        // Act
        Object resolved = findBeanNameByInstance.invoke(target, applicationContext,
                DataSource.class, targetDs);

        // Assert
        assertEquals("<unknown>", resolved);
    }

    @Test
    void beforeTestExecution_異常ケース_クラスパス上にテストリソースが無い場合_IllegalStateExceptionが送出されること()
            throws Exception {
        class NoLoadDataClass {
        }
        ExtensionContext context = mockContextForClass(NoLoadDataClass.class);
        assertThrows(IllegalStateException.class, () -> target.beforeTestExecution(context));
    }

    @Test
    void rollbackAllIfBegan_正常ケース_記録が空の場合は何もしない_例外とならないこと() throws Exception {
        ExtensionContext context = mock(ExtensionContext.class);
        mockStore(context, new HashMap<>());
        Method rollback = LoadDataExtension.class.getDeclaredMethod("rollbackAllIfBegan",
                ExtensionContext.class);
        rollback.setAccessible(true);
        assertDoesNotThrow(() -> rollback.invoke(target, context));
    }

    @Test
    void getTxRecords_正常ケース_ストア未格納時に空リストを返す_空であること() throws Exception {
        ExtensionContext context = mock(ExtensionContext.class);
        mockStore(context, new HashMap<>());
        Method getTxRecords =
                LoadDataExtension.class.getDeclaredMethod("getTxRecords", ExtensionContext.class);
        getTxRecords.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) getTxRecords.invoke(target, context);
        assertTrue(list.isEmpty());
    }

    @Test
    void findTmByMetadata_正常ケース_dsNullとmeta取得失敗を除外して一致TMを返す_一致件数が1件であること() throws Exception {
        DataSource dsFail = mock(DataSource.class);
        when(dsFail.getConnection()).thenThrow(new RuntimeException("meta fail"));
        DataSource dsMatch = dataSourceWithMeta("jdbc:h2:mem:ftm", "sa");

        DataSourceTransactionManager tmNullDs = mock(DataSourceTransactionManager.class);
        when(tmNullDs.getDataSource()).thenReturn(null);
        DataSourceTransactionManager tmFail = new DataSourceTransactionManager(dsFail);
        DataSourceTransactionManager tmMatch = new DataSourceTransactionManager(dsMatch);

        ApplicationContext ac = mock(ApplicationContext.class);
        when(ac.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tmNull", "tmFail", "tmMatch"});
        when(ac.getBean("tmNull", PlatformTransactionManager.class)).thenReturn(tmNullDs);
        when(ac.getBean("tmFail", PlatformTransactionManager.class)).thenReturn(tmFail);
        when(ac.getBean("tmMatch", PlatformTransactionManager.class)).thenReturn(tmMatch);
        when(ac.getBeanNamesForType(DataSource.class)).thenReturn(new String[] {"dsMatch"});
        when(ac.getBean("dsMatch", DataSource.class)).thenReturn(dsMatch);

        Method findTmByMetadata = LoadDataExtension.class.getDeclaredMethod("findTmByMetadata",
                ApplicationContext.class, String.class, String.class);
        findTmByMetadata.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<Object> list =
                (List<Object>) findTmByMetadata.invoke(target, ac, "jdbc:h2:mem:ftm", "sa");
        assertEquals(1, list.size());
    }

    @Test
    void resolveComponentsForDbId_異常ケース_TM一致候補が複数存在する_IllegalStateExceptionが送出されること()
            throws Exception {
        Properties props = new Properties();
        props.setProperty("spring.datasource.db1.url", "jdbc:h2:mem:dup");
        props.setProperty("spring.datasource.db1.username", "sa");
        setTrc(props);

        DataSource ds1 = dataSourceWithMeta("jdbc:h2:mem:dup", "sa");
        DataSource ds2 = dataSourceWithMeta("jdbc:h2:mem:dup", "sa");
        DataSourceTransactionManager tm1 = new DataSourceTransactionManager(ds1);
        DataSourceTransactionManager tm2 = new DataSourceTransactionManager(ds2);

        ApplicationContext ac = mock(ApplicationContext.class);
        when(ac.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tm1", "tm2"});
        when(ac.getBean("tm1", PlatformTransactionManager.class)).thenReturn(tm1);
        when(ac.getBean("tm2", PlatformTransactionManager.class)).thenReturn(tm2);
        when(ac.getBeanNamesForType(DataSource.class)).thenReturn(new String[] {"ds1", "ds2"});
        when(ac.getBean("ds1", DataSource.class)).thenReturn(ds1);
        when(ac.getBean("ds2", DataSource.class)).thenReturn(ds2);

        Method resolveComponentsForDbId = LoadDataExtension.class.getDeclaredMethod(
                "resolveComponentsForDbId", ApplicationContext.class, String.class);
        resolveComponentsForDbId.setAccessible(true);

        Exception ex = assertThrows(Exception.class,
                () -> resolveComponentsForDbId.invoke(target, ac, "db1"));
        assertTrue(ex.getCause() instanceof IllegalStateException);
    }

    @Test
    void resolveComponentsForDbId_異常ケース_DS一致候補が存在しない_IllegalStateExceptionが送出されること()
            throws Exception {
        Properties props = new Properties();
        props.setProperty("spring.datasource.db1.url", "jdbc:h2:mem:notfound");
        props.setProperty("spring.datasource.db1.username", "sa");
        setTrc(props);

        DataSource dsMismatch = dataSourceWithMeta("jdbc:h2:mem:other", "sa");
        DataSourceTransactionManager tmMismatch = new DataSourceTransactionManager(dsMismatch);

        ApplicationContext ac = mock(ApplicationContext.class);
        when(ac.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tm1"});
        when(ac.getBean("tm1", PlatformTransactionManager.class)).thenReturn(tmMismatch);
        when(ac.getBeanNamesForType(DataSource.class)).thenReturn(new String[] {"ds1"});
        when(ac.getBean("ds1", DataSource.class)).thenReturn(dsMismatch);

        Method resolveComponentsForDbId = LoadDataExtension.class.getDeclaredMethod(
                "resolveComponentsForDbId", ApplicationContext.class, String.class);
        resolveComponentsForDbId.setAccessible(true);

        Exception ex = assertThrows(Exception.class,
                () -> resolveComponentsForDbId.invoke(target, ac, "db1"));
        assertTrue(ex.getCause() instanceof IllegalStateException);
    }

    @Test
    void resolveComponentsForDbId_正常ケース_tm側失敗後にds単一一致で解決する_DSとTMが返ること() throws Exception {
        Properties props = new Properties();
        props.setProperty("spring.datasource.db1.url", "jdbc:h2:mem:single");
        props.setProperty("spring.datasource.db1.username", "sa");
        setTrc(props);

        DataSource ds = mock(DataSource.class);
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(ds.getConnection()).thenThrow(new RuntimeException("first fail")).thenReturn(conn);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getURL()).thenReturn("jdbc:h2:mem:single");
        when(meta.getUserName()).thenReturn("sa");
        DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);

        ApplicationContext ac = mock(ApplicationContext.class);
        when(ac.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tm1"});
        when(ac.getBean("tm1", PlatformTransactionManager.class)).thenReturn(tm);
        when(ac.getBeanNamesForType(DataSource.class)).thenReturn(new String[] {"ds1"});
        when(ac.getBean("ds1", DataSource.class)).thenReturn(ds);

        Method resolveComponentsForDbId = LoadDataExtension.class.getDeclaredMethod(
                "resolveComponentsForDbId", ApplicationContext.class, String.class);
        resolveComponentsForDbId.setAccessible(true);
        Object components = resolveComponentsForDbId.invoke(target, ac, "db1");

        Class<?> componentsClass =
                Class.forName("io.github.yok.flexdblink.junit.LoadDataExtension$Components");
        assertSame(ds, readField(componentsClass, components, "dataSource"));
        assertSame(tm, readField(componentsClass, components, "txManager"));
    }

    @Test
    void resolveComponentsForDbId_正常ケース_primary採用で解決する_primaryDSとTMが返ること() throws Exception {
        Properties props = new Properties();
        props.setProperty("spring.datasource.db1.url", "jdbc:h2:mem:pri");
        props.setProperty("spring.datasource.db1.username", "sa");
        setTrc(props);

        DataSource ds1 = dataSourceWithMeta("jdbc:h2:mem:pri", "sa");
        DataSource ds2 = dataSourceWithMeta("jdbc:h2:mem:pri", "sa");

        DataSourceTransactionManager tm1 = new DataSourceTransactionManager(ds1);

        ConfigurableApplicationContext ac = mock(ConfigurableApplicationContext.class);
        ConfigurableListableBeanFactory bf = mock(ConfigurableListableBeanFactory.class);
        BeanDefinition bd1 = mock(BeanDefinition.class);
        BeanDefinition bd2 = mock(BeanDefinition.class);
        when(ac.getBeanFactory()).thenReturn(bf);
        when(bf.containsBeanDefinition(Mockito.anyString())).thenReturn(true);
        when(bf.getBeanDefinition("ds1")).thenReturn(bd1);
        when(bf.getBeanDefinition("ds2")).thenReturn(bd2);
        when(bd1.isPrimary()).thenReturn(true);
        when(bd2.isPrimary()).thenReturn(false);
        when(ac.getBeanNamesForType(PlatformTransactionManager.class)).thenReturn(new String[0],
                new String[] {"tm1"});
        when(ac.getBean("tm1", PlatformTransactionManager.class)).thenReturn(tm1);
        when(ac.getBeanNamesForType(DataSource.class)).thenReturn(new String[] {"ds1", "ds2"});
        when(ac.getBean("ds1", DataSource.class)).thenReturn(ds1);
        when(ac.getBean("ds2", DataSource.class)).thenReturn(ds2);

        Method resolveComponentsForDbId = LoadDataExtension.class.getDeclaredMethod(
                "resolveComponentsForDbId", ApplicationContext.class, String.class);
        resolveComponentsForDbId.setAccessible(true);
        Object components = resolveComponentsForDbId.invoke(target, ac, "db1");

        Class<?> componentsClass =
                Class.forName("io.github.yok.flexdblink.junit.LoadDataExtension$Components");
        assertSame(ds1, readField(componentsClass, components, "dataSource"));
        assertSame(tm1, readField(componentsClass, components, "txManager"));
    }

    @Test
    void resolveComponentsForDbId_異常ケース_ds候補が曖昧で解決不能の場合_IllegalStateExceptionが送出されること()
            throws Exception {
        Properties props = new Properties();
        props.setProperty("spring.datasource.db1.url", "jdbc:h2:mem:amb");
        props.setProperty("spring.datasource.db1.username", "sa");
        setTrc(props);

        DataSource ds1 = mock(DataSource.class);
        DataSource ds2 = mock(DataSource.class);
        Connection c1 = mock(Connection.class);
        Connection c2 = mock(Connection.class);
        DatabaseMetaData m1 = mock(DatabaseMetaData.class);
        DatabaseMetaData m2 = mock(DatabaseMetaData.class);
        when(ds1.getConnection()).thenReturn(c1);
        when(ds2.getConnection()).thenReturn(c2);
        when(c1.getMetaData()).thenReturn(m1);
        when(c2.getMetaData()).thenReturn(m2);
        when(m1.getURL()).thenReturn("jdbc:h2:mem:amb");
        when(m1.getUserName()).thenReturn("sa");
        when(m2.getURL()).thenReturn("jdbc:h2:mem:amb");
        when(m2.getUserName()).thenReturn("sa");

        ApplicationContext ac = mock(ApplicationContext.class);
        when(ac.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tm1"});
        when(ac.getBean("tm1", PlatformTransactionManager.class))
                .thenReturn(mock(PlatformTransactionManager.class));
        when(ac.getBeanNamesForType(DataSource.class)).thenReturn(new String[] {"ds1", "ds2"});
        when(ac.getBean("ds1", DataSource.class)).thenReturn(ds1);
        when(ac.getBean("ds2", DataSource.class)).thenReturn(ds2);

        Method resolveComponentsForDbId = LoadDataExtension.class.getDeclaredMethod(
                "resolveComponentsForDbId", ApplicationContext.class, String.class);
        resolveComponentsForDbId.setAccessible(true);

        Exception ex = assertThrows(Exception.class,
                () -> resolveComponentsForDbId.invoke(target, ac, "db1"));
        assertTrue(ex.getCause() instanceof IllegalStateException);
    }

    @Test
    void resolveComponentsForDbId_正常ケース_primaryなしでTM参照が1件の場合_参照DSが採用されること() throws Exception {
        Properties props = new Properties();
        props.setProperty("spring.datasource.db1.url", "jdbc:h2:mem:ref");
        props.setProperty("spring.datasource.db1.username", "sa");
        setTrc(props);

        DataSource ds1 = mock(DataSource.class);
        DataSource ds2 = mock(DataSource.class);
        Connection c1 = mock(Connection.class);
        Connection c2 = mock(Connection.class);
        DatabaseMetaData m1 = mock(DatabaseMetaData.class);
        DatabaseMetaData m2 = mock(DatabaseMetaData.class);
        when(ds1.getConnection()).thenThrow(new RuntimeException("tm probe fail")).thenReturn(c1);
        when(ds2.getConnection()).thenReturn(c2);
        when(c1.getMetaData()).thenReturn(m1);
        when(c2.getMetaData()).thenReturn(m2);
        when(m1.getURL()).thenReturn("jdbc:h2:mem:ref");
        when(m1.getUserName()).thenReturn("sa");
        when(m2.getURL()).thenReturn("jdbc:h2:mem:ref");
        when(m2.getUserName()).thenReturn("sa");

        DataSourceTransactionManager tm1 = new DataSourceTransactionManager(ds1);
        PlatformTransactionManager tm2 = mock(PlatformTransactionManager.class);

        ApplicationContext ac = mock(ApplicationContext.class);
        when(ac.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tm1", "tm2"});
        when(ac.getBean("tm1", PlatformTransactionManager.class)).thenReturn(tm1);
        when(ac.getBean("tm2", PlatformTransactionManager.class)).thenReturn(tm2);
        when(ac.getBeanNamesForType(DataSource.class)).thenReturn(new String[] {"ds1", "ds2"});
        when(ac.getBean("ds1", DataSource.class)).thenReturn(ds1);
        when(ac.getBean("ds2", DataSource.class)).thenReturn(ds2);

        Method resolveComponentsForDbId = LoadDataExtension.class.getDeclaredMethod(
                "resolveComponentsForDbId", ApplicationContext.class, String.class);
        resolveComponentsForDbId.setAccessible(true);
        Object components = resolveComponentsForDbId.invoke(target, ac, "db1");
        Class<?> componentsClass =
                Class.forName("io.github.yok.flexdblink.junit.LoadDataExtension$Components");
        assertSame(ds1, readField(componentsClass, components, "dataSource"));
    }

    @Test
    void pickPrimary_異常ケース_primary複数と非Configurableを判定する_nullが返ること() throws Exception {
        DataSource ds1 = mock(DataSource.class);
        DataSource ds2 = mock(DataSource.class);
        Class<?> namedDsClass =
                Class.forName("io.github.yok.flexdblink.junit.LoadDataExtension$NamedDs");
        Constructor<?> ctor = namedDsClass.getDeclaredConstructor(String.class, DataSource.class);
        ctor.setAccessible(true);
        Object n1 = ctor.newInstance("ds1", ds1);
        Object n2 = ctor.newInstance("ds2", ds2);

        ConfigurableApplicationContext ac = mock(ConfigurableApplicationContext.class);
        ConfigurableListableBeanFactory bf = mock(ConfigurableListableBeanFactory.class);
        BeanDefinition bd1 = mock(BeanDefinition.class);
        BeanDefinition bd2 = mock(BeanDefinition.class);
        when(ac.getBeanFactory()).thenReturn(bf);
        when(bf.containsBeanDefinition("ds1")).thenReturn(true);
        when(bf.containsBeanDefinition("ds2")).thenReturn(true);
        when(bf.getBeanDefinition("ds1")).thenReturn(bd1);
        when(bf.getBeanDefinition("ds2")).thenReturn(bd2);
        when(bd1.isPrimary()).thenReturn(true);
        when(bd2.isPrimary()).thenReturn(true);

        Method pickPrimary = LoadDataExtension.class.getDeclaredMethod("pickPrimary",
                ApplicationContext.class, List.class);
        pickPrimary.setAccessible(true);
        assertEquals(null, pickPrimary.invoke(target, ac, List.of(n1, n2)));

        ApplicationContext nonConfigurable = mock(ApplicationContext.class);
        assertEquals(null, pickPrimary.invoke(target, nonConfigurable, List.of(n1, n2)));
    }

    @Test
    void pickSingleReferencedByTm_異常ケース_TMがDataSourceTransactionManager以外の場合_参照なしでnullが返ること()
            throws Exception {
        DataSource ds1 = mock(DataSource.class);
        Class<?> namedDsClass =
                Class.forName("io.github.yok.flexdblink.junit.LoadDataExtension$NamedDs");
        Constructor<?> ctor = namedDsClass.getDeclaredConstructor(String.class, DataSource.class);
        ctor.setAccessible(true);
        Object n1 = ctor.newInstance("ds1", ds1);

        ApplicationContext ac = mock(ApplicationContext.class);
        when(ac.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tm1"});
        when(ac.getBean("tm1", PlatformTransactionManager.class))
                .thenReturn(mock(PlatformTransactionManager.class));

        Method pickSingleReferencedByTm = LoadDataExtension.class.getDeclaredMethod(
                "pickSingleReferencedByTm", ApplicationContext.class, List.class);
        pickSingleReferencedByTm.setAccessible(true);
        assertEquals(null, pickSingleReferencedByTm.invoke(target, ac, List.of(n1)));
    }

    @Test
    void helperBranches_正常ケース_URL比較とnull比較の残分岐を通す_期待値が返ること() throws Exception {
        Method urlRoughMatch = LoadDataExtension.class.getDeclaredMethod("urlRoughMatch",
                String.class, String.class);
        urlRoughMatch.setAccessible(true);
        assertEquals(true,
                urlRoughMatch.invoke(target, "jdbc:h2:mem:a", "jdbc:h2:mem:a;MODE=Oracle"));
        assertEquals(true, urlRoughMatch.invoke(target, "jdbc:h2:mem:abcdef", "jdbc:h2:mem:abc"));
        assertEquals(false, urlRoughMatch.invoke(target, null, "jdbc:h2:mem:a"));
        assertEquals(false, urlRoughMatch.invoke(target, "jdbc:h2:mem:a", null));

        Method equalsIgnoreCaseSafe = LoadDataExtension.class
                .getDeclaredMethod("equalsIgnoreCaseSafe", String.class, String.class);
        equalsIgnoreCaseSafe.setAccessible(true);
        assertEquals(false, equalsIgnoreCaseSafe.invoke(target, "A", null));
        assertEquals(false, equalsIgnoreCaseSafe.invoke(target, null, "A"));
    }

    @Test
    void probeDataSourceMeta_正常ケース_getMetaDataがnullの場合はurluserにnullを保持する_ProbeMetaが返ること()
            throws Exception {
        DataSource ds = mock(DataSource.class);
        Connection conn = mock(Connection.class);
        when(ds.getConnection()).thenReturn(conn);
        when(conn.getMetaData()).thenReturn(null);

        Method probeDataSourceMeta =
                LoadDataExtension.class.getDeclaredMethod("probeDataSourceMeta", DataSource.class);
        probeDataSourceMeta.setAccessible(true);
        Object meta = probeDataSourceMeta.invoke(target, ds);
        Class<?> metaClass =
                Class.forName("io.github.yok.flexdblink.junit.LoadDataExtension$ProbeMeta");
        assertEquals(null, readField(metaClass, meta, "url"));
        assertEquals(null, readField(metaClass, meta, "user"));
    }

    @Test
    void findDataSourceByMetadata_正常ケース_USER不一致を除外して一致のみ返す_一致件数が1件であること() throws Exception {
        DataSource match = dataSourceWithMeta("jdbc:h2:mem:fds", "sa");
        DataSource userMismatch = dataSourceWithMeta("jdbc:h2:mem:fds", "other");

        ApplicationContext ac = mock(ApplicationContext.class);
        when(ac.getBeanNamesForType(DataSource.class)).thenReturn(new String[] {"ds1", "ds2"});
        when(ac.getBean("ds1", DataSource.class)).thenReturn(match);
        when(ac.getBean("ds2", DataSource.class)).thenReturn(userMismatch);

        Method findDataSourceByMetadata = LoadDataExtension.class.getDeclaredMethod(
                "findDataSourceByMetadata", ApplicationContext.class, String.class, String.class);
        findDataSourceByMetadata.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<Object> list =
                (List<Object>) findDataSourceByMetadata.invoke(target, ac, "jdbc:h2:mem:fds", "sa");
        assertEquals(1, list.size());
    }

    @Test
    void findDataSourceByMetadata_正常ケース_メタデータ取得失敗候補を除外する_一致件数が1件であること() throws Exception {
        DataSource fail = mock(DataSource.class);
        when(fail.getConnection()).thenThrow(new RuntimeException("meta fail"));
        DataSource match = dataSourceWithMeta("jdbc:h2:mem:fds2", "sa");

        ApplicationContext ac = mock(ApplicationContext.class);
        when(ac.getBeanNamesForType(DataSource.class)).thenReturn(new String[] {"fail", "ok"});
        when(ac.getBean("fail", DataSource.class)).thenReturn(fail);
        when(ac.getBean("ok", DataSource.class)).thenReturn(match);

        Method findDataSourceByMetadata = LoadDataExtension.class.getDeclaredMethod(
                "findDataSourceByMetadata", ApplicationContext.class, String.class, String.class);
        findDataSourceByMetadata.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) findDataSourceByMetadata.invoke(target, ac,
                "jdbc:h2:mem:fds2", "sa");
        assertEquals(1, list.size());
    }

    @Test
    void restoreTxInterceptorDefaultManager_正常ケース_touched空の場合は即時復帰する_ストア値が維持されること()
            throws Exception {
        ExtensionContext context = mock(ExtensionContext.class);
        Map<Object, Object> storeMap = new HashMap<>();
        mockStore(context, storeMap);

        Class<?> recClass = Class.forName(
                "io.github.yok.flexdblink.junit.LoadDataExtension$TxInterceptorSwitchRecord");
        Constructor<?> recCtor = recClass.getDeclaredConstructor();
        recCtor.setAccessible(true);
        Object rec = recCtor.newInstance();
        storeMap.put("TXI_DEFAULT_SWITCH", rec);

        Method restore = LoadDataExtension.class
                .getDeclaredMethod("restoreTxInterceptorDefaultManager", ExtensionContext.class);
        restore.setAccessible(true);
        assertDoesNotThrow(() -> restore.invoke(target, context));
        assertNotNull(storeMap.get("TXI_DEFAULT_SWITCH"));
    }

    @Test
    void loadScenarioParticipating_正常ケース_DumpConfigにflywayが既存時は重複追加しない_例外とならないこと()
            throws Exception {
        Path scenarioInput = dummyClassResourcesDir.resolve("S_DUMP_CFG").resolve("input");
        mkDirs(scenarioInput);

        Properties props = new Properties();
        props.setProperty("spring.datasource.url", "jdbc:h2:mem:dcfg");
        props.setProperty("spring.datasource.username", "sa");
        setTrc(props);

        ExtensionContext context = mock(ExtensionContext.class);
        mockStore(context, new HashMap<>());

        ApplicationContext applicationContext = mock(ApplicationContext.class);
        DataSource ds = mock(DataSource.class);
        PlatformTransactionManager tm = mock(PlatformTransactionManager.class);
        TransactionStatus status = mock(TransactionStatus.class);
        Connection conn = mock(Connection.class);
        when(applicationContext.getBean("transactionManager", PlatformTransactionManager.class))
                .thenReturn(tm);
        when(applicationContext.getBean("dataSource", DataSource.class)).thenReturn(ds);
        when(applicationContext.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"transactionManager"});
        when(applicationContext.getBeansOfType(TransactionInterceptor.class))
                .thenReturn(Collections.emptyMap());
        when(tm.getTransaction(Mockito.any())).thenReturn(status);

        Method loadScenarioParticipating = LoadDataExtension.class.getDeclaredMethod(
                "loadScenarioParticipating", ExtensionContext.class, String.class, String[].class);
        loadScenarioParticipating.setAccessible(true);

        try (MockedStatic<SpringExtension> spring = Mockito.mockStatic(SpringExtension.class);
                MockedStatic<DataSourceUtils> dsUtils = Mockito.mockStatic(DataSourceUtils.class);
                MockedConstruction<DumpConfig> dumpConfigConstruction =
                        Mockito.mockConstruction(DumpConfig.class, (mock, context2) -> {
                            when(mock.getExcludeTables())
                                    .thenReturn(List.of("flyway_schema_history"));
                            doNothing().when(mock).setExcludeTables(Mockito.any());
                        });
                MockedConstruction<DataLoader> loaders = Mockito.mockConstruction(DataLoader.class,
                        (loader, context2) -> doNothing().when(loader).executeWithConnection(
                                Mockito.any(), Mockito.any(), Mockito.any()))) {
            spring.when(() -> SpringExtension.getApplicationContext(context))
                    .thenReturn(applicationContext);
            dsUtils.when(() -> DataSourceUtils.getConnection(ds)).thenReturn(conn);
            assertDoesNotThrow(
                    () -> loadScenarioParticipating.invoke(target, context, "S_DUMP_CFG", null));
            assertEquals(1, dumpConfigConstruction.constructed().size());
            assertEquals(1, loaders.constructed().size());
        }
    }

    @Test
    void beforeTestExecution_正常ケース_クラスアノテーションblankScenarioを解決する_シナリオロードが実行されること() throws Exception {
        mkDirs(dummyClassClassPathDir.resolve("input"));

        Properties props = new Properties();
        props.setProperty("spring.datasource.url", "jdbc:h2:mem:blank_class");
        props.setProperty("spring.datasource.username", "sa");
        TestResourceContext trc = newTrc(dummyClassClassPathDir, props);
        var trcField = LoadDataExtension.class.getDeclaredField("trc");
        trcField.setAccessible(true);
        trcField.set(target, trc);

        ExtensionContext context = mock(ExtensionContext.class);
        doReturn(BlankScenarioClass.class).when(context).getRequiredTestClass();
        doReturn(Optional.empty()).when(context).getTestMethod();
        mockStore(context, new HashMap<>());

        ApplicationContext applicationContext = mock(ApplicationContext.class);
        DataSource ds = mock(DataSource.class);
        PlatformTransactionManager tm = mock(PlatformTransactionManager.class);
        TransactionStatus status = mock(TransactionStatus.class);
        Connection conn = mock(Connection.class);
        when(applicationContext.getBean("transactionManager", PlatformTransactionManager.class))
                .thenReturn(tm);
        when(applicationContext.getBean("dataSource", DataSource.class)).thenReturn(ds);
        when(applicationContext.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"transactionManager"});
        when(applicationContext.getBeansOfType(TransactionInterceptor.class))
                .thenReturn(Collections.emptyMap());
        when(tm.getTransaction(Mockito.any())).thenReturn(status);

        try (MockedStatic<SpringExtension> spring = Mockito.mockStatic(SpringExtension.class);
                MockedStatic<DataSourceUtils> dsUtils = Mockito.mockStatic(DataSourceUtils.class);
                MockedConstruction<DataLoader> loaders = Mockito.mockConstruction(DataLoader.class,
                        (loader, context2) -> doNothing().when(loader).executeWithConnection(
                                Mockito.any(), Mockito.any(), Mockito.any()))) {
            spring.when(() -> SpringExtension.getApplicationContext(context))
                    .thenReturn(applicationContext);
            dsUtils.when(() -> DataSourceUtils.getConnection(ds)).thenReturn(conn);
            assertDoesNotThrow(() -> target.beforeTestExecution(context));
            verify(loaders.constructed().get(0)).executeWithConnection(Mockito.any(), Mockito.any(),
                    Mockito.any());
        }
    }

    @Test
    void beforeTestExecution_正常ケース_メソッドアノテーションblankScenarioを解決する_シナリオロードが実行されること()
            throws Exception {
        mkDirs(dummyClassClassPathDir.resolve("input"));

        Properties props = new Properties();
        props.setProperty("spring.datasource.url", "jdbc:h2:mem:blank_method");
        props.setProperty("spring.datasource.username", "sa");
        TestResourceContext trc = newTrc(dummyClassClassPathDir, props);
        var trcField = LoadDataExtension.class.getDeclaredField("trc");
        trcField.setAccessible(true);
        trcField.set(target, trc);

        Method method = BlankScenarioMethodClass.class.getDeclaredMethod("blankScenarioMethod");
        ExtensionContext context = mock(ExtensionContext.class);
        doReturn(BlankScenarioMethodClass.class).when(context).getRequiredTestClass();
        doReturn(Optional.of(method)).when(context).getTestMethod();
        mockStore(context, new HashMap<>());

        ApplicationContext applicationContext = mock(ApplicationContext.class);
        DataSource ds = mock(DataSource.class);
        PlatformTransactionManager tm = mock(PlatformTransactionManager.class);
        TransactionStatus status = mock(TransactionStatus.class);
        Connection conn = mock(Connection.class);
        when(applicationContext.getBean("transactionManager", PlatformTransactionManager.class))
                .thenReturn(tm);
        when(applicationContext.getBean("dataSource", DataSource.class)).thenReturn(ds);
        when(applicationContext.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"transactionManager"});
        when(applicationContext.getBeansOfType(TransactionInterceptor.class))
                .thenReturn(Collections.emptyMap());
        when(tm.getTransaction(Mockito.any())).thenReturn(status);

        try (MockedStatic<SpringExtension> spring = Mockito.mockStatic(SpringExtension.class);
                MockedStatic<DataSourceUtils> dsUtils = Mockito.mockStatic(DataSourceUtils.class);
                MockedConstruction<DataLoader> loaders = Mockito.mockConstruction(DataLoader.class,
                        (loader, context2) -> doNothing().when(loader).executeWithConnection(
                                Mockito.any(), Mockito.any(), Mockito.any()))) {
            spring.when(() -> SpringExtension.getApplicationContext(context))
                    .thenReturn(applicationContext);
            dsUtils.when(() -> DataSourceUtils.getConnection(ds)).thenReturn(conn);
            assertDoesNotThrow(() -> target.beforeTestExecution(context));
            verify(loaders.constructed().get(0)).executeWithConnection(Mockito.any(), Mockito.any(),
                    Mockito.any());
        }
    }

    @Test
    void loadScenarioParticipating_正常ケース_singleDBで既存TXかつDSバインド済みの場合_警告分岐を通過して処理継続すること()
            throws Exception {
        Path scenarioInput = dummyClassResourcesDir.resolve("S_SINGLE_BOUND").resolve("input");
        mkDirs(scenarioInput);

        Properties props = new Properties();
        props.setProperty("spring.datasource.url", "jdbc:h2:mem:single_bound");
        props.setProperty("spring.datasource.username", "sa");
        setTrc(props);

        ExtensionContext context = mock(ExtensionContext.class);
        mockStore(context, new HashMap<>());

        ApplicationContext applicationContext = mock(ApplicationContext.class);
        DataSource ds = mock(DataSource.class);
        PlatformTransactionManager tm = mock(PlatformTransactionManager.class);
        Connection conn = mock(Connection.class);
        when(applicationContext.getBean("transactionManager", PlatformTransactionManager.class))
                .thenReturn(tm);
        when(applicationContext.getBean("dataSource", DataSource.class)).thenReturn(ds);
        when(applicationContext.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"transactionManager"});
        when(applicationContext.getBeansOfType(TransactionInterceptor.class))
                .thenReturn(Collections.emptyMap());

        Method loadScenarioParticipating = LoadDataExtension.class.getDeclaredMethod(
                "loadScenarioParticipating", ExtensionContext.class, String.class, String[].class);
        loadScenarioParticipating.setAccessible(true);

        try (MockedStatic<SpringExtension> spring = Mockito.mockStatic(SpringExtension.class);
                MockedStatic<DataSourceUtils> dsUtils = Mockito.mockStatic(DataSourceUtils.class);
                MockedStatic<TransactionSynchronizationManager> txSync =
                        Mockito.mockStatic(TransactionSynchronizationManager.class);
                MockedConstruction<DataLoader> loaders = Mockito.mockConstruction(DataLoader.class,
                        (loader, context2) -> doNothing().when(loader).executeWithConnection(
                                Mockito.any(), Mockito.any(), Mockito.any()))) {
            spring.when(() -> SpringExtension.getApplicationContext(context))
                    .thenReturn(applicationContext);
            dsUtils.when(() -> DataSourceUtils.getConnection(ds)).thenReturn(conn);
            txSync.when(TransactionSynchronizationManager::isActualTransactionActive)
                    .thenReturn(true);
            txSync.when(() -> TransactionSynchronizationManager.hasResource(ds)).thenReturn(true);
            assertDoesNotThrow(() -> loadScenarioParticipating.invoke(target, context,
                    "S_SINGLE_BOUND", new String[0]));
            verify(loaders.constructed().get(0)).executeWithConnection(Mockito.any(), Mockito.any(),
                    Mockito.any());
        }
    }

    @Test
    void findTmByMetadata_正常ケース_USER不一致時は一致候補に含めない_空リストが返ること() throws Exception {
        DataSource ds = dataSourceWithMeta("jdbc:h2:mem:u_mismatch", "other");
        DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);

        ApplicationContext ac = mock(ApplicationContext.class);
        when(ac.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tm1"});
        when(ac.getBean("tm1", PlatformTransactionManager.class)).thenReturn(tm);
        when(ac.getBeanNamesForType(DataSource.class)).thenReturn(new String[] {"ds1"});
        when(ac.getBean("ds1", DataSource.class)).thenReturn(ds);

        Method findTmByMetadata = LoadDataExtension.class.getDeclaredMethod("findTmByMetadata",
                ApplicationContext.class, String.class, String.class);
        findTmByMetadata.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<Object> list =
                (List<Object>) findTmByMetadata.invoke(target, ac, "jdbc:h2:mem:u_mismatch", "sa");
        assertTrue(list.isEmpty());
    }

    @Test
    void pickPrimary_正常ケース_BeanDefinition未登録候補をスキップする_登録済primaryが返ること() throws Exception {
        DataSource ds1 = mock(DataSource.class);
        DataSource ds2 = mock(DataSource.class);
        Class<?> namedDsClass =
                Class.forName("io.github.yok.flexdblink.junit.LoadDataExtension$NamedDs");
        Constructor<?> ctor = namedDsClass.getDeclaredConstructor(String.class, DataSource.class);
        ctor.setAccessible(true);
        Object n1 = ctor.newInstance("ds1", ds1);
        Object n2 = ctor.newInstance("ds2", ds2);

        ConfigurableApplicationContext ac = mock(ConfigurableApplicationContext.class);
        ConfigurableListableBeanFactory bf = mock(ConfigurableListableBeanFactory.class);
        BeanDefinition bd2 = mock(BeanDefinition.class);
        when(ac.getBeanFactory()).thenReturn(bf);
        when(bf.containsBeanDefinition("ds1")).thenReturn(false);
        when(bf.containsBeanDefinition("ds2")).thenReturn(true);
        when(bf.getBeanDefinition("ds2")).thenReturn(bd2);
        when(bd2.isPrimary()).thenReturn(true);

        Method pickPrimary = LoadDataExtension.class.getDeclaredMethod("pickPrimary",
                ApplicationContext.class, List.class);
        pickPrimary.setAccessible(true);
        Object picked = pickPrimary.invoke(target, ac, List.of(n1, n2));
        assertEquals("ds2", readField(namedDsClass, picked, "name"));
    }

    private ExtensionContext mockContextForClass(Class<?> clazz) {
        ExtensionContext ctx = mock(ExtensionContext.class);
        doReturn(clazz).when(ctx).getRequiredTestClass();
        doReturn(Optional.empty()).when(ctx).getTestMethod();
        return ctx;
    }

    private void setTrc(Properties props) throws Exception {
        TestResourceContext trc = newTrc(dummyClassResourcesDir, props);
        var trcField = LoadDataExtension.class.getDeclaredField("trc");
        trcField.setAccessible(true);
        trcField.set(target, trc);
    }

    private DataSource dataSourceWithMeta(String url, String user) throws Exception {
        DataSource ds = mock(DataSource.class);
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(ds.getConnection()).thenReturn(conn);
        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getURL()).thenReturn(url);
        when(meta.getUserName()).thenReturn(user);
        return ds;
    }

    private Store mockStore(ExtensionContext context, Map<Object, Object> storeMap) {
        Store store = mock(Store.class);
        when(context.getStore(Mockito.any(Namespace.class))).thenReturn(store);
        when(store.getOrComputeIfAbsent(Mockito.any(), Mockito.<Function<Object, Object>>any(),
                Mockito.any())).thenAnswer(invocation -> {
                    Object key = invocation.getArgument(0);
                    Function<Object, Object> creator = invocation.getArgument(1);
                    if (!storeMap.containsKey(key)) {
                        storeMap.put(key, creator.apply(key));
                    }
                    return storeMap.get(key);
                });
        when(store.get(Mockito.any(), Mockito.any()))
                .thenAnswer(invocation -> storeMap.get(invocation.getArgument(0)));
        Mockito.doAnswer(invocation -> {
            storeMap.remove(invocation.getArgument(0));
            return null;
        }).when(store).remove(Mockito.any());
        return store;
    }

    private Object readField(Class<?> owner, Object target, String fieldName) throws Exception {
        java.lang.reflect.Field field = owner.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    private String readTxManagerBeanName(TransactionInterceptor interceptor) throws Exception {
        Class<?> superClass = interceptor.getClass().getSuperclass();
        Method getter = superClass.getDeclaredMethod("getTransactionManagerBeanName");
        getter.setAccessible(true);
        return (String) getter.invoke(interceptor);
    }

    private void mkDirs(Path p) {
        try {
            Files.createDirectories(p);
            createdPaths.add(p);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void writeToFile(Path p, String content) {
        try {
            Files.createDirectories(p.getParent());
            Files.write(p, content.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
            createdPaths.add(p);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private DataSource dummyDataSource() throws Exception {
        Connection conn = mock(Connection.class, Mockito.withSettings().name("DummyConnection"));
        when(conn.getWarnings()).thenReturn(new SQLWarning());
        when(conn.getMetaData()).thenReturn(mock(DatabaseMetaData.class));

        DataSource ds = mock(DataSource.class);
        when(ds.getConnection()).thenReturn(conn);
        return ds;
    }

    // リフレクション用
    @SneakyThrows
    private static TestResourceContext getTrc(LoadDataExtension target) {
        var f = LoadDataExtension.class.getDeclaredField("trc");
        f.setAccessible(true);
        return (TestResourceContext) f.get(target);
    }

    @SneakyThrows
    private static TestResourceContext newTrc(Path classRoot, Properties props) {
        var c = TestResourceContext.class.getDeclaredConstructor(Path.class, Properties.class);
        c.setAccessible(true);
        return c.newInstance(classRoot, props);
    }
}
