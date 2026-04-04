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
import java.lang.annotation.Annotation;
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
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import javax.sql.DataSource;
import org.apache.commons.io.FileUtils;
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

        @LoadData(scenario = {"AUTO"}, dbNames = {"bbb"})
        void methodScenarioAutoDetect() {}
    }

    @LoadData(scenario = {""}, dbNames = {"bbb"})
    static class BlankScenarioClass {
    }

    static class BlankScenarioMethodClass {
        @LoadData(scenario = {""}, dbNames = {"bbb"})
        void blankScenarioMethod() {}
    }

    // テストで使用するパス情報（ターゲットに合わせて両系統を作成）
    private Path classpathRoot; // target/test-classes
    @TempDir
    Path tempDir;
    private Path resourcesRoot; // temp test resources
    private Path dummyClassClassPathDir; // target/test-classes/<pkg>/<ClassName>
    private Path dummyClassResourcesDir; // temp test resources/<pkg>/<ClassName>

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
        Files.createDirectories(dummyClassClassPathDir);
        Files.createDirectories(dummyClassResourcesDir);

        // シナリオ配下のディレクトリは各テストで必要に応じて作成する

        // application.properties をクラスパス直下に生成（プロファイル無し）
        writeToFile(classpathRoot.resolve("application.properties"),
                "spring.datasource.bbb.url=jdbc:h2:mem:test\n"
                        + "spring.datasource.bbb.username=sa\n"
                        + "spring.datasource.bbb.password=\n"
                        + "spring.datasource.bbb.driver-class-name=org.h2.Driver\n");
    }

    @AfterEach
    void tearDown() throws IOException {
        System.clearProperty("spring.profiles.active");
        // TransactionSynchronizationManager にバインドしたものがあれば解除
        try {
            Map<Object, Object> map = TransactionSynchronizationManager.getResourceMap();
            for (Object key : new ArrayList<>(map.keySet())) {
                TransactionSynchronizationManager.unbindResourceIfPossible(key);
            }
        } catch (Exception ignore) {
        }

        // classpathRoot 配下に作成したディレクトリ・ファイルを削除
        FileUtils.deleteDirectory(dummyClassClassPathDir.toFile());
        Files.deleteIfExists(classpathRoot.resolve("application.properties"));
    }

    @Test
    void beforeAll_正常ケース_クラスルートとプロパティが読み込まれること() throws Exception {
        // Arrange
        ExtensionContext ctx = mockContextForClass(DummyTargetTest.class);

        // application.properties は target/test-classes に配置済み

        // Act
        target.beforeAll(ctx);

        // Assert：TestResourceContext が設定されていること
        TestResourceContext trc = target.getTestResourceContext();
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

        // 対象メソッド（@LoadData(scenario={"METHOD_ONLY"}, dbNames={"bbb"})）
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

        TestResourceContext trc = new TestResourceContext(tempDir, props);
        target.setTestResourceContext(trc);

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
        LoadDataExtension.TxRecordList txRecordList = new LoadDataExtension.TxRecordList();

        PlatformTransactionManager tm1 = mock(PlatformTransactionManager.class);
        PlatformTransactionManager tm2 = mock(PlatformTransactionManager.class);
        TransactionStatus st1 = mock(TransactionStatus.class);
        TransactionStatus st2 = mock(TransactionStatus.class);

        txRecordList.add(new LoadDataExtension.TxRecord("db1", tm1, st1));
        txRecordList.add(new LoadDataExtension.TxRecord("db2", tm2, st2));

        // STORE_KEY_TX は "TX_RECORDS"
        storeMap.put("TX_RECORDS", txRecordList);

        // ===== (2) restoreTxInterceptorDefaultManager 用の TXI_DEFAULT_SWITCH を仕込む =====
        LoadDataExtension.TxInterceptorSwitchRecord rec =
                new LoadDataExtension.TxInterceptorSwitchRecord();
        rec.touched.put("txInterceptor", Boolean.TRUE);

        // STORE_KEY_TXI_DEFAULT は "TXI_DEFAULT_SWITCH"
        storeMap.put("TXI_DEFAULT_SWITCH", rec);

        // ApplicationContext と TransactionInterceptor（復元対象）を用意
        ApplicationContext applicationContext = mock(ApplicationContext.class);

        InspectableTransactionInterceptor interceptor = new InspectableTransactionInterceptor();
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

        Connection proxy = target.wrapConnectionNoClose(original);

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

        TestResourceContext trc = new TestResourceContext(dummyClassResourcesDir, new Properties());
        Optional<DataSource> opt = trc.springManagedDataSource();

        // Assert
        assertTrue(opt.isPresent());
        assertSame(ds, opt.get());
    }

    @Test
    void maybeGetSpringManagedDataSource_異常ケース_未バインドの場合は空で返ること() throws Exception {
        TestResourceContext trc = new TestResourceContext(dummyClassResourcesDir, new Properties());
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
        TestResourceContext trc = new TestResourceContext(dummyClassResourcesDir, p);
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
        TestResourceContext trc = new TestResourceContext(dummyClassResourcesDir, p);
        ConnectionConfig.Entry entry = trc.buildEntryFromProps(null);

        // Assert
        assertEquals("default", entry.getId());
        assertEquals("jdbc:h2:mem:default", entry.getUrl());
        assertEquals("sa", entry.getUser());
    }

    @Test
    void resolveDatasetDir_正常ケース_シナリオありの完全パスが構築されること() throws Exception {
        TestResourceContext trc = new TestResourceContext(dummyClassResourcesDir, new Properties());
        Path resolved = trc.inputDir("CSV", "bbb");
        assertTrue(resolved.toString()
                .endsWith(DummyTargetTest.class.getSimpleName() + "/CSV/input/bbb"));
    }

    @Test
    void resolveDatasetDir_正常ケース_シナリオなしの完全パスが構築されること() throws Exception {
        TestResourceContext trc = new TestResourceContext(dummyClassResourcesDir, new Properties());
        Path resolved = trc.inputDir("", "bbb");
        assertTrue(
                resolved.toString().endsWith(DummyTargetTest.class.getSimpleName() + "/input/bbb"));
    }

    @Test
    void resolveBaseForDbDetection_正常ケース_シナリオありのベースパスが構築されること() throws Exception {
        TestResourceContext trc = new TestResourceContext(dummyClassResourcesDir, new Properties());
        Path base = trc.baseInputDir("CSV");
        assertTrue(base.toString().endsWith(DummyTargetTest.class.getSimpleName() + "/CSV/input"));
    }

    @Test
    void resolveBaseForDbDetection_正常ケース_シナリオなしのベースパスが構築されること() throws Exception {
        TestResourceContext trc = new TestResourceContext(dummyClassResourcesDir, new Properties());
        Path base = trc.baseInputDir(null);
        assertTrue(base.toString().endsWith(DummyTargetTest.class.getSimpleName() + "/input"));
    }

    @Test
    void detectDbNames_正常ケース_input配下のDBフォルダが検出されること() throws Exception {
        Path base = dummyClassResourcesDir.resolve("AUTO_SCENARIO").resolve("input");
        Files.createDirectories(base.resolve("bbb"));
        TestResourceContext trc = new TestResourceContext(dummyClassResourcesDir, new Properties());
        List<String> names = trc.detectDbIds(base);

        // Assert
        assertTrue(names.contains("bbb"));
    }

    @Test
    void expectedDir_正常ケース_シナリオありのexpectedパスが構築されること() throws Exception {
        TestResourceContext trc = new TestResourceContext(dummyClassResourcesDir, new Properties());
        Path expected = trc.expectedDir("CSV", "bbb");
        assertTrue(expected.toString()
                .endsWith(DummyTargetTest.class.getSimpleName() + "/CSV/expected/bbb"));
    }

    @Test
    void baseExpectedDir_正常ケース_シナリオなしのexpectedベースパスが構築されること() throws Exception {
        TestResourceContext trc = new TestResourceContext(dummyClassResourcesDir, new Properties());
        Path expected = trc.baseExpectedDir("");
        assertTrue(
                expected.toString().endsWith(DummyTargetTest.class.getSimpleName() + "/expected"));
    }

    @Test
    void listDirectories_正常ケース_親がディレクトリでない場合_空リストが返ること() throws Exception {
        TestResourceContext trc = new TestResourceContext(dummyClassResourcesDir, new Properties());
        List<String> dirs = trc.listDirectories(dummyClassResourcesDir.resolve("not-exists"));
        assertTrue(dirs.isEmpty());
    }

    @Test
    void springManagedConnection_正常ケース_datasource未指定の場合_空が返ること() throws Exception {
        TestResourceContext trc = new TestResourceContext(dummyClassResourcesDir, new Properties());
        Optional<Connection> conn = trc.springManagedConnection(Optional.empty());
        assertTrue(conn.isEmpty());
    }

    @Test
    void springManagedConnection_正常ケース_datasource指定の場合_接続が返ること() throws Exception {
        DataSource ds = dummyDataSource();
        Connection c = mock(Connection.class);
        when(ds.getConnection()).thenReturn(c);
        TestResourceContext trc = new TestResourceContext(dummyClassResourcesDir, new Properties());
        Optional<Connection> conn = trc.springManagedConnection(Optional.of(ds));
        assertTrue(conn.isPresent());
    }

    @Test
    void testResourceContextPrivateMethods_正常ケース_profile解決と列挙変換を行う_期待値が返ること() throws Exception {
        assertEquals("dev", TestResourceContext.extractProfile("application-dev.properties"));
        assertEquals("qa", TestResourceContext.extractProfile("application-qa.yml"));
        assertEquals("prod", TestResourceContext.extractProfile("application-prod.yaml"));
        assertEquals("", TestResourceContext.extractProfile("other.properties"));

        Properties p = new Properties();
        p.setProperty("spring.profiles.active", "a,b;c");
        List<String> actives = TestResourceContext.resolveActiveProfiles(p);
        assertEquals(List.of("a", "b", "c"), actives);

        System.setProperty("spring.profiles.active", "x;y");
        try {
            List<String> sysActives = TestResourceContext.resolveActiveProfiles(p);
            assertEquals(List.of("x", "y"), sysActives);
        } finally {
            System.clearProperty("spring.profiles.active");
        }

        Enumeration<URL> e =
                Collections.enumeration(List.of(new URL("file:/u1"), new URL("file:/u2")));
        List<URL> list = TestResourceContext.enumToList(e);
        assertEquals("file:/u1", list.get(0).toString());
        assertEquals("file:/u2", list.get(1).toString());
    }

    @Test
    void innerClasses_正常ケース_private内部クラスを生成する_フィールド値が保持されること() throws Exception {
        PlatformTransactionManager tm = mock(PlatformTransactionManager.class);
        TransactionStatus status = mock(TransactionStatus.class);
        DataSource ds = dummyDataSource();

        LoadDataExtension.TxRecord txRecord = new LoadDataExtension.TxRecord("db1", tm, status);
        assertEquals("db1", txRecord.key);
        assertSame(tm, txRecord.tm);
        assertSame(status, txRecord.status);

        LoadDataExtension.Components components = new LoadDataExtension.Components(ds, tm);
        assertSame(ds, components.dataSource);
        assertSame(tm, components.txManager);

        LoadDataExtension.TmWithDs tmWithDs = new LoadDataExtension.TmWithDs("tm1", "ds1", tm, ds);
        assertEquals("tm1", tmWithDs.tmName);
        assertEquals("ds1", tmWithDs.dsName);
        assertSame(tm, tmWithDs.tm);
        assertSame(ds, tmWithDs.ds);

        LoadDataExtension.NamedDs namedDs = new LoadDataExtension.NamedDs("ds2", ds);
        assertEquals("ds2", namedDs.name);
        assertSame(ds, namedDs.ds);

        LoadDataExtension.ProbeMeta probe = new LoadDataExtension.ProbeMeta("jdbc:h2:mem:x", "sa");
        assertEquals("jdbc:h2:mem:x", probe.url);
        assertEquals("sa", probe.user);

        LoadDataExtension.TxInterceptorSwitchRecord txSwitch =
                new LoadDataExtension.TxInterceptorSwitchRecord();
        assertNotNull(txSwitch.touched);
        assertTrue(txSwitch.touched instanceof Map);
    }

    @Test
    void helperMethods_正常ケース_URL比較と正規化を実行する_期待結果が返ること() throws Exception {
        assertEquals("", target.simplifyUrl(null));
        assertEquals("jdbc:h2:mem:test", target.simplifyUrl("JDBC:H2:MEM:TEST?a=1;b=2"));

        assertEquals(false, target.equalsIgnoreCaseSafe("A", null));
        assertEquals(true, target.equalsIgnoreCaseSafe("AbC", "aBc"));

        assertEquals(true, target.urlRoughMatch("jdbc:h2:mem:t1", "jdbc:h2:mem:t1;MODE=Oracle"));
        assertEquals(false, target.urlRoughMatch("", "jdbc:h2:mem:t1"));
    }

    @Test
    void probeDataSourceMeta_正常ケース_メタデータ取得可否を判定する_成功時はProbeMeta失敗時はnullが返ること() throws Exception {
        Connection okConn = mock(Connection.class);
        DatabaseMetaData md = mock(DatabaseMetaData.class);
        when(okConn.getMetaData()).thenReturn(md);
        when(md.getURL()).thenReturn("jdbc:h2:mem:ok");
        when(md.getUserName()).thenReturn("sa");
        DataSource okDs = mock(DataSource.class);
        when(okDs.getConnection()).thenReturn(okConn);
        LoadDataExtension.ProbeMeta meta = target.probeDataSourceMeta(okDs);
        assertNotNull(meta);
        assertEquals("jdbc:h2:mem:ok", meta.url);
        assertEquals("sa", meta.user);

        DataSource ngDs = mock(DataSource.class);
        when(ngDs.getConnection()).thenThrow(new RuntimeException("x"));
        LoadDataExtension.ProbeMeta nullMeta = target.probeDataSourceMeta(ngDs);
        assertEquals(null, nullMeta);
    }

    @Test
    void resolveTxAndDsSingle_異常ケース_必須Beanが無い場合に例外化する_IllegalStateExceptionが送出されること()
            throws Exception {
        ApplicationContext ac = mock(ApplicationContext.class);
        when(ac.getBean("transactionManager", PlatformTransactionManager.class))
                .thenThrow(new RuntimeException("missing"));
        when(ac.getBean("dataSource", DataSource.class)).thenThrow(new RuntimeException("missing"));

        assertThrows(Exception.class, () -> target.resolveTxManagerSingle(ac));
        assertThrows(Exception.class, () -> target.resolveDataSourceSingle(ac));
    }

    @Test
    void resolveTxAndDsSingle_正常ケース_必須Beanが存在する場合に解決する_設定済みBeanが返ること() throws Exception {
        ApplicationContext ac = mock(ApplicationContext.class);
        PlatformTransactionManager tm = mock(PlatformTransactionManager.class);
        DataSource ds = mock(DataSource.class);
        when(ac.getBean("transactionManager", PlatformTransactionManager.class)).thenReturn(tm);
        when(ac.getBean("dataSource", DataSource.class)).thenReturn(ds);

        assertSame(tm, target.resolveTxManagerSingle(ac));
        assertSame(ds, target.resolveDataSourceSingle(ac));
    }

    @Test
    void setTxInterceptorDefaultManager_正常ケース_interceptorが空の場合は切替不要として終了する_ストアへ記録されないこと()
            throws Exception {
        ExtensionContext context = mock(ExtensionContext.class);
        Map<Object, Object> storeMap = new HashMap<>();
        mockStore(context, storeMap);

        ApplicationContext ac = mock(ApplicationContext.class);
        PlatformTransactionManager picked = mock(PlatformTransactionManager.class);
        when(ac.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tm1"});
        when(ac.getBean("tm1", PlatformTransactionManager.class)).thenReturn(picked);
        when(ac.getBeansOfType(TransactionInterceptor.class)).thenReturn(Collections.emptyMap());

        target.setTxInterceptorDefaultManager(context, ac, picked, "K_EMPTY");

        assertEquals(null, storeMap.get("TXI_DEFAULT_SWITCH"));
    }

    @Test
    void resolveClassLoader_正常ケース_requiredTestClassがnullの場合はスレッドコンテキストを採用する_設定ローダーが返ること()
            throws Exception {
        ExtensionContext context = mock(ExtensionContext.class);
        doReturn(null).when(context).getRequiredTestClass();

        ClassLoader original = Thread.currentThread().getContextClassLoader();
        ClassLoader custom = new ClassLoader(original) {};
        try {
            Thread.currentThread().setContextClassLoader(custom);
            ClassLoader resolved = target.resolveClassLoader(context);
            assertSame(custom, resolved);
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    @Test
    void resolveClassLoader_異常ケース_requiredTestClass取得で例外かつスレッドローダーなしの場合_拡張クラスローダーが返ること()
            throws Exception {
        ExtensionContext context = mock(ExtensionContext.class);
        when(context.getRequiredTestClass())
                .thenThrow(new RuntimeException("required class error"));

        ClassLoader original = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(null);
            ClassLoader resolved = target.resolveClassLoader(context);
            assertSame(LoadDataExtension.class.getClassLoader(), resolved);
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    @Test
    void resolveDataSourceSingle_正常ケース_default指定を優先する_指定DataSourceが返ること() throws Exception {
        ApplicationContext ac = mock(ApplicationContext.class);
        DataSource ds = mock(DataSource.class);
        when(ac.getBean("defaultDs", DataSource.class)).thenReturn(ds);

        Map<String, String> mapping = new HashMap<>();
        mapping.put("default", "defaultDs");
        mapping.put("db1", "ignoredDs");

        DataSource resolved = target.resolveDataSourceSingle(ac, mapping);
        assertSame(ds, resolved);
    }

    @Test
    void resolveDataSourceSingle_正常ケース_mappingが1件のみの場合はそのDataSourceを採用する_指定DataSourceが返ること()
            throws Exception {
        ApplicationContext ac = mock(ApplicationContext.class);
        DataSource ds = mock(DataSource.class);
        when(ac.getBean("singleDs", DataSource.class)).thenReturn(ds);

        Map<String, String> mapping = new HashMap<>();
        mapping.put("db1", "singleDs");

        DataSource resolved = target.resolveDataSourceSingle(ac, mapping);
        assertSame(ds, resolved);
    }

    @Test
    void resolveDataSourceSingle_正常ケース_dataSource名がなくても単一候補なら採用する_単一DataSourceが返ること()
            throws Exception {
        ApplicationContext ac = mock(ApplicationContext.class);
        DataSource ds = mock(DataSource.class);
        when(ac.getBean("dataSource", DataSource.class))
                .thenThrow(new RuntimeException("not found"));
        when(ac.getBeanNamesForType(DataSource.class))
                .thenReturn(new String[] {"aaaRoutingDataSource"});
        when(ac.getBean("aaaRoutingDataSource", DataSource.class)).thenReturn(ds);

        DataSource resolved = target.resolveDataSourceSingle(ac, Collections.emptyMap());
        assertSame(ds, resolved);
    }

    @Test
    void resolveDataSourceSingle_正常ケース_primary候補が一意の場合に採用する_primaryDataSourceが返ること()
            throws Exception {
        ConfigurableApplicationContext ac = mock(ConfigurableApplicationContext.class);
        ConfigurableListableBeanFactory bf = mock(ConfigurableListableBeanFactory.class);
        BeanDefinition bd1 = mock(BeanDefinition.class);
        BeanDefinition bd2 = mock(BeanDefinition.class);
        DataSource ds1 = mock(DataSource.class);
        DataSource ds2 = mock(DataSource.class);

        when(ac.getBean("dataSource", DataSource.class))
                .thenThrow(new RuntimeException("not found"));
        when(ac.getBeanNamesForType(DataSource.class)).thenReturn(new String[] {"ds1", "ds2"});
        when(ac.getBean("ds1", DataSource.class)).thenReturn(ds1);
        when(ac.getBean("ds2", DataSource.class)).thenReturn(ds2);
        when(ac.getBeanFactory()).thenReturn(bf);
        when(bf.containsBeanDefinition("ds1")).thenReturn(true);
        when(bf.containsBeanDefinition("ds2")).thenReturn(true);
        when(bf.getBeanDefinition("ds1")).thenReturn(bd1);
        when(bf.getBeanDefinition("ds2")).thenReturn(bd2);
        when(bd1.isPrimary()).thenReturn(true);
        when(bd2.isPrimary()).thenReturn(false);

        DataSource resolved = target.resolveDataSourceSingle(ac, Collections.emptyMap());
        assertSame(ds1, resolved);
    }

    @Test
    void resolveDataSourceSingle_異常ケース_primaryも特定できない場合は失敗する_IllegalStateExceptionが送出されること()
            throws Exception {
        ApplicationContext ac = mock(ApplicationContext.class);
        when(ac.getBean("dataSource", DataSource.class))
                .thenThrow(new RuntimeException("not found"));
        when(ac.getBeanNamesForType(DataSource.class)).thenReturn(new String[] {"ds1", "ds2"});
        when(ac.getBean("ds1", DataSource.class)).thenReturn(mock(DataSource.class));
        when(ac.getBean("ds2", DataSource.class)).thenReturn(mock(DataSource.class));

        assertThrows(IllegalStateException.class,
                () -> target.resolveDataSourceSingle(ac, Collections.emptyMap()));
    }

    @Test
    void resolveDataSourceByDbId_異常ケース_dbId対応の設定がない場合は失敗する_IllegalStateExceptionが送出されること()
            throws Exception {
        ApplicationContext ac = mock(ApplicationContext.class);
        assertThrows(IllegalStateException.class,
                () -> target.resolveDataSourceByDbId(ac, "db1", Collections.emptyMap()));
    }

    @Test
    void resolveDataSourceByBeanName_異常ケース_指定Beanが存在しない場合は失敗する_IllegalStateExceptionが送出されること()
            throws Exception {
        ApplicationContext ac = mock(ApplicationContext.class);
        when(ac.getBean("missingDs", DataSource.class))
                .thenThrow(new RuntimeException("not found"));

        assertThrows(IllegalStateException.class,
                () -> target.resolveDataSourceByBeanName(ac, "db1", "missingDs"));
    }

    @Test
    void readConfiguredDataSourceBeanNames_異常ケース_dbId部分が空文字の場合は失敗する_IllegalStateExceptionが送出されること()
            throws Exception {
        Properties props = new Properties();
        props.setProperty("flexdblink.load.datasource.   ", "ds1");

        assertThrows(IllegalStateException.class,
                () -> target.readConfiguredDataSourceBeanNames(props));
    }

    @Test
    void readConfiguredDataSourceBeanNames_異常ケース_bean名が空文字の場合は失敗する_IllegalStateExceptionが送出されること()
            throws Exception {
        Properties props = new Properties();
        props.setProperty("flexdblink.load.datasource.db1", "  ");

        assertThrows(IllegalStateException.class,
                () -> target.readConfiguredDataSourceBeanNames(props));
    }

    @Test
    void readConfiguredDataSourceBeanNames_異常ケース_同一dbIdで別bean名が競合する場合は失敗する_IllegalStateExceptionが送出されること()
            throws Exception {
        Properties props = new Properties();
        props.setProperty("flexdblink.load.datasource.db1", "dsA");
        props.setProperty("flexdblink.load.datasource.DB1", "dsB");

        assertThrows(IllegalStateException.class,
                () -> target.readConfiguredDataSourceBeanNames(props));
    }

    @Test
    void readConfiguredDataSourceBeanNames_正常ケース_プレフィックス外を無視し同一値重複を許容する_正規化済みマップが返ること()
            throws Exception {
        Properties props = new Properties();
        props.setProperty("some.other.key", "ignored");
        props.setProperty("flexdblink.load.datasource.db1", "dsA");
        props.setProperty("flexdblink.load.datasource.DB1", " dsA ");

        Map<String, String> mapping = target.readConfiguredDataSourceBeanNames(props);

        assertEquals(1, mapping.size());
        assertEquals("dsA", mapping.get("db1"));
    }

    @Test
    void loadFlexDbLinkProperties_正常ケース_テストリソースのみを読み込む_テストリソースのBean名が保持されること()
            throws Exception {
        Path testPropsPath = tempDir.resolve("target").resolve("test-classes")
                .resolve("flexdblink.properties");
        Path classesPropsPath = tempDir.resolve("target").resolve("classes")
                .resolve("flexdblink.properties");
        Path jarLikePropsPath = tempDir.resolve("m2").resolve("repository").resolve("lib")
                .resolve("flexdblink.properties");
        Files.createDirectories(testPropsPath.getParent());
        Files.createDirectories(classesPropsPath.getParent());
        Files.createDirectories(jarLikePropsPath.getParent());
        writeToFile(testPropsPath, "flexdblink.load.datasource.bbb=bbbRoutingDataSource\n");
        writeToFile(classesPropsPath, "flexdblink.load.datasource.bbb=dataSource\n");
        writeToFile(jarLikePropsPath, "flexdblink.load.datasource.bbb=jarDefaultDataSource\n");

        ClassLoader classLoader = new ClassLoader(getClass().getClassLoader()) {
            @Override
            public Enumeration<URL> getResources(String name) throws IOException {
                if (!"flexdblink.properties".equals(name)) {
                    return super.getResources(name);
                }
                List<URL> urls = new ArrayList<>();
                urls.add(testPropsPath.toUri().toURL());
                urls.add(classesPropsPath.toUri().toURL());
                urls.add(jarLikePropsPath.toUri().toURL());
                return Collections.enumeration(urls);
            }
        };

        Properties loaded = target.loadFlexDbLinkProperties(classLoader);
        Map<String, String> mapping = target.readConfiguredDataSourceBeanNames(loaded);
        assertEquals("bbbRoutingDataSource", mapping.get("bbb"));
        assertEquals(1, mapping.size());
    }

    @Test
    void loadFlexDbLinkProperties_正常ケース_同一キーが複数リソースに存在する_先勝ちで保持されること()
            throws Exception {
        Path firstPropsPath = tempDir.resolve("target").resolve("test-classes")
                .resolve("first").resolve("flexdblink.properties");
        Path secondPropsPath = tempDir.resolve("target").resolve("test-classes")
                .resolve("second").resolve("flexdblink.properties");
        Files.createDirectories(firstPropsPath.getParent());
        Files.createDirectories(secondPropsPath.getParent());
        writeToFile(firstPropsPath, "flexdblink.load.datasource.bbb=bbbRoutingDataSource\n");
        writeToFile(secondPropsPath, "flexdblink.load.datasource.bbb=anotherDataSource\n");

        ClassLoader classLoader = new ClassLoader(getClass().getClassLoader()) {
            @Override
            public Enumeration<URL> getResources(String name) throws IOException {
                if (!"flexdblink.properties".equals(name)) {
                    return super.getResources(name);
                }
                List<URL> urls = new ArrayList<>();
                urls.add(firstPropsPath.toUri().toURL());
                urls.add(secondPropsPath.toUri().toURL());
                return Collections.enumeration(urls);
            }
        };

        Properties loaded = target.loadFlexDbLinkProperties(classLoader);
        Map<String, String> mapping = target.readConfiguredDataSourceBeanNames(loaded);
        assertEquals("bbbRoutingDataSource", mapping.get("bbb"));
        assertEquals(1, mapping.size());
    }

    @Test
    void indexDbIdsByNormalized_異常ケース_大文字小文字のみ異なる重複がある場合は失敗する_IllegalStateExceptionが送出されること()
            throws Exception {
        Set<String> dbIds = Set.of("DB1", "db1");
        assertThrows(IllegalStateException.class, () -> target.indexDbIdsByNormalized(dbIds));
    }

    @Test
    void normalizeDbIdKey_正常ケース_null入力の場合_空文字が返ること() throws Exception {
        assertEquals("", target.normalizeDbIdKey(null));
    }

    @Test
    void resolveTxManagerByDataSource_正常異常ケース_TM候補数を判定する_単一は返却し複数ゼロは例外であること() throws Exception {
        DataSource ds = mock(DataSource.class);
        ApplicationContext single = mock(ApplicationContext.class);
        DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);
        when(single.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tm1"});
        when(single.getBean("tm1", PlatformTransactionManager.class)).thenReturn(tm);
        Object resolved = target.resolveTxManagerByDataSource(single, "db1", ds);
        assertSame(tm, resolved);

        ApplicationContext none = mock(ApplicationContext.class);
        when(none.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tm1"});
        when(none.getBean("tm1", PlatformTransactionManager.class))
                .thenReturn(new DataSourceTransactionManager(mock(DataSource.class)));
        assertThrows(Exception.class, () -> target.resolveTxManagerByDataSource(none, "db1", ds));

        ApplicationContext multiple = mock(ApplicationContext.class);
        when(multiple.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tm1", "tm2"});
        when(multiple.getBean("tm1", PlatformTransactionManager.class))
                .thenReturn(new DataSourceTransactionManager(ds));
        when(multiple.getBean("tm2", PlatformTransactionManager.class))
                .thenReturn(new DataSourceTransactionManager(ds));
        assertThrows(Exception.class,
                () -> target.resolveTxManagerByDataSource(multiple, "db1", ds));

        ApplicationContext alias = mock(ApplicationContext.class);
        when(alias.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tm1", "tmAlias"});
        when(alias.getBean("tm1", PlatformTransactionManager.class)).thenReturn(tm);
        when(alias.getBean("tmAlias", PlatformTransactionManager.class)).thenReturn(tm);
        Object aliasResolved = target.resolveTxManagerByDataSource(alias, "db1", ds);
        assertSame(tm, aliasResolved);
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

        InspectableTransactionInterceptor interceptor = new InspectableTransactionInterceptor();
        Map<String, TransactionInterceptor> interceptors = new HashMap<>();
        interceptors.put("txInterceptor", interceptor);
        when(applicationContext.getBeansOfType(TransactionInterceptor.class))
                .thenReturn(interceptors);

        // Act
        target.setTxInterceptorDefaultManager(context, applicationContext, transactionManager,
                "single");
        assertEquals("tmA", readTxManagerBeanName(interceptor));

        try (MockedStatic<SpringExtension> mocked = Mockito.mockStatic(SpringExtension.class)) {
            mocked.when(() -> SpringExtension.getApplicationContext(context))
                    .thenReturn(applicationContext);
            target.restoreTxInterceptorDefaultManager(context);
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

        LoadDataExtension.TxRecordList txRecordList = new LoadDataExtension.TxRecordList();

        PlatformTransactionManager transactionManager = mock(PlatformTransactionManager.class);
        TransactionStatus transactionStatus = mock(TransactionStatus.class);
        txRecordList
                .add(new LoadDataExtension.TxRecord("db1", transactionManager, transactionStatus));

        doReturn(txRecordList).when(store).get("TX_RECORDS", LoadDataExtension.TxRecordList.class);

        // Act
        target.rollbackAllIfBegan(context);

        // Assert
        verify(transactionManager, times(1)).rollback(transactionStatus);
    }

    @Test
    void resolveComponentsForDbId_正常ケース_TMメタデータ一致時にコンポーネントを解決する_DSとTMが返ること() throws Exception {
        // Arrange
        Properties props = new Properties();
        props.setProperty("spring.datasource.db1.url", "jdbc:h2:mem:match");
        props.setProperty("spring.datasource.db1.username", "sa");
        TestResourceContext trc = new TestResourceContext(dummyClassResourcesDir, props);
        target.setTestResourceContext(trc);

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

        // Act
        LoadDataExtension.Components components = target.resolveComponentsForDbId(ac, "db1");

        // Assert
        assertSame(ds, components.dataSource);
        assertSame(tm, components.txManager);

    }

    @Test
    void listDbIdsFromFolder_正常ケース_入力配下のディレクトリ名を抽出する_DB名セットが返ること() throws Exception {
        // Arrange
        Path base = dummyClassResourcesDir.resolve("LIST_DBIDS").resolve("input");
        Files.createDirectories(base.resolve("db1"));
        Files.createDirectories(base.resolve("db2"));
        writeToFile(base.resolve("ignore.txt"), "x");

        // Act
        Set<String> dbIds = target.listDbIdsFromFolder(base);

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

        // Act
        Object resolved = target.findBeanNameByInstance(applicationContext, DataSource.class, ds2);

        // Assert
        assertEquals("dataSource2", resolved);
    }

    @Test
    void loadScenarioParticipating_正常ケース_singleDBでシナリオ読込する_DataLoader実行まで到達すること() throws Exception {
        // Arrange
        Path scenarioInput = dummyClassResourcesDir.resolve("S1").resolve("input");
        Files.createDirectories(scenarioInput);

        Properties props = new Properties();
        props.setProperty("spring.datasource.url", "jdbc:h2:mem:s1");
        props.setProperty("spring.datasource.username", "sa");
        TestResourceContext trc = new TestResourceContext(dummyClassResourcesDir, props);
        target.setTestResourceContext(trc);

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
        Connection conn = mock(Connection.class);
        when(ds.getConnection()).thenReturn(conn);

        when(applicationContext.getBean("dataSource", DataSource.class)).thenReturn(ds);

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
                    () -> target.loadScenarioParticipating(context, "S1", new String[0]));

            DataLoader loader = loaders.constructed().get(0);
            verify(loader, times(1)).executeWithConnection(Mockito.any(), Mockito.any(),
                    Mockito.any());
        }
    }

    @Test
    void loadScenarioParticipating_異常ケース_multiDBで指定フォルダ不足の場合_IllegalStateExceptionが送出されること()
            throws Exception {
        Path baseInput = dummyClassResourcesDir.resolve("S_MULTI_MISSING").resolve("input");
        Files.createDirectories(baseInput.resolve("db1"));

        Properties props = new Properties();
        props.setProperty("spring.datasource.db1.url", "jdbc:h2:mem:db1");
        props.setProperty("spring.datasource.db1.username", "sa");
        TestResourceContext trc = new TestResourceContext(dummyClassResourcesDir, props);
        target.setTestResourceContext(trc);

        ExtensionContext context = mock(ExtensionContext.class);
        when(context.getStore(Mockito.any(Namespace.class))).thenReturn(mock(Store.class));

        try (MockedStatic<SpringExtension> spring = Mockito.mockStatic(SpringExtension.class)) {
            spring.when(() -> SpringExtension.getApplicationContext(context))
                    .thenReturn(mock(ApplicationContext.class));
            assertThrows(Exception.class, () -> target.loadScenarioParticipating(context,
                    "S_MULTI_MISSING", new String[] {"db1", "db2"}));
        }
    }

    @Test
    void loadScenarioParticipating_正常ケース_multiDBで既存TXにDS未バインドの場合_ローカルTXで継続すること() throws Exception {
        Path baseInput = dummyClassResourcesDir.resolve("S_MULTI_ACTIVE").resolve("input");
        Files.createDirectories(baseInput.resolve("db1"));

        Properties props = new Properties();
        props.setProperty("spring.datasource.db1.url", "jdbc:h2:mem:match");
        props.setProperty("spring.datasource.db1.username", "sa");
        TestResourceContext trc = new TestResourceContext(dummyClassResourcesDir, props);
        target.setTestResourceContext(trc);

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
        mockStore(context, new HashMap<>());

        try (MockedStatic<SpringExtension> spring = Mockito.mockStatic(SpringExtension.class);
                MockedStatic<TransactionSynchronizationManager> txSync =
                        Mockito.mockStatic(TransactionSynchronizationManager.class);
                MockedConstruction<DataLoader> loaders = Mockito.mockConstruction(DataLoader.class,
                        (loader, context2) -> doNothing().when(loader).executeWithConnection(
                                Mockito.any(), Mockito.any(), Mockito.any()))) {
            spring.when(() -> SpringExtension.getApplicationContext(context)).thenReturn(ac);
            txSync.when(TransactionSynchronizationManager::isActualTransactionActive)
                    .thenReturn(true);
            txSync.when(() -> TransactionSynchronizationManager.hasResource(ds)).thenReturn(false);
            assertDoesNotThrow(() -> target.loadScenarioParticipating(context, "S_MULTI_ACTIVE",
                    new String[] {"db1"}));
            verify(loaders.constructed().get(0)).executeWithConnection(Mockito.any(), Mockito.any(),
                    Mockito.any());
            assertEquals(1, target.getTxRecords(context).size());
        }
    }

    @Test
    void loadScenarioParticipating_正常ケース_multiDBで既存TXに参加する場合_TransactionInterceptor既定TMが対象DB向けに設定されること()
            throws Exception {
        Path baseInput = dummyClassResourcesDir.resolve("S_MULTI_BOUND_SWITCH").resolve("input");
        Files.createDirectories(baseInput.resolve("db1"));

        Properties props = new Properties();
        props.setProperty("spring.datasource.db1.url", "jdbc:h2:mem:db1");
        props.setProperty("spring.datasource.db1.username", "sa");
        setTrc(props);

        DataSource dsDb1 = mock(DataSource.class);
        DataSource dsAaa = mock(DataSource.class);
        DataSource dsCcc = mock(DataSource.class);
        Connection conn = mock(Connection.class);
        when(dsDb1.getConnection()).thenReturn(conn);
        when(conn.getMetaData()).thenReturn(mock(DatabaseMetaData.class));
        when(dsAaa.getConnection()).thenReturn(mock(Connection.class));
        when(dsCcc.getConnection()).thenReturn(mock(Connection.class));

        DataSourceTransactionManager tmAaa = new DataSourceTransactionManager(dsAaa);
        DataSourceTransactionManager tmBbb = new DataSourceTransactionManager(dsDb1);
        DataSourceTransactionManager tmCcc = new DataSourceTransactionManager(dsCcc);

        ApplicationContext ac = mock(ApplicationContext.class);
        when(ac.getBean("ds1", DataSource.class)).thenReturn(dsDb1);
        when(ac.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"txManagerAaa", "txManagerBbb", "txManagerCcc"});
        when(ac.getBean("txManagerAaa", PlatformTransactionManager.class)).thenReturn(tmAaa);
        when(ac.getBean("txManagerBbb", PlatformTransactionManager.class)).thenReturn(tmBbb);
        when(ac.getBean("txManagerCcc", PlatformTransactionManager.class)).thenReturn(tmCcc);
        when(ac.getBeanNamesForType(DataSource.class)).thenReturn(new String[] {"ds1"});
        when(ac.getBean("ds1", DataSource.class)).thenReturn(dsDb1);

        InspectableTransactionInterceptor interceptor = new InspectableTransactionInterceptor();
        Map<String, TransactionInterceptor> interceptors = new HashMap<>();
        interceptors.put("txInterceptor", interceptor);
        when(ac.getBeansOfType(TransactionInterceptor.class)).thenReturn(interceptors);

        ExtensionContext context = mock(ExtensionContext.class);
        mockStore(context, new HashMap<>());

        try (MockedStatic<SpringExtension> spring = Mockito.mockStatic(SpringExtension.class);
                MockedStatic<DataSourceUtils> dsUtils = Mockito.mockStatic(DataSourceUtils.class);
                MockedStatic<TransactionSynchronizationManager> txSync =
                        Mockito.mockStatic(TransactionSynchronizationManager.class);
                MockedConstruction<DataLoader> loaders = Mockito.mockConstruction(DataLoader.class,
                        (loader, context2) -> doNothing().when(loader).executeWithConnection(
                                Mockito.any(), Mockito.any(), Mockito.any()))) {
            spring.when(() -> SpringExtension.getApplicationContext(context)).thenReturn(ac);
            dsUtils.when(() -> DataSourceUtils.getConnection(dsDb1)).thenReturn(conn);
            txSync.when(TransactionSynchronizationManager::isActualTransactionActive)
                    .thenReturn(true);
            txSync.when(() -> TransactionSynchronizationManager.hasResource(dsDb1))
                    .thenReturn(true);

            assertDoesNotThrow(() -> target.loadScenarioParticipating(context,
                    "S_MULTI_BOUND_SWITCH", new String[] {"db1"}));
            verify(loaders.constructed().get(0)).executeWithConnection(Mockito.any(), Mockito.any(),
                    Mockito.any());
        }

        assertEquals("txManagerBbb", readTxManagerBeanName(interceptor));
    }

    @Test
    void pickPrimary_正常ケース_primaryが1件のみ存在する_primary候補が返ること() throws Exception {
        DataSource ds1 = mock(DataSource.class);
        DataSource ds2 = mock(DataSource.class);
        LoadDataExtension.NamedDs n1 = new LoadDataExtension.NamedDs("ds1", ds1);
        LoadDataExtension.NamedDs n2 = new LoadDataExtension.NamedDs("ds2", ds2);

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

        LoadDataExtension.NamedDs picked = target.pickPrimary(ac, List.of(n1, n2));
        assertEquals("ds2", picked.name);
    }

    @Test
    void pickSingleReferencedByTm_正常ケース_TM参照が1件のみの場合_該当候補が返ること() throws Exception {
        DataSource ds1 = mock(DataSource.class);
        DataSource ds2 = mock(DataSource.class);
        DataSource ds3 = mock(DataSource.class);

        LoadDataExtension.NamedDs n1 = new LoadDataExtension.NamedDs("ds1", ds1);
        LoadDataExtension.NamedDs n2 = new LoadDataExtension.NamedDs("ds2", ds2);

        ApplicationContext ac = mock(ApplicationContext.class);
        when(ac.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tm1"});
        when(ac.getBean("tm1", PlatformTransactionManager.class))
                .thenReturn(new DataSourceTransactionManager(ds2));

        LoadDataExtension.NamedDs picked = target.pickSingleReferencedByTm(ac, List.of(n1, n2));
        assertEquals("ds2", picked.name);

        when(ac.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tm1", "tm2"});
        when(ac.getBean("tm2", PlatformTransactionManager.class))
                .thenReturn(new DataSourceTransactionManager(ds1));
        LoadDataExtension.NamedDs ambiguous = target.pickSingleReferencedByTm(ac, List.of(n1, n2));
        assertEquals(null, ambiguous);

        when(ac.getBean("tm2", PlatformTransactionManager.class))
                .thenReturn(new DataSourceTransactionManager(ds3));
        LoadDataExtension.NamedDs none = target.pickSingleReferencedByTm(ac, List.of(n1));
        assertEquals(null, none);
    }

    @Test
    void loadScenarioParticipating_正常ケース_multiDBで既存TXなしの場合_DBごとにロード実行されること() throws Exception {
        Path baseInput = dummyClassResourcesDir.resolve("S_MULTI_OK").resolve("input");
        Files.createDirectories(baseInput.resolve("db1"));
        Files.createDirectories(baseInput.resolve("db2"));
        Files.createDirectories(baseInput.resolve("extra_db"));

        Properties props = new Properties();
        props.setProperty("spring.datasource.db1.url", "jdbc:h2:mem:db1");
        props.setProperty("spring.datasource.db1.username", "sa");
        props.setProperty("spring.datasource.db2.url", "jdbc:h2:mem:db2");
        props.setProperty("spring.datasource.db2.username", "sa2");
        TestResourceContext trc = new TestResourceContext(dummyClassResourcesDir, props);
        target.setTestResourceContext(trc);

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

        try (MockedStatic<SpringExtension> spring = Mockito.mockStatic(SpringExtension.class);
                MockedStatic<DataSourceUtils> dsUtils = Mockito.mockStatic(DataSourceUtils.class);
                MockedConstruction<DataLoader> loaders = Mockito.mockConstruction(DataLoader.class,
                        (loader, context2) -> doNothing().when(loader).executeWithConnection(
                                Mockito.any(), Mockito.any(), Mockito.any()))) {
            spring.when(() -> SpringExtension.getApplicationContext(context)).thenReturn(ac);
            dsUtils.when(() -> DataSourceUtils.getConnection(ds1)).thenReturn(loadConn1);
            dsUtils.when(() -> DataSourceUtils.getConnection(ds2)).thenReturn(loadConn2);

            assertDoesNotThrow(() -> target.loadScenarioParticipating(context, "S_MULTI_OK",
                    new String[] {"db1", "db2"}));

            DataLoader loader = loaders.constructed().get(0);
            verify(loader, times(2)).executeWithConnection(Mockito.any(), Mockito.any(),
                    Mockito.any());
            assertEquals(2, target.getTxRecords(context).size());
        }
    }

    @Test
    void beforeTestExecution_正常ケース_trc未初期化時にbeforeAllを実行する_trcが設定されること() throws Exception {
        // Arrange
        target.setTestResourceContext(null);

        ExtensionContext context = mockContextForClass(DummyTargetTest.class);

        // Act
        assertDoesNotThrow(() -> target.beforeTestExecution(context));

        // Assert
        assertNotNull(target.getTestResourceContext());
    }

    @Test
    void beforeTestExecution_正常ケース_メソッドアノテーションのシナリオ存在時にロードする_DataLoaderが実行されること() throws Exception {
        // Arrange
        Path inputDir =
                dummyClassClassPathDir.resolve("METHOD_ONLY").resolve("input").resolve("bbb");
        Files.createDirectories(inputDir);

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
        Files.createDirectories(scenarioBase);

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
        Files.createDirectories(scenarioInput);

        Properties props = new Properties();
        props.setProperty("spring.datasource.url", "jdbc:h2:mem:paths");
        props.setProperty("spring.datasource.username", "sa");
        TestResourceContext trc = new TestResourceContext(dummyClassResourcesDir, props);
        target.setTestResourceContext(trc);

        ExtensionContext context = mock(ExtensionContext.class);
        Store store = mockStore(context, new HashMap<>());

        ApplicationContext applicationContext = mock(ApplicationContext.class);
        DataSource ds = mock(DataSource.class);
        Connection conn = mock(Connection.class);
        when(ds.getConnection()).thenReturn(conn);
        when(applicationContext.getBean("dataSource", DataSource.class)).thenReturn(ds);

        // Act / Assert
        try (MockedStatic<SpringExtension> spring = Mockito.mockStatic(SpringExtension.class);
                MockedStatic<DataSourceUtils> dsUtils = Mockito.mockStatic(DataSourceUtils.class);
                MockedConstruction<DataLoader> loaders =
                        Mockito.mockConstruction(DataLoader.class, (loader, context2) -> {
                            PathsConfig pathsConfig = (PathsConfig) context2.arguments().get(0);
                            assertTrue(pathsConfig.getLoad().contains("DummyTargetTest"));
                            assertTrue(pathsConfig.getDataPath().contains("DummyTargetTest"));
                            assertTrue(pathsConfig.getDump().contains("target/dbunit/dump"));

                            assertTrue(context2.arguments().get(2) instanceof Function);

                            doNothing().when(loader).executeWithConnection(Mockito.any(),
                                    Mockito.any(), Mockito.any());
                        })) {
            spring.when(() -> SpringExtension.getApplicationContext(context))
                    .thenReturn(applicationContext);
            dsUtils.when(() -> DataSourceUtils.getConnection(ds)).thenReturn(conn);

            assertDoesNotThrow(
                    () -> target.loadScenarioParticipating(context, "S_PATHS", new String[0]));

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
        Files.createDirectories(scenarioInput);

        Properties props = new Properties();
        props.setProperty("spring.datasource.url", "jdbc:h2:mem:single_active");
        props.setProperty("spring.datasource.username", "sa");
        TestResourceContext trc = new TestResourceContext(dummyClassResourcesDir, props);
        target.setTestResourceContext(trc);

        ExtensionContext context = mock(ExtensionContext.class);
        mockStore(context, new HashMap<>());

        ApplicationContext applicationContext = mock(ApplicationContext.class);
        DataSource ds = mock(DataSource.class);
        Connection conn = mock(Connection.class);
        when(ds.getConnection()).thenReturn(conn);
        when(applicationContext.getBean("dataSource", DataSource.class)).thenReturn(ds);

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

            assertDoesNotThrow(() -> target.loadScenarioParticipating(context, "S_SINGLE_ACTIVE",
                    new String[0]));

            DataLoader loader = loaders.constructed().get(0);
            verify(loader).executeWithConnection(Mockito.any(), Mockito.any(), Mockito.any());
            assertEquals(1, target.getTxRecords(context).size());
        }
    }

    @Test
    void loadScenarioParticipating_異常ケース_singleDBで入力ディレクトリが存在しない_IllegalStateExceptionが送出されること()
            throws Exception {
        // Arrange
        Properties props = new Properties();
        props.setProperty("spring.datasource.url", "jdbc:h2:mem:missing");
        props.setProperty("spring.datasource.username", "sa");
        TestResourceContext trc = new TestResourceContext(dummyClassResourcesDir, props);
        target.setTestResourceContext(trc);

        ExtensionContext context = mock(ExtensionContext.class);
        mockStore(context, new HashMap<>());

        ApplicationContext applicationContext = mock(ApplicationContext.class);
        DataSource ds = mock(DataSource.class);
        Connection conn = mock(Connection.class);
        when(ds.getConnection()).thenReturn(conn);
        when(applicationContext.getBean("transactionManager", PlatformTransactionManager.class))
                .thenReturn(mock(PlatformTransactionManager.class));
        when(applicationContext.getBean("dataSource", DataSource.class)).thenReturn(ds);

        // Act / Assert
        try (MockedStatic<SpringExtension> spring = Mockito.mockStatic(SpringExtension.class)) {
            spring.when(() -> SpringExtension.getApplicationContext(context))
                    .thenReturn(applicationContext);
            assertThrows(IllegalStateException.class, () -> target
                    .loadScenarioParticipating(context, "NOT_EXISTS_SINGLE", new String[0]));
        }
    }

    @Test
    void loadScenarioParticipating_異常ケース_multiDBでdatasetDir解決先が存在しない_IllegalStateExceptionが送出されること()
            throws Exception {
        // Arrange
        Path listedBase = dummyClassResourcesDir.resolve("S_MULTI_LISTED").resolve("input");
        Files.createDirectories(listedBase.resolve("db1"));

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
        target.setTestResourceContext(mockTrc);

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

        // Act / Assert
        try (MockedStatic<SpringExtension> spring = Mockito.mockStatic(SpringExtension.class);
                MockedStatic<TransactionSynchronizationManager> txSync =
                        Mockito.mockStatic(TransactionSynchronizationManager.class)) {
            spring.when(() -> SpringExtension.getApplicationContext(context)).thenReturn(ac);
            txSync.when(TransactionSynchronizationManager::isActualTransactionActive)
                    .thenReturn(false);
            assertThrows(IllegalStateException.class, () -> target
                    .loadScenarioParticipating(context, "S_MULTI_LISTED", new String[] {"db1"}));
        }
    }

    @Test
    void resolveScenarios_異常ケース_アノテーションscenario未指定時_IllegalStateExceptionが送出されること()
            throws Exception {
        // Arrange
        Properties props = new Properties();
        TestResourceContext trc = new TestResourceContext(dummyClassResourcesDir, props);
        target.setTestResourceContext(trc);
        LoadData ann = new LoadData() {
            @Override
            public Class<? extends Annotation> annotationType() {
                return LoadData.class;
            }

            @Override
            public String[] scenario() {
                return new String[0];
            }

            @Override
            public String[] dbNames() {
                return new String[] {"bbb"};
            }
        };

        // Act / Assert
        IllegalStateException ex =
                assertThrows(IllegalStateException.class, () -> target.resolveScenarios(ann));
        assertTrue(ex.getMessage().contains("scenario"));
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

        // Act
        target.setTxInterceptorDefaultManager(context, ac, picked, "K1");

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

        // Act
        target.setTxInterceptorDefaultManager(context, ac, picked, "K2");

        // Assert
        assertNotNull(storeMap.get("TXI_DEFAULT_SWITCH"));
    }

    @Test
    void switchTxInterceptorDefaultManagerForBoundDataSource_異常ケース_default以外でTM解決不可の場合は失敗する_IllegalStateExceptionが再スローされること()
            throws Exception {
        ExtensionContext context = mock(ExtensionContext.class);
        ApplicationContext ac = mock(ApplicationContext.class);
        DataSource targetDs = mock(DataSource.class);
        DataSource otherDs = mock(DataSource.class);
        DataSourceTransactionManager otherTm = new DataSourceTransactionManager(otherDs);
        when(ac.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tm1"});
        when(ac.getBean("tm1", PlatformTransactionManager.class)).thenReturn(otherTm);

        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> target
                .switchTxInterceptorDefaultManagerForBoundDataSource(context, ac, "db1", targetDs));
        assertTrue(ex.getMessage().contains("No TransactionManager for DataSource. dbId=db1"));
    }

    @Test
    void restoreTxInterceptorDefaultManager_異常ケース_復元対象なしや復元失敗を許容する_例外とならないこと() throws Exception {
        // Arrange
        ExtensionContext context = mock(ExtensionContext.class);
        Map<Object, Object> storeMap = new HashMap<>();
        mockStore(context, storeMap);

        // rec == null 分岐
        assertDoesNotThrow(() -> target.restoreTxInterceptorDefaultManager(context));

        // rec 有り、存在しない interceptor 名を含める分岐
        LoadDataExtension.TxInterceptorSwitchRecord rec =
                new LoadDataExtension.TxInterceptorSwitchRecord();
        rec.touched.put("missing", Boolean.TRUE);
        rec.touched.put("ng", Boolean.TRUE);
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
            assertDoesNotThrow(() -> target.restoreTxInterceptorDefaultManager(context));
        }
    }

    @Test
    void rollbackAllIfBegan_異常ケース_rollback例外時も継続して削除する_ストアキーが削除されること() throws Exception {
        // Arrange
        ExtensionContext context = mock(ExtensionContext.class);
        Map<Object, Object> storeMap = new HashMap<>();
        mockStore(context, storeMap);

        LoadDataExtension.TxRecordList txRecordList = new LoadDataExtension.TxRecordList();

        PlatformTransactionManager tm = mock(PlatformTransactionManager.class);
        Mockito.doThrow(new RuntimeException("rollback error")).when(tm).rollback(Mockito.any());
        TransactionStatus status = mock(TransactionStatus.class);
        txRecordList.add(new LoadDataExtension.TxRecord("db1", tm, status));
        storeMap.put("TX_RECORDS", txRecordList);

        // Act
        assertDoesNotThrow(() -> target.rollbackAllIfBegan(context));

        // Assert
        assertEquals(null, storeMap.get("TX_RECORDS"));
    }

    @Test
    void listDbIdsFromFolder_異常ケース_ディレクトリ判定や走査失敗時を処理する_空集合または例外が返ること() throws Exception {
        // Arrange
        // 非ディレクトリ分岐
        Set<String> empty = target
                .listDbIdsFromFolder(dummyClassResourcesDir.resolve("not-directory-file.txt"));
        assertEquals(Collections.emptySet(), empty);

        // scan 失敗分岐
        Path fakeDir = dummyClassResourcesDir.resolve("fake-dir");
        try (MockedStatic<Files> files =
                Mockito.mockStatic(Files.class, Mockito.CALLS_REAL_METHODS)) {
            files.when(() -> Files.isDirectory(fakeDir)).thenReturn(true);
            files.when(() -> Files.list(fakeDir)).thenThrow(new RuntimeException("scan failure"));

            assertThrows(Exception.class, () -> target.listDbIdsFromFolder(fakeDir));
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

        // Act
        Object resolved =
                target.findBeanNameByInstance(applicationContext, DataSource.class, targetDs);

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
        assertDoesNotThrow(() -> target.rollbackAllIfBegan(context));
    }

    @Test
    void getTxRecords_正常ケース_ストア未格納時に空リストを返す_空であること() throws Exception {
        ExtensionContext context = mock(ExtensionContext.class);
        mockStore(context, new HashMap<>());
        List<LoadDataExtension.TxRecord> list = target.getTxRecords(context);
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

        List<LoadDataExtension.TmWithDs> list =
                target.findTmByMetadata(ac, "jdbc:h2:mem:ftm", "sa");
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

        assertThrows(IllegalStateException.class, () -> target.resolveComponentsForDbId(ac, "db1"));
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

        assertThrows(IllegalStateException.class, () -> target.resolveComponentsForDbId(ac, "db1"));
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
        when(ds.getConnection()).thenReturn(conn);
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

        LoadDataExtension.Components components = target.resolveComponentsForDbId(ac, "db1");

        assertSame(ds, components.dataSource);
        assertSame(tm, components.txManager);

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
        when(ac.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tm1"});
        when(ac.getBean("tm1", PlatformTransactionManager.class)).thenReturn(tm1);
        when(ac.getBeanNamesForType(DataSource.class)).thenReturn(new String[] {"ds1", "ds2"});
        when(ac.getBean("ds1", DataSource.class)).thenReturn(ds1);
        when(ac.getBean("ds2", DataSource.class)).thenReturn(ds2);

        LoadDataExtension.Components components = target.resolveComponentsForDbId(ac, "db1");

        assertSame(ds1, components.dataSource);
        assertSame(tm1, components.txManager);

    }

    @Test
    void resolveComponentsForDbId_正常ケース_dbId名一致でDataSourceを選択する_対応するDSとTMが返ること() throws Exception {
        Properties props = new Properties();
        props.setProperty("spring.datasource.db1.url", "jdbc:h2:mem:same");
        props.setProperty("spring.datasource.db1.username", "sa");
        setTrc(props);

        DataSource dsDb1 = dataSourceWithMeta("jdbc:h2:mem:same", "sa");
        DataSource dsOther = dataSourceWithMeta("jdbc:h2:mem:same", "sa");
        DataSourceTransactionManager tmDb1 = new DataSourceTransactionManager(dsDb1);
        DataSourceTransactionManager tmOther = new DataSourceTransactionManager(dsOther);

        ApplicationContext ac = mock(ApplicationContext.class);
        when(ac.getBeanNamesForType(DataSource.class))
                .thenReturn(new String[] {"db1RoutingDataSource", "otherRoutingDataSource"});
        when(ac.getBean("db1RoutingDataSource", DataSource.class)).thenReturn(dsDb1);
        when(ac.getBean("otherRoutingDataSource", DataSource.class)).thenReturn(dsOther);
        when(ac.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tmDb1", "tmOther"});
        when(ac.getBean("tmDb1", PlatformTransactionManager.class)).thenReturn(tmDb1);
        when(ac.getBean("tmOther", PlatformTransactionManager.class)).thenReturn(tmOther);

        LoadDataExtension.Components components = target.resolveComponentsForDbId(ac, "db1");

        assertSame(dsDb1, components.dataSource);
        assertSame(tmDb1, components.txManager);

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

        assertThrows(IllegalStateException.class, () -> target.resolveComponentsForDbId(ac, "db1"));
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
        when(ds1.getConnection()).thenReturn(c1);
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

        LoadDataExtension.Components components = target.resolveComponentsForDbId(ac, "db1");
        assertSame(ds1, components.dataSource);
    }

    @Test
    void pickPrimary_異常ケース_primary複数と非Configurableを判定する_nullが返ること() throws Exception {
        DataSource ds1 = mock(DataSource.class);
        DataSource ds2 = mock(DataSource.class);
        LoadDataExtension.NamedDs n1 = new LoadDataExtension.NamedDs("ds1", ds1);
        LoadDataExtension.NamedDs n2 = new LoadDataExtension.NamedDs("ds2", ds2);

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

        assertEquals(null, target.pickPrimary(ac, List.of(n1, n2)));

        ApplicationContext nonConfigurable = mock(ApplicationContext.class);
        assertEquals(null, target.pickPrimary(nonConfigurable, List.of(n1, n2)));
    }

    @Test
    void pickSingleReferencedByTm_異常ケース_TMがDataSourceTransactionManager以外の場合_参照なしでnullが返ること()
            throws Exception {
        DataSource ds1 = mock(DataSource.class);
        LoadDataExtension.NamedDs n1 = new LoadDataExtension.NamedDs("ds1", ds1);

        ApplicationContext ac = mock(ApplicationContext.class);
        when(ac.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tm1"});
        when(ac.getBean("tm1", PlatformTransactionManager.class))
                .thenReturn(mock(PlatformTransactionManager.class));

        assertEquals(null, target.pickSingleReferencedByTm(ac, List.of(n1)));
    }

    @Test
    void pickSingleByDbIdName_正常異常ケース_dbId名一致と曖昧一致を判定する_単一一致のみ返ること() throws Exception {
        DataSource ds1 = mock(DataSource.class);
        DataSource ds2 = mock(DataSource.class);
        LoadDataExtension.NamedDs n1 = new LoadDataExtension.NamedDs("db1RoutingDataSource", ds1);
        LoadDataExtension.NamedDs n2 = new LoadDataExtension.NamedDs("otherDataSource", ds2);
        LoadDataExtension.NamedDs picked = target.pickSingleByDbIdName(List.of(n1, n2), "db1");
        assertSame(n1, picked);

        assertEquals(null, target.pickSingleByDbIdName(List.of(n1, n2), " "));

        LoadDataExtension.NamedDs n3 = new LoadDataExtension.NamedDs("db1AutoDataSource", ds2);
        assertEquals(null, target.pickSingleByDbIdName(List.of(n1, n3), "db1"));
    }

    @Test
    void normalizeForAffinityMatch_正常ケース_nullと記号文字を正規化する_英数字小文字だけが返ること() throws Exception {
        assertEquals("", target.normalizeForAffinityMatch(null));
        assertEquals("db1routingdatasource",
                target.normalizeForAffinityMatch("DB-1_Routing.DataSource"));
    }

    @Test
    void findTmByMetadata_正常ケース_DataSourceTransactionManager以外と不一致を除外する_一致件数が1件であること()
            throws Exception {
        DataSource dsMismatch = dataSourceWithMeta("jdbc:h2:mem:other", "sa");
        DataSource dsMatch = dataSourceWithMeta("jdbc:h2:mem:tm2", "sa");
        DataSourceTransactionManager tmMismatch = new DataSourceTransactionManager(dsMismatch);
        DataSourceTransactionManager tmMatch = new DataSourceTransactionManager(dsMatch);
        PlatformTransactionManager tmOtherType = mock(PlatformTransactionManager.class);

        ApplicationContext ac = mock(ApplicationContext.class);
        when(ac.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tmOtherType", "tmMismatch", "tmMatch"});
        when(ac.getBean("tmOtherType", PlatformTransactionManager.class)).thenReturn(tmOtherType);
        when(ac.getBean("tmMismatch", PlatformTransactionManager.class)).thenReturn(tmMismatch);
        when(ac.getBean("tmMatch", PlatformTransactionManager.class)).thenReturn(tmMatch);
        when(ac.getBeanNamesForType(DataSource.class)).thenReturn(new String[] {"dsMatch"});
        when(ac.getBean("dsMatch", DataSource.class)).thenReturn(dsMatch);

        List<LoadDataExtension.TmWithDs> list =
                target.findTmByMetadata(ac, "jdbc:h2:mem:tm2", "sa");
        assertEquals(1, list.size());
        assertEquals("tmMatch", list.get(0).tmName);
    }

    @Test
    void helperBranches_正常ケース_URL比較とnull比較の残分岐を通す_期待値が返ること() throws Exception {
        assertEquals(true, target.urlRoughMatch("jdbc:h2:mem:a", "jdbc:h2:mem:a;MODE=Oracle"));
        assertEquals(true, target.urlRoughMatch("jdbc:h2:mem:abcdef", "jdbc:h2:mem:abc"));
        assertEquals(false, target.urlRoughMatch(null, "jdbc:h2:mem:a"));
        assertEquals(false, target.urlRoughMatch("jdbc:h2:mem:a", null));

        assertEquals(false, target.equalsIgnoreCaseSafe("A", null));
        assertEquals(false, target.equalsIgnoreCaseSafe(null, "A"));
    }

    @Test
    void probeDataSourceMeta_正常ケース_getMetaDataがnullの場合はurluserにnullを保持する_ProbeMetaが返ること()
            throws Exception {
        DataSource ds = mock(DataSource.class);
        Connection conn = mock(Connection.class);
        when(ds.getConnection()).thenReturn(conn);
        when(conn.getMetaData()).thenReturn(null);

        LoadDataExtension.ProbeMeta meta = target.probeDataSourceMeta(ds);
        assertEquals(null, meta.url);
        assertEquals(null, meta.user);

    }

    @Test
    void resolveTxManagerByDataSource_異常ケース_同一TM別名付きで複数実体一致する_例外メッセージにaliasesが含まれること()
            throws Exception {
        DataSource ds1 = mock(DataSource.class);
        PlatformTransactionManager tmOtherType = mock(PlatformTransactionManager.class);
        DataSourceTransactionManager tmA = new DataSourceTransactionManager(ds1);
        DataSourceTransactionManager tmB = new DataSourceTransactionManager(ds1);

        ApplicationContext ac = mock(ApplicationContext.class);
        when(ac.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tmOtherType", "tmA", "tmAAlias", "tmB"});
        when(ac.getBean("tmOtherType", PlatformTransactionManager.class)).thenReturn(tmOtherType);
        when(ac.getBean("tmA", PlatformTransactionManager.class)).thenReturn(tmA);
        when(ac.getBean("tmAAlias", PlatformTransactionManager.class)).thenReturn(tmA);
        when(ac.getBean("tmB", PlatformTransactionManager.class)).thenReturn(tmB);

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> target.resolveTxManagerByDataSource(ac, "db1", ds1));
        assertTrue(ex.getMessage().contains("aliases="));
    }

    @Test
    void findDataSourceByMetadata_正常ケース_USER不一致を除外して一致のみ返す_一致件数が1件であること() throws Exception {
        DataSource match = dataSourceWithMeta("jdbc:h2:mem:fds", "sa");
        DataSource userMismatch = dataSourceWithMeta("jdbc:h2:mem:fds", "other");

        ApplicationContext ac = mock(ApplicationContext.class);
        when(ac.getBeanNamesForType(DataSource.class)).thenReturn(new String[] {"ds1", "ds2"});
        when(ac.getBean("ds1", DataSource.class)).thenReturn(match);
        when(ac.getBean("ds2", DataSource.class)).thenReturn(userMismatch);

        List<LoadDataExtension.NamedDs> list =
                target.findDataSourceByMetadata(ac, "jdbc:h2:mem:fds", "sa");
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

        List<LoadDataExtension.NamedDs> list =
                target.findDataSourceByMetadata(ac, "jdbc:h2:mem:fds2", "sa");
        assertEquals(1, list.size());
    }

    @Test
    void restoreTxInterceptorDefaultManager_正常ケース_touched空の場合は即時復帰する_ストア値が維持されること()
            throws Exception {
        ExtensionContext context = mock(ExtensionContext.class);
        Map<Object, Object> storeMap = new HashMap<>();
        mockStore(context, storeMap);

        LoadDataExtension.TxInterceptorSwitchRecord rec =
                new LoadDataExtension.TxInterceptorSwitchRecord();
        storeMap.put("TXI_DEFAULT_SWITCH", rec);

        assertDoesNotThrow(() -> target.restoreTxInterceptorDefaultManager(context));
        assertNotNull(storeMap.get("TXI_DEFAULT_SWITCH"));
    }

    @Test
    void loadScenarioParticipating_正常ケース_DumpConfigにflywayが既存時は重複追加しない_例外とならないこと()
            throws Exception {
        Path scenarioInput = dummyClassResourcesDir.resolve("S_DUMP_CFG").resolve("input");
        Files.createDirectories(scenarioInput);

        Properties props = new Properties();
        props.setProperty("spring.datasource.url", "jdbc:h2:mem:dcfg");
        props.setProperty("spring.datasource.username", "sa");
        setTrc(props);

        ExtensionContext context = mock(ExtensionContext.class);
        mockStore(context, new HashMap<>());

        ApplicationContext applicationContext = mock(ApplicationContext.class);
        DataSource ds = mock(DataSource.class);
        Connection conn = mock(Connection.class);
        when(ds.getConnection()).thenReturn(conn);
        when(applicationContext.getBean("dataSource", DataSource.class)).thenReturn(ds);

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
            assertDoesNotThrow(() -> target.loadScenarioParticipating(context, "S_DUMP_CFG", null));
            assertEquals(1, dumpConfigConstruction.constructed().size());
            assertEquals(1, loaders.constructed().size());
        }
    }

    @Test
    void beforeTestExecution_異常ケース_クラスアノテーションscenario空文字指定時_IllegalStateExceptionが送出されること()
            throws Exception {
        TestResourceContext trc = new TestResourceContext(dummyClassClassPathDir, new Properties());
        target.setTestResourceContext(trc);

        ExtensionContext context = mock(ExtensionContext.class);
        doReturn(BlankScenarioClass.class).when(context).getRequiredTestClass();
        doReturn(Optional.empty()).when(context).getTestMethod();
        mockStore(context, new HashMap<>());

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> target.beforeTestExecution(context));
        assertTrue(ex.getMessage().contains("scenario"));
    }

    @Test
    void beforeTestExecution_異常ケース_メソッドアノテーションscenario空文字指定時_IllegalStateExceptionが送出されること()
            throws Exception {
        TestResourceContext trc = new TestResourceContext(dummyClassClassPathDir, new Properties());
        target.setTestResourceContext(trc);

        Method method = BlankScenarioMethodClass.class.getDeclaredMethod("blankScenarioMethod");
        ExtensionContext context = mock(ExtensionContext.class);
        doReturn(BlankScenarioMethodClass.class).when(context).getRequiredTestClass();
        doReturn(Optional.of(method)).when(context).getTestMethod();
        mockStore(context, new HashMap<>());

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> target.beforeTestExecution(context));
        assertTrue(ex.getMessage().contains("scenario"));
    }

    @Test
    void validateLoadDataAnnotation_異常ケース_dbNames空配列指定時_IllegalStateExceptionが送出されること() {
        LoadData ann = new LoadData() {
            @Override
            public Class<? extends Annotation> annotationType() {
                return LoadData.class;
            }

            @Override
            public String[] scenario() {
                return new String[] {"CSV"};
            }

            @Override
            public String[] dbNames() {
                return new String[0];
            }
        };
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> target.validateLoadDataAnnotation(ann, "test-location"));
        assertTrue(ex.getMessage().contains("dbNames"));
    }

    @Test
    void loadScenarioParticipating_正常ケース_singleDBで既存TXかつDSバインド済みの場合_警告分岐を通過して処理継続すること()
            throws Exception {
        Path scenarioInput = dummyClassResourcesDir.resolve("S_SINGLE_BOUND").resolve("input");
        Files.createDirectories(scenarioInput);

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
            assertDoesNotThrow(() -> target.loadScenarioParticipating(context, "S_SINGLE_BOUND",
                    new String[0]));
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

        List<LoadDataExtension.TmWithDs> list =
                target.findTmByMetadata(ac, "jdbc:h2:mem:u_mismatch", "sa");
        assertTrue(list.isEmpty());
    }

    @Test
    void pickPrimary_正常ケース_BeanDefinition未登録候補をスキップする_登録済primaryが返ること() throws Exception {
        DataSource ds1 = mock(DataSource.class);
        DataSource ds2 = mock(DataSource.class);
        LoadDataExtension.NamedDs n1 = new LoadDataExtension.NamedDs("ds1", ds1);
        LoadDataExtension.NamedDs n2 = new LoadDataExtension.NamedDs("ds2", ds2);

        ConfigurableApplicationContext ac = mock(ConfigurableApplicationContext.class);
        ConfigurableListableBeanFactory bf = mock(ConfigurableListableBeanFactory.class);
        BeanDefinition bd2 = mock(BeanDefinition.class);
        when(ac.getBeanFactory()).thenReturn(bf);
        when(bf.containsBeanDefinition("ds1")).thenReturn(false);
        when(bf.containsBeanDefinition("ds2")).thenReturn(true);
        when(bf.getBeanDefinition("ds2")).thenReturn(bd2);
        when(bd2.isPrimary()).thenReturn(true);

        LoadDataExtension.NamedDs picked = target.pickPrimary(ac, List.of(n1, n2));
        assertEquals("ds2", picked.name);
    }

    /**
     * Creates a mock {@link ExtensionContext} whose required test class returns the given class and
     * whose test method is empty.
     */
    private ExtensionContext mockContextForClass(Class<?> clazz) {
        ExtensionContext ctx = mock(ExtensionContext.class);
        doReturn(clazz).when(ctx).getRequiredTestClass();
        doReturn(Optional.empty()).when(ctx).getTestMethod();
        return ctx;
    }

    /**
     * Builds a {@link TestResourceContext} from the given properties and installs it into the test
     * target.
     */
    private void setTrc(Properties props) throws Exception {
        TestResourceContext trc = new TestResourceContext(dummyClassResourcesDir, props);
        target.setTestResourceContext(trc);
    }

    /**
     * Creates a mock {@link DataSource} whose connection metadata returns the specified JDBC URL
     * and user name.
     */
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

    /**
     * Creates a mock {@link Store} backed by the given map, wiring it to the supplied
     * {@link ExtensionContext} so that get, getOrComputeIfAbsent, and remove operations delegate to
     * the map.
     */
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

    /**
     * Extracts the transaction manager bean name from the given interceptor by down-casting to
     * {@link InspectableTransactionInterceptor}.
     */
    private String readTxManagerBeanName(TransactionInterceptor interceptor) {
        return ((InspectableTransactionInterceptor) interceptor).transactionManagerBeanName();
    }

    /**
     * A subclass of {@link TransactionInterceptor} that exposes the otherwise inaccessible
     * transaction manager bean name for test assertions.
     */
    private static final class InspectableTransactionInterceptor extends TransactionInterceptor {

        private static final long serialVersionUID = 1L;

        /**
         * Returns the configured transaction manager bean name.
         *
         * @return transaction manager bean name
         */
        private String transactionManagerBeanName() {
            return getTransactionManagerBeanName();
        }
    }

    /**
     * Writes the given content to the specified path, creating parent directories if they do not
     * exist and truncating any existing file.
     */
    private void writeToFile(Path p, String content) throws IOException {
        Files.createDirectories(p.getParent());
        Files.write(p, content.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
    }

    /**
     * Creates a minimal mock {@link DataSource} with a named dummy connection, suitable for tests
     * that need a DataSource but do not inspect its data.
     */
    private DataSource dummyDataSource() throws Exception {
        Connection conn = mock(Connection.class, Mockito.withSettings().name("DummyConnection"));
        when(conn.getWarnings()).thenReturn(new SQLWarning());
        when(conn.getMetaData()).thenReturn(mock(DatabaseMetaData.class));

        DataSource ds = mock(DataSource.class);
        when(ds.getConnection()).thenReturn(conn);
        return ds;
    }
}
