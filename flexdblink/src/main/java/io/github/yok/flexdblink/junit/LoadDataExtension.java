package io.github.yok.flexdblink.junit;

import com.google.common.collect.Lists;
import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.CsvDateTimeFormatProperties;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.core.DataLoader;
import io.github.yok.flexdblink.db.DbDialectHandlerFactory;
import io.github.yok.flexdblink.db.DbUnitConfigFactory;
import io.github.yok.flexdblink.util.DateTimeFormatUtil;
import io.github.yok.flexdblink.util.ErrorHandler;
import io.github.yok.flexdblink.util.LogPathUtil;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
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
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * JUnit 5 Extension that interprets {@link LoadData} and loads CSV/LOB test data.
 *
 * <ul>
 * <li><b>TX policy:</b> Join an existing test transaction if present; otherwise start one and
 * always roll it back after the test.</li>
 * <li><b>Single-DB mode:</b> when {@code dbNames} is not specified. Uses {@code input/} directly
 * and resolves DataSource from configuration or Spring beans.</li>
 * <li><b>Multi-DB mode:</b> when {@code dbNames} is specified. Uses {@code input/{dbId}/} and
 * resolves each DataSource from {@code flexdblink.properties}
 * ({@code flexdblink.load.datasource.<dbId>=<beanName>}).</li>
 * <li><b>Directory validation:</b> missing dbId folders cause errors; extra ones are warned.</li>
 * <li>For Spring-managed connections, {@code close()} is disabled to prevent accidental
 * closing.</li>
 * </ul>
 *
 * <h3>Directory layout</h3>
 *
 * <pre>
 * Single DB: src/test/resources/{pkg}/{TestClass}/{scenario}/input/
 * Multi  DB: src/test/resources/{pkg}/{TestClass}/{scenario}/input/{dbId}/
 * </pre>
 *
 * @author Yasuharu.Okawauchi
 */
@Slf4j
public class LoadDataExtension
        implements BeforeAllCallback, BeforeTestExecutionCallback, AfterTestExecutionCallback {

    // Shared context (classRoot / application*.properties, etc.)
    private TestResourceContext trc;

    // Store namespace and keys
    private static final Namespace NS = Namespace.create(LoadDataExtension.class.getName(), "TX");
    private static final String STORE_KEY_TX = "TX_RECORDS";
    private static final String STORE_KEY_TXI_DEFAULT = "TXI_DEFAULT_SWITCH";
    private static final String FLEXDBLINK_PROPERTIES = "flexdblink.properties";
    private static final String LOAD_DS_PREFIX = "flexdblink.load.datasource.";

    /**
     * Replaces the test resource context used by this extension.
     *
     * @param trc replacement test resource context
     */
    void setTestResourceContext(TestResourceContext trc) {
        this.trc = trc;
    }

    /**
     * Returns the current test resource context.
     *
     * @return current test resource context
     */
    TestResourceContext getTestResourceContext() {
        return trc;
    }

    /**
     * Records, per data source key, whether this extension switched the
     * {@code TransactionInterceptor}'s default {@code TransactionManager} and needs to restore it.
     */
    static final class TxInterceptorSwitchRecord {
        final Map<String, Boolean> touched = new LinkedHashMap<>();
    }

    /**
     * Initialize shared context at class start.
     *
     * @param context execution context.
     * @throws Exception when initialization fails.
     */
    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        this.trc = TestResourceContext.init(context);
        log.info("Initialization completed. classRoot={}, loadedProperties={}", trc.getClassRoot(),
                trc.getAppProps().size());
    }

    /**
     * Interpret {@link LoadData} and load data right before each test. Joins an existing
     * Spring-managed test transaction if available.
     *
     * @param context execution context.
     * @throws Exception on failure.
     */
    @Override
    public void beforeTestExecution(ExtensionContext context) throws Exception {
        ErrorHandler.disableExitForCurrentThread();
        try {
            if (trc == null) {
                beforeAll(context);
            }

            Class<?> testClass = context.getRequiredTestClass();
            log.info("Extension is active. testClass={}", testClass.getName());

            // Class-level @LoadData
            LoadData classAnn = testClass.getAnnotation(LoadData.class);
            if (classAnn != null) {
                for (String scenario : resolveScenarios(classAnn)) {
                    Path base = StringUtils.isBlank(scenario) ? trc.getClassRoot()
                            : trc.getClassRoot().resolve(scenario);
                    if (Files.isDirectory(base)) {
                        log.info("Found class-level @LoadData. scenario={}, dir={}", scenario,
                                base);
                        loadScenarioParticipating(context, scenario, classAnn.dbNames());
                    } else {
                        log.warn("Class-level scenario directory not found. scenario={}, dir={}",
                                scenario, base);
                    }
                }
            }

            // Method-level @LoadData
            context.getTestMethod().ifPresent(m -> {
                LoadData methodAnn = m.getAnnotation(LoadData.class);
                if (methodAnn == null) {
                    return;
                }
                try {
                    for (String scenario : resolveScenarios(methodAnn)) {
                        Path base = StringUtils.isBlank(scenario) ? trc.getClassRoot()
                                : trc.getClassRoot().resolve(scenario);
                        if (Files.isDirectory(base)) {
                            log.info("Found method-level @LoadData. method={}, scenario={}, dir={}",
                                    m.getName(), scenario,
                                    LogPathUtil.renderDirForLog(base.toFile()));
                            loadScenarioParticipating(context, scenario, methodAnn.dbNames());
                        } else {
                            log.info(
                                    "Method-level scenario directory not found."
                                            + "method={}, scenario={}, dir={}",
                                    m.getName(), scenario,
                                    LogPathUtil.renderDirForLog(base.toFile()));
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }
    }

    /**
     * Roll back test transactions started by this extension and restore AOP default TM.
     *
     * @param context execution context.
     */
    @Override
    public void afterTestExecution(ExtensionContext context) {
        rollbackAllIfBegan(context);
        restoreTxInterceptorDefaultManager(context);
    }

    /**
     * Load data according to the scenario/dbNames. Joins an existing transaction or starts a new
     * one (rolled back after the test).
     *
     * @param context execution context.
     * @param scenarioName scenario name (nullable/empty allowed).
     * @param dbNamesAttr dbIds specified by annotation (nullable/empty for single-DB mode).
     * @throws Exception on failure.
     */
    void loadScenarioParticipating(ExtensionContext context, String scenarioName,
            String[] dbNamesAttr) throws Exception {

        // Prepare dump output directory
        Path dumpRoot = Paths.get(System.getProperty("user.dir"), "target", "dbunit", "dump")
                .toAbsolutePath().normalize();
        Files.createDirectories(dumpRoot);
        log.info("Dump directory prepared: {}", dumpRoot);

        // CSV/DateTime settings (Oracle compatible)
        CsvDateTimeFormatProperties dtProps = new CsvDateTimeFormatProperties();
        dtProps.setDate("yyyy-MM-dd");
        dtProps.setTime("HH:mm:ss");
        dtProps.setDateTimeWithMillis("yyyy-MM-dd HH:mm:ss.SSS");
        dtProps.setDateTime("yyyy-MM-dd HH:mm:ss");

        DumpConfig dumpConfig = new DumpConfig();
        log.info("Excluded tables: {}", dumpConfig.getExcludeTables());

        DbUnitConfigFactory configFactory = new DbUnitConfigFactory();
        ConnectionConfig connectionConfig = new ConnectionConfig();
        DateTimeFormatUtil dateTimeUtil = new DateTimeFormatUtil(dtProps);

        // Resolve paths
        Path classRoot = trc.getClassRoot();
        PathsConfig pathsConfig = new PathsConfig() {
            /**
             * {@inheritDoc}
             */
            @Override
            public String getLoad() {
                return classRoot.toAbsolutePath().toString();
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public String getDataPath() {
                return classRoot.toAbsolutePath().toString();
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public String getDump() {
                return dumpRoot.toString();
            }
        };

        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        DbDialectHandlerFactory handlerFactory = new DbDialectHandlerFactory(dbUnitConfig,
                dumpConfig, pathsConfig, dateTimeUtil, configFactory);

        DataLoader loader = new DataLoader(pathsConfig, connectionConfig, handlerFactory::create,
                dbUnitConfig, dumpConfig);

        // Mode detection
        boolean multi = dbNamesAttr != null && dbNamesAttr.length > 0;
        ApplicationContext ac = getApplicationContext(context);
        ClassLoader cl = resolveClassLoader(context);
        Properties flexProps = loadFlexDbLinkProperties(cl);
        Map<String, String> dsBeanNamesByDbId = readConfiguredDataSourceBeanNames(flexProps);

        // ===== Single-DB mode =====
        if (!multi) {
            DataSource ds = resolveDataSourceSingle(ac, dsBeanNamesByDbId);
            if (isTxActive() && TransactionSynchronizationManager.hasResource(ds)) {
                log.info("Single-DB: Existing TX found and DS is bound; participating in it.");
            } else {
                beginTestTxWithDataSource("SINGLE", ds, context);
            }

            // Dataset: input/
            Path datasetDir = trc.baseInputDir(scenarioName);
            if (!Files.isDirectory(datasetDir)) {
                throw new IllegalStateException(
                        "Dataset directory not found: " + datasetDir.toAbsolutePath());
            }

            // Connection info (single DB)
            ConnectionConfig.Entry entry = trc.buildEntryFromProps("");
            log.info("Connection info resolved. url={}, user={}", entry.getUrl(), entry.getUser());

            // Obtain connection (joins the TX automatically). Disable close().
            Connection conn = DataSourceUtils.getConnection(ds);
            conn = wrapConnectionNoClose(conn);
            log.info("Transactional connection obtained (single-DB mode).");

            // Execute
            loader.executeWithConnection(datasetDir.toFile(), entry, conn);
            log.info("Data load completed (single-DB mode). dir={}", datasetDir);
            return;
        }

        // ===== Multi-DB mode =====
        Set<String> expected = new LinkedHashSet<>(Arrays.asList(dbNamesAttr));
        Path baseInput = trc.baseInputDir(scenarioName);
        Set<String> found = listDbIdsFromFolder(baseInput);
        Map<String, String> foundByNormalized = indexDbIdsByNormalized(found);
        List<String> dbIds = new ArrayList<>();
        Set<String> missing = new LinkedHashSet<>();
        Set<String> requestedNormalized = new LinkedHashSet<>();
        for (String requestedDbId : expected) {
            String normalized = normalizeDbIdKey(requestedDbId);
            requestedNormalized.add(normalized);
            String actualDbId = foundByNormalized.get(normalized);
            if (actualDbId == null) {
                missing.add(requestedDbId);
                continue;
            }
            dbIds.add(actualDbId);
        }

        // Missing = error; Extras = warn
        if (!missing.isEmpty()) {
            throw new IllegalStateException(
                    "Missing dbNames folders: missing=" + missing + ", base=" + baseInput);
        }
        Set<String> extras = new LinkedHashSet<>();
        for (Map.Entry<String, String> e : foundByNormalized.entrySet()) {
            if (!requestedNormalized.contains(e.getKey())) {
                extras.add(e.getValue());
            }
        }
        if (!extras.isEmpty()) {
            log.warn("Unspecified folders under input (ignored): extras={}", extras);
        }

        // Pre-resolve DS set for each dbId
        List<DataSource> dss = new ArrayList<>(dbIds.size());
        for (String dbId : dbIds) {
            DataSource ds = resolveDataSourceByDbId(ac, dbId, dsBeanNamesByDbId);
            dss.add(ds);
        }

        // Check existing TX and begin local TX only when needed
        boolean active = isTxActive();
        for (int i = 0; i < dbIds.size(); i++) {
            DataSource ds = dss.get(i);
            String dbId = dbIds.get(i);
            if (active && TransactionSynchronizationManager.hasResource(ds)) {
                log.info("Multi-DB: dbId={} is bound to existing TX; participating in it.", dbId);
                continue;
            }
            beginTestTxWithDataSource(dbId, ds, context);
        }

        // Execute per dbId
        for (int i = 0; i < dbIds.size(); i++) {
            String dbId = dbIds.get(i);
            DataSource ds = dss.get(i);

            // Dataset: input/{dbId}/
            Path datasetDir = trc.inputDir(scenarioName, dbId);
            if (!Files.isDirectory(datasetDir)) {
                throw new IllegalStateException(
                        "Dataset directory not found: " + datasetDir.toAbsolutePath());
            }

            // Connection info
            ConnectionConfig.Entry entry = trc.buildEntryFromProps(dbId);
            log.info("Connection info resolved. dbId={}, url={}, user={}", entry.getId(),
                    entry.getUrl(), entry.getUser());

            // Obtain connection (joins the TX automatically). Disable close().
            Connection conn = DataSourceUtils.getConnection(ds);
            conn = wrapConnectionNoClose(conn);
            log.info("Transactional connection obtained. dbId={}", dbId);

            // Execute
            loader.executeWithConnection(datasetDir.toFile(), entry, conn);
            log.info("Data load completed. dbId={}, dir={}", dbId,
                    LogPathUtil.renderDirForLog(datasetDir.toFile()));
        }
    }

    /**
     * Switch the default TransactionManager of every {@link TransactionInterceptor} in the context
     * to the selected TM's bean name. Restored after the test.
     *
     * @param context JUnit extension context
     * @param ac Spring application context
     * @param picked selected transaction manager
     * @param key logical key used for logging and store association
     */
    void setTxInterceptorDefaultManager(ExtensionContext context, ApplicationContext ac,
            PlatformTransactionManager picked, String key) {

        String pickedName = findBeanNameByInstance(ac, PlatformTransactionManager.class, picked);
        if ("<unknown>".equals(pickedName)) {
            log.warn("AOP: cannot resolve TM bean name; skip switch. key={}", key);
            return;
        }

        Map<String, TransactionInterceptor> tis = ac.getBeansOfType(TransactionInterceptor.class);
        if (tis.isEmpty()) {
            log.info("AOP: no TransactionInterceptor beans; no switch needed.");
            return;
        }

        Store store = context.getStore(NS);
        TxInterceptorSwitchRecord rec = store.getOrComputeIfAbsent(STORE_KEY_TXI_DEFAULT,
                k -> new TxInterceptorSwitchRecord(), TxInterceptorSwitchRecord.class);

        for (Map.Entry<String, TransactionInterceptor> e : tis.entrySet()) {
            String tiName = e.getKey();
            TransactionInterceptor ti = e.getValue();
            try {
                // Clear explicit TM instance and switch to bean-name resolution
                ti.setTransactionManager(null);
                ti.setTransactionManagerBeanName(pickedName);
                rec.touched.put(tiName, Boolean.TRUE);
            } catch (Exception ex) {
                log.warn("AOP coordination: Failed to switch default TransactionManager."
                        + "interceptor={}, key={}", tiName, key, ex);
            }
        }
        log.info(
                "AOP coordination: Default TransactionManager switched."
                        + "picked={}, key={}, affectedInterceptors={}",
                pickedName, key, tis.size());
    }

    /**
     * Restore the default TransactionManager settings for {@link TransactionInterceptor}s that were
     * modified by {@link #setTxInterceptorDefaultManager}.
     *
     * @param context JUnit extension context
     */
    void restoreTxInterceptorDefaultManager(ExtensionContext context) {
        Store store = context.getStore(NS);
        TxInterceptorSwitchRecord rec =
                store.get(STORE_KEY_TXI_DEFAULT, TxInterceptorSwitchRecord.class);
        if (rec == null || rec.touched.isEmpty()) {
            return;
        }

        ApplicationContext ac = getApplicationContext(context);
        Map<String, TransactionInterceptor> tis = ac.getBeansOfType(TransactionInterceptor.class);
        for (Map.Entry<String, Boolean> e : rec.touched.entrySet()) {
            String tiName = e.getKey();
            TransactionInterceptor ti = tis.get(tiName);
            if (ti == null) {
                continue;
            }
            try {
                ti.setTransactionManagerBeanName(null);
                ti.setTransactionManager(null);
            } catch (Exception ex) {
                log.warn("AOP coordination: Failed to restore default TransactionManager."
                        + "interceptor={}", tiName, ex);
            }
        }
        store.remove(STORE_KEY_TXI_DEFAULT);
        log.info("AOP coordination: Default TransactionManager restored. affectedInterceptors={}",
                rec.touched.size());
    }

    /**
     * Resolve {@link LoadData#scenario()}; if not specified, list directories under the class root.
     *
     * @param ann annotation.
     * @return scenario names.
     * @throws Exception on failure.
     */
    List<String> resolveScenarios(LoadData ann) throws Exception {
        if (ann.scenario().length > 0) {
            return Arrays.asList(ann.scenario());
        }
        return trc.listDirectories(trc.getClassRoot());
    }

    /**
     * Wrap a connection to ignore {@code close()} while participating in a Spring-managed TX.
     *
     * @param original original connection.
     * @return proxy connection that ignores {@code close()}.
     */
    Connection wrapConnectionNoClose(Connection original) {
        return (Connection) Proxy.newProxyInstance(original.getClass().getClassLoader(),
                new Class<?>[] {Connection.class}, (proxy, method, args) -> {
                    if ("close".equals(method.getName())) {
                        return null;
                    }
                    return method.invoke(original, args);
                });
    }

    /**
     * Obtain {@link ApplicationContext}.
     *
     * @param context execution context.
     * @return application context.
     */
    private ApplicationContext getApplicationContext(ExtensionContext context) {
        return SpringExtension.getApplicationContext(context);
    }

    /**
     * Resolve a class loader for reading classpath resources.
     *
     * @param context execution context.
     * @return class loader.
     */
    ClassLoader resolveClassLoader(ExtensionContext context) {
        try {
            Class<?> testClass = context.getRequiredTestClass();
            if (testClass != null) {
                return testClass.getClassLoader();
            }
        } catch (Exception e) {
            log.debug("Failed to resolve test class loader from ExtensionContext.", e);
        }

        ClassLoader threadLoader = Thread.currentThread().getContextClassLoader();
        if (threadLoader != null) {
            return threadLoader;
        }
        return LoadDataExtension.class.getClassLoader();
    }

    /**
     * Whether a Spring-managed transaction is active on the current thread.
     *
     * @return {@code true} if active.
     */
    private boolean isTxActive() {
        return TransactionSynchronizationManager.isActualTransactionActive();
    }

    /**
     * Begin a test transaction and record it so it can be rolled back in the After phase.
     *
     * @param key identifier for logging (dbId, etc.).
     * @param tm transaction manager.
     * @param context execution context.
     */
    private void beginTestTx(String key, PlatformTransactionManager tm, ExtensionContext context) {
        DefaultTransactionDefinition def = new DefaultTransactionDefinition();
        TransactionStatus status = tm.getTransaction(def);
        List<TxRecord> records = getOrCreateTxRecords(context);
        records.add(new TxRecord(key, tm, status));
        log.info("Started a test transaction. key={}", key);
    }

    /**
     * Begin a local transaction using a temporary {@link DataSourceTransactionManager} bound to the
     * target {@link DataSource}.
     *
     * @param key identifier for logging (dbId, etc.).
     * @param ds target data source.
     * @param context execution context.
     */
    void beginTestTxWithDataSource(String key, DataSource ds, ExtensionContext context) {
        DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);
        beginTestTx(key, tm, context);
    }

    /**
     * Roll back all test transactions started by this extension in reverse order.
     *
     * @param context execution context.
     */
    void rollbackAllIfBegan(ExtensionContext context) {
        List<TxRecord> records = getTxRecords(context);
        if (records.isEmpty()) {
            return;
        }
        List<TxRecord> rev = Lists.reverse(records);
        for (TxRecord r : rev) {
            try {
                r.tm.rollback(r.status);
                log.info("Rolled back due to test completion. key={}", r.key);
            } catch (Exception e) {
                log.error("Exception occurred during rollback. key={}", r.key, e);
            }
        }
        context.getStore(NS).remove(STORE_KEY_TX);
    }

    /**
     * Resolve the conventional TM bean ('transactionManager') for single-DB mode.
     *
     * @param ac application context.
     * @return PlatformTransactionManager.
     */
    PlatformTransactionManager resolveTxManagerSingle(ApplicationContext ac) {
        try {
            return ac.getBean("transactionManager", PlatformTransactionManager.class);
        } catch (Exception e) {
            throw new IllegalStateException("Transaction manager 'transactionManager' not found."
                    + "It is required in single-DB mode.", e);
        }
    }

    /**
     * Resolve the conventional DS bean ('dataSource') for single-DB mode.
     *
     * @param ac application context.
     * @return DataSource.
     */
    DataSource resolveDataSourceSingle(ApplicationContext ac) {
        try {
            return ac.getBean("dataSource", DataSource.class);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "DataSource 'dataSource' not found. It is required in single-DB mode.", e);
        }
    }

    /**
     * Resolve DataSource for single-DB mode without requiring {@code flexdblink.properties}.
     *
     * <p>
     * Selection order:
     * </p>
     * <ol>
     * <li>Configured mapping in {@code flexdblink.properties} (default or single entry).</li>
     * <li>Bean name {@code dataSource}.</li>
     * <li>The only {@link DataSource} bean.</li>
     * <li>Single {@code @Primary} {@link DataSource} bean.</li>
     * </ol>
     *
     * @param ac application context.
     * @param dsBeanNamesByDbId configured mapping.
     * @return selected DataSource.
     */
    DataSource resolveDataSourceSingle(ApplicationContext ac, Map<String, String> dsBeanNamesByDbId) {
        String configuredDefault = dsBeanNamesByDbId.get("default");
        if (!StringUtils.isBlank(configuredDefault)) {
            return resolveDataSourceByBeanName(ac, "default", configuredDefault);
        }
        if (dsBeanNamesByDbId.size() == 1) {
            String onlyBeanName = dsBeanNamesByDbId.values().iterator().next();
            return resolveDataSourceByBeanName(ac, "default", onlyBeanName);
        }

        try {
            return ac.getBean("dataSource", DataSource.class);
        } catch (Exception e) {
            log.debug("DataSource bean named 'dataSource' was not found in single-DB mode.", e);
        }

        String[] dsNames = ac.getBeanNamesForType(DataSource.class);
        if (dsNames.length == 1) {
            return ac.getBean(dsNames[0], DataSource.class);
        }

        NamedDs primary = pickPrimaryFromAll(ac);
        if (primary != null) {
            return primary.ds;
        }

        throw new IllegalStateException("DataSource could not be resolved in single-DB mode."
                + " candidates=" + Arrays.toString(dsNames)
                + ", configure " + LOAD_DS_PREFIX + "<dbId>=<beanName> in "
                + FLEXDBLINK_PROPERTIES + " if needed.");
    }

    /**
     * Resolve DataSource for multi-DB mode from configured dbId-to-bean mapping.
     *
     * @param ac application context.
     * @param dbId dbId from test input folder.
     * @param dsBeanNamesByDbId configured mapping.
     * @return selected DataSource.
     */
    DataSource resolveDataSourceByDbId(ApplicationContext ac, String dbId,
            Map<String, String> dsBeanNamesByDbId) {
        String normalizedDbId = normalizeDbIdKey(dbId);
        String beanName = dsBeanNamesByDbId.get(normalizedDbId);
        if (StringUtils.isBlank(beanName)) {
            throw new IllegalStateException("DataSource mapping was not found for dbId=" + dbId
                    + ". Define " + LOAD_DS_PREFIX + dbId.toLowerCase(Locale.ROOT)
                    + "=<beanName> in " + FLEXDBLINK_PROPERTIES + ".");
        }
        return resolveDataSourceByBeanName(ac, dbId, beanName);
    }

    /**
     * Resolve DataSource bean by explicit bean name.
     *
     * @param ac application context.
     * @param dbId dbId for error context.
     * @param beanName data source bean name.
     * @return resolved DataSource.
     */
    DataSource resolveDataSourceByBeanName(ApplicationContext ac, String dbId, String beanName) {
        try {
            return ac.getBean(beanName, DataSource.class);
        } catch (Exception e) {
            throw new IllegalStateException("Configured DataSource bean was not found. dbId=" + dbId
                    + ", beanName=" + beanName, e);
        }
    }

    /**
     * Load {@code flexdblink.properties} files from classpath and merge them (last-write-wins).
     *
     * @param cl class loader.
     * @return merged properties.
     * @throws Exception on load failure.
     */
    Properties loadFlexDbLinkProperties(ClassLoader cl) throws Exception {
        Properties props = new Properties();
        List<URL> urls = new ArrayList<>();
        var resources = cl.getResources(FLEXDBLINK_PROPERTIES);
        while (resources.hasMoreElements()) {
            urls.add(resources.nextElement());
        }
        urls.sort((a, b) -> a.toString().compareTo(b.toString()));
        for (URL url : urls) {
            try (InputStream in = url.openStream();
                    InputStreamReader reader = new InputStreamReader(in, java.nio.charset.StandardCharsets.UTF_8)) {
                props.load(reader);
                log.info("Loaded {}: {}", FLEXDBLINK_PROPERTIES, url);
            }
        }
        return props;
    }

    /**
     * Parse dbId-to-DataSource mapping from {@code flexdblink.properties}.
     *
     * @param props loaded properties.
     * @return normalized dbId to bean name mapping.
     */
    Map<String, String> readConfiguredDataSourceBeanNames(Properties props) {
        Map<String, String> result = new LinkedHashMap<>();
        for (String key : props.stringPropertyNames()) {
            if (!key.startsWith(LOAD_DS_PREFIX)) {
                continue;
            }
            String rawDbId = key.substring(LOAD_DS_PREFIX.length());
            String normalizedDbId = normalizeDbIdKey(rawDbId);
            if (StringUtils.isBlank(normalizedDbId)) {
                throw new IllegalStateException("Invalid DataSource mapping key: " + key);
            }
            String beanName = props.getProperty(key);
            if (StringUtils.isBlank(beanName)) {
                throw new IllegalStateException(
                        "DataSource bean name must not be blank. key=" + key);
            }
            String existing = result.get(normalizedDbId);
            if (existing != null && !existing.equals(beanName.trim())) {
                throw new IllegalStateException("Conflicting DataSource mappings for dbId="
                        + rawDbId + ": " + existing + " vs " + beanName);
            }
            result.put(normalizedDbId, beanName.trim());
        }
        return result;
    }

    /**
     * Index dbIds by lowercase normalized key and detect case-insensitive duplicates.
     *
     * @param dbIds raw dbIds.
     * @return normalized key to original dbId.
     */
    Map<String, String> indexDbIdsByNormalized(Set<String> dbIds) {
        Map<String, String> indexed = new LinkedHashMap<>();
        for (String dbId : dbIds) {
            String normalized = normalizeDbIdKey(dbId);
            if (indexed.containsKey(normalized)) {
                throw new IllegalStateException("Duplicate dbId folders ignoring case: "
                        + indexed.get(normalized) + ", " + dbId);
            }
            indexed.put(normalized, dbId);
        }
        return indexed;
    }

    /**
     * Normalize dbId key for case-insensitive matching.
     *
     * @param dbId raw dbId.
     * @return normalized dbId.
     */
    String normalizeDbIdKey(String dbId) {
        if (dbId == null) {
            return "";
        }
        return dbId.trim().toLowerCase(Locale.ROOT);
    }

    /**
     * Pick a single primary DataSource from all DataSource beans.
     *
     * @param ac application context.
     * @return primary DataSource wrapper or {@code null}.
     */
    NamedDs pickPrimaryFromAll(ApplicationContext ac) {
        String[] dsNames = ac.getBeanNamesForType(DataSource.class);
        List<NamedDs> all = new ArrayList<>();
        for (String dsName : dsNames) {
            DataSource ds = ac.getBean(dsName, DataSource.class);
            all.add(new NamedDs(dsName, ds));
        }
        return pickPrimary(ac, all);
    }

    /**
     * Resolve DS/TM pair for a given dbId based on actual connection metadata and types.
     *
     * @param ac application context.
     * @param dbId DB identifier.
     * @return resolved components.
     */
    Components resolveComponentsForDbId(ApplicationContext ac, String dbId) {
        ConnectionConfig.Entry entry = trc.buildEntryFromProps(dbId);
        String expectedUrl = entry.getUrl();
        String expectedUser = entry.getUser();

        // Decide from DS side
        List<NamedDs> dsMatches = findDataSourceByMetadata(ac, expectedUrl, expectedUser);
        if (dsMatches.isEmpty()) {
            String[] all = ac.getBeanNamesForType(DataSource.class);
            throw new IllegalStateException("No matching DataSource was found for dbId=" + dbId
                    + ". expectedUrl=" + expectedUrl + ", expectedUser=" + expectedUser
                    + " candidates=" + Arrays.toString(all));
        }
        NamedDs selectedDs = selectDataSourceForDbId(ac, dbId, dsMatches);
        PlatformTransactionManager tm = resolveTxManagerByDataSource(ac, dbId, selectedDs.ds);
        return new Components(selectedDs.ds, tm);
    }

    /**
     * Select a single DataSource candidate for a logical DB id.
     *
     * <p>
     * Selection order:
     * </p>
     * <ol>
     * <li>Single metadata match.</li>
     * <li>Single bean name containing normalized dbId.</li>
     * <li>Single {@code @Primary} bean among candidates.</li>
     * <li>Single candidate referenced by any {@link DataSourceTransactionManager}.</li>
     * </ol>
     *
     * @param ac application context.
     * @param dbId DB identifier.
     * @param dsMatches metadata-matched DataSource candidates.
     * @return selected DataSource.
     */
    NamedDs selectDataSourceForDbId(ApplicationContext ac, String dbId, List<NamedDs> dsMatches) {
        if (dsMatches.size() == 1) {
            return dsMatches.get(0);
        }

        // Prefer bean-name affinity to dbId
        NamedDs byName = pickSingleByDbIdName(dsMatches, dbId);
        if (byName != null) {
            log.info("Adopted dbId-affinity DataSource. dbId={}, ds={}", dbId, byName.name);
            return byName;
        }

        // Prefer single @Primary DS
        NamedDs primary = pickPrimary(ac, dsMatches);
        if (primary != null) {
            log.info("Adopted @Primary DataSource. dbId={}, ds={}", dbId, primary.name);
            return primary;
        }

        // If exactly one DS is referenced by TMs, adopt it
        NamedDs referencedSingle = pickSingleReferencedByTm(ac, dsMatches);
        if (referencedSingle != null) {
            log.info("Adopted the only DS referenced by a TM. dbId={}, ds={}", dbId,
                    referencedSingle.name);
            return referencedSingle;
        }

        // Still ambiguous
        List<String> names = new ArrayList<>();
        for (NamedDs n : dsMatches) {
            names.add(n.name);
        }
        throw new IllegalStateException("Multiple matching DataSources were found for dbId=" + dbId
                + ". candidates=" + names);
    }

    /**
     * Return the only DataSource whose bean name includes the logical DB identifier.
     *
     * @param candidates DataSource candidates.
     * @param dbId logical DB identifier.
     * @return matched candidate or {@code null} when none or multiple match.
     */
    NamedDs pickSingleByDbIdName(List<NamedDs> candidates, String dbId) {
        String normalizedDbId = normalizeForAffinityMatch(dbId);
        if (StringUtils.isBlank(normalizedDbId)) {
            return null;
        }

        NamedDs found = null;
        for (NamedDs candidate : candidates) {
            String normalizedBeanName = normalizeForAffinityMatch(candidate.name);
            if (normalizedBeanName.contains(normalizedDbId)) {
                if (found != null) {
                    return null;
                }
                found = candidate;
            }
        }
        return found;
    }

    /**
     * Normalize names for affinity matching.
     *
     * @param value raw value.
     * @return lowercase alphanumeric token.
     */
    String normalizeForAffinityMatch(String value) {
        if (value == null) {
            return "";
        }
        String lower = value.toLowerCase(Locale.ROOT);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < lower.length(); i++) {
            char ch = lower.charAt(i);
            if (Character.isLetterOrDigit(ch)) {
                sb.append(ch);
            }
        }
        return sb.toString();
    }

    /**
     * Find TMs whose DS metadata matches URL/USER.
     *
     * @param ac application context.
     * @param expectedUrl expected URL.
     * @param expectedUser expected USER.
     * @return matching TMs.
     */
    List<TmWithDs> findTmByMetadata(ApplicationContext ac, String expectedUrl,
            String expectedUser) {
        List<TmWithDs> result = new ArrayList<>();
        String[] tmNames = ac.getBeanNamesForType(PlatformTransactionManager.class);
        for (String tmName : tmNames) {
            PlatformTransactionManager tm = ac.getBean(tmName, PlatformTransactionManager.class);
            if (tm instanceof DataSourceTransactionManager) {
                DataSource ds = ((DataSourceTransactionManager) tm).getDataSource();
                if (ds == null) {
                    continue;
                }
                ProbeMeta meta = probeDataSourceMeta(ds);
                if (meta == null) {
                    continue;
                }
                if (urlRoughMatch(expectedUrl, meta.url)
                        && equalsIgnoreCaseSafe(expectedUser, meta.user)) {
                    String dsName = findBeanNameByInstance(ac, DataSource.class, ds);
                    result.add(new TmWithDs(tmName, dsName, tm, ds));
                }
            }
        }
        return result;
    }

    /**
     * Find DS that match URL/USER.
     *
     * @param ac application context.
     * @param expectedUrl expected URL.
     * @param expectedUser expected USER.
     * @return matching DS list.
     */
    List<NamedDs> findDataSourceByMetadata(ApplicationContext ac, String expectedUrl,
            String expectedUser) {
        List<NamedDs> result = new ArrayList<>();
        String[] dsNames = ac.getBeanNamesForType(DataSource.class);
        for (String name : dsNames) {
            DataSource ds = ac.getBean(name, DataSource.class);
            ProbeMeta meta = probeDataSourceMeta(ds);
            if (meta == null) {
                continue;
            }
            if (urlRoughMatch(expectedUrl, meta.url)
                    && equalsIgnoreCaseSafe(expectedUser, meta.user)) {
                result.add(new NamedDs(name, ds));
            }
        }
        return result;
    }

    /**
     * Return single {@code @Primary} DS among candidates, if present.
     *
     * @param ac application context.
     * @param candidates DS candidates.
     * @return single primary DS or {@code null}.
     */
    NamedDs pickPrimary(ApplicationContext ac, List<NamedDs> candidates) {
        if (ac instanceof ConfigurableApplicationContext) {
            ConfigurableListableBeanFactory bf =
                    ((ConfigurableApplicationContext) ac).getBeanFactory();
            NamedDs found = null;
            for (NamedDs n : candidates) {
                if (bf.containsBeanDefinition(n.name)) {
                    BeanDefinition bd = bf.getBeanDefinition(n.name);
                    if (bd.isPrimary()) {
                        if (found != null) {
                            // Multiple primaries → treat as ambiguous
                            return null;
                        }
                        found = n;
                    }
                }
            }
            return found;
        }
        return null;
    }

    /**
     * Among DS candidates, return the only DS referenced by any TM.
     *
     * @param ac application context.
     * @param candidates DS candidates.
     * @return single referenced DS or {@code null}.
     */
    NamedDs pickSingleReferencedByTm(ApplicationContext ac, List<NamedDs> candidates) {
        String[] tmNames = ac.getBeanNamesForType(PlatformTransactionManager.class);
        NamedDs only = null;
        for (NamedDs n : candidates) {
            boolean referenced = false;
            for (String tmName : tmNames) {
                PlatformTransactionManager tm =
                        ac.getBean(tmName, PlatformTransactionManager.class);
                if (tm instanceof DataSourceTransactionManager) {
                    DataSource tmDs = ((DataSourceTransactionManager) tm).getDataSource();
                    if (tmDs == n.ds) {
                        referenced = true;
                        break;
                    }
                }
            }
            if (referenced) {
                if (only != null) {
                    // More than one referenced → ambiguous
                    return null;
                }
                only = n;
            }
        }
        return only;
    }

    /**
     * Identify the single {@link DataSourceTransactionManager} that manages the given DS (by
     * reference equality).
     *
     * @param ac application context.
     * @param dbId DB identifier (for logs).
     * @param ds target DS.
     * @return PlatformTransactionManager.
     */
    PlatformTransactionManager resolveTxManagerByDataSource(ApplicationContext ac, String dbId,
            DataSource ds) {
        String[] tmNames = ac.getBeanNamesForType(PlatformTransactionManager.class);
        List<PlatformTransactionManager> uniqueMatches = new ArrayList<>();
        Map<PlatformTransactionManager, List<String>> aliasesByManager = new IdentityHashMap<>();

        for (String name : tmNames) {
            PlatformTransactionManager tm = ac.getBean(name, PlatformTransactionManager.class);
            if (tm instanceof DataSourceTransactionManager) {
                DataSource tmDs = ((DataSourceTransactionManager) tm).getDataSource();
                if (tmDs == ds) {
                    List<String> aliases = aliasesByManager.get(tm);
                    if (aliases == null) {
                        aliases = new ArrayList<>();
                        aliasesByManager.put(tm, aliases);
                        uniqueMatches.add(tm);
                    }
                    aliases.add(name);
                }
            }
        }

        if (uniqueMatches.isEmpty()) {
            throw new IllegalStateException("No TransactionManager for DataSource. dbId=" + dbId
                    + ", candidates=" + Arrays.toString(tmNames));
        }
        if (uniqueMatches.size() > 1) {
            List<String> hits = new ArrayList<>();
            for (PlatformTransactionManager manager : uniqueMatches) {
                List<String> aliases = aliasesByManager.get(manager);
                if (aliases.size() == 1) {
                    hits.add(aliases.get(0));
                } else {
                    hits.add(aliases.get(0) + "(aliases=" + aliases + ")");
                }
            }
            throw new IllegalStateException("Multiple TransactionManagers for DataSource. dbId="
                    + dbId + ", candidates=" + hits);
        }

        PlatformTransactionManager matched = uniqueMatches.get(0);
        List<String> aliases = aliasesByManager.get(matched);
        if (aliases.size() > 1) {
            log.info("Resolved TransactionManager with aliases. dbId={}, aliases={}", dbId, aliases);
        }
        return matched;
    }

    /**
     * List subdirectory names (= DB IDs) directly under {@code input/}.
     *
     * @param baseInputDir scenario input directory.
     * @return set of subdirectory names.
     * @throws Exception on failure.
     */
    Set<String> listDbIdsFromFolder(Path baseInputDir) throws Exception {
        if (!Files.isDirectory(baseInputDir)) {
            return Collections.emptySet();
        }
        try {
            Set<String> result = new LinkedHashSet<>();
            try (var stream = Files.list(baseInputDir)) {
                stream.filter(Files::isDirectory).map(p -> p.getFileName().toString())
                        .forEach(result::add);
            }
            return result;
        } catch (Exception e) {
            throw new Exception("Failed to scan DB ID folders under 'input/': " + baseInputDir, e);
        }
    }

    /**
     * Get connection metadata (URL/USER) from a DataSource. Returns {@code null} on failure. Uses a
     * direct {@code getConnection()} and immediate close to avoid joining a TX.
     *
     * @param ds DataSource.
     * @return metadata or {@code null}.
     */
    ProbeMeta probeDataSourceMeta(DataSource ds) {
        try (Connection c = ds.getConnection()) {
            String url = (c.getMetaData() != null) ? c.getMetaData().getURL() : null;
            String user = (c.getMetaData() != null) ? c.getMetaData().getUserName() : null;
            return new ProbeMeta(url, user);
        } catch (Exception e) {
            log.debug("Failed to obtain DataSource metadata.", e);
            return null;
        }
    }

    /**
     * Simplify URL and check partial containment (strip query/semicolon and lowercase).
     *
     * @param expected expected URL.
     * @param actual actual URL.
     * @return whether they roughly match.
     */
    boolean urlRoughMatch(String expected, String actual) {
        String e = simplifyUrl(expected);
        String a = simplifyUrl(actual);
        if (e.isEmpty() || a.isEmpty()) {
            return false;
        }
        return a.contains(e) || e.contains(a);
    }

    /**
     * Strip query/semicolon and lowercase.
     *
     * @param url URL.
     * @return normalized URL.
     */
    String simplifyUrl(String url) {
        if (url == null) {
            return "";
        }
        String s = url;
        int q = s.indexOf('?');
        if (q >= 0) {
            s = s.substring(0, q);
        }
        int sc = s.indexOf(';');
        if (sc >= 0) {
            s = s.substring(0, sc);
        }
        return s.toLowerCase(Locale.ROOT).trim();
    }

    /**
     * Null-safe, case-insensitive equality.
     *
     * @param a string A.
     * @param b string B.
     * @return equality result.
     */
    boolean equalsIgnoreCaseSafe(String a, String b) {
        if (a == null || b == null) {
            return false;
        }
        return a.equalsIgnoreCase(b);
    }

    /**
     * Resolve bean name by instance (reference equality).
     *
     * @param ac application context.
     * @param type bean type.
     * @param instance bean instance.
     * @param <T> type parameter.
     * @return bean name or {@code "<unknown>"}.
     */
    <T> String findBeanNameByInstance(ApplicationContext ac, Class<T> type, T instance) {
        String[] names = ac.getBeanNamesForType(type);
        for (String name : names) {
            T bean = ac.getBean(name, type);
            if (bean == instance) {
                return name;
            }
        }
        return "<unknown>";
    }

    /**
     * Record for a test transaction started by this extension.
     */
    static final class TxRecord {
        final String key;
        final PlatformTransactionManager tm;
        final TransactionStatus status;

        TxRecord(String key, PlatformTransactionManager tm, TransactionStatus status) {
            this.key = key;
            this.tm = tm;
            this.status = status;
        }
    }

    /**
     * Pair of DS/TM.
     */
    static final class Components {
        final DataSource dataSource;
        final PlatformTransactionManager txManager;

        Components(DataSource dataSource, PlatformTransactionManager txManager) {
            this.dataSource = dataSource;
            this.txManager = txManager;
        }
    }

    /**
     * Pair of TM and DS names/instances.
     */
    static final class TmWithDs {
        final String tmName;
        final String dsName;
        final PlatformTransactionManager tm;
        final DataSource ds;

        TmWithDs(String tmName, String dsName, PlatformTransactionManager tm, DataSource ds) {
            this.tmName = tmName;
            this.dsName = dsName;
            this.tm = tm;
            this.ds = ds;
        }
    }

    /**
     * Named DataSource wrapper.
     */
    static final class NamedDs {
        final String name;
        final DataSource ds;

        NamedDs(String name, DataSource ds) {
            this.name = name;
            this.ds = ds;
        }
    }

    /**
     * Get or create transaction records list.
     *
     * @param context execution context.
     * @return records list.
     */
    private List<TxRecord> getOrCreateTxRecords(ExtensionContext context) {
        Store store = context.getStore(NS);
        TxRecordList list = store.getOrComputeIfAbsent(STORE_KEY_TX, k -> new TxRecordList(),
                TxRecordList.class);
        return list;
    }

    /**
     * Get transaction records list started by this extension (empty if none).
     *
     * @param context execution context.
     * @return records list.
     */
    List<TxRecord> getTxRecords(ExtensionContext context) {
        Store store = context.getStore(NS);
        TxRecordList list = store.get(STORE_KEY_TX, TxRecordList.class);
        return (list != null) ? list : new TxRecordList();
    }

    /**
     * Type-safe list container for TxRecord.
     */
    static final class TxRecordList extends ArrayList<TxRecord> {
        private static final long serialVersionUID = 1L;
    }

    /**
     * Internal container for DS connection metadata (URL and USER).
     */
    static final class ProbeMeta {
        final String url;
        final String user;

        ProbeMeta(String url, String user) {
            this.url = url;
            this.user = user;
        }
    }
}
