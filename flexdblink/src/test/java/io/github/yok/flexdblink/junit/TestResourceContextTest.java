package io.github.yok.flexdblink.junit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import com.google.common.base.Splitter;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.support.TransactionSynchronizationManager;

class TestResourceContextTest {

    @TempDir
    Path tempDir;

    @Test
    void init_異常ケース_nullを指定する_NullPointerExceptionが送出されること() {
        assertThrows(NullPointerException.class, () -> TestResourceContext.init(null));
    }

    @Test
    void expectedDir_正常ケース_シナリオ文字列を指定する_expected配下パスが返ること() throws Exception {
        TestResourceContext trc = new TestResourceContext(tempDir, new Properties());
        Path path = trc.expectedDir("scn", "db1");
        assertEquals(tempDir.resolve("scn").resolve("expected").resolve("db1").toAbsolutePath()
                .normalize(), path);
    }

    @Test
    void baseExpectedDir_正常ケース_シナリオ文字列を指定する_expected配下パスが返ること() throws Exception {
        TestResourceContext trc = new TestResourceContext(tempDir, new Properties());
        Path path = trc.baseExpectedDir("scn");
        assertEquals(tempDir.resolve("scn").resolve("expected").toAbsolutePath().normalize(), path);
    }

    @Test
    void expectedDir_正常ケース_シナリオ空文字を指定する_classRoot配下パスが返ること() throws Exception {
        TestResourceContext trc = new TestResourceContext(tempDir, new Properties());
        Path path = trc.expectedDir("", "db1");
        assertEquals(tempDir.resolve("expected").resolve("db1").toAbsolutePath().normalize(), path);
    }

    @Test
    void springManagedDataSource_正常ケース_非DataSourceキーのみを指定する_空Optionalが返ること() throws Exception {
        TestResourceContext trc = new TestResourceContext(tempDir, new Properties());
        Map<Object, Object> map = new LinkedHashMap<>();
        map.put("not-ds", "x");
        try (MockedStatic<TransactionSynchronizationManager> mocked =
                mockStatic(TransactionSynchronizationManager.class)) {
            mocked.when(TransactionSynchronizationManager::getResourceMap).thenReturn(map);
            Optional<DataSource> out = trc.springManagedDataSource();
            assertTrue(out.isEmpty());
        }
    }

    @Test
    void buildEntryFromProps_異常ケース_デフォルトDBで必須項目不足を指定する_IllegalStateExceptionが送出されること()
            throws Exception {
        TestResourceContext trc = new TestResourceContext(tempDir, new Properties());
        IllegalStateException ex =
                assertThrows(IllegalStateException.class, () -> trc.buildEntryFromProps(" "));
        assertTrue(ex.getMessage().contains("<default>"));
    }

    @Test
    void buildEntryFromProps_異常ケース_指定DBで必須項目不足を指定する_IllegalStateExceptionが送出されること()
            throws Exception {
        TestResourceContext trc = new TestResourceContext(tempDir, new Properties());
        IllegalStateException ex =
                assertThrows(IllegalStateException.class, () -> trc.buildEntryFromProps("db1"));
        assertTrue(ex.getMessage().contains("DB=db1"));
    }

    @Test
    void resolvePropertyForDb_正常ケース_非文字列エントリを含める_有効キーの値が返ること() throws Exception {
        TestResourceContext trc = new TestResourceContext(tempDir, new Properties());
        Properties props = new Properties();
        props.setProperty("spring.datasource.aaa.url", "jdbc:a");
        props.put(100, "invalid");
        props.put("spring.datasource.aaa.username", 200);

        String out = trc.resolvePropertyForDb(props, "aaa", "url");
        assertEquals("jdbc:a", out);
    }

    @Test
    void scoreKeyWithSegments_正常ケース_デフォルトDBでspringDatasource形式を指定する_加点されること() throws Exception {
        TestResourceContext trc = new TestResourceContext(tempDir, new Properties());
        int score = trc.scoreKeyWithSegments("spring.datasource.url", new String[0], "url");
        assertTrue(score > 50);
    }

    @Test
    void scoreKeyWithSegments_正常ケース_DB一致かつfirstIdx1を指定する_一致優先スコアとなること() throws Exception {
        TestResourceContext trc = new TestResourceContext(tempDir, new Properties());
        int score =
                trc.scoreKeyWithSegments("spring.foo.datasource.url", new String[] {"foo"}, "url");
        assertTrue(score >= 100);
    }

    @Test
    void scoreKeyWithSegments_正常ケース_DB一致かつfirstIdx2を指定する_一致優先スコアとなること() throws Exception {
        TestResourceContext trc = new TestResourceContext(tempDir, new Properties());
        int score =
                trc.scoreKeyWithSegments("spring.datasource.foo.url", new String[] {"foo"}, "url");
        assertTrue(score >= 100);
    }

    @Test
    void scoreKeyWithSegments_正常ケース_DB不一致かつspringDatasource形式を指定する_非一致分岐が実行されること()
            throws Exception {
        TestResourceContext trc = new TestResourceContext(tempDir, new Properties());
        int score = trc.scoreKeyWithSegments("spring.datasource.other.url", new String[] {"foo"},
                "url");
        assertTrue(score >= 0);
    }

    @Test
    void scoreKeyWithSegments_正常ケース_DB未指定かつspringDatasource以外を指定する_デフォルト加点のみとなること()
            throws Exception {
        TestResourceContext trc = new TestResourceContext(tempDir, new Properties());
        int score = trc.scoreKeyWithSegments("x.url", new String[0], "url");
        assertTrue(score >= 50);
    }

    @Test
    void scoreKeyWithSegments_正常ケース_DB一致かつfirstIdx1でdatasource不一致を指定する_条件分岐が実行されること()
            throws Exception {
        TestResourceContext trc = new TestResourceContext(tempDir, new Properties());
        int score = trc.scoreKeyWithSegments("spring.foo.bar.url", new String[] {"foo"}, "url");
        assertTrue(score >= 100);
    }

    @Test
    void scoreKeyWithSegments_正常ケース_DB一致かつfirstIdx2で不一致を指定する_条件分岐が実行されること() throws Exception {
        TestResourceContext trc = new TestResourceContext(tempDir, new Properties());
        int score = trc.scoreKeyWithSegments("spring.datasource.x.foo.url", new String[] {"foo"},
                "url");
        assertTrue(score >= 100);
    }

    @Test
    void scoreKeyWithSegments_正常ケース_DB不一致かつspringDatasource以外を指定する_非一致分岐が実行されること()
            throws Exception {
        TestResourceContext trc = new TestResourceContext(tempDir, new Properties());
        int score = trc.scoreKeyWithSegments("abc.def.url", new String[] {"foo"}, "url");
        assertTrue(score >= 0);
    }

    @Test
    void scoreKeyWithSegments_正常ケース_DB未指定でspring以外を指定する_条件分岐の逆側が実行されること() throws Exception {
        TestResourceContext trc = new TestResourceContext(tempDir, new Properties());
        int score = trc.scoreKeyWithSegments("x.datasource.url", new String[0], "url");
        assertTrue(score >= 50);
    }

    @Test
    void scoreKeyWithSegments_正常ケース_DB未指定でspring配下かつdatasource不一致を指定する_分岐の逆側が実行されること()
            throws Exception {
        TestResourceContext trc = new TestResourceContext(tempDir, new Properties());
        int score = trc.scoreKeyWithSegments("spring.foo.url", new String[0], "url");
        assertTrue(score >= 50);
    }

    @Test
    void scoreKeyWithSegments_正常ケース_DB一致でトークン数不足を指定する_条件分岐の逆側が実行されること() throws Exception {
        TestResourceContext trc = new TestResourceContext(tempDir, new Properties());
        int score = trc.scoreKeyWithSegments("foo.url", new String[] {"foo"}, "url");
        assertTrue(score >= 100);
    }

    @Test
    void scoreKeyWithSegments_正常ケース_DB一致かつspring配下だがdatasource不一致を指定する_条件分岐の逆側が実行されること()
            throws Exception {
        TestResourceContext trc = new TestResourceContext(tempDir, new Properties());
        int score = trc.scoreKeyWithSegments("spring.foo.bar.url", new String[] {"foo"}, "url");
        assertTrue(score >= 100);
    }

    @Test
    void scoreKeyWithSegments_正常ケース_DB不一致かつspring配下でdatasource不一致を指定する_条件分岐の逆側が実行されること()
            throws Exception {
        TestResourceContext trc = new TestResourceContext(tempDir, new Properties());
        int score = trc.scoreKeyWithSegments("spring.x.y.url", new String[] {"foo"}, "url");
        assertTrue(score >= 0);
    }

    @Test
    void scoreKeyWithSegments_正常ケース_DB一致かつspring以外を指定する_if条件の逆側が実行されること() throws Exception {
        TestResourceContext trc = new TestResourceContext(tempDir, new Properties());
        int score = trc.scoreKeyWithSegments("x.foo.datasource.url", new String[] {"foo"}, "url");
        assertTrue(score >= 100);
    }

    @Test
    void scoreKeyWithSegments_正常ケース_DB不一致かつspring配下でdatasource不一致を指定する_else条件の逆側が実行されること()
            throws Exception {
        TestResourceContext trc = new TestResourceContext(tempDir, new Properties());
        int score = trc.scoreKeyWithSegments("spring.x.url", new String[] {"foo"}, "url");
        assertTrue(score >= 0);
    }

    @Test
    void loadAllApplicationProperties_正常ケース_プロファイル混在のリソースを指定する_アクティブ値が優先されること() throws Exception {
        Path cp = tempDir.resolve("cp");
        Files.createDirectories(cp);
        Files.writeString(cp.resolve("application.properties"),
                "spring.profiles.active=dev\nmerge.order=base\n", StandardCharsets.UTF_8);
        Files.writeString(cp.resolve("application-dev.properties"), "merge.order=dev\n",
                StandardCharsets.UTF_8);
        Files.writeString(cp.resolve("application-other.properties"), "merge.order=other\n",
                StandardCharsets.UTF_8);
        Files.writeString(cp.resolve("application-.properties"), "ignored=true\n",
                StandardCharsets.UTF_8);
        Files.writeString(cp.resolve("application-dev.txt"), "ignored", StandardCharsets.UTF_8);
        Files.writeString(cp.resolve("application-extra.yaml"), "yaml.key: v\n",
                StandardCharsets.UTF_8);
        Files.writeString(cp.resolve("application-yml.yml"), "yml.key: value\n",
                StandardCharsets.UTF_8);

        try (URLClassLoader cl = new URLClassLoader(new URL[] {cp.toUri().toURL()}, null)) {
            Properties merged = TestResourceContext.loadAllApplicationProperties(cl);
            assertEquals("dev", merged.getProperty("merge.order"));
        }
    }

    @Test
    void loadAllApplicationProperties_正常ケース_システムプロパティプレースホルダを指定する_接続情報が展開されること() throws Exception {
        Path cp = tempDir.resolve("cp_placeholder");
        Files.createDirectories(cp);
        Files.writeString(cp.resolve("application.properties"),
                "spring.datasource.operator.bbb.url=${DB_BBB_URL}\n"
                        + "spring.datasource.operator.bbb.username=${DB_BBB_USER}\n",
                StandardCharsets.UTF_8);
        System.setProperty("DB_BBB_URL", "jdbc:oracle:thin:@localhost:11521/BBB");
        System.setProperty("DB_BBB_USER", "BBB_USER");
        try (URLClassLoader cl = new URLClassLoader(new URL[] {cp.toUri().toURL()}, null)) {
            Properties merged = TestResourceContext.loadAllApplicationProperties(cl);
            assertEquals("jdbc:oracle:thin:@localhost:11521/BBB",
                    merged.getProperty("spring.datasource.operator.bbb.url"));
            assertEquals("BBB_USER", merged.getProperty("spring.datasource.operator.bbb.username"));
        } finally {
            System.clearProperty("DB_BBB_URL");
            System.clearProperty("DB_BBB_USER");
        }
    }

    @Test
    void loadAllApplicationProperties_正常ケース_ベース設定3形式を指定する_全形式のキーが読み込まれること() throws Exception {
        Path cp = tempDir.resolve("cp_base_all_formats");
        Files.createDirectories(cp);
        Files.writeString(cp.resolve("application.properties"), "base.properties.key=prop\n",
                StandardCharsets.UTF_8);
        Files.writeString(cp.resolve("application.yml"), "base.yml.key: yml\n",
                StandardCharsets.UTF_8);
        Files.writeString(cp.resolve("application.yaml"), "base.yaml.key: yaml\n",
                StandardCharsets.UTF_8);

        try (URLClassLoader cl = new URLClassLoader(new URL[] {cp.toUri().toURL()}, null)) {
            Properties merged = TestResourceContext.loadAllApplicationProperties(cl);
            assertEquals("prop", merged.getProperty("base.properties.key"));
            assertEquals("yml", merged.getProperty("base.yml.key"));
            assertEquals("yaml", merged.getProperty("base.yaml.key"));
        }
    }

    @Test
    void loadAllApplicationProperties_正常ケース_プロファイル設定3形式を指定する_全形式のキーが読み込まれること() throws Exception {
        Path cp = tempDir.resolve("cp_profile_all_formats");
        Files.createDirectories(cp);
        Files.writeString(cp.resolve("application.properties"), "spring.profiles.active=dev\n",
                StandardCharsets.UTF_8);
        Files.writeString(cp.resolve("application-dev.properties"), "profile.properties.key=prop\n",
                StandardCharsets.UTF_8);
        Files.writeString(cp.resolve("application-dev.yml"), "profile.yml.key: yml\n",
                StandardCharsets.UTF_8);
        Files.writeString(cp.resolve("application-dev.yaml"), "profile.yaml.key: yaml\n",
                StandardCharsets.UTF_8);

        try (URLClassLoader cl = new URLClassLoader(new URL[] {cp.toUri().toURL()}, null)) {
            Properties merged = TestResourceContext.loadAllApplicationProperties(cl);
            assertEquals("prop", merged.getProperty("profile.properties.key"));
            assertEquals("yml", merged.getProperty("profile.yml.key"));
            assertEquals("yaml", merged.getProperty("profile.yaml.key"));
        }
    }

    @Test
    void loadAllApplicationProperties_正常ケース_srcMainResources配下プロファイルを指定する_アクティブ値が優先されること()
            throws Exception {
        Path cp = tempDir.resolve("cp_src_main_resources");
        Path srcMainResources = cp.resolve("src").resolve("main").resolve("resources");
        Files.createDirectories(srcMainResources);
        Files.writeString(cp.resolve("application.properties"),
                "spring.profiles.active=development\nmerge.order=base\n", StandardCharsets.UTF_8);
        Files.writeString(srcMainResources.resolve("application-development.properties"),
                "merge.order=development\n", StandardCharsets.UTF_8);

        try (URLClassLoader cl = new URLClassLoader(new URL[] {cp.toUri().toURL()}, null)) {
            Properties merged = TestResourceContext.loadAllApplicationProperties(cl);
            assertEquals("development", merged.getProperty("merge.order"));
        }
    }

    @Test
    void loadAllApplicationProperties_正常ケース_srcMainResources配下ベース設定を指定する_ベース設定が読み込まれること()
            throws Exception {
        Path cp = tempDir.resolve("cp_src_main_resources_base");
        Path srcMainResources = cp.resolve("src").resolve("main").resolve("resources");
        Files.createDirectories(srcMainResources);
        Files.writeString(srcMainResources.resolve("application.yml"),
                "src.main.resources.base: enabled\n", StandardCharsets.UTF_8);

        try (URLClassLoader cl = new URLClassLoader(new URL[] {cp.toUri().toURL()}, null)) {
            Properties merged = TestResourceContext.loadAllApplicationProperties(cl);
            assertEquals("enabled", merged.getProperty("src.main.resources.base"));
        }
    }

    @Test
    void loadAllApplicationProperties_正常ケース_非application接頭辞を含むプロファイルURLを指定する_非application接頭辞が無視されること()
            throws Exception {
        Path cp = tempDir.resolve("cp_ignore_non_application_profile");
        Files.createDirectories(cp);
        Files.writeString(cp.resolve("application.properties"),
                "spring.profiles.active=dev\nmerge.order=base\n", StandardCharsets.UTF_8);
        Files.writeString(cp.resolve("application-dev.properties"), "merge.order=dev\n",
                StandardCharsets.UTF_8);
        Files.writeString(cp.resolve("notapplication-dev.properties"), "merge.order=invalid\n",
                StandardCharsets.UTF_8);

        try (URLClassLoader cl = new URLClassLoader(new URL[] {cp.toUri().toURL()}, null);
                MockedStatic<TestResourceContext> mocked =
                        mockStatic(TestResourceContext.class, CALLS_REAL_METHODS)) {
            URL baseUrl = cp.resolve("application.properties").toUri().toURL();
            URL nonApplicationUrl = cp.resolve("notapplication-dev.properties").toUri().toURL();
            URL applicationUrl = cp.resolve("application-dev.properties").toUri().toURL();
            mocked.when(() -> TestResourceContext.findAllBaseResources(cl))
                    .thenReturn(new ArrayList<>(List.of(baseUrl)));
            mocked.when(() -> TestResourceContext.findAllProfileResources(cl))
                    .thenReturn(new ArrayList<>(List.of(nonApplicationUrl, applicationUrl)));

            Properties merged = TestResourceContext.loadAllApplicationProperties(cl);
            assertEquals("dev", merged.getProperty("merge.order"));
        }
    }

    @Test
    void findAllProfileResources_正常ケース_重複URLを含む結果を指定する_重複を除外したURL一覧が返ること() throws Exception {
        URL duplicated = new URL("file:/tmp/application-dev.properties");
        URL yml = new URL("file:/tmp/application-dev.yml");
        Resource prop1 = mock(Resource.class);
        Resource prop2 = mock(Resource.class);
        Resource ymlResource = mock(Resource.class);
        when(prop1.getURL()).thenReturn(duplicated);
        when(prop2.getURL()).thenReturn(duplicated);
        when(ymlResource.getURL()).thenReturn(yml);

        try (MockedConstruction<PathMatchingResourcePatternResolver> mockedConstruction =
                mockConstruction(PathMatchingResourcePatternResolver.class, (resolver, context) -> {
                    when(resolver.getResources("classpath*:**/application-*.properties"))
                            .thenReturn(new Resource[] {prop1, prop2});
                    when(resolver.getResources("classpath*:**/application-*.yml"))
                            .thenReturn(new Resource[] {ymlResource});
                    when(resolver.getResources("classpath*:**/application-*.yaml"))
                            .thenReturn(new Resource[0]);
                })) {
            List<URL> urls = TestResourceContext
                    .findAllProfileResources(Thread.currentThread().getContextClassLoader());
            assertEquals(2, urls.size());
            assertEquals(duplicated, urls.get(0));
            assertEquals(yml, urls.get(1));
            assertEquals(1, mockedConstruction.constructed().size());
        }
    }

    @Test
    void toSimpleName_正常ケース_pathNullとスラッシュなしを指定する_パスに応じた値が返ること() throws Exception {
        URL nullPathUrl = mock(URL.class);
        when(nullPathUrl.getPath()).thenReturn(null);
        assertEquals("", TestResourceContext.toSimpleName(nullPathUrl));

        URL noSlashPathUrl = mock(URL.class);
        when(noSlashPathUrl.getPath()).thenReturn("application-dev.properties");
        assertEquals("application-dev.properties",
                TestResourceContext.toSimpleName(noSlashPathUrl));
    }

    @Test
    void resolvePlaceholders_正常ケース_システムプロパティ参照を指定する_値が展開されること() throws Exception {
        Properties props = new Properties();
        props.setProperty("spring.datasource.operator.bbb.url", "${DB_BBB_URL}");
        props.setProperty("spring.datasource.operator.bbb.username", "${DB_BBB_USER}");
        System.setProperty("DB_BBB_URL", "jdbc:oracle:thin:@localhost:11521/BBB");
        System.setProperty("DB_BBB_USER", "BBB_USER");
        try {
            TestResourceContext.resolvePlaceholders(props);
            assertEquals("jdbc:oracle:thin:@localhost:11521/BBB",
                    props.getProperty("spring.datasource.operator.bbb.url"));
            assertEquals("BBB_USER", props.getProperty("spring.datasource.operator.bbb.username"));
        } finally {
            System.clearProperty("DB_BBB_URL");
            System.clearProperty("DB_BBB_USER");
        }
    }

    @Test
    void resolvePlaceholders_正常ケース_同一Properties参照を指定する_値が展開されること() throws Exception {
        Properties props = new Properties();
        props.setProperty("DB_BBB_URL", "jdbc:oracle:thin:@localhost:11521/BBB");
        props.setProperty("spring.datasource.operator.bbb.url", "${DB_BBB_URL}");
        TestResourceContext.resolvePlaceholders(props);
        assertEquals("jdbc:oracle:thin:@localhost:11521/BBB",
                props.getProperty("spring.datasource.operator.bbb.url"));
    }

    @Test
    void resolvePlaceholders_正常ケース_未定義プレースホルダを指定する_プレースホルダ文字列が保持されること() throws Exception {
        Properties props = new Properties();
        props.setProperty("spring.datasource.operator.bbb.url", "${UNDEFINED_KEY}");
        TestResourceContext.resolvePlaceholders(props);
        assertEquals("${UNDEFINED_KEY}", props.getProperty("spring.datasource.operator.bbb.url"));
    }

    @Test
    void resolvePlaceholders_正常ケース_環境変数参照を指定する_環境変数の値が展開されること() throws Exception {
        String envName = "PATH";
        String envValue = System.getenv(envName);
        if (envValue == null) {
            Map<String, String> env = System.getenv();
            if (env.isEmpty()) {
                return;
            }
            envName = env.keySet().iterator().next();
            envValue = env.get(envName);
        }

        String systemValue = System.getProperty(envName);
        System.clearProperty(envName);
        try {
            Properties props = new Properties();
            props.setProperty("resolved.value", "${" + envName + "}");
            TestResourceContext.resolvePlaceholders(props);
            assertEquals(envValue, props.getProperty("resolved.value"));
        } finally {
            if (systemValue != null) {
                System.setProperty(envName, systemValue);
            }
        }
    }

    @Test
    void resolvePlaceholders_正常ケース_非文字列キーを指定する_例外なく処理が継続されること() throws Exception {
        Properties props = new Properties();
        props.put(100, "${DB_BBB_URL}");
        props.setProperty("spring.datasource.operator.bbb.url", "${DB_BBB_URL}");
        System.setProperty("DB_BBB_URL", "jdbc:oracle:thin:@localhost:11521/BBB");
        try {
            assertDoesNotThrow(() -> TestResourceContext.resolvePlaceholders(props));
            assertEquals("jdbc:oracle:thin:@localhost:11521/BBB",
                    props.getProperty("spring.datasource.operator.bbb.url"));
            assertEquals("${DB_BBB_URL}", props.get(100));
        } finally {
            System.clearProperty("DB_BBB_URL");
        }
    }

    @Test
    void resolvePlaceholders_正常ケース_反復上限0を指定する_プレースホルダ文字列が保持されること() throws Exception {
        Properties props = new Properties();
        props.setProperty("spring.datasource.operator.bbb.url", "${DB_BBB_URL}");
        System.setProperty("DB_BBB_URL", "jdbc:oracle:thin:@localhost:11521/BBB");
        try {
            TestResourceContext.resolvePlaceholders(props, 0);
            assertEquals("${DB_BBB_URL}", props.getProperty("spring.datasource.operator.bbb.url"));
        } finally {
            System.clearProperty("DB_BBB_URL");
        }
    }

    @Test
    void extractProfile_正常ケース_yaml拡張子を指定する_プロファイル名が返ること() throws Exception {
        String profile = TestResourceContext.extractProfile("application-dev.yaml");
        assertEquals("dev", profile);
    }

    @Test
    void extractProfile_正常ケース_対象外ファイル名を指定する_空文字が返ること() throws Exception {
        String profile = TestResourceContext.extractProfile("something.txt");
        assertEquals("", profile);
    }

    @Test
    void extractProfile_正常ケース_application接頭辞で対象外拡張子を指定する_空文字が返ること() throws Exception {
        String profile = TestResourceContext.extractProfile("application-dev.txt");
        assertEquals("", profile);
    }

    @Test
    void loadResourceToProps_正常ケース_yamlファイルを指定する_キーが読み込まれること() throws Exception {
        Path yaml = tempDir.resolve("app.yaml");
        Files.writeString(yaml, "a.b: c\n", StandardCharsets.UTF_8);
        Properties props = new Properties();
        TestResourceContext.loadResourceToProps(props, yaml.toUri().toURL());
        assertEquals("c", props.getProperty("a.b"));
    }

    @Test
    void loadResourceToProps_正常ケース_ymlファイルを指定する_キーが読み込まれること() throws Exception {
        Path yml = tempDir.resolve("app.yml");
        Files.writeString(yml, "m.n: z\n", StandardCharsets.UTF_8);
        Properties props = new Properties();
        TestResourceContext.loadResourceToProps(props, yml.toUri().toURL());
        assertEquals("z", props.getProperty("m.n"));
    }

    @Test
    void loadResourceToProps_正常ケース_対象外拡張子を指定する_読み込みされないこと() throws Exception {
        Path txt = tempDir.resolve("app.txt");
        Files.writeString(txt, "k=v\n", StandardCharsets.UTF_8);
        Properties props = new Properties();
        TestResourceContext.loadResourceToProps(props, txt.toUri().toURL());
        assertTrue(props.isEmpty());
    }

    @Test
    void loadYamlToProps_正常ケース_空yamlを指定する_例外が送出されないこと() throws Exception {
        Path yaml = tempDir.resolve("empty.yml");
        Files.writeString(yaml, "", StandardCharsets.UTF_8);
        Properties props = new Properties();
        assertDoesNotThrow(() -> TestResourceContext.loadYamlToProps(props, yaml.toUri().toURL()));
    }

    @Test
    void loadYamlToProps_正常ケース_factoryがnullを返す設定を行う_空のまま返ること() throws Exception {
        Path yaml = tempDir.resolve("n.yml");
        Files.writeString(yaml, "a: 1\n", StandardCharsets.UTF_8);
        Properties props = new Properties();
        try (MockedConstruction<YamlPropertiesFactoryBean> mocked =
                mockConstruction(YamlPropertiesFactoryBean.class,
                        (factory, context) -> when(factory.getObject()).thenReturn(null))) {
            TestResourceContext.loadYamlToProps(props, yaml.toUri().toURL());
            assertTrue(props.isEmpty());
            assertEquals(1, mocked.constructed().size());
        }
    }

    @Test
    void resolveActiveProfiles_正常ケース_blank判定のみを満たすトークンを指定する_空リストが返ること() throws Exception {
        String candidate = findBlankButNotEmptyToken();
        if (candidate == null) {
            return;
        }
        Properties p = new Properties();
        p.setProperty("spring.profiles.active", candidate);
        List<String> out = TestResourceContext.resolveActiveProfiles(p);
        assertTrue(out.isEmpty());
    }

    @Test
    void resolveActiveProfiles_正常ケース_isNotBlankを偽に差し替える_分岐の逆側が実行されること() throws Exception {
        Properties p = new Properties();
        p.setProperty("spring.profiles.active", "dev");
        try (MockedStatic<StringUtils> mocked = mockStatic(StringUtils.class, CALLS_REAL_METHODS)) {
            mocked.when(() -> StringUtils.isNotBlank("dev")).thenReturn(false);
            List<String> out = TestResourceContext.resolveActiveProfiles(p);
            assertTrue(out.isEmpty());
        }
    }

    @Test
    void resolveActiveProfiles_正常ケース_システムプロパティに複数プロファイルを指定する_システム値が優先されること() throws Exception {
        System.setProperty("spring.profiles.active", "qa;prod");
        try {
            Properties p = new Properties();
            p.setProperty("spring.profiles.active", "dev");
            List<String> out = TestResourceContext.resolveActiveProfiles(p);
            assertEquals(List.of("qa", "prod"), out);
        } finally {
            System.clearProperty("spring.profiles.active");
        }
    }

    @Test
    void springManagedConnection_正常ケース_DataSourceを指定する_Connectionが返ること() throws Exception {
        TestResourceContext trc = new TestResourceContext(tempDir, new Properties());
        DataSource ds = mock(DataSource.class);
        Connection conn = mock(Connection.class);
        try (MockedStatic<DataSourceUtils> mocked = mockStatic(DataSourceUtils.class)) {
            mocked.when(() -> DataSourceUtils.getConnection(ds)).thenReturn(conn);
            Optional<Connection> out = trc.springManagedConnection(Optional.of(ds));
            assertTrue(out.isPresent());
            assertEquals(conn, out.get());
        }
    }

    /**
     * Finds a single-character token that is blank (per {@link StringUtils#isNotBlank}) but not
     * empty after splitting by comma/semicolon delimiters. Used by
     * {@code resolveActiveProfiles_正常ケース_blank判定のみを満たすトークンを指定する_空リストが返ること} to exercise the
     * blank-token branch in profile resolution.
     *
     * @return a blank-but-non-empty token, or {@code null} if no such character exists
     */
    private static String findBlankButNotEmptyToken() {
        for (int c = 0; c <= Character.MAX_VALUE; c++) {
            char ch = (char) c;
            String token = String.valueOf(ch);
            if (token.isEmpty()) {
                continue;
            }
            if (!StringUtils.isNotBlank(token)) {
                Iterable<String> split =
                        Splitter.onPattern("[,;]").omitEmptyStrings().trimResults().split(token);
                for (String s : split) {
                    if (!s.isEmpty()) {
                        return token;
                    }
                }
            }
        }
        return null;
    }
}
