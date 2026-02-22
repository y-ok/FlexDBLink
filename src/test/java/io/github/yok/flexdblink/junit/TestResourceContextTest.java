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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
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
        TestResourceContext trc = newTrc(tempDir, new Properties());
        Path path = trc.expectedDir("scn", "db1");
        assertEquals(tempDir.resolve("scn").resolve("expected").resolve("db1").toAbsolutePath()
                .normalize(), path);
    }

    @Test
    void baseExpectedDir_正常ケース_シナリオ文字列を指定する_expected配下パスが返ること() throws Exception {
        TestResourceContext trc = newTrc(tempDir, new Properties());
        Path path = trc.baseExpectedDir("scn");
        assertEquals(tempDir.resolve("scn").resolve("expected").toAbsolutePath().normalize(), path);
    }

    @Test
    void expectedDir_正常ケース_シナリオ空文字を指定する_classRoot配下パスが返ること() throws Exception {
        TestResourceContext trc = newTrc(tempDir, new Properties());
        Path path = trc.expectedDir("", "db1");
        assertEquals(tempDir.resolve("expected").resolve("db1").toAbsolutePath().normalize(), path);
    }

    @Test
    void springManagedDataSource_正常ケース_非DataSourceキーのみを指定する_空Optionalが返ること() throws Exception {
        TestResourceContext trc = newTrc(tempDir, new Properties());
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
        TestResourceContext trc = newTrc(tempDir, new Properties());
        IllegalStateException ex =
                assertThrows(IllegalStateException.class, () -> trc.buildEntryFromProps(" "));
        assertTrue(ex.getMessage().contains("<default>"));
    }

    @Test
    void buildEntryFromProps_異常ケース_指定DBで必須項目不足を指定する_IllegalStateExceptionが送出されること()
            throws Exception {
        TestResourceContext trc = newTrc(tempDir, new Properties());
        IllegalStateException ex =
                assertThrows(IllegalStateException.class, () -> trc.buildEntryFromProps("db1"));
        assertTrue(ex.getMessage().contains("DB=db1"));
    }

    @Test
    void resolvePropertyForDb_正常ケース_非文字列エントリを含める_有効キーの値が返ること() throws Exception {
        TestResourceContext trc = newTrc(tempDir, new Properties());
        Properties props = new Properties();
        props.setProperty("spring.datasource.aaa.url", "jdbc:a");
        props.put(100, "invalid");
        props.put("spring.datasource.aaa.username", 200);

        String out = (String) invokePrivateInstance(trc, "resolvePropertyForDb",
                new Class[] {Properties.class, String.class, String.class},
                new Object[] {props, "aaa", "url"});
        assertEquals("jdbc:a", out);
    }

    @Test
    void scoreKeyWithSegments_正常ケース_デフォルトDBでspringDatasource形式を指定する_加点されること() throws Exception {
        TestResourceContext trc = newTrc(tempDir, new Properties());
        int score = (int) invokePrivateInstance(trc, "scoreKeyWithSegments",
                new Class[] {String.class, String[].class, String.class},
                new Object[] {"spring.datasource.url", new String[0], "url"});
        assertTrue(score > 50);
    }

    @Test
    void scoreKeyWithSegments_正常ケース_DB一致かつfirstIdx1を指定する_一致優先スコアとなること() throws Exception {
        TestResourceContext trc = newTrc(tempDir, new Properties());
        int score = (int) invokePrivateInstance(trc, "scoreKeyWithSegments",
                new Class[] {String.class, String[].class, String.class},
                new Object[] {"spring.foo.datasource.url", new String[] {"foo"}, "url"});
        assertTrue(score >= 100);
    }

    @Test
    void scoreKeyWithSegments_正常ケース_DB一致かつfirstIdx2を指定する_一致優先スコアとなること() throws Exception {
        TestResourceContext trc = newTrc(tempDir, new Properties());
        int score = (int) invokePrivateInstance(trc, "scoreKeyWithSegments",
                new Class[] {String.class, String[].class, String.class},
                new Object[] {"spring.datasource.foo.url", new String[] {"foo"}, "url"});
        assertTrue(score >= 100);
    }

    @Test
    void scoreKeyWithSegments_正常ケース_DB不一致かつspringDatasource形式を指定する_非一致分岐が実行されること()
            throws Exception {
        TestResourceContext trc = newTrc(tempDir, new Properties());
        int score = (int) invokePrivateInstance(trc, "scoreKeyWithSegments",
                new Class[] {String.class, String[].class, String.class},
                new Object[] {"spring.datasource.other.url", new String[] {"foo"}, "url"});
        assertTrue(score >= 0);
    }

    @Test
    void scoreKeyWithSegments_正常ケース_DB未指定かつspringDatasource以外を指定する_デフォルト加点のみとなること()
            throws Exception {
        TestResourceContext trc = newTrc(tempDir, new Properties());
        int score = (int) invokePrivateInstance(trc, "scoreKeyWithSegments",
                new Class[] {String.class, String[].class, String.class},
                new Object[] {"x.url", new String[0], "url"});
        assertTrue(score >= 50);
    }

    @Test
    void scoreKeyWithSegments_正常ケース_DB一致かつfirstIdx1でdatasource不一致を指定する_条件分岐が実行されること()
            throws Exception {
        TestResourceContext trc = newTrc(tempDir, new Properties());
        int score = (int) invokePrivateInstance(trc, "scoreKeyWithSegments",
                new Class[] {String.class, String[].class, String.class},
                new Object[] {"spring.foo.bar.url", new String[] {"foo"}, "url"});
        assertTrue(score >= 100);
    }

    @Test
    void scoreKeyWithSegments_正常ケース_DB一致かつfirstIdx2で不一致を指定する_条件分岐が実行されること() throws Exception {
        TestResourceContext trc = newTrc(tempDir, new Properties());
        int score = (int) invokePrivateInstance(trc, "scoreKeyWithSegments",
                new Class[] {String.class, String[].class, String.class},
                new Object[] {"spring.datasource.x.foo.url", new String[] {"foo"}, "url"});
        assertTrue(score >= 100);
    }

    @Test
    void scoreKeyWithSegments_正常ケース_DB不一致かつspringDatasource以外を指定する_非一致分岐が実行されること()
            throws Exception {
        TestResourceContext trc = newTrc(tempDir, new Properties());
        int score = (int) invokePrivateInstance(trc, "scoreKeyWithSegments",
                new Class[] {String.class, String[].class, String.class},
                new Object[] {"abc.def.url", new String[] {"foo"}, "url"});
        assertTrue(score >= 0);
    }

    @Test
    void scoreKeyWithSegments_正常ケース_DB未指定でspring以外を指定する_条件分岐の逆側が実行されること() throws Exception {
        TestResourceContext trc = newTrc(tempDir, new Properties());
        int score = (int) invokePrivateInstance(trc, "scoreKeyWithSegments",
                new Class[] {String.class, String[].class, String.class},
                new Object[] {"x.datasource.url", new String[0], "url"});
        assertTrue(score >= 50);
    }

    @Test
    void scoreKeyWithSegments_正常ケース_DB未指定でspring配下かつdatasource不一致を指定する_分岐の逆側が実行されること()
            throws Exception {
        TestResourceContext trc = newTrc(tempDir, new Properties());
        int score = (int) invokePrivateInstance(trc, "scoreKeyWithSegments",
                new Class[] {String.class, String[].class, String.class},
                new Object[] {"spring.foo.url", new String[0], "url"});
        assertTrue(score >= 50);
    }

    @Test
    void scoreKeyWithSegments_正常ケース_DB一致でトークン数不足を指定する_条件分岐の逆側が実行されること() throws Exception {
        TestResourceContext trc = newTrc(tempDir, new Properties());
        int score = (int) invokePrivateInstance(trc, "scoreKeyWithSegments",
                new Class[] {String.class, String[].class, String.class},
                new Object[] {"foo.url", new String[] {"foo"}, "url"});
        assertTrue(score >= 100);
    }

    @Test
    void scoreKeyWithSegments_正常ケース_DB一致かつspring配下だがdatasource不一致を指定する_条件分岐の逆側が実行されること()
            throws Exception {
        TestResourceContext trc = newTrc(tempDir, new Properties());
        int score = (int) invokePrivateInstance(trc, "scoreKeyWithSegments",
                new Class[] {String.class, String[].class, String.class},
                new Object[] {"spring.foo.bar.url", new String[] {"foo"}, "url"});
        assertTrue(score >= 100);
    }

    @Test
    void scoreKeyWithSegments_正常ケース_DB不一致かつspring配下でdatasource不一致を指定する_条件分岐の逆側が実行されること()
            throws Exception {
        TestResourceContext trc = newTrc(tempDir, new Properties());
        int score = (int) invokePrivateInstance(trc, "scoreKeyWithSegments",
                new Class[] {String.class, String[].class, String.class},
                new Object[] {"spring.x.y.url", new String[] {"foo"}, "url"});
        assertTrue(score >= 0);
    }

    @Test
    void scoreKeyWithSegments_正常ケース_DB一致かつspring以外を指定する_if条件の逆側が実行されること() throws Exception {
        TestResourceContext trc = newTrc(tempDir, new Properties());
        int score = (int) invokePrivateInstance(trc, "scoreKeyWithSegments",
                new Class[] {String.class, String[].class, String.class},
                new Object[] {"x.foo.datasource.url", new String[] {"foo"}, "url"});
        assertTrue(score >= 100);
    }

    @Test
    void scoreKeyWithSegments_正常ケース_DB不一致かつspring配下でdatasource不一致を指定する_else条件の逆側が実行されること()
            throws Exception {
        TestResourceContext trc = newTrc(tempDir, new Properties());
        int score = (int) invokePrivateInstance(trc, "scoreKeyWithSegments",
                new Class[] {String.class, String[].class, String.class},
                new Object[] {"spring.x.url", new String[] {"foo"}, "url"});
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
            Properties merged = (Properties) invokePrivateStatic(TestResourceContext.class,
                    "loadAllApplicationProperties", new Class[] {ClassLoader.class},
                    new Object[] {cl});
            assertEquals("dev", merged.getProperty("merge.order"));
        }
    }

    @Test
    void extractProfile_正常ケース_yaml拡張子を指定する_プロファイル名が返ること() throws Exception {
        String profile = (String) invokePrivateStatic(TestResourceContext.class, "extractProfile",
                new Class[] {String.class}, new Object[] {"application-dev.yaml"});
        assertEquals("dev", profile);
    }

    @Test
    void extractProfile_正常ケース_対象外ファイル名を指定する_空文字が返ること() throws Exception {
        String profile = (String) invokePrivateStatic(TestResourceContext.class, "extractProfile",
                new Class[] {String.class}, new Object[] {"something.txt"});
        assertEquals("", profile);
    }

    @Test
    void extractProfile_正常ケース_application接頭辞で対象外拡張子を指定する_空文字が返ること() throws Exception {
        String profile = (String) invokePrivateStatic(TestResourceContext.class, "extractProfile",
                new Class[] {String.class}, new Object[] {"application-dev.txt"});
        assertEquals("", profile);
    }

    @Test
    void loadResourceToProps_正常ケース_yamlファイルを指定する_キーが読み込まれること() throws Exception {
        Path yaml = tempDir.resolve("app.yaml");
        Files.writeString(yaml, "a.b: c\n", StandardCharsets.UTF_8);
        Properties props = new Properties();
        invokePrivateStatic(TestResourceContext.class, "loadResourceToProps",
                new Class[] {Properties.class, URL.class},
                new Object[] {props, yaml.toUri().toURL()});
        assertEquals("c", props.getProperty("a.b"));
    }

    @Test
    void loadResourceToProps_正常ケース_ymlファイルを指定する_キーが読み込まれること() throws Exception {
        Path yml = tempDir.resolve("app.yml");
        Files.writeString(yml, "m.n: z\n", StandardCharsets.UTF_8);
        Properties props = new Properties();
        invokePrivateStatic(TestResourceContext.class, "loadResourceToProps",
                new Class[] {Properties.class, URL.class},
                new Object[] {props, yml.toUri().toURL()});
        assertEquals("z", props.getProperty("m.n"));
    }

    @Test
    void loadResourceToProps_正常ケース_対象外拡張子を指定する_読み込みされないこと() throws Exception {
        Path txt = tempDir.resolve("app.txt");
        Files.writeString(txt, "k=v\n", StandardCharsets.UTF_8);
        Properties props = new Properties();
        invokePrivateStatic(TestResourceContext.class, "loadResourceToProps",
                new Class[] {Properties.class, URL.class},
                new Object[] {props, txt.toUri().toURL()});
        assertTrue(props.isEmpty());
    }

    @Test
    void loadYamlToProps_正常ケース_空yamlを指定する_例外が送出されないこと() throws Exception {
        Path yaml = tempDir.resolve("empty.yml");
        Files.writeString(yaml, "", StandardCharsets.UTF_8);
        Properties props = new Properties();
        assertDoesNotThrow(() -> invokePrivateStatic(TestResourceContext.class, "loadYamlToProps",
                new Class[] {Properties.class, URL.class},
                new Object[] {props, yaml.toUri().toURL()}));
    }

    @Test
    void loadYamlToProps_正常ケース_factoryがnullを返す設定を行う_空のまま返ること() throws Exception {
        Path yaml = tempDir.resolve("n.yml");
        Files.writeString(yaml, "a: 1\n", StandardCharsets.UTF_8);
        Properties props = new Properties();
        try (MockedConstruction<YamlPropertiesFactoryBean> mocked =
                mockConstruction(YamlPropertiesFactoryBean.class,
                        (factory, context) -> when(factory.getObject()).thenReturn(null))) {
            invokePrivateStatic(TestResourceContext.class, "loadYamlToProps",
                    new Class[] {Properties.class, URL.class},
                    new Object[] {props, yaml.toUri().toURL()});
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
        @SuppressWarnings("unchecked")
        List<String> out = (List<String>) invokePrivateStatic(TestResourceContext.class,
                "resolveActiveProfiles", new Class[] {Properties.class}, new Object[] {p});
        assertTrue(out.isEmpty());
    }

    @Test
    void resolveActiveProfiles_正常ケース_isNotBlankを偽に差し替える_分岐の逆側が実行されること() throws Exception {
        Properties p = new Properties();
        p.setProperty("spring.profiles.active", "dev");
        try (MockedStatic<StringUtils> mocked = mockStatic(StringUtils.class, CALLS_REAL_METHODS)) {
            mocked.when(() -> StringUtils.isNotBlank("dev")).thenReturn(false);
            @SuppressWarnings("unchecked")
            List<String> out = (List<String>) invokePrivateStatic(TestResourceContext.class,
                    "resolveActiveProfiles", new Class[] {Properties.class}, new Object[] {p});
            assertTrue(out.isEmpty());
        }
    }

    @Test
    void resolveActiveProfiles_正常ケース_システムプロパティに複数プロファイルを指定する_システム値が優先されること() throws Exception {
        System.setProperty("spring.profiles.active", "qa;prod");
        try {
            Properties p = new Properties();
            p.setProperty("spring.profiles.active", "dev");
            @SuppressWarnings("unchecked")
            List<String> out = (List<String>) invokePrivateStatic(TestResourceContext.class,
                    "resolveActiveProfiles", new Class[] {Properties.class}, new Object[] {p});
            assertEquals(List.of("qa", "prod"), out);
        } finally {
            System.clearProperty("spring.profiles.active");
        }
    }

    @Test
    void springManagedConnection_正常ケース_DataSourceを指定する_Connectionが返ること() throws Exception {
        TestResourceContext trc = newTrc(tempDir, new Properties());
        DataSource ds = mock(DataSource.class);
        Connection conn = mock(Connection.class);
        try (MockedStatic<DataSourceUtils> mocked = mockStatic(DataSourceUtils.class)) {
            mocked.when(() -> DataSourceUtils.getConnection(ds)).thenReturn(conn);
            Optional<Connection> out = trc.springManagedConnection(Optional.of(ds));
            assertTrue(out.isPresent());
            assertEquals(conn, out.get());
        }
    }

    private static TestResourceContext newTrc(Path classRoot, Properties props) throws Exception {
        Constructor<TestResourceContext> c =
                TestResourceContext.class.getDeclaredConstructor(Path.class, Properties.class);
        c.setAccessible(true);
        return c.newInstance(classRoot, props);
    }

    private static Object invokePrivateInstance(Object target, String method, Class<?>[] types,
            Object[] args) throws Exception {
        Method m = target.getClass().getDeclaredMethod(method, types);
        m.setAccessible(true);
        try {
            return m.invoke(target, args);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new IllegalStateException(cause);
        }
    }

    private static Object invokePrivateStatic(Class<?> target, String method, Class<?>[] types,
            Object[] args) throws Exception {
        Method m = target.getDeclaredMethod(method, types);
        m.setAccessible(true);
        try {
            return m.invoke(null, args);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new IllegalStateException(cause);
        }
    }

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
