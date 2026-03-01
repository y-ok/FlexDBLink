package io.github.yok.flexdblink.maven.plugin.support;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.github.yok.flexdblink.config.ConnectionConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.maven.settings.Server;
import org.apache.maven.settings.Settings;
import org.apache.maven.settings.crypto.SettingsDecrypter;
import org.apache.maven.settings.crypto.SettingsDecryptionRequest;
import org.apache.maven.settings.crypto.SettingsDecryptionResult;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.junit.jupiter.api.Test;

class SettingsConnectionResolverTest {

    private final SettingsConnectionResolver target = new SettingsConnectionResolver();

    @Test
    void resolve_正常ケース_1件解決する_ConnectionConfigが返ること() {
        Settings settings = new Settings();
        Server server = server("flexdblink-db1", "app", "pw", "DB1",
                "jdbc:postgresql://localhost:5432/app", "org.postgresql.Driver");
        settings.setServers(List.of(server));
        SettingsDecrypter decrypter = passThroughDecrypter(server);

        ConnectionConfig actual = target.resolve(settings, decrypter, List.of("flexdblink-db1"));

        assertEquals(1, actual.getConnections().size());
        assertEquals("DB1", actual.getConnections().get(0).getId());
        assertEquals("jdbc:postgresql://localhost:5432/app",
                actual.getConnections().get(0).getUrl());
        assertEquals("app", actual.getConnections().get(0).getUser());
        assertEquals("org.postgresql.Driver", actual.getConnections().get(0).getDriverClass());
    }

    @Test
    void resolve_正常ケース_driverClass省略を解決する_nullのdriverClassが返ること() {
        Settings settings = new Settings();
        Server server = server("flexdblink-db1", "app", "pw", "DB1",
                "jdbc:postgresql://localhost:5432/app", null);
        settings.setServers(List.of(server));
        SettingsDecrypter decrypter = passThroughDecrypter(server);

        ConnectionConfig actual = target.resolve(settings, decrypter, List.of("flexdblink-db1"));

        assertEquals(1, actual.getConnections().size());
        assertNull(actual.getConnections().get(0).getDriverClass());
    }

    @Test
    void resolve_正常ケース_サーバリストにnull要素を含む_null要素をスキップして解決すること() {
        Settings settings = new Settings();
        Server server = server("s1", "app", "pw", "DB1", "jdbc:postgresql://localhost/a", null);
        List<Server> servers = new ArrayList<>();
        servers.add(null);
        servers.add(server);
        settings.setServers(servers);
        SettingsDecrypter decrypter = passThroughDecrypter(server);

        ConnectionConfig actual = target.resolve(settings, decrypter, List.of("s1"));

        assertEquals(1, actual.getConnections().size());
        assertEquals("DB1", actual.getConnections().get(0).getId());
    }

    @Test
    void resolve_正常ケース_サーバIDがnullの要素を含む_null要素をスキップして解決すること() {
        Settings settings = new Settings();
        Server nullIdServer = new Server();
        nullIdServer.setId(null);
        Server server = server("s1", "app", "pw", "DB1", "jdbc:postgresql://localhost/a", null);
        List<Server> servers = new ArrayList<>();
        servers.add(nullIdServer);
        servers.add(server);
        settings.setServers(servers);
        SettingsDecrypter decrypter = passThroughDecrypter(server);

        ConnectionConfig actual = target.resolve(settings, decrypter, List.of("s1"));

        assertEquals(1, actual.getConnections().size());
    }

    @Test
    void resolve_正常ケース_configuration非Xpp3Domを解決する_config値がnullで返ること() {
        Settings settings = new Settings();
        Server server = new Server();
        server.setId("s1");
        server.setUsername("app");
        server.setPassword("pw");
        server.setConfiguration("not-xpp3dom");
        settings.setServers(List.of(server));
        SettingsDecrypter decrypter = passThroughDecrypter(server);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> target.resolve(settings, decrypter, List.of("s1")));

        assertTrue(ex.getMessage().contains("dbId"));
    }

    @Test
    void resolve_正常ケース_子要素の値がnullを解決する_null扱いで返ること() {
        Settings settings = new Settings();
        Server server = new Server();
        server.setId("s1");
        server.setUsername("app");
        server.setPassword("pw");
        Xpp3Dom root = new Xpp3Dom("configuration");
        Xpp3Dom dbIdChild = new Xpp3Dom("dbId");
        root.addChild(dbIdChild);
        addChild(root, "url", "jdbc:postgresql://localhost/a");
        server.setConfiguration(root);
        settings.setServers(List.of(server));
        SettingsDecrypter decrypter = passThroughDecrypter(server);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> target.resolve(settings, decrypter, List.of("s1")));

        assertTrue(ex.getMessage().contains("dbId"));
    }

    @Test
    void resolve_正常ケース_子要素の値が空白を解決する_null扱いで返ること() {
        Settings settings = new Settings();
        Server server = new Server();
        server.setId("s1");
        server.setUsername("app");
        server.setPassword("pw");
        Xpp3Dom root = new Xpp3Dom("configuration");
        addChild(root, "dbId", "   ");
        addChild(root, "url", "jdbc:postgresql://localhost/a");
        server.setConfiguration(root);
        settings.setServers(List.of(server));
        SettingsDecrypter decrypter = passThroughDecrypter(server);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> target.resolve(settings, decrypter, List.of("s1")));

        assertTrue(ex.getMessage().contains("dbId"));
    }

    @Test
    void resolve_異常ケース_settingsがnullを検証する_IllegalArgumentExceptionが送出されること() {
        SettingsDecrypter decrypter = mock(SettingsDecrypter.class);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> target.resolve(null, decrypter, List.of("s1")));

        assertTrue(ex.getMessage().contains("settings is required"));
    }

    @Test
    void resolve_異常ケース_decrypterがnullを検証する_IllegalArgumentExceptionが送出されること() {
        Settings settings = new Settings();

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> target.resolve(settings, null, List.of("s1")));

        assertTrue(ex.getMessage().contains("settingsDecrypter is required"));
    }

    @Test
    void resolve_異常ケース_serverIdsがnullを検証する_IllegalArgumentExceptionが送出されること() {
        Settings settings = new Settings();
        SettingsDecrypter decrypter = mock(SettingsDecrypter.class);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> target.resolve(settings, decrypter, null));

        assertTrue(ex.getMessage().contains("serverIds is required"));
    }

    @Test
    void resolve_異常ケース_serverIdsが空を検証する_IllegalArgumentExceptionが送出されること() {
        Settings settings = new Settings();
        SettingsDecrypter decrypter = mock(SettingsDecrypter.class);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> target.resolve(settings, decrypter, Collections.emptyList()));

        assertTrue(ex.getMessage().contains("serverIds is required"));
    }

    @Test
    void resolve_異常ケース_servers空リストを検証する_IllegalArgumentExceptionが送出されること() {
        Settings settings = new Settings();
        settings.setServers(Collections.emptyList());
        SettingsDecrypter decrypter = mock(SettingsDecrypter.class);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> target.resolve(settings, decrypter, List.of("s1")));

        assertTrue(ex.getMessage().contains("No servers"));
    }

    @Test
    void resolve_異常ケース_serverId不存在を解決する_IllegalArgumentExceptionが送出されること() {
        Settings settings = new Settings();
        settings.setServers(List.of(server("flexdblink-db1", "app", "pw", "DB1",
                "jdbc:postgresql://localhost:5432/app", null)));
        SettingsDecrypter decrypter = mock(SettingsDecrypter.class);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> target.resolve(settings, decrypter, List.of("missing")));

        assertTrue(ex.getMessage().contains("missing"));
    }

    @Test
    void resolve_異常ケース_復号結果がnullを検証する_IllegalArgumentExceptionが送出されること() {
        Settings settings = new Settings();
        Server server = server("s1", "app", "pw", "DB1", "jdbc:postgresql://localhost/a", null);
        settings.setServers(List.of(server));
        SettingsDecrypter decrypter = mock(SettingsDecrypter.class);
        SettingsDecryptionResult result = mock(SettingsDecryptionResult.class);
        when(result.getServer()).thenReturn(null);
        when(decrypter.decrypt(any(SettingsDecryptionRequest.class))).thenReturn(result);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> target.resolve(settings, decrypter, List.of("s1")));

        assertTrue(ex.getMessage().contains("Failed to decrypt server"));
    }

    @Test
    void resolve_異常ケース_必須項目dbId不足を検証する_IllegalArgumentExceptionが送出されること() {
        Settings settings = new Settings();
        Server server = server("flexdblink-db1", "app", "pw", null,
                "jdbc:postgresql://localhost:5432/app", null);
        settings.setServers(List.of(server));
        SettingsDecrypter decrypter = passThroughDecrypter(server);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> target.resolve(settings, decrypter, List.of("flexdblink-db1")));

        assertTrue(ex.getMessage().contains("dbId"));
    }

    @Test
    void resolve_異常ケース_必須項目url不足を検証する_IllegalArgumentExceptionが送出されること() {
        Settings settings = new Settings();
        Server server = server("s1", "app", "pw", "DB1", null, null);
        settings.setServers(List.of(server));
        SettingsDecrypter decrypter = passThroughDecrypter(server);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> target.resolve(settings, decrypter, List.of("s1")));

        assertTrue(ex.getMessage().contains("url"));
    }

    @Test
    void resolve_異常ケース_必須項目username不足を検証する_IllegalArgumentExceptionが送出されること() {
        Settings settings = new Settings();
        Server server = server("s1", null, "pw", "DB1", "jdbc:postgresql://localhost/a", null);
        settings.setServers(List.of(server));
        SettingsDecrypter decrypter = passThroughDecrypter(server);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> target.resolve(settings, decrypter, List.of("s1")));

        assertTrue(ex.getMessage().contains("username"));
    }

    @Test
    void resolve_異常ケース_必須項目username空白を検証する_IllegalArgumentExceptionが送出されること() {
        Settings settings = new Settings();
        Server server = server("s1", "   ", "pw", "DB1", "jdbc:postgresql://localhost/a", null);
        settings.setServers(List.of(server));
        SettingsDecrypter decrypter = passThroughDecrypter(server);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> target.resolve(settings, decrypter, List.of("s1")));

        assertTrue(ex.getMessage().contains("username"));
    }

    @Test
    void resolve_異常ケース_dbId重複を解決する_IllegalArgumentExceptionが送出されること() {
        Settings settings = new Settings();
        Server server1 = server("s1", "app", "pw", "DB1", "jdbc:postgresql://localhost/a", null);
        Server server2 = server("s2", "app", "pw", "db1", "jdbc:postgresql://localhost/b", null);
        settings.setServers(List.of(server1, server2));
        SettingsDecrypter decrypter = mock(SettingsDecrypter.class);
        SettingsDecryptionResult result1 = mock(SettingsDecryptionResult.class);
        SettingsDecryptionResult result2 = mock(SettingsDecryptionResult.class);
        when(result1.getServer()).thenReturn(server1);
        when(result2.getServer()).thenReturn(server2);
        when(decrypter.decrypt(any(SettingsDecryptionRequest.class))).thenReturn(result1, result2);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> target.resolve(settings, decrypter, List.of("s1", "s2")));

        assertTrue(ex.getMessage().contains("Duplicate dbId"));
    }

    private Server server(String id, String user, String password, String dbId, String url,
            String driverClass) {
        Server server = new Server();
        server.setId(id);
        server.setUsername(user);
        server.setPassword(password);
        Xpp3Dom root = new Xpp3Dom("configuration");
        if (dbId != null) {
            addChild(root, "dbId", dbId);
        }
        if (url != null) {
            addChild(root, "url", url);
        }
        if (driverClass != null) {
            addChild(root, "driverClass", driverClass);
        }
        server.setConfiguration(root);
        return server;
    }

    private void addChild(Xpp3Dom parent, String name, String value) {
        Xpp3Dom child = new Xpp3Dom(name);
        child.setValue(value);
        parent.addChild(child);
    }

    private SettingsDecrypter passThroughDecrypter(Server server) {
        SettingsDecrypter decrypter = mock(SettingsDecrypter.class);
        SettingsDecryptionResult result = mock(SettingsDecryptionResult.class);
        when(result.getServer()).thenReturn(server);
        when(decrypter.decrypt(any(SettingsDecryptionRequest.class))).thenReturn(result);
        return decrypter;
    }
}
