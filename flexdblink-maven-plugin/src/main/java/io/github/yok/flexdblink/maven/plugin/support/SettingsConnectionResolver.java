package io.github.yok.flexdblink.maven.plugin.support;

import io.github.yok.flexdblink.config.ConnectionConfig;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.maven.settings.Server;
import org.apache.maven.settings.Settings;
import org.apache.maven.settings.crypto.DefaultSettingsDecryptionRequest;
import org.apache.maven.settings.crypto.SettingsDecrypter;
import org.apache.maven.settings.crypto.SettingsDecryptionRequest;
import org.apache.maven.settings.crypto.SettingsDecryptionResult;
import org.codehaus.plexus.util.xml.Xpp3Dom;

/**
 * Resolves FlexDBLink connection entries from Maven {@code settings.xml} server definitions.
 *
 * <p>
 * For each configured server ID, this component locates the matching Maven server, decrypts the
 * credentials, reads the plugin-specific {@code <configuration>} block, and converts the result
 * into a {@link ConnectionConfig.Entry}. It also rejects duplicate logical DB IDs before the core
 * layer is invoked.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
public class SettingsConnectionResolver {

    /**
     * Resolves connection entries from the provided server IDs.
     *
     * @param settings Maven settings
     * @param decrypter settings decrypter
     * @param serverIds target server IDs
     * @return connection configuration for FlexDBLink core
     */
    public ConnectionConfig resolve(Settings settings, SettingsDecrypter decrypter,
            List<String> serverIds) {
        if (settings == null) {
            throw new IllegalArgumentException("settings is required.");
        }
        if (decrypter == null) {
            throw new IllegalArgumentException("settingsDecrypter is required.");
        }
        if (serverIds == null || serverIds.isEmpty()) {
            throw new IllegalArgumentException("serverIds is required.");
        }

        List<ConnectionConfig.Entry> entries = new ArrayList<>();
        for (String serverId : serverIds) {
            Server server = findServerById(settings, serverId);
            Server decrypted = decryptServer(decrypter, server);
            ConnectionConfig.Entry entry = toConnectionEntry(decrypted);
            entries.add(entry);
        }
        validateDuplicateDbIds(entries);

        ConnectionConfig config = new ConnectionConfig();
        config.setConnections(entries);
        return config;
    }

    /**
     * Finds a server definition by ID.
     *
     * @param settings Maven settings
     * @param serverId target server ID
     * @return matching server
     */
    private Server findServerById(Settings settings, String serverId) {
        List<Server> servers = settings.getServers();
        if (servers.isEmpty()) {
            throw new IllegalArgumentException("No servers are defined in settings.xml.");
        }
        for (Server server : servers) {
            if (server == null) {
                continue;
            }
            String id = server.getId();
            if (id == null) {
                continue;
            }
            if (id.equals(serverId)) {
                return server;
            }
        }
        throw new IllegalArgumentException("Server not found in settings.xml: " + serverId);
    }

    /**
     * Decrypts a server definition using Maven's settings decrypter.
     *
     * @param decrypter settings decrypter
     * @param encrypted encrypted server definition
     * @return decrypted server definition
     */
    private Server decryptServer(SettingsDecrypter decrypter, Server encrypted) {
        SettingsDecryptionRequest request = new DefaultSettingsDecryptionRequest(encrypted);
        SettingsDecryptionResult result = decrypter.decrypt(request);
        Server decrypted = result.getServer();
        if (decrypted == null) {
            throw new IllegalArgumentException(
                    "Failed to decrypt server in settings.xml: " + encrypted.getId());
        }
        return decrypted;
    }

    /**
     * Converts a Maven server definition to a FlexDBLink connection entry.
     *
     * @param server Maven server definition
     * @return connection entry
     */
    private ConnectionConfig.Entry toConnectionEntry(Server server) {
        String dbId = readConfigValue(server, "dbId");
        String url = readConfigValue(server, "url");
        String driverClass = readConfigValue(server, "driverClass");
        String username = server.getUsername();
        String password = server.getPassword();

        validateRequiredServerFields(server, dbId, url, username);

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId(dbId);
        entry.setUrl(url);
        entry.setDriverClass(driverClass);
        entry.setUser(username);
        entry.setPassword(password);
        return entry;
    }

    /**
     * Reads a server configuration value from {@code <server><configuration>}.
     *
     * @param server Maven server definition
     * @param key child element name
     * @return element value, or {@code null} when not present
     */
    private String readConfigValue(Server server, String key) {
        Object configuration = server.getConfiguration();
        if (!(configuration instanceof Xpp3Dom)) {
            return null;
        }
        Xpp3Dom dom = (Xpp3Dom) configuration;
        Xpp3Dom child = dom.getChild(key);
        if (child == null) {
            return null;
        }
        String value = child.getValue();
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        return trimmed;
    }

    /**
     * Validates mandatory fields in the resolved server definition.
     *
     * @param server Maven server
     * @param dbId database logical ID
     * @param url JDBC URL
     * @param username database username
     */
    private void validateRequiredServerFields(Server server, String dbId, String url,
            String username) {
        String serverId = server.getId();
        if (isBlank(dbId)) {
            throw new IllegalArgumentException(
                    "settings.xml server configuration requires <dbId>: serverId=" + serverId);
        }
        if (isBlank(url)) {
            throw new IllegalArgumentException(
                    "settings.xml server configuration requires <url>: serverId=" + serverId);
        }
        if (isBlank(username)) {
            throw new IllegalArgumentException(
                    "settings.xml server requires <username>: serverId=" + serverId);
        }
    }

    /**
     * Validates duplicate DB IDs in the resolved connection list.
     *
     * @param entries resolved entries
     */
    private void validateDuplicateDbIds(List<ConnectionConfig.Entry> entries) {
        Set<String> seen = new HashSet<>();
        for (ConnectionConfig.Entry entry : entries) {
            String dbId = entry.getId();
            String normalized = dbId.toLowerCase();
            if (seen.contains(normalized)) {
                throw new IllegalArgumentException(
                        "Duplicate dbId in settings.xml servers: " + dbId);
            }
            seen.add(normalized);
        }
    }

    /**
     * Returns whether the string is null or blank.
     *
     * @param value target string
     * @return {@code true} when blank
     */
    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
