package io.github.yok.flexdblink.maven.plugin.support;

import io.github.yok.flexdblink.config.ConnectionConfig;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Generated;

/**
 * Utility for masking sensitive values in plugin logs.
 *
 * <p>
 * This helper hides credentials before connection information is written to the Maven log. It masks
 * plain text fields, embedded credentials in JDBC URLs, and password query parameters while keeping
 * enough detail for troubleshooting.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
public final class MaskingLogUtil {

    /**
     * Pattern that matches embedded credentials in authority-style JDBC URLs.
     */
    private static final Pattern JDBC_AUTH_PATTERN =
            Pattern.compile("(jdbc:[^:]+://[^:/?#]+:)([^@/]+)(@.*)", Pattern.CASE_INSENSITIVE);
    /**
     * Pattern that matches password query parameters in JDBC URLs.
     */
    private static final Pattern PASSWORD_QUERY_PATTERN =
            Pattern.compile("(?i)(password=)([^;&]+)");

    /**
     * Prevents instantiation of this utility class.
     */
    @Generated
    private MaskingLogUtil() {}

    /**
     * Masks a generic sensitive text.
     *
     * @param value raw text
     * @return masked text, or {@code null} when input is {@code null}
     */
    public static String maskText(String value) {
        if (value == null) {
            return null;
        }
        if (value.isEmpty()) {
            return value;
        }
        return "***";
    }

    /**
     * Masks password-like fragments in a JDBC URL.
     *
     * @param url JDBC URL
     * @return masked URL, or {@code null} when input is {@code null}
     */
    public static String maskJdbcUrl(String url) {
        if (url == null) {
            return null;
        }
        String masked = url;
        Matcher authMatcher = JDBC_AUTH_PATTERN.matcher(masked);
        if (authMatcher.find()) {
            masked = authMatcher.replaceFirst("$1***$3");
        }
        Matcher queryMatcher = PASSWORD_QUERY_PATTERN.matcher(masked);
        if (queryMatcher.find()) {
            masked = queryMatcher.replaceAll("$1***");
        }
        return masked;
    }

    /**
     * Formats a connection entry for logging with masked sensitive values.
     *
     * @param entry connection entry
     * @return formatted log string
     */
    public static String maskConnection(ConnectionConfig.Entry entry) {
        if (entry == null) {
            return "<null>";
        }
        StringBuilder builder = new StringBuilder();
        builder.append("id=").append(entry.getId());
        builder.append(", url=").append(maskJdbcUrl(entry.getUrl()));
        builder.append(", user=").append(maskText(entry.getUser()));
        builder.append(", driverClass=").append(entry.getDriverClass());
        return builder.toString();
    }
}
