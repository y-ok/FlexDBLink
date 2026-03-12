package io.github.yok.flexdblink.db.oracle;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import lombok.Generated;

/**
 * Shared Oracle session timezone constants.
 *
 * @author Yasuharu.Okawauchi
 */
final class OracleTimeZoneSupport {
    static final String SESSION_TIME_ZONE = "+09:00";
    static final ZoneOffset SESSION_ZONE_OFFSET = ZoneOffset.of(SESSION_TIME_ZONE);
    static final DateTimeFormatter TIMESTAMP_WITH_OFFSET_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss Z", Locale.ROOT);

    /**
     * Utility class constructor.
     */
    @Generated
    private OracleTimeZoneSupport() {}
}
