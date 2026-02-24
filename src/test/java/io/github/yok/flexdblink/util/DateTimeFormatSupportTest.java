package io.github.yok.flexdblink.util;

import static org.junit.jupiter.api.Assertions.assertNull;
import java.sql.Connection;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DateTimeFormatSupport} default method implementations.
 */
class DateTimeFormatSupportTest {

    private final DateTimeFormatSupport support = new DateTimeFormatSupport() {
        @Override
        public String formatJdbcDateTime(String columnName, Object value, Connection connection) {
            return null;
        }
    };

    @Test
    public void parseConfiguredDate_正常ケース_デフォルト実装_nullが返ること() {
        assertNull(support.parseConfiguredDate("2026-02-25"));
    }

    @Test
    public void parseConfiguredTime_正常ケース_デフォルト実装_nullが返ること() {
        assertNull(support.parseConfiguredTime("12:34:56"));
    }

    @Test
    public void parseConfiguredTimestamp_正常ケース_デフォルト実装_nullが返ること() {
        assertNull(support.parseConfiguredTimestamp("2026-02-25 12:34:56"));
    }
}
