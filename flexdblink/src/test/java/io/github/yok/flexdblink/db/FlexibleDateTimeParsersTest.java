package io.github.yok.flexdblink.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link FlexibleDateTimeParsers} constants.
 *
 * <p>
 * Verifies that every documented date/time format accepted on CSV load can be parsed by the
 * corresponding formatter constant.
 * </p>
 */
class FlexibleDateTimeParsersTest {

    @Test
    void DATE_ONLY_FORMATTERS_正常ケース_ISO形式日付を指定する_LocalDateが返ること() {
        assertEquals(LocalDate.of(2026, 2, 25),
                LocalDate.parse("2026-02-25", FlexibleDateTimeParsers.DATE_ONLY_FORMATTERS[0]));
    }

    @Test
    void DATE_ONLY_FORMATTERS_異常ケース_ISO形式フォーマッタに不正値を指定する_DateTimeParseExceptionが送出されること() {
        assertThrows(DateTimeParseException.class, () -> LocalDate.parse("not-a-date",
                FlexibleDateTimeParsers.DATE_ONLY_FORMATTERS[0]));
    }

    @Test
    void DATE_ONLY_FORMATTERS_正常ケース_スラッシュ区切り日付を指定する_LocalDateが返ること() {
        assertEquals(LocalDate.of(2026, 2, 25),
                LocalDate.parse("2026/02/25", FlexibleDateTimeParsers.DATE_ONLY_FORMATTERS[1]));
    }

    @Test
    void DATE_ONLY_FORMATTERS_正常ケース_basicISO形式日付を指定する_LocalDateが返ること() {
        assertEquals(LocalDate.of(2026, 2, 25),
                LocalDate.parse("20260225", FlexibleDateTimeParsers.DATE_ONLY_FORMATTERS[2]));
    }

    @Test
    void DATE_ONLY_FORMATTERS_正常ケース_ドット区切り日付を指定する_LocalDateが返ること() {
        assertEquals(LocalDate.of(2026, 2, 25),
                LocalDate.parse("2026.02.25", FlexibleDateTimeParsers.DATE_ONLY_FORMATTERS[3]));
    }

    @Test
    void DATE_ONLY_FORMATTERS_正常ケース_日本語形式日付を指定する_LocalDateが返ること() {
        assertEquals(LocalDate.of(2026, 2, 25),
                LocalDate.parse("2026年2月25日", FlexibleDateTimeParsers.DATE_ONLY_FORMATTERS[4]));
    }

    @Test
    void FLEXIBLE_LOCAL_TIME_PARSER_COLON_正常ケース_時分のみを指定する_LocalTimeが返ること() {
        assertEquals(LocalTime.of(14, 30),
                LocalTime.parse("14:30", FlexibleDateTimeParsers.FLEXIBLE_LOCAL_TIME_PARSER_COLON));
    }

    @Test
    void FLEXIBLE_LOCAL_TIME_PARSER_COLON_正常ケース_時分秒を指定する_LocalTimeが返ること() {
        assertEquals(LocalTime.of(14, 30, 0), LocalTime.parse("14:30:00",
                FlexibleDateTimeParsers.FLEXIBLE_LOCAL_TIME_PARSER_COLON));
    }

    @Test
    void FLEXIBLE_LOCAL_TIME_PARSER_COLON_正常ケース_時分秒小数秒を指定する_LocalTimeが返ること() {
        assertEquals(LocalTime.of(14, 30, 0, 123_000_000), LocalTime.parse("14:30:00.123",
                FlexibleDateTimeParsers.FLEXIBLE_LOCAL_TIME_PARSER_COLON));
    }

    @Test
    void FLEXIBLE_LOCAL_TIME_PARSER_COLON_異常ケース_コロン無し形式を指定する_DateTimeParseExceptionが送出されること() {
        assertThrows(DateTimeParseException.class, () -> LocalTime.parse("143000",
                FlexibleDateTimeParsers.FLEXIBLE_LOCAL_TIME_PARSER_COLON));
    }

    @Test
    void FLEXIBLE_LOCAL_TIME_PARSER_NO_COLON_正常ケース_時分のみを指定する_LocalTimeが返ること() {
        assertEquals(LocalTime.of(14, 30), LocalTime.parse("1430",
                FlexibleDateTimeParsers.FLEXIBLE_LOCAL_TIME_PARSER_NO_COLON));
    }

    @Test
    void FLEXIBLE_LOCAL_TIME_PARSER_NO_COLON_正常ケース_時分秒を指定する_LocalTimeが返ること() {
        assertEquals(LocalTime.of(14, 30, 0), LocalTime.parse("143000",
                FlexibleDateTimeParsers.FLEXIBLE_LOCAL_TIME_PARSER_NO_COLON));
    }

    @Test
    void FLEXIBLE_LOCAL_TIME_PARSER_NO_COLON_正常ケース_時分秒小数秒を指定する_LocalTimeが返ること() {
        assertEquals(LocalTime.of(14, 30, 0, 123_000_000), LocalTime.parse("143000.123",
                FlexibleDateTimeParsers.FLEXIBLE_LOCAL_TIME_PARSER_NO_COLON));
    }

    @Test
    void FLEXIBLE_LOCAL_TIME_PARSER_NO_COLON_異常ケース_コロン区切り形式を指定する_DateTimeParseExceptionが送出されること() {
        assertThrows(DateTimeParseException.class, () -> LocalTime.parse("14:30",
                FlexibleDateTimeParsers.FLEXIBLE_LOCAL_TIME_PARSER_NO_COLON));
    }
}
