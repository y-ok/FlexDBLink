package io.github.yok.flexdblink.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.github.yok.flexdblink.config.CsvDateTimeFormatProperties;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import org.junit.jupiter.api.Test;

public class DateTimeFormatUtilTest {

    @Test
    public void constructor_異常ケース_不正なパターンを指定する_IllegalArgumentExceptionが送出されること() {
        CsvDateTimeFormatProperties props = mock(CsvDateTimeFormatProperties.class);
        when(props.getDate()).thenReturn("yyyy-MM-dd");
        when(props.getTime()).thenReturn("HH:mm:ss");
        when(props.getDateTimeWithMillis()).thenReturn("invalid[");

        assertThrows(IllegalArgumentException.class, () -> new DateTimeFormatUtil(props));
    }

    @Test
    public void formatJdbcDateTime_正常ケース_valueがnull_nullが返ること() {
        DateTimeFormatUtil util = createUtil();
        assertNull(util.formatJdbcDateTime("COL", null, null));
    }

    @Test
    public void formatJdbcDateTime_正常ケース_Timestampを指定する_ミリ秒付きフォーマットが返ること() {
        DateTimeFormatUtil util = createUtil();
        Timestamp ts = Timestamp.valueOf(LocalDateTime.of(2026, 2, 15, 1, 2, 3, 123_000_000));

        String actual = util.formatJdbcDateTime("ANY_COL", ts, null);

        assertEquals("2026-02-15 01:02:03.123", actual);
    }

    @Test
    public void formatJdbcDateTime_正常ケース_Dateを指定する_日付フォーマットが返ること() {
        DateTimeFormatUtil util = createUtil();
        Date d = Date.valueOf("2026-02-15");

        String actual = util.formatJdbcDateTime("ANY_COL", d, null);

        assertEquals("2026-02-15", actual);
    }

    @Test
    public void formatJdbcDateTime_正常ケース_Timeを指定する_時刻フォーマットが返ること() {
        DateTimeFormatUtil util = createUtil();
        Time t = Time.valueOf("01:02:03");

        String actual = util.formatJdbcDateTime("ANY_COL", t, null);

        assertEquals("01:02:03", actual);
    }

    @Test
    public void formatJdbcDateTime_正常ケース_Stringを指定する_そのまま返ること() {
        DateTimeFormatUtil util = createUtil();

        String actual = util.formatJdbcDateTime("TIMESTAMP_TZ_COL", "abc", null);

        assertEquals("abc", actual);
    }

    @Test
    public void formatJdbcDateTime_正常ケース_otherを指定する_toStringが返ること() {
        DateTimeFormatUtil util = createUtil();
        Object v = new Object() {
            @Override
            public String toString() {
                return "X";
            }
        };

        String actual = util.formatJdbcDateTime("ANY_COL", v, null);

        assertEquals("X", actual);
    }

    @Test
    public void formatJdbcDateTime_正常ケース_TIMESTAMP_LTZ_COL_正規化が適用されること() {
        DateTimeFormatUtil util = createUtil();

        // 1) 空白あり入力：offset除去後に末尾空白が残るため、".0" は残る
        String in1 = "2026-02-15 01:02:03.0 +09:00 Asia/Tokyo";
        String actual1 = util.formatJdbcDateTime("TIMESTAMP_LTZ_COL", in1, null);
        assertEquals("2026-02-15 01:02:03.0 ", actual1);

        // 2) 空白なし入力：offset除去後に ".0" が末尾になり、"\\.0+$" で削除される
        String in2 = "2026-02-15 01:02:03.0+09:00 Asia/Tokyo";
        String actual2 = util.formatJdbcDateTime("TIMESTAMP_LTZ_COL", in2, null);
        assertEquals("2026-02-15 01:02:03", actual2);
    }

    @Test
    public void formatJdbcDateTime_正常ケース_TIMESTAMP_COL_末尾の_0_が削除されること() {
        DateTimeFormatUtil util = createUtil();

        String actual = util.formatJdbcDateTime("TIMESTAMP_COL", "2026-02-15 01:02:03.0", null);

        assertEquals("2026-02-15 01:02:03", actual);
    }

    @Test
    public void formatJdbcDateTime_正常ケース_DATE_COL_末尾の_0_が削除されること() {
        DateTimeFormatUtil util = createUtil();

        String actual = util.formatJdbcDateTime("DATE_COL", "2026-02-15.0", null);

        assertEquals("2026-02-15", actual);
    }

    @Test
    public void formatJdbcDateTime_正常ケース_INTERVAL_YM_COL_正規化されること() {
        DateTimeFormatUtil util = createUtil();

        assertEquals("+01-06", util.formatJdbcDateTime("INTERVAL_YM_COL", "1-6", null));
        assertEquals("-02-03", util.formatJdbcDateTime("INTERVAL_YM_COL", "-2-03", null));
        assertEquals("abc", util.formatJdbcDateTime("INTERVAL_YM_COL", "abc", null));
    }

    @Test
    public void formatJdbcDateTime_正常ケース_INTERVAL_DS_COL_正規化されること() {
        DateTimeFormatUtil util = createUtil();

        assertEquals("+00 05:00:00", util.formatJdbcDateTime("INTERVAL_DS_COL", "0 5:0:0.0", null));
        assertEquals("-02 10:02:03",
                util.formatJdbcDateTime("INTERVAL_DS_COL", "-2 10:02:03", null));
        assertEquals("abc", util.formatJdbcDateTime("INTERVAL_DS_COL", "abc", null));
    }

    @Test
    public void formatJdbcDateTime_正常ケース_INTERVAL_YM_COL_空白を含む値を指定する_正規化された文字列が返ること() {
        DateTimeFormatUtil util = createUtil();

        // Whitespace in the raw interval string must be stripped before matching
        assertEquals("+01-06", util.formatJdbcDateTime("INTERVAL_YM_COL", " 1-6 ", null));
    }

    @Test
    public void formatJdbcDateTime_正常ケース_INTERVAL_DS_COL_小数秒を含む値を指定する_小数秒なしで正規化された文字列が返ること() {
        DateTimeFormatUtil util = createUtil();

        // Fractional seconds must be dropped during normalization
        assertEquals("+03 14:15:09",
                util.formatJdbcDateTime("INTERVAL_DS_COL", "3 14:15:09.265", null));
    }

    @Test
    public void formatJdbcDateTime_異常ケース_colNameがnull_NPEをcatchしてtoStringが返ること() {
        DateTimeFormatUtil util = createUtil();
        Timestamp ts = Timestamp.valueOf("2026-02-15 01:02:03.123");

        String actual = util.formatJdbcDateTime(null, ts, null);

        assertEquals(ts.toString(), actual);
    }

    private static DateTimeFormatUtil createUtil() {
        CsvDateTimeFormatProperties props = mock(CsvDateTimeFormatProperties.class);
        when(props.getDate()).thenReturn("yyyy-MM-dd");
        when(props.getTime()).thenReturn("HH:mm:ss");
        when(props.getDateTimeWithMillis()).thenReturn("yyyy-MM-dd HH:mm:ss.SSS");
        return new DateTimeFormatUtil(props);
    }
}
