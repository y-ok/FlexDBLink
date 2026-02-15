package io.github.yok.flexdblink.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import io.github.yok.flexdblink.config.CsvDateTimeFormatProperties;
import java.lang.reflect.Method;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.FixedValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OracleDateTimeFormatUtilTest {

    private OracleDateTimeFormatUtil util;

    @BeforeEach
    void setup() {
        CsvDateTimeFormatProperties props = new CsvDateTimeFormatProperties();
        props.setDate("yyyy-MM-dd");
        props.setTime("HH:mm:ss");
        props.setDateTime("yyyy-MM-dd HH:mm:ss");
        props.setDateTimeWithMillis("yyyy-MM-dd HH:mm:ss.SSS");
        util = new OracleDateTimeFormatUtil(props);
    }

    @Test
    void formatJdbcDateTime_正常ケース_null入力はnullが返ること() {
        assertNull(util.formatJdbcDateTime("ANY", null, null));
    }

    @Test
    void formatJdbcDateTime_正常ケース_Timestampはミリ秒付きでフォーマットされること() {
        Timestamp ts = Timestamp.valueOf(LocalDateTime.of(2020, 1, 2, 3, 4, 5, 678_000_000));
        String out = util.formatJdbcDateTime("ANY", ts, null);
        assertEquals("2020-01-02 03:04:05.678", out);
    }

    @Test
    void formatJdbcDateTime_正常ケース_Dateは日付フォーマットされること() {
        Date d = Date.valueOf("2020-12-31");
        String out = util.formatJdbcDateTime("ANY", d, null);
        assertEquals("2020-12-31", out);
    }

    @Test
    void formatJdbcDateTime_正常ケース_Timeは時刻フォーマットされること() {
        Time t = Time.valueOf("23:59:58");
        String out = util.formatJdbcDateTime("ANY", t, null);
        assertEquals("23:59:58", out);
    }

    @Test
    void formatJdbcDateTime_正常ケース_Stringはそのまま返ること() {
        String out = util.formatJdbcDateTime("ANY", "rawstring", null);
        assertEquals("rawstring", out);
    }

    @Test
    void formatJdbcDateTime_正常ケース_その他オブジェクトはtoStringが返ること() {
        Object o = new Object() {
            @Override
            public String toString() {
                return "custom";
            }
        };
        String out = util.formatJdbcDateTime("ANY", o, null);
        assertEquals("custom", out);
    }

    @Test
    void formatJdbcDateTime_正常ケース_colNameがTIMESTAMP_LTZ_COLでタイムゾーン削除されること() {
        String in = "2020-01-01 10:00:00 Asia/Tokyo";
        String out = util.formatJdbcDateTime("TIMESTAMP_LTZ_COL", in, null);
        assertEquals("2020-01-01 10:00:00", out);
    }

    @Test
    void formatJdbcDateTime_正常ケース_colNameがTIMESTAMP_COLで小数部削除されること() {
        String in = "2020-01-01 10:00:00.0";
        String out = util.formatJdbcDateTime("TIMESTAMP_COL", in, null);
        assertEquals("2020-01-01 10:00:00", out);
    }

    @Test
    void formatJdbcDateTime_正常ケース_colNameがDATE_COLで小数部削除されること() {
        String in = "2020-01-01.0";
        String out = util.formatJdbcDateTime("DATE_COL", in, null);
        assertEquals("2020-01-01", out);
    }

    @Test
    void formatJdbcDateTime_正常ケース_colNameがINTERVAL_YM_COLで正規化されること() {
        String in = "2-3";
        String out = util.formatJdbcDateTime("INTERVAL_YM_COL", in, null);
        assertEquals("+02-03", out);
    }

    @Test
    void formatJdbcDateTime_正常ケース_colNameがINTERVAL_DS_COLで正規化されること() {
        String in = "2 3:4:5.0";
        String out = util.formatJdbcDateTime("INTERVAL_DS_COL", in, null);
        assertEquals("+02 03:04:05", out);
    }

    @Test
    void formatJdbcDateTime_正常ケース_未知の列名は変化しないこと() {
        String in = "nochange";
        String out = util.formatJdbcDateTime("OTHER_COL", in, null);
        assertEquals("nochange", out);
    }

    @Test
    void formatJdbcDateTime_正常ケース_TimestampTz_null入力はnullが返ること() {
        String out = util.formatJdbcDateTime("ANY", (String) null, null);
        assertNull(out);
    }

    @Test
    void formatJdbcDateTime_正常ケース_TimestampTz_リージョン異常は元の文字列が返ること() {
        String in = "2020-01-01 12:00:00 Not/AZone";
        String out = util.formatJdbcDateTime("ANY", in, null);
        assertEquals("2020-01-01 12:00:00 Not/AZone", out);
    }

    @Test
    void formatJdbcDateTime_正常ケース_TimestampCol_小数部付きは小数が削除されること() {
        String in = "2020-01-01 12:34:56.0";
        String out = util.formatJdbcDateTime("TIMESTAMP_COL", in, null);
        assertEquals("2020-01-01 12:34:56", out);
    }

    @Test
    void formatJdbcDateTime_正常ケース_oracleJdbcTimestampTzクラス名を動的生成する_数値オフセットが正規化されること()
            throws Exception {
        Object value = newDynamicOracleTemporalObject("oracle.jdbc.OracleTIMESTAMPTZ",
                "2020-01-01 12:34:56 +09:00");
        String out = util.formatJdbcDateTime("ANY", value, null);
        assertEquals("2020-01-01 12:34:56 +0900", out);
    }

    @Test
    void formatJdbcDateTime_正常ケース_oracleJdbcTimestampLtzクラス名を動的生成する_リージョンオフセットが正規化されること()
            throws Exception {
        Object value = newDynamicOracleTemporalObject("oracle.jdbc.OracleTimestampltz",
                "2020-01-01 12:34:56.0 Asia/Tokyo");
        String out = util.formatJdbcDateTime("ANY", value, null);
        assertEquals("2020-01-01 12:34:56 +0900", out);
    }

    @Test
    void formatJdbcDateTime_異常ケース_colNameにnullを指定する_toString値が返ること() {
        String out = util.formatJdbcDateTime(null, "fallback-value", null);
        assertEquals("fallback-value", out);
    }

    @Test
    void normalizeTimestampTz_正常ケース_リージョン日時形式で日時が不正の場合_入力値が返ること() throws Exception {
        Method method = OracleDateTimeFormatUtil.class.getDeclaredMethod("normalizeTimestampTz",
                String.class);
        method.setAccessible(true);
        String out = (String) method.invoke(util, "2020-13-99 99:99:99 Asia/Tokyo");
        assertEquals("2020-13-99 99:99:99 Asia/Tokyo", out);
    }

    @Test
    void normalizeTimestampTz_正常ケース_数値オフセット形式を指定する_コロン無しオフセットが返ること() throws Exception {
        Method method = OracleDateTimeFormatUtil.class.getDeclaredMethod("normalizeTimestampTz",
                String.class);
        method.setAccessible(true);
        String out = (String) method.invoke(util, "2020-01-01 12:00:00 +09:00");
        assertEquals("2020-01-01 12:00:00 +0900", out);
    }

    @Test
    void normalizeIntervalYm_正常ケース_書式不一致を指定する_入力値が返ること() throws Exception {
        Method method = OracleDateTimeFormatUtil.class.getDeclaredMethod("normalizeIntervalYm",
                String.class);
        method.setAccessible(true);
        String out = (String) method.invoke(util, "invalid-value");
        assertEquals("invalid-value", out);
    }

    @Test
    void normalizeIntervalDs_正常ケース_書式不一致を指定する_入力値が返ること() throws Exception {
        Method method = OracleDateTimeFormatUtil.class.getDeclaredMethod("normalizeIntervalDs",
                String.class);
        method.setAccessible(true);
        String out = (String) method.invoke(util, "invalid-value");
        assertEquals("invalid-value", out);
    }

    @Test
    void normalizeTimestampTz_正常ケース_nullを指定する_nullが返ること() throws Exception {
        Method method = OracleDateTimeFormatUtil.class.getDeclaredMethod("normalizeTimestampTz",
                String.class);
        method.setAccessible(true);
        String out = (String) method.invoke(util, new Object[] {null});
        assertNull(out);
    }

    @Test
    void normalizeIntervalYm_正常ケース_負値を指定する_マイナス符号付きで返ること() throws Exception {
        Method method = OracleDateTimeFormatUtil.class.getDeclaredMethod("normalizeIntervalYm",
                String.class);
        method.setAccessible(true);
        String out = (String) method.invoke(util, "-2-3");
        assertEquals("-02-03", out);
    }

    @Test
    void normalizeIntervalDs_正常ケース_負値を指定する_マイナス符号付きで返ること() throws Exception {
        Method method = OracleDateTimeFormatUtil.class.getDeclaredMethod("normalizeIntervalDs",
                String.class);
        method.setAccessible(true);
        String out = (String) method.invoke(util, "-2 3:4:5.0");
        assertEquals("-02 03:04:05", out);
    }

    /**
     * Builds a runtime class with a specific Oracle JDBC-like FQCN and a
     * {@code stringValue(Connection)} method.
     *
     * @param className fully qualified class name to expose through {@code getClass().getName()}
     * @param rawValue return value of {@code stringValue(Connection)}
     * @return instantiated dynamic object
     * @throws Exception if bytecode generation or class loading fails
     */
    private Object newDynamicOracleTemporalObject(String className, String rawValue)
            throws Exception {
        Class<?> dynamicClass = new ByteBuddy().subclass(Object.class).name(className)
                .defineMethod("stringValue", String.class, Visibility.PUBLIC)
                .withParameters(java.sql.Connection.class).intercept(FixedValue.value(rawValue))
                .make().load(getClass().getClassLoader(), ClassLoadingStrategy.Default.INJECTION)
                .getLoaded();
        return dynamicClass.getDeclaredConstructor().newInstance();
    }
}
