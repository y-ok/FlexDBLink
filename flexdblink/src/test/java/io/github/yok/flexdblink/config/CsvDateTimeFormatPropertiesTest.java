package io.github.yok.flexdblink.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;

class CsvDateTimeFormatPropertiesTest {

    @Test
    void getDate_正常ケース_デフォルト値を取得する_nullが返ること() {
        CsvDateTimeFormatProperties props = new CsvDateTimeFormatProperties();
        assertNull(props.getDate());
    }

    @Test
    void getTime_正常ケース_デフォルト値を取得する_nullが返ること() {
        CsvDateTimeFormatProperties props = new CsvDateTimeFormatProperties();
        assertNull(props.getTime());
    }

    @Test
    void getDateTime_正常ケース_デフォルト値を取得する_nullが返ること() {
        CsvDateTimeFormatProperties props = new CsvDateTimeFormatProperties();
        assertNull(props.getDateTime());
    }

    @Test
    void getDateTimeWithMillis_正常ケース_デフォルト値を取得する_nullが返ること() {
        CsvDateTimeFormatProperties props = new CsvDateTimeFormatProperties();
        assertNull(props.getDateTimeWithMillis());
    }

    @Test
    void setter_正常ケース_各フォーマットを設定して取得する_設定値が返ること() {
        CsvDateTimeFormatProperties props = new CsvDateTimeFormatProperties();
        props.setDate("yyyy-MM-dd");
        props.setTime("HH:mm:ss");
        props.setDateTime("yyyy-MM-dd HH:mm:ss");
        props.setDateTimeWithMillis("yyyy-MM-dd HH:mm:ss.SSS");

        assertEquals("yyyy-MM-dd", props.getDate());
        assertEquals("HH:mm:ss", props.getTime());
        assertEquals("yyyy-MM-dd HH:mm:ss", props.getDateTime());
        assertEquals("yyyy-MM-dd HH:mm:ss.SSS", props.getDateTimeWithMillis());
    }
}
