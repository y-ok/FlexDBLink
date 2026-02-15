package io.github.yok.flexdblink.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FilePatternConfigTest {

    @Test
    void getPattern_正常ケース_定義済みのテーブル列を指定する_パターンが返ること() {
        FilePatternConfig config = new FilePatternConfig();
        Map<String, String> cols = new HashMap<>();
        cols.put("BLOB_COL", "blob_{ID}.bin");
        Map<String, Map<String, String>> map = new HashMap<>();
        map.put("TBL", cols);
        config.setFilePatterns(map);

        assertTrue(config.getPattern("TBL", "BLOB_COL").isPresent());
        assertEquals("blob_{ID}.bin", config.getPattern("TBL", "BLOB_COL").get());
    }

    @Test
    void getPattern_正常ケース_未定義のテーブル列を指定する_空のOptionalが返ること() {
        FilePatternConfig config = new FilePatternConfig();
        assertTrue(config.getPattern("UNKNOWN", "COL").isEmpty());
    }

    @Test
    void getPatternsForTable_正常ケース_定義済みテーブルを指定する_変更不可Mapが返ること() {
        FilePatternConfig config = new FilePatternConfig();
        Map<String, String> cols = new HashMap<>();
        cols.put("C1", "x");
        Map<String, Map<String, String>> map = new HashMap<>();
        map.put("TBL", cols);
        config.setFilePatterns(map);

        Map<String, String> result = config.getPatternsForTable("TBL");
        assertEquals("x", result.get("C1"));
        assertThrows(UnsupportedOperationException.class, () -> result.put("C2", "y"));
    }

    @Test
    void getPatternsForTable_正常ケース_未定義テーブルを指定する_空Mapが返ること() {
        FilePatternConfig config = new FilePatternConfig();
        assertTrue(config.getPatternsForTable("UNKNOWN").isEmpty());
    }
}

