package io.github.yok.flexdblink.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FileNameResolverTest {

    @Test
    void resolve_正常ケース_パターン定義ありでプレースホルダを含む_置換済みファイル名が返ること() {
        FilePatternConfig config = new FilePatternConfig();
        Map<String, String> cols = new LinkedHashMap<>();
        cols.put("BLOB_COL", "blob_{ID}_{SEQ}.bin");
        Map<String, Map<String, String>> tableMap = new LinkedHashMap<>();
        tableMap.put("TBL", cols);
        config.setFilePatterns(tableMap);

        FileNameResolver resolver = new FileNameResolver(config);
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("ID", 10);
        values.put("SEQ", 2);

        String actual = resolver.resolve("TBL", "BLOB_COL", values, "bin");
        assertEquals("blob_10_2.bin", actual);
    }

    @Test
    void resolve_正常ケース_パターン未定義の場合_デフォルト命名が返ること() {
        FileNameResolver resolver = new FileNameResolver(new FilePatternConfig());
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("ID", 1);
        values.put("SEQ", 9);

        String actual = resolver.resolve("TBL", "BLOB_COL", values, "bin");
        assertEquals("TBL_1_9_BLOB_COL.bin", actual);
    }

    @Test
    void resolve_正常ケース_拡張子がnullの場合_末尾ドット付きで返ること() {
        FileNameResolver resolver = new FileNameResolver(new FilePatternConfig());
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("ID", 1);

        String actual = resolver.resolve("TBL", "CLOB_COL", values, null);
        assertEquals("TBL_1_CLOB_COL.", actual);
    }

    @Test
    void resolve_正常ケース_patternConfigがnullの場合_デフォルト命名が返ること() {
        FileNameResolver resolver = new FileNameResolver(null);
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("ID", 3);

        String actual = resolver.resolve("TBL", "BLOB_COL", values, "bin");
        assertEquals("TBL_3_BLOB_COL.bin", actual);
    }

    @Test
    void resolve_正常ケース_空白パターンを指定する_デフォルト命名が返ること() {
        FilePatternConfig config = new FilePatternConfig();
        Map<String, String> cols = new LinkedHashMap<>();
        cols.put("BLOB_COL", " ");
        Map<String, Map<String, String>> tableMap = new LinkedHashMap<>();
        tableMap.put("TBL", cols);
        config.setFilePatterns(tableMap);

        FileNameResolver resolver = new FileNameResolver(config);
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("ID", 4);

        String actual = resolver.resolve("TBL", "BLOB_COL", values, "bin");
        assertEquals("TBL_4_BLOB_COL.bin", actual);
    }

    @Test
    void resolve_正常ケース_filePatternsがnullの場合_デフォルト命名が返ること() {
        FilePatternConfig config = new FilePatternConfig();
        config.setFilePatterns(null);
        FileNameResolver resolver = new FileNameResolver(config);
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("ID", 5);
        String actual = resolver.resolve("TBL", "BLOB_COL", values, "bin");
        assertEquals("TBL_5_BLOB_COL.bin", actual);
    }
}
