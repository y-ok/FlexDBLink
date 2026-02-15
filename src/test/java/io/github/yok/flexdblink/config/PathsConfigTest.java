package io.github.yok.flexdblink.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

class PathsConfigTest {

    @Test
    void getLoad_正常ケース_dataPath末尾スラッシュなしを指定する_loadパスが返ること() {
        PathsConfig config = new PathsConfig();
        config.setDataPath("/tmp/data");
        assertEquals("/tmp/data/load", config.getLoad());
    }

    @Test
    void getLoad_正常ケース_dataPath末尾スラッシュありを指定する_loadパスが返ること() {
        PathsConfig config = new PathsConfig();
        config.setDataPath("/tmp/data/");
        assertEquals("/tmp/data/load", config.getLoad());
    }

    @Test
    void getLoad_異常ケース_dataPath未設定を指定する_IllegalStateExceptionが送出されること() {
        PathsConfig config = new PathsConfig();
        assertThrows(IllegalStateException.class, config::getLoad);
    }

    @Test
    void getLoad_異常ケース_dataPath空文字を指定する_IllegalStateExceptionが送出されること() {
        PathsConfig config = new PathsConfig();
        config.setDataPath("");
        assertThrows(IllegalStateException.class, config::getLoad);
    }

    @Test
    void getDump_正常ケース_dataPath末尾スラッシュなしを指定する_dumpパスが返ること() {
        PathsConfig config = new PathsConfig();
        config.setDataPath("/tmp/data");
        assertEquals("/tmp/data/dump", config.getDump());
    }

    @Test
    void getDump_正常ケース_dataPath末尾スラッシュありを指定する_dumpパスが返ること() {
        PathsConfig config = new PathsConfig();
        config.setDataPath("/tmp/data/");
        assertEquals("/tmp/data/dump", config.getDump());
    }

    @Test
    void getDump_異常ケース_dataPath空文字を指定する_IllegalStateExceptionが送出されること() {
        PathsConfig config = new PathsConfig();
        config.setDataPath("");
        assertThrows(IllegalStateException.class, config::getDump);
    }

    @Test
    void getDump_異常ケース_dataPath未設定を指定する_IllegalStateExceptionが送出されること() {
        PathsConfig config = new PathsConfig();
        assertThrows(IllegalStateException.class, config::getDump);
    }
}
