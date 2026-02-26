package io.github.yok.flexdblink.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DbUnitConfig}.
 */
class DbUnitConfigTest {

    @Test
    void getPreDirName_正常ケース_デフォルト値を取得する_preが返ること() {
        DbUnitConfig config = new DbUnitConfig();
        assertEquals("pre", config.getPreDirName());
    }

    @Test
    void isConfirmBeforeLoad_正常ケース_デフォルト値を取得する_falseが返ること() {
        DbUnitConfig config = new DbUnitConfig();
        assertFalse(config.isConfirmBeforeLoad());
    }

    @Test
    void setter_正常ケース_各プロパティを設定して取得する_設定値が返ること() {
        DbUnitConfig config = new DbUnitConfig();
        config.setPreDirName("init");
        config.setConfirmBeforeLoad(true);

        assertEquals("init", config.getPreDirName());
        assertTrue(config.isConfirmBeforeLoad());
    }
}
