package io.github.yok.flexdblink.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class DumpConfigTest {

    @Test
    void getExcludeTables_正常ケース_デフォルト値を取得する_Flyway履歴テーブルが返ること() {
        DumpConfig config = new DumpConfig();
        assertNotNull(config.getExcludeTables());
        assertEquals(FlexDbLinkDefaults.DEFAULT_EXCLUDE_TABLES, config.getExcludeTables());
    }

    @Test
    void setExcludeTables_正常ケース_リストを設定して取得する_設定値が返ること() {
        DumpConfig config = new DumpConfig();
        config.setExcludeTables(Arrays.asList("TABLE_A", "TABLE_B"));
        assertEquals(Arrays.asList("TABLE_A", "TABLE_B"), config.getExcludeTables());
    }
}
