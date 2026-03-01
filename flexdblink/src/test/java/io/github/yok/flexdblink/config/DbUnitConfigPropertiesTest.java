package io.github.yok.flexdblink.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

class DbUnitConfigPropertiesTest {

    @Test
    void isAllowEmptyFields_正常ケース_デフォルト値を取得する_trueが返ること() {
        DbUnitConfigProperties props = new DbUnitConfigProperties();
        assertTrue(props.isAllowEmptyFields());
    }

    @Test
    void isBatchedStatements_正常ケース_デフォルト値を取得する_trueが返ること() {
        DbUnitConfigProperties props = new DbUnitConfigProperties();
        assertTrue(props.isBatchedStatements());
    }

    @Test
    void getBatchSize_正常ケース_デフォルト値を取得する_100が返ること() {
        DbUnitConfigProperties props = new DbUnitConfigProperties();
        assertEquals(100, props.getBatchSize());
    }

    @Test
    void setter_正常ケース_各プロパティを設定して取得する_設定値が返ること() {
        DbUnitConfigProperties props = new DbUnitConfigProperties();
        props.setAllowEmptyFields(false);
        props.setBatchedStatements(false);
        props.setBatchSize(50);

        assertFalse(props.isAllowEmptyFields());
        assertFalse(props.isBatchedStatements());
        assertEquals(50, props.getBatchSize());
    }
}
