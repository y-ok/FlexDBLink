package io.github.yok.flexdblink.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

class DumpResultTest {

    @Test
    void getRowCount_正常ケース_コンストラクタで設定した値を取得する_設定値が返ること() {
        DumpResult result = new DumpResult(10, 3);
        assertEquals(10, result.getRowCount());
    }

    @Test
    void getFileCount_正常ケース_コンストラクタで設定した値を取得する_設定値が返ること() {
        DumpResult result = new DumpResult(10, 3);
        assertEquals(3, result.getFileCount());
    }

    @Test
    void equals_正常ケース_同値のオブジェクトと比較する_trueが返ること() {
        DumpResult a = new DumpResult(5, 2);
        DumpResult b = new DumpResult(5, 2);
        assertEquals(a, b);
    }

    @Test
    void equals_正常ケース_異なる値のオブジェクトと比較する_falseが返ること() {
        DumpResult a = new DumpResult(5, 2);
        DumpResult b = new DumpResult(5, 3);
        assertNotEquals(a, b);
    }

    @Test
    void hashCode_正常ケース_同値のオブジェクトのhashCodeを取得する_同じ値が返ること() {
        DumpResult a = new DumpResult(5, 2);
        DumpResult b = new DumpResult(5, 2);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void toString_正常ケース_文字列表現を取得する_フィールド値を含む文字列が返ること() {
        DumpResult result = new DumpResult(10, 3);
        String str = result.toString();
        assertTrue(str.contains("10"));
        assertTrue(str.contains("3"));
    }
}
