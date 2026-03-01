package io.github.yok.flexdblink.util;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link JdbcDriverLoader}.
 */
class JdbcDriverLoaderTest {

    @Test
    void loadIfConfigured_正常ケース_nullを指定する_例外なく完了すること() {
        assertDoesNotThrow(() -> JdbcDriverLoader.loadIfConfigured(null));
    }

    @Test
    void loadIfConfigured_正常ケース_blank文字列を指定する_例外なく完了すること() {
        assertDoesNotThrow(() -> JdbcDriverLoader.loadIfConfigured("   "));
    }

    @Test
    void loadIfConfigured_正常ケース_既存クラス名を指定する_例外なく完了すること() {
        assertDoesNotThrow(() -> JdbcDriverLoader.loadIfConfigured("java.lang.String"));
    }

    @Test
    void loadIfConfigured_異常ケース_未知クラス名を指定する_ClassNotFoundExceptionが送出されること() {
        assertThrows(ClassNotFoundException.class,
                () -> JdbcDriverLoader.loadIfConfigured("com.example.DoesNotExistDriver"));
    }
}
