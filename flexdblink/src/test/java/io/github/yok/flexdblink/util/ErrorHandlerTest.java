package io.github.yok.flexdblink.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

class ErrorHandlerTest {

    @Test
    void errorAndExit_異常ケース_exit無効を指定する_IllegalStateExceptionが送出されること() {
        ErrorHandler.disableExitForCurrentThread();
        try {
            RuntimeException cause = new RuntimeException("root");
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> ErrorHandler.errorAndExit("boom", cause));
            assertEquals("boom", ex.getMessage());
            assertSame(cause, ex.getCause());

            IllegalStateException ex2 = assertThrows(IllegalStateException.class,
                    () -> ErrorHandler.errorAndExit("boom2"));
            assertEquals("boom2", ex2.getMessage());
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }
    }

    @Test
    void errorAndExit_異常ケース_通常設定で原因例外を渡す_原因を保持した例外通知であること() {
        ErrorHandler.restoreExitForCurrentThread();
        RuntimeException cause = new RuntimeException("root");
        try {
            RuntimeException failure = assertThrows(RuntimeException.class,
                    () -> ErrorHandler.errorAndExit("boom", cause));
            assertEquals("boom", failure.getMessage());
            assertSame(cause, failure.getCause());
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }
    }

    @Test
    void errorAndExit_異常ケース_通常設定でメッセージを渡す_呼び出し元への例外通知であること() {
        ErrorHandler.restoreExitForCurrentThread();
        try {
            RuntimeException failure =
                    assertThrows(RuntimeException.class, () -> ErrorHandler.errorAndExit("boom2"));
            assertEquals("boom2", failure.getMessage());
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }
    }
}
