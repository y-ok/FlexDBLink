package io.github.yok.flexdblink.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.jupiter.api.Assertions;
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
    void errorAndExit_正常ケース_exit有効でThrowableありを指定する_標準エラーへ出力されること() {
        PrintStream originalErr = System.err;
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        try {
            System.setErr(new PrintStream(err));
            ErrorHandler.errorAndExit("boom", new RuntimeException("root"));
        } finally {
            System.setErr(originalErr);
        }
        String message = err.toString();
        Assertions.assertTrue(message.contains("ERROR: boom"));
        Assertions.assertTrue(message.contains("root"));
    }

    @Test
    void errorAndExit_正常ケース_exit有効でメッセージのみを指定する_例外が送出されないこと() {
        ErrorHandler.errorAndExit("boom2");
    }
}
