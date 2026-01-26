package io.github.yok.flexdblink.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class ErrorHandlerTest {

    @Test
    void errorAndExit_whenExitDisabled_throwsInsteadOfExit() {
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
}
