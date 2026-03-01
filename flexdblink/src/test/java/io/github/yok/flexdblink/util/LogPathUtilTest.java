package io.github.yok.flexdblink.util;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LogPathUtilTest {

    @TempDir
    Path tempDir;

    @Test
    void renderDirForLog_正常ケース_testClasses配下を指定する_相対パスが返ること() {
        Path target = Paths.get(System.getProperty("user.dir"), "target", "test-classes", "a", "b");
        String actual = LogPathUtil.renderDirForLog(target.toFile());
        assertTrue(actual.endsWith("a" + File.separator + "b"));
    }

    @Test
    void renderDirForLog_正常ケース_testClasses配下以外を指定する_絶対パスが返ること() {
        String actual = LogPathUtil.renderDirForLog(tempDir.toFile());
        assertTrue(actual.startsWith(File.separator));
    }

    @Test
    void renderDirForLog_異常ケース_nullを指定する_NullPointerExceptionが送出されること() {
        assertThrows(NullPointerException.class, () -> LogPathUtil.renderDirForLog(null));
    }

}

