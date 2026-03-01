package io.github.yok.flexdblink.maven.plugin.support;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.github.yok.flexdblink.config.ConnectionConfig;
import org.junit.jupiter.api.Test;

class MaskingLogUtilTest {

    @Test
    void maskText_正常ケース_nullを変換する_nullが返ること() {
        assertNull(MaskingLogUtil.maskText(null));
    }

    @Test
    void maskText_正常ケース_空文字を変換する_空文字が返ること() {
        assertEquals("", MaskingLogUtil.maskText(""));
    }

    @Test
    void maskText_正常ケース_通常文字列を変換する_マスク文字列が返ること() {
        assertEquals("***", MaskingLogUtil.maskText("secret"));
    }

    @Test
    void maskJdbcUrl_正常ケース_nullを変換する_nullが返ること() {
        assertNull(MaskingLogUtil.maskJdbcUrl(null));
    }

    @Test
    void maskJdbcUrl_正常ケース_認証情報付きURLを変換する_パスワードがマスクされること() {
        String actual = MaskingLogUtil.maskJdbcUrl(
                "jdbc:postgresql://user:secret@localhost:5432/app");

        assertEquals("jdbc:postgresql://user:***@localhost:5432/app", actual);
    }

    @Test
    void maskJdbcUrl_正常ケース_クエリ文字列を変換する_password値がマスクされること() {
        String actual = MaskingLogUtil.maskJdbcUrl(
                "jdbc:sqlserver://localhost;password=secret;encrypt=true");

        assertTrue(actual.contains("password=***"));
        assertTrue(actual.contains("encrypt=true"));
    }

    @Test
    void maskJdbcUrl_正常ケース_認証情報なしURLを変換する_変更なしで返ること() {
        String url = "jdbc:postgresql://localhost:5432/app";

        String actual = MaskingLogUtil.maskJdbcUrl(url);

        assertEquals(url, actual);
    }

    @Test
    void maskConnection_正常ケース_nullエントリを整形する_null文字列が返ること() {
        assertEquals("<null>", MaskingLogUtil.maskConnection(null));
    }

    @Test
    void maskConnection_正常ケース_接続情報を整形する_秘匿値がマスクされること() {
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("DB1");
        entry.setUrl("jdbc:postgresql://user:secret@localhost:5432/app");
        entry.setUser("app");
        entry.setDriverClass("org.postgresql.Driver");

        String actual = MaskingLogUtil.maskConnection(entry);

        assertTrue(actual.contains("id=DB1"));
        assertTrue(actual.contains("user=***"));
        assertTrue(actual.contains("driverClass=org.postgresql.Driver"));
        assertTrue(actual.contains(
                "jdbc:postgresql://user:***@localhost:5432/app"));
    }
}
