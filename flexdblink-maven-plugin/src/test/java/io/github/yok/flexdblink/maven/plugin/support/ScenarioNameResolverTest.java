package io.github.yok.flexdblink.maven.plugin.support;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class ScenarioNameResolverTest {

    private final ScenarioNameResolver target = new ScenarioNameResolver();
    private final Clock fixedClock =
            Clock.fixed(Instant.parse("2026-02-26T12:34:56Z"), ZoneOffset.UTC);

    @Test
    void resolveForLoad_正常ケース_nullを解決する_nullが返ること() {
        assertNull(target.resolveForLoad(null));
    }

    @Test
    void resolveForLoad_正常ケース_空白文字列を解決する_nullが返ること() {
        assertNull(target.resolveForLoad("   "));
    }

    @Test
    void resolveForLoad_正常ケース_空文字列を解決する_nullが返ること() {
        assertNull(target.resolveForLoad(""));
    }

    @Test
    void resolveForLoad_正常ケース_指定値を解決する_trim済み文字列が返ること() {
        assertEquals("scenarioA", target.resolveForLoad("  scenarioA  "));
    }

    @Test
    void resolveForDump_正常ケース_nullを解決する_14桁タイムスタンプが返ること() {
        String actual = target.resolveForDump(null, fixedClock);

        assertEquals("20260226123456", actual);
        assertTrue(actual.matches("\\d{14}"));
    }

    @Test
    void resolveForDump_正常ケース_空白文字列を解決する_14桁タイムスタンプが返ること() {
        String actual = target.resolveForDump("   ", fixedClock);

        assertEquals("20260226123456", actual);
    }

    @Test
    void resolveForDump_正常ケース_空文字列を解決する_14桁タイムスタンプが返ること() {
        String actual = target.resolveForDump("", fixedClock);

        assertEquals("20260226123456", actual);
    }

    @Test
    void resolveForDump_正常ケース_指定値を解決する_trim済み文字列が返ること() {
        String actual = target.resolveForDump("  manual  ", fixedClock);

        assertEquals("manual", actual);
    }

    @Test
    void generateTimestamp_正常ケース_年末境界を生成する_yyyy形式の文字列が返ること() {
        Clock fixed =
                Clock.fixed(Instant.parse("2026-12-31T23:59:59Z"), ZoneOffset.UTC);

        String actual = target.generateTimestamp(fixed);

        assertEquals("20261231235959", actual);
    }
}
