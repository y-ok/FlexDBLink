package io.github.yok.flexdblink.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.sql.Types;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class DbDialectHandlerDefaultMethodTest {

    @Test
    void shouldUseRawTemporalValueForDump_正常ケース_default実装を呼び出す_falseが返ること() {
        DbDialectHandler handler = Mockito.mock(DbDialectHandler.class, Mockito.CALLS_REAL_METHODS);
        assertFalse(handler.shouldUseRawTemporalValueForDump("COL", 0, "VARCHAR2"));
    }

    @Test
    void normalizeRawTemporalValueForDump_正常ケース_default実装を呼び出す_nullは空文字で返ること() {
        DbDialectHandler handler = Mockito.mock(DbDialectHandler.class, Mockito.CALLS_REAL_METHODS);
        assertEquals("", handler.normalizeRawTemporalValueForDump("COL", null));
        assertEquals("v", handler.normalizeRawTemporalValueForDump("COL", "v"));
    }

    @Test
    void shouldUseRawValueForComparison_正常ケース_default実装を呼び出す_falseが返ること() {
        DbDialectHandler handler = Mockito.mock(DbDialectHandler.class, Mockito.CALLS_REAL_METHODS);
        assertFalse(handler.shouldUseRawValueForComparison("VARCHAR2"));
    }

    @Test
    void normalizeValueForComparison_正常ケース_default実装を呼び出す_入力値がそのまま返ること() {
        DbDialectHandler handler = Mockito.mock(DbDialectHandler.class, Mockito.CALLS_REAL_METHODS);
        assertEquals("v", handler.normalizeValueForComparison("COL", "VARCHAR2", "v"));
    }

    @Test
    void isDateTimeTypeForDump_正常ケース_default実装を呼び出す_標準日時型でtrueが返ること() {
        DbDialectHandler handler = Mockito.mock(DbDialectHandler.class, Mockito.CALLS_REAL_METHODS);
        assertTrue(handler.isDateTimeTypeForDump(Types.DATE, "DATE"));
        assertTrue(handler.isDateTimeTypeForDump(Types.TIME, "TIME"));
        assertTrue(handler.isDateTimeTypeForDump(Types.TIMESTAMP, "TIMESTAMP"));
        assertFalse(handler.isDateTimeTypeForDump(Types.VARCHAR, "VARCHAR"));
    }

    @Test
    void isBinaryTypeForDump_正常ケース_default実装を呼び出す_標準バイナリ型でtrueが返ること() {
        DbDialectHandler handler = Mockito.mock(DbDialectHandler.class, Mockito.CALLS_REAL_METHODS);
        assertTrue(handler.isBinaryTypeForDump(Types.BINARY, "RAW"));
        assertTrue(handler.isBinaryTypeForDump(Types.VARBINARY, "VARBINARY"));
        assertTrue(handler.isBinaryTypeForDump(Types.LONGVARBINARY, "LONGVARBINARY"));
        assertFalse(handler.isBinaryTypeForDump(Types.VARCHAR, "VARCHAR"));
    }
}
