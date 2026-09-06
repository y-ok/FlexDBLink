package io.github.yok.flexdblink.db.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;

class OracleLobFormattingTest {
    @Test
    void formatDbValueForCsv_正常ケース_LOBをCSV値に変換する_期待する文字列であること() throws Exception {
        OracleDialectHandler handler = mock(OracleDialectHandler.class, CALLS_REAL_METHODS);
        Blob blob = mock(Blob.class);
        when(blob.length()).thenReturn(2L);
        when(blob.getBytes(1L, 2)).thenReturn(new byte[] {0, 15});
        Clob clob = mock(Clob.class);
        when(clob.length()).thenReturn(4L);
        when(clob.getSubString(1L, 4)).thenReturn("text");
        assertEquals("000F", handler.formatDbValueForCsv("BINARY", blob));
        assertEquals("text", handler.formatDbValueForCsv("TEXT", clob));
    }

    @Test
    void formatDbValueForCsv_異常ケース_LOBの読込が失敗する_SQLExceptionであること() throws Exception {
        OracleDialectHandler handler = mock(OracleDialectHandler.class, CALLS_REAL_METHODS);
        Blob blob = mock(Blob.class);
        when(blob.length()).thenThrow(new SQLException("blob unavailable"));
        Clob clob = mock(Clob.class);
        when(clob.length()).thenThrow(new SQLException("clob unavailable"));
        assertThrows(SQLException.class, () -> handler.formatDbValueForCsv("BINARY", blob));
        assertThrows(SQLException.class, () -> handler.formatDbValueForCsv("TEXT", clob));
    }
}
