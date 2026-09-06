package io.github.yok.flexdblink.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.io.File;
import java.io.IOException;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.datatype.DataType;
import org.junit.jupiter.api.Test;

class LobValueReuseTest {
    @Test
    void getValue_正常ケース_同じ参照を繰り返し取得する_列ごとに一度の読み込みであること() throws Exception {
        File directory = new File(".");
        DefaultTable table = new DefaultTable("T",
                new Column[] {new Column("A", DataType.CLOB), new Column("B", DataType.CLOB)});
        table.addRow(new Object[] {"file:shared", "file:shared"});
        table.addRow(new Object[] {"file:shared", "file:shared"});
        DbDialectHandler dialect = mock(DbDialectHandler.class);
        when(dialect.readLobFile("shared", "T", "A", directory)).thenReturn("text");
        when(dialect.readLobFile("shared", "T", "B", directory)).thenReturn("other conversion");
        LobResolvingTableWrapper wrapper = new LobResolvingTableWrapper(table, directory, dialect);
        for (int row = 0; row < 2; row++) {
            assertEquals("text", wrapper.getValue(row, "A"));
            assertEquals("text", wrapper.getValue(row, "A"));
            assertEquals("other conversion", wrapper.getValue(row, "B"));
        }
        verify(dialect).readLobFile("shared", "T", "A", directory);
        verify(dialect).readLobFile("shared", "T", "B", directory);
    }

    @Test
    void getValue_正常ケース_参照またはロードを変更する_変更後の内容であること() throws Exception {
        File directory = new File(".");
        DefaultTable table = new DefaultTable("T", new Column[] {new Column("A", DataType.CLOB)});
        table.addRow(new Object[] {"file:first"});
        table.addRow(new Object[] {"file:second"});
        DbDialectHandler dialect = mock(DbDialectHandler.class);
        when(dialect.readLobFile("first", "T", "A", directory)).thenReturn("old", "reloaded",
                "new");
        when(dialect.readLobFile("second", "T", "A", directory)).thenReturn("second");
        LobResolvingTableWrapper wrapper = new LobResolvingTableWrapper(table, directory, dialect);
        assertEquals("old", wrapper.getValue(0, "A"));
        assertEquals("second", wrapper.getValue(1, "A"));
        assertEquals("reloaded", wrapper.getValue(0, "A"));
        assertEquals("new",
                new LobResolvingTableWrapper(table, directory, dialect).getValue(0, "A"));
        verify(dialect, times(3)).readLobFile("first", "T", "A", directory);
    }

    @Test
    void getValue_異常ケース_読み込みが失敗する_再取得時に正常な内容であること() throws Exception {
        File directory = new File(".");
        DefaultTable table = new DefaultTable("T", new Column[] {new Column("A", DataType.CLOB)});
        table.addRow(new Object[] {"file:value"});
        DbDialectHandler dialect = mock(DbDialectHandler.class);
        when(dialect.readLobFile("value", "T", "A", directory))
                .thenThrow(new IOException("unavailable")).thenReturn(null);
        LobResolvingTableWrapper wrapper = new LobResolvingTableWrapper(table, directory, dialect);
        assertThrows(DataSetException.class, () -> wrapper.getValue(0, "A"));
        assertNull(wrapper.getValue(0, "A"));
        assertNull(wrapper.getValue(0, "A"));
        verify(dialect, times(2)).readLobFile("value", "T", "A", directory);
    }
}
