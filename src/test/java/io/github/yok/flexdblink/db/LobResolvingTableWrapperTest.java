package io.github.yok.flexdblink.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.io.File;
import java.io.IOException;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;
import org.junit.jupiter.api.Test;

class LobResolvingTableWrapperTest {

    @Test
    void getTableMetaData_正常ケース_委譲先を呼び出す_同一オブジェクトが返ること() {
        ITable delegate = mock(ITable.class);
        ITableMetaData metaData = mock(ITableMetaData.class);
        when(delegate.getTableMetaData()).thenReturn(metaData);

        LobResolvingTableWrapper wrapper =
                new LobResolvingTableWrapper(delegate, new File("."), mock(DbDialectHandler.class));
        assertSame(metaData, wrapper.getTableMetaData());
    }

    @Test
    void getRowCount_正常ケース_委譲先を呼び出す_行数が返ること() {
        ITable delegate = mock(ITable.class);
        when(delegate.getRowCount()).thenReturn(3);

        LobResolvingTableWrapper wrapper =
                new LobResolvingTableWrapper(delegate, new File("."), mock(DbDialectHandler.class));
        assertEquals(3, wrapper.getRowCount());
    }

    @Test
    void getValue_正常ケース_file参照文字列を指定する_lob読込結果が返ること() throws Exception {
        ITable delegate = mock(ITable.class);
        ITableMetaData metaData = mock(ITableMetaData.class);
        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);

        when(delegate.getValue(0, "CLOB_COL")).thenReturn("file:a.txt");
        when(delegate.getTableMetaData()).thenReturn(metaData);
        when(metaData.getTableName()).thenReturn("TBL");
        when(dialectHandler.readLobFile("a.txt", "TBL", "CLOB_COL", new File(".")))
                .thenReturn("lob-data");

        LobResolvingTableWrapper wrapper =
                new LobResolvingTableWrapper(delegate, new File("."), dialectHandler);
        Object actual = wrapper.getValue(0, "CLOB_COL");
        assertEquals("lob-data", actual);
    }

    @Test
    void getValue_異常ケース_file参照読み込みでIOExceptionが発生する_DataSetExceptionが送出されること()
            throws Exception {
        ITable delegate = mock(ITable.class);
        ITableMetaData metaData = mock(ITableMetaData.class);
        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);

        when(delegate.getValue(0, "CLOB_COL")).thenReturn("file:a.txt");
        when(delegate.getTableMetaData()).thenReturn(metaData);
        when(metaData.getTableName()).thenReturn("TBL");
        when(dialectHandler.readLobFile("a.txt", "TBL", "CLOB_COL", new File(".")))
                .thenThrow(new IOException("x"));

        LobResolvingTableWrapper wrapper =
                new LobResolvingTableWrapper(delegate, new File("."), dialectHandler);
        assertThrows(DataSetException.class, () -> wrapper.getValue(0, "CLOB_COL"));
    }

    @Test
    void getValue_正常ケース_通常文字列を指定する_DB型変換結果が返ること() throws Exception {
        ITable delegate = mock(ITable.class);
        ITableMetaData metaData = mock(ITableMetaData.class);
        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);

        when(delegate.getValue(0, "ID")).thenReturn("10");
        when(delegate.getTableMetaData()).thenReturn(metaData);
        when(metaData.getTableName()).thenReturn("TBL");
        when(dialectHandler.convertCsvValueToDbType("TBL", "ID", "10")).thenReturn(10);

        LobResolvingTableWrapper wrapper =
                new LobResolvingTableWrapper(delegate, new File("."), dialectHandler);
        Object actual = wrapper.getValue(0, "ID");
        assertEquals(10, actual);
        verify(dialectHandler).convertCsvValueToDbType("TBL", "ID", "10");
    }

    @Test
    void getValue_正常ケース_非文字列値を指定する_そのまま返ること() throws Exception {
        ITable delegate = mock(ITable.class);
        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);
        when(delegate.getValue(0, "NUM")).thenReturn(5);

        LobResolvingTableWrapper wrapper =
                new LobResolvingTableWrapper(delegate, new File("."), dialectHandler);
        Object actual = wrapper.getValue(0, "NUM");
        assertEquals(5, actual);
    }
}

