package io.github.yok.flexdblink.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;
import org.dbunit.database.IMetadataHandler;
import org.junit.jupiter.api.Test;

class CachedColumnMetadataHandlerTest {
    @Test
    void getColumns_正常ケース_同じテーブルを繰り返し取得する_独立したカーソルと単一のDB取得であること() throws Exception {
        IMetadataHandler original = mock(IMetadataHandler.class);
        DatabaseMetaData jdbc = mock(DatabaseMetaData.class);
        when(original.getColumns(jdbc, "APP", "T")).thenReturn(columns("ID"));
        CachedColumnMetadataHandler handler = new CachedColumnMetadataHandler(original);
        try (ResultSet first = handler.getColumns(jdbc, "APP", "T");
                ResultSet second = handler.getColumns(jdbc, "APP", "T")) {
            assertTrue(first.next());
            assertEquals("ID", first.getString("COLUMN_NAME"));
            assertTrue(second.next());
            assertEquals("ID", second.getString("COLUMN_NAME"));
            assertFalse(first.next());
        }
        try (ResultSet conversion = handler.getConversionColumns(jdbc, "APP", "T")) {
            assertTrue(conversion.next());
            assertEquals("ID", conversion.getString("COLUMN_NAME"));
        }
        verify(original).getColumns(jdbc, "APP", "T");
    }

    @Test
    void getColumns_正常ケース_スキーマとロードを変更する_変更後のメタデータであること() throws Exception {
        IMetadataHandler original = mock(IMetadataHandler.class);
        DatabaseMetaData jdbc = mock(DatabaseMetaData.class);
        when(original.getColumns(jdbc, "APP", "T")).thenReturn(columns("OLD"), columns("NEW"));
        when(original.getColumns(jdbc, null, "T")).thenReturn(columns("OTHER"));
        CachedColumnMetadataHandler handler = new CachedColumnMetadataHandler(original);
        try (ResultSet old = handler.getColumns(jdbc, "APP", "T");
                ResultSet other = handler.getColumns(jdbc, null, "T");
                ResultSet fresh =
                        new CachedColumnMetadataHandler(original).getColumns(jdbc, "APP", "T")) {
            assertTrue(old.next());
            assertEquals("OLD", old.getString(1));
            assertTrue(other.next());
            assertEquals("OTHER", other.getString(1));
            assertTrue(fresh.next());
            assertEquals("NEW", fresh.getString(1));
        }
    }

    @Test
    void getColumns_異常ケース_メタデータ取得が失敗する_例外が通知され再取得可能であること() throws Exception {
        IMetadataHandler original = mock(IMetadataHandler.class);
        DatabaseMetaData jdbc = mock(DatabaseMetaData.class);
        SQLException failure = new SQLException("metadata unavailable");
        when(original.getColumns(jdbc, "APP", "T")).thenThrow(failure).thenReturn(columns("ID"));
        CachedColumnMetadataHandler handler = new CachedColumnMetadataHandler(original);
        assertSame(failure,
                assertThrows(SQLException.class, () -> handler.getColumns(jdbc, "APP", "T")));
        try (ResultSet result = handler.getColumns(jdbc, "APP", "T")) {
            assertTrue(result.next());
        }
        verify(original, times(2)).getColumns(jdbc, "APP", "T");
    }

    @Test
    void getConversionColumns_正常ケース_DBUnitが列定義を提供する_未取得のJDBCメタデータを委譲する結果であること() throws Exception {
        IMetadataHandler original = mock(IMetadataHandler.class);
        DatabaseMetaData jdbc = mock(DatabaseMetaData.class);
        ResultSet result = mock(ResultSet.class);
        when(original.getColumns(jdbc, "APP", "T")).thenReturn(result);
        assertSame(result,
                new CachedColumnMetadataHandler(original).getConversionColumns(jdbc, "APP", "T"));
    }

    @Test
    void matches_正常ケース_独自のメタデータ規則を指定する_元の判定と取得結果であること() throws Exception {
        IMetadataHandler original = mock(IMetadataHandler.class);
        DatabaseMetaData jdbc = mock(DatabaseMetaData.class);
        ResultSet result = mock(ResultSet.class);
        String[] types = {"TABLE"};
        when(original.matches(result, "APP", "T", true)).thenReturn(true);
        when(original.matches(result, "CAT", "APP", "T", "ID", false)).thenReturn(true);
        when(original.getSchema(result)).thenReturn("APP");
        when(original.tableExists(jdbc, "APP", "T")).thenReturn(true);
        when(original.getTables(jdbc, "APP", types)).thenReturn(result);
        when(original.getPrimaryKeys(jdbc, "APP", "T")).thenReturn(result);
        CachedColumnMetadataHandler handler = new CachedColumnMetadataHandler(original);
        assertTrue(handler.matches(result, "APP", "T", true));
        assertTrue(handler.matches(result, "CAT", "APP", "T", "ID", false));
        assertEquals("APP", handler.getSchema(result));
        assertTrue(handler.tableExists(jdbc, "APP", "T"));
        assertSame(result, handler.getTables(jdbc, "APP", types));
        assertSame(result, handler.getPrimaryKeys(jdbc, "APP", "T"));
    }

    private CachedRowSet columns(String name) throws SQLException {
        RowSetMetaDataImpl metadata = new RowSetMetaDataImpl();
        metadata.setColumnCount(1);
        metadata.setColumnName(1, "COLUMN_NAME");
        metadata.setColumnType(1, Types.VARCHAR);
        CachedRowSet rows = RowSetProvider.newFactory().createCachedRowSet();
        rows.setMetaData(metadata);
        rows.moveToInsertRow();
        rows.updateString(1, name);
        rows.insertRow();
        rows.moveToCurrentRow();
        rows.beforeFirst();
        return rows;
    }
}
