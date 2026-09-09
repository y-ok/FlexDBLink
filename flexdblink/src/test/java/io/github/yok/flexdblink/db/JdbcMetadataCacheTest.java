package io.github.yok.flexdblink.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class JdbcMetadataCacheTest {
    @Test
    void wrap_正常ケース_接続を変更して列とキーを取得する_DB取得一回と独立したカーソルであること() throws Exception {
        Connection first = connection("jdbc:test:one", "USER", "CAT", "APP");
        Connection second = connection("jdbc:test:one", "USER", "CAT", "APP");
        DatabaseMetaData original = first.getMetaData();
        when(original.getColumns("CAT", "APP", "T", "%")).thenReturn(rows("COLUMN"));
        when(original.getPrimaryKeys("CAT", "APP", "T")).thenReturn(rows("PRIMARY"));
        when(original.getImportedKeys("CAT", "APP", "T")).thenReturn(rows("FOREIGN"));
        JdbcMetadataCache cache = new JdbcMetadataCache();
        Connection wrapped = cache.wrap(first);
        DatabaseMetaData a = wrapped.getMetaData();
        assertSame(a, wrapped.getMetaData());
        try (ResultSet columns = a.getColumns("CAT", "APP", "T", "%")) {
            assertEquals("COLUMN", value(columns));
        }
        assertEquals("PRIMARY", value(a.getPrimaryKeys("CAT", "APP", "T")));
        assertEquals("FOREIGN", value(a.getImportedKeys("CAT", "APP", "T")));
        wrapped.close();
        verify(first).close();
        DatabaseMetaData b = cache.wrap(second).getMetaData();
        try (ResultSet one = b.getColumns("CAT", "APP", "T", "%");
                ResultSet two = b.getColumns("CAT", "APP", "T", "%")) {
            assertTrue(one.next());
            assertEquals("COLUMN", one.getString(1));
            assertTrue(two.next());
            assertEquals("COLUMN", two.getString(1));
            assertFalse(one.next());
        }
        assertEquals("PRIMARY", value(b.getPrimaryKeys("CAT", "APP", "T")));
        assertEquals("FOREIGN", value(b.getImportedKeys("CAT", "APP", "T")));
        verify(original).getColumns("CAT", "APP", "T", "%");
        verify(original).getPrimaryKeys("CAT", "APP", "T");
        verify(original).getImportedKeys("CAT", "APP", "T");
        verify(second.getMetaData(), never()).getColumns(any(), any(), any(), any());
        verify(second.getMetaData(), never()).getPrimaryKeys(any(), any(), any());
        verify(second.getMetaData(), never()).getImportedKeys(any(), any(), any());
    }

    @ParameterizedTest
    @ValueSource(strings = {"url", "user", "catalog", "schema"})
    void wrap_正常ケース_接続先または名前空間を変更する_別のメタデータであること(String dimension) throws Exception {
        Connection first = connection("jdbc:test:one", "USER", "CAT", "APP");
        Connection second = connection("jdbc:test:one", "USER", "CAT", "APP");
        switch (dimension) {
            case "url":
                when(second.getMetaData().getURL()).thenReturn("jdbc:test:two");
                break;
            case "user":
                when(second.getMetaData().getUserName()).thenReturn("OTHER");
                break;
            case "catalog":
                when(second.getCatalog()).thenReturn("OTHER");
                break;
            default:
                when(second.getSchema()).thenReturn("OTHER");
                break;
        }
        when(first.getMetaData().getColumns(null, null, "T", null)).thenReturn(rows("FIRST"));
        when(second.getMetaData().getColumns(null, null, "T", null)).thenReturn(rows("SECOND"));
        JdbcMetadataCache cache = new JdbcMetadataCache();
        assertEquals("FIRST",
                value(cache.wrap(first).getMetaData().getColumns(null, null, "T", null)));
        assertEquals("SECOND",
                value(cache.wrap(second).getMetaData().getColumns(null, null, "T", null)));
    }

    @Test
    void clear_正常ケース_スキーマ変更後に破棄する_変更後の列とキーであること() throws Exception {
        Connection jdbc = connection("jdbc:test:one", "USER", null, null);
        DatabaseMetaData original = jdbc.getMetaData();
        when(original.getColumns(null, "APP", "T", "%")).thenReturn(rows("OLD"), rows("NEW"));
        when(original.getPrimaryKeys(null, "APP", "T")).thenReturn(rows("OLD_PK"), rows("NEW_PK"));
        when(original.getImportedKeys(null, "APP", "T")).thenReturn(rows("OLD_FK"), rows("NEW_FK"));
        when(original.getColumns(null, "APP", "t", "%")).thenReturn(rows("LOWER"));
        when(original.getColumns(null, "APP", "T", null)).thenReturn(rows("NULL_PATTERN"));
        JdbcMetadataCache cache = new JdbcMetadataCache();
        DatabaseMetaData metadata = cache.wrap(jdbc).getMetaData();
        assertEquals("OLD", value(metadata.getColumns(null, "APP", "T", "%")));
        assertEquals("OLD_PK", value(metadata.getPrimaryKeys(null, "APP", "T")));
        assertEquals("OLD_FK", value(metadata.getImportedKeys(null, "APP", "T")));
        assertEquals("LOWER", value(metadata.getColumns(null, "APP", "t", "%")));
        assertEquals("NULL_PATTERN", value(metadata.getColumns(null, "APP", "T", null)));
        cache.clear();
        assertEquals("NEW", value(metadata.getColumns(null, "APP", "T", "%")));
        assertEquals("NEW_PK", value(metadata.getPrimaryKeys(null, "APP", "T")));
        assertEquals("NEW_FK", value(metadata.getImportedKeys(null, "APP", "T")));
    }

    @Test
    void wrap_正常ケース_空のキーと通常のJDBC操作を実行する_空結果の再利用と元の操作結果であること() throws Exception {
        Connection jdbc = connection("jdbc:test:one", "USER", null, "APP");
        CachedRowSet empty = rows("unused");
        empty.next();
        empty.deleteRow();
        empty.beforeFirst();
        when(jdbc.getMetaData().getImportedKeys(null, "APP", "T")).thenReturn(empty);
        when(jdbc.getMetaData().getDatabaseProductName()).thenReturn("Oracle");
        Connection wrapped = new JdbcMetadataCache().wrap(jdbc);
        wrapped.rollback();
        verify(jdbc).rollback();
        assertEquals("Oracle", wrapped.getMetaData().getDatabaseProductName());
        assertFalse(wrapped.getMetaData().getImportedKeys(null, "APP", "T").next());
        assertFalse(wrapped.getMetaData().getImportedKeys(null, "APP", "T").next());
        verify(jdbc.getMetaData()).getImportedKeys(null, "APP", "T");
    }

    @Test
    void wrap_異常ケース_JDBC操作とメタデータ読込に失敗する_元の例外と再取得可能な状態であること() throws Exception {
        Connection jdbc = connection("jdbc:test:one", "USER", null, "APP");
        SQLException failure = new SQLException("unavailable");
        when(jdbc.getAutoCommit()).thenThrow(failure);
        when(jdbc.getMetaData().getColumns(null, "APP", "T", "%")).thenThrow(failure)
                .thenReturn(rows("RETRIED"));
        Connection wrapped = new JdbcMetadataCache().wrap(jdbc);
        assertSame(failure, assertThrows(SQLException.class, wrapped::getAutoCommit));
        DatabaseMetaData metadata = wrapped.getMetaData();
        assertSame(failure,
                assertThrows(SQLException.class, () -> metadata.getColumns(null, "APP", "T", "%")));
        assertEquals("RETRIED", value(metadata.getColumns(null, "APP", "T", "%")));
        verify(jdbc.getMetaData(), times(2)).getColumns(null, "APP", "T", "%");
    }

    @Test
    void wrap_異常ケース_結果セットの読込に失敗する_結果セットの解放と再取得可能な状態であること() throws Exception {
        Connection jdbc = connection("jdbc:test:one", "USER", null, "APP");
        ResultSet broken = mock(ResultSet.class);
        SQLException failure = new SQLException("read failed");
        when(broken.getMetaData()).thenThrow(failure);
        when(jdbc.getMetaData().getPrimaryKeys(null, "APP", "T")).thenReturn(broken,
                rows("RETRIED"));
        DatabaseMetaData metadata = new JdbcMetadataCache().wrap(jdbc).getMetaData();
        assertSame(failure,
                assertThrows(SQLException.class, () -> metadata.getPrimaryKeys(null, "APP", "T")));
        verify(broken).close();
        assertEquals("RETRIED", value(metadata.getPrimaryKeys(null, "APP", "T")));
    }

    @Test
    void wrap_正常ケース_テーブルとスキーマを別接続から取得する_DB取得一回と独立したカーソルであること() throws Exception {
        Connection first = connection("jdbc:test:one", "USER", "CAT", "APP");
        Connection second = connection("jdbc:test:one", "USER", "CAT", "APP");
        DatabaseMetaData original = first.getMetaData();
        when(original.getTables("CAT", "APP", "%", new String[] {"TABLE"}))
                .thenReturn(rows("TABLES"), rows("NEW_TABLES"));
        when(original.getTables("CAT", "APP", "%", new String[] {"VIEW"}))
                .thenReturn(rows("VIEWS"));
        when(original.getTables("CAT", "APP", "%", null)).thenReturn(rows("ALL_TYPES"));
        when(original.getSchemas()).thenReturn(rows("SCHEMAS"), rows("NEW_SCHEMAS"));
        when(original.getSchemas("CAT", "APP")).thenReturn(rows("FILTERED"), rows("NEW_FILTERED"));
        JdbcMetadataCache cache = new JdbcMetadataCache();
        DatabaseMetaData a = cache.wrap(first).getMetaData();
        String[] types = {"TABLE"};
        assertEquals("TABLES", value(a.getTables("CAT", "APP", "%", types)));
        verify(original).getTables("CAT", "APP", "%", new String[] {"TABLE"});
        types[0] = "VIEW";
        assertEquals("VIEWS", value(a.getTables("CAT", "APP", "%", types)));
        assertEquals("ALL_TYPES", value(a.getTables("CAT", "APP", "%", null)));
        assertEquals("SCHEMAS", value(a.getSchemas()));
        assertEquals("FILTERED", value(a.getSchemas("CAT", "APP")));
        DatabaseMetaData b = cache.wrap(second).getMetaData();
        try (ResultSet one = b.getTables("CAT", "APP", "%", new String[] {"TABLE"});
                ResultSet two = b.getTables("CAT", "APP", "%", new String[] {"TABLE"})) {
            assertTrue(one.next());
            assertEquals("TABLES", one.getString(1));
            assertEquals("TABLES", value(two));
            assertFalse(one.next());
        }
        assertEquals("SCHEMAS", value(b.getSchemas()));
        assertEquals("FILTERED", value(b.getSchemas("CAT", "APP")));
        verify(original).getSchemas();
        verify(original).getSchemas("CAT", "APP");
        verify(second.getMetaData(), never()).getTables(any(), any(), any(), any());
        verify(second.getMetaData(), never()).getSchemas();
        verify(second.getMetaData(), never()).getSchemas(any(), any());
        cache.clear();
        assertEquals("NEW_TABLES", value(a.getTables("CAT", "APP", "%", new String[] {"TABLE"})));
        assertEquals("NEW_SCHEMAS", value(a.getSchemas()));
        assertEquals("NEW_FILTERED", value(a.getSchemas("CAT", "APP")));
    }

    private Connection connection(String url, String user, String catalog, String schema)
            throws SQLException {
        Connection jdbc = mock(Connection.class);
        DatabaseMetaData metadata = mock(DatabaseMetaData.class);
        when(jdbc.getMetaData()).thenReturn(metadata);
        when(metadata.getURL()).thenReturn(url);
        when(metadata.getUserName()).thenReturn(user);
        when(jdbc.getCatalog()).thenReturn(catalog);
        when(jdbc.getSchema()).thenReturn(schema);
        return jdbc;
    }

    private String value(ResultSet result) throws SQLException {
        try (ResultSet cursor = result) {
            assertTrue(cursor.next());
            return cursor.getString(1);
        }
    }

    private CachedRowSet rows(String name) throws SQLException {
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
