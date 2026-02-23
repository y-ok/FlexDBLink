package io.github.yok.flexdblink.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.github.yok.flexdblink.db.DbDialectHandler;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.datatype.DataType;
import org.junit.jupiter.api.Test;

class ScenarioDuplicateHandlerTest {

    private final ScenarioDuplicateHandler handler = new ScenarioDuplicateHandler();

    // -------------------------------------------------------------------------
    // rowsEqual
    // -------------------------------------------------------------------------

    @Test
    void rowsEqual_正常ケース_INTERVAL列が同値である_trueが返ること() throws Exception {
        ITable csvTable = mock(ITable.class);
        ITable dbTable = mock(ITable.class);
        Connection jdbc = mock(Connection.class);
        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);
        Column[] cols = new Column[] {new Column("IYM", DataType.VARCHAR)};
        when(dialectHandler.getColumnTypeName(jdbc, "APP", "TBL", "IYM"))
                .thenReturn("INTERVAL YEAR TO MONTH");
        when(dialectHandler.shouldUseRawValueForComparison("INTERVAL YEAR TO MONTH"))
                .thenReturn(true);
        when(dialectHandler.normalizeValueForComparison("IYM", "INTERVAL YEAR TO MONTH", "+2-3"))
                .thenReturn("+02-03");
        when(dialectHandler.normalizeValueForComparison("IYM", "INTERVAL YEAR TO MONTH", "2-3"))
                .thenReturn("+02-03");
        when(csvTable.getValue(0, "IYM")).thenReturn("+2-3");
        when(dbTable.getValue(0, "IYM")).thenReturn("2-3");

        assertTrue(handler.rowsEqual(csvTable, dbTable, jdbc, "APP", "TBL", 0, 0, cols,
                dialectHandler));
    }

    @Test
    void rowsEqual_正常ケース_通常列が不一致である_falseが返ること() throws Exception {
        ITable csvTable = mock(ITable.class);
        ITable dbTable = mock(ITable.class);
        Connection jdbc = mock(Connection.class);
        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);
        Column[] cols = new Column[] {new Column("C1", DataType.VARCHAR)};
        when(dialectHandler.getColumnTypeName(jdbc, "APP", "TBL", "C1")).thenReturn("VARCHAR2");
        when(dialectHandler.formatDbValueForCsv("C1", "B")).thenReturn("B");
        when(csvTable.getValue(0, "C1")).thenReturn("A");
        when(dbTable.getValue(0, "C1")).thenReturn("B");

        assertFalse(handler.rowsEqual(csvTable, dbTable, jdbc, "APP", "TBL", 0, 0, cols,
                dialectHandler));
    }

    @Test
    void rowsEqual_正常ケース_DAYSECONDと空白列を比較する_trueが返ること() throws Exception {
        ITable csvTable = mock(ITable.class);
        ITable dbTable = mock(ITable.class);
        Connection jdbc = mock(Connection.class);
        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);
        Column[] cols = new Column[] {new Column("IDS", DataType.VARCHAR),
                new Column("C1", DataType.VARCHAR)};
        when(dialectHandler.getColumnTypeName(jdbc, "APP", "TBL", "IDS"))
                .thenReturn("INTERVAL DAY TO SECOND");
        when(dialectHandler.shouldUseRawValueForComparison("INTERVAL DAY TO SECOND"))
                .thenReturn(true);
        when(dialectHandler.normalizeValueForComparison("IDS", "INTERVAL DAY TO SECOND", "1 2:3:4"))
                .thenReturn("+01 02:03:04");
        when(dialectHandler.normalizeValueForComparison("IDS", "INTERVAL DAY TO SECOND",
                "+01 02:03:04")).thenReturn("+01 02:03:04");
        when(dialectHandler.getColumnTypeName(jdbc, "APP", "TBL", "C1")).thenReturn("VARCHAR2");
        when(dialectHandler.formatDbValueForCsv("C1", " ")).thenReturn(" ");
        when(dialectHandler.normalizeValueForComparison("C1", "VARCHAR2", null)).thenReturn(null);
        when(csvTable.getValue(0, "IDS")).thenReturn("1 2:3:4");
        when(dbTable.getValue(0, "IDS")).thenReturn("+01 02:03:04");
        when(csvTable.getValue(0, "C1")).thenReturn(" ");
        when(dbTable.getValue(0, "C1")).thenReturn(" ");

        assertTrue(handler.rowsEqual(csvTable, dbTable, jdbc, "APP", "TBL", 0, 0, cols,
                dialectHandler));
    }

    @Test
    void rowsEqual_正常ケース_DB値がnullである_trueが返ること() throws Exception {
        ITable csv = mock(ITable.class);
        ITable db = mock(ITable.class);
        Connection jdbc = mock(Connection.class);
        DbDialectHandler dialect = mock(DbDialectHandler.class);
        Column[] cols = new Column[] {new Column("C1", DataType.VARCHAR)};
        when(dialect.getColumnTypeName(jdbc, "APP", "T1", "C1")).thenReturn("VARCHAR2");
        when(dialect.formatDbValueForCsv("C1", null)).thenReturn(null);
        when(csv.getValue(0, "C1")).thenReturn(null);
        when(db.getValue(0, "C1")).thenReturn(null);

        assertTrue(handler.rowsEqual(csv, db, jdbc, "APP", "T1", 0, 0, cols, dialect));
    }

    // -------------------------------------------------------------------------
    // detectDuplicates
    // -------------------------------------------------------------------------

    @Test
    void detectDuplicates_正常ケース_PK一致行がある_重複マップが返ること() throws Exception {
        ITable csvTable = mock(ITable.class);
        ITable dbTable = mock(ITable.class);
        ITableMetaData metaData = mock(ITableMetaData.class);
        when(csvTable.getRowCount()).thenReturn(1);
        when(dbTable.getRowCount()).thenReturn(1);
        when(csvTable.getTableMetaData()).thenReturn(metaData);
        when(metaData.getColumns()).thenReturn(new Column[] {new Column("ID", DataType.VARCHAR)});
        when(csvTable.getValue(0, "ID")).thenReturn("1");
        when(dbTable.getValue(0, "ID")).thenReturn("1");

        Map<Integer, Integer> actual = handler.detectDuplicates(csvTable, dbTable, List.of("ID"),
                mock(Connection.class), "APP", "TBL", mock(DbDialectHandler.class));

        assertEquals(1, actual.size());
        assertEquals(0, actual.get(0));
    }

    @Test
    void detectDuplicates_異常ケース_比較中に例外が起きる_DataSetExceptionが送出されること() throws Exception {
        ITable csvTable = mock(ITable.class);
        ITable dbTable = mock(ITable.class);
        when(csvTable.getRowCount()).thenThrow(new RuntimeException("x"));

        assertThrows(DataSetException.class, () -> handler.detectDuplicates(csvTable, dbTable,
                List.of("ID"), mock(Connection.class), "APP", "TBL", mock(DbDialectHandler.class)));
    }

    @Test
    void detectDuplicates_正常ケース_PK不一致がある_重複に追加されないこと() throws Exception {
        ITable csv = mock(ITable.class);
        ITable db = mock(ITable.class);
        ITableMetaData meta = mock(ITableMetaData.class);
        when(csv.getTableMetaData()).thenReturn(meta);
        when(meta.getColumns()).thenReturn(new Column[] {new Column("ID", DataType.VARCHAR)});
        when(csv.getRowCount()).thenReturn(1);
        when(db.getRowCount()).thenReturn(1);
        when(csv.getValue(0, "ID")).thenReturn("1");
        when(db.getValue(0, "ID")).thenReturn("2");

        Map<Integer, Integer> result = handler.detectDuplicates(csv, db, List.of("ID"),
                mock(Connection.class), "APP", "T1", mock(DbDialectHandler.class));
        assertTrue(result.isEmpty());
    }

    @Test
    void detectDuplicates_正常ケース_PKなし重複がある_重複マップが返ること() throws Exception {
        ITable csv = mock(ITable.class);
        ITable db = mock(ITable.class);
        ITableMetaData meta = mock(ITableMetaData.class);
        when(csv.getTableMetaData()).thenReturn(meta);
        when(meta.getColumns()).thenReturn(new Column[] {new Column("C1", DataType.VARCHAR)});
        when(csv.getRowCount()).thenReturn(1);
        when(db.getRowCount()).thenReturn(1);
        when(csv.getValue(0, "C1")).thenReturn("A");
        when(db.getValue(0, "C1")).thenReturn("A");
        DbDialectHandler dialect = mock(DbDialectHandler.class);
        when(dialect.getColumnTypeName(any(), eq("APP"), eq("T1"), eq("C1")))
                .thenReturn("VARCHAR2");
        when(dialect.formatDbValueForCsv("C1", "A")).thenReturn("A");

        Map<Integer, Integer> result = handler.detectDuplicates(csv, db, List.of(),
                mock(Connection.class), "APP", "T1", dialect);
        assertEquals(0, result.get(0));
    }

    @Test
    void detectDuplicates_正常ケース_PK列null同士を比較する_重複マップが返ること() throws Exception {
        ITable csv = mock(ITable.class);
        ITable db = mock(ITable.class);
        ITableMetaData meta = mock(ITableMetaData.class);
        when(csv.getTableMetaData()).thenReturn(meta);
        when(meta.getColumns()).thenReturn(new Column[] {new Column("ID", DataType.VARCHAR)});
        when(csv.getRowCount()).thenReturn(1);
        when(db.getRowCount()).thenReturn(1);
        when(csv.getValue(0, "ID")).thenReturn(null);
        when(db.getValue(0, "ID")).thenReturn(null);

        Map<Integer, Integer> result = handler.detectDuplicates(csv, db, List.of("ID"),
                mock(Connection.class), "APP", "T1", mock(DbDialectHandler.class));
        assertEquals(0, result.get(0));
    }

    @Test
    void detectDuplicates_正常ケース_PK列が片側nullである_重複に追加されないこと() throws Exception {
        ITable csv = mock(ITable.class);
        ITable db = mock(ITable.class);
        ITableMetaData meta = mock(ITableMetaData.class);
        when(csv.getTableMetaData()).thenReturn(meta);
        when(meta.getColumns()).thenReturn(new Column[] {new Column("ID", DataType.VARCHAR)});
        when(csv.getRowCount()).thenReturn(1);
        when(db.getRowCount()).thenReturn(1);
        when(csv.getValue(0, "ID")).thenReturn(null);
        when(db.getValue(0, "ID")).thenReturn("1");

        Map<Integer, Integer> result = handler.detectDuplicates(csv, db, List.of("ID"),
                mock(Connection.class), "APP", "T1", mock(DbDialectHandler.class));
        assertTrue(result.isEmpty());
    }

    // -------------------------------------------------------------------------
    // deleteDuplicates
    // -------------------------------------------------------------------------

    @Test
    void deleteDuplicates_正常ケース_null値列をISNULL条件で削除すること() throws Exception {
        Connection jdbc = mock(Connection.class);
        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);
        when(dialectHandler.quoteIdentifier("APP")).thenReturn("\"APP\"");
        when(dialectHandler.quoteIdentifier("TBL")).thenReturn("\"TBL\"");
        when(dialectHandler.quoteIdentifier("COL_A")).thenReturn("\"COL_A\"");
        when(dialectHandler.quoteIdentifier("COL_B")).thenReturn("\"COL_B\"");

        ITable originalDbTable = mock(ITable.class);
        when(originalDbTable.getValue(0, "COL_A")).thenReturn(null);
        when(originalDbTable.getValue(0, "COL_B")).thenReturn("VAL_B");

        Column colA = mock(Column.class);
        Column colB = mock(Column.class);
        when(colA.getColumnName()).thenReturn("COL_A");
        when(colB.getColumnName()).thenReturn("COL_B");
        Column[] cols = new Column[] {colA, colB};

        String expectedSql =
                "DELETE FROM \"APP\".\"TBL\" WHERE \"COL_A\" IS NULL AND \"COL_B\" = ?";
        PreparedStatement ps = mock(PreparedStatement.class);
        when(jdbc.prepareStatement(expectedSql)).thenReturn(ps);
        when(ps.executeUpdate()).thenReturn(1);

        handler.deleteDuplicates(jdbc, "APP", "TBL", List.of(), cols, originalDbTable, Map.of(0, 0),
                dialectHandler, "DB1");

        verify(jdbc).prepareStatement(expectedSql);
        verify(ps).setObject(1, "VAL_B");
        verify(ps, never()).setObject(2, null);
        verify(ps).executeUpdate();
    }

    @Test
    void deleteDuplicates_正常ケース_PK列で重複削除する_executeBatchが呼ばれること() throws Exception {
        Connection jdbc = mock(Connection.class);
        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);
        when(dialectHandler.quoteIdentifier("APP")).thenReturn("\"APP\"");
        when(dialectHandler.quoteIdentifier("TBL")).thenReturn("\"TBL\"");
        when(dialectHandler.quoteIdentifier("ID")).thenReturn("\"ID\"");

        ITable originalDbTable = mock(ITable.class);
        when(originalDbTable.getValue(0, "ID")).thenReturn(100);

        PreparedStatement ps = mock(PreparedStatement.class);
        when(jdbc.prepareStatement("DELETE FROM \"APP\".\"TBL\" WHERE \"ID\" = ?")).thenReturn(ps);
        when(ps.executeBatch()).thenReturn(new int[] {1});

        handler.deleteDuplicates(jdbc, "APP", "TBL", List.of("ID"),
                new Column[] {new Column("ID", DataType.INTEGER)}, originalDbTable, Map.of(0, 0),
                dialectHandler, "DB1");

        verify(ps).setObject(1, 100);
        verify(ps, times(1)).addBatch();
        verify(ps).executeBatch();
    }

    @Test
    void deleteDuplicates_異常ケース_SQL例外が発生する_DataSetExceptionが送出されること() throws Exception {
        Connection jdbc = mock(Connection.class);
        when(jdbc.prepareStatement(anyString())).thenThrow(new java.sql.SQLException("x"));
        DbDialectHandler dialect = mock(DbDialectHandler.class);
        when(dialect.quoteIdentifier(anyString()))
                .thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");
        ITable original = mock(ITable.class);
        when(original.getValue(0, "ID")).thenReturn("1");

        assertThrows(DataSetException.class,
                () -> handler.deleteDuplicates(jdbc, "APP", "T1", List.of("ID"),
                        new Column[] {new Column("ID", DataType.VARCHAR)}, original, Map.of(0, 0),
                        dialect, "db1"));
    }

    @Test
    void deleteDuplicates_正常ケース_重複マップが空である_何も実行されないこと() throws Exception {
        Connection jdbc = mock(Connection.class);
        handler.deleteDuplicates(jdbc, "APP", "T1", List.of(), new Column[0], mock(ITable.class),
                Map.of(), mock(DbDialectHandler.class), "db1");
        verify(jdbc, never()).prepareStatement(anyString());
    }

    // -------------------------------------------------------------------------
    // FilteredTable
    // -------------------------------------------------------------------------

    @Test
    void FilteredTable_正常ケース_除外行を指定する_行数と値が正しく返ること() throws Exception {
        ITable delegate = mock(ITable.class);
        ITableMetaData metaData = mock(ITableMetaData.class);
        when(delegate.getTableMetaData()).thenReturn(metaData);
        when(delegate.getRowCount()).thenReturn(4);
        when(delegate.getValue(0, "ID")).thenReturn("0");
        when(delegate.getValue(2, "ID")).thenReturn("2");

        ScenarioDuplicateHandler.FilteredTable filtered =
                new ScenarioDuplicateHandler.FilteredTable(delegate, Set.of(1, 3));

        assertEquals(2, filtered.getRowCount());
        assertSame(metaData, filtered.getTableMetaData());
        assertEquals("0", filtered.getValue(0, "ID"));
        assertEquals("2", filtered.getValue(1, "ID"));
    }

    @Test
    void filteredTable_skipRowsなし_全行が正しく取得できること() throws Exception {
        ITable delegate = mock(ITable.class);
        when(delegate.getRowCount()).thenReturn(3);
        ITableMetaData meta = mock(ITableMetaData.class);
        when(delegate.getTableMetaData()).thenReturn(meta);
        when(delegate.getValue(0, "ID")).thenReturn("A");
        when(delegate.getValue(1, "ID")).thenReturn("B");
        when(delegate.getValue(2, "ID")).thenReturn("C");

        ScenarioDuplicateHandler.FilteredTable filtered =
                new ScenarioDuplicateHandler.FilteredTable(delegate, Set.of());

        assertEquals(3, filtered.getRowCount());
        assertEquals("A", filtered.getValue(0, "ID"));
        assertEquals("B", filtered.getValue(1, "ID"));
        assertEquals("C", filtered.getValue(2, "ID"));
    }

    @Test
    void filteredTable_複数行スキップ_論理インデックスが正しくシフトされること() throws Exception {
        // 物理行 0,1,2,3,4 のうち行インデックス 1,3 をスキップ
        // → 論理行 0,1,2 は物理行 0,2,4 にそれぞれ対応する
        ITable delegate = mock(ITable.class);
        when(delegate.getRowCount()).thenReturn(5);
        ITableMetaData meta = mock(ITableMetaData.class);
        when(delegate.getTableMetaData()).thenReturn(meta);
        when(delegate.getValue(0, "ID")).thenReturn("row0");
        when(delegate.getValue(2, "ID")).thenReturn("row2");
        when(delegate.getValue(4, "ID")).thenReturn("row4");

        ScenarioDuplicateHandler.FilteredTable filtered =
                new ScenarioDuplicateHandler.FilteredTable(delegate, Set.of(1, 3));

        assertEquals(3, filtered.getRowCount());
        assertEquals("row0", filtered.getValue(0, "ID"));
        assertEquals("row2", filtered.getValue(1, "ID"));
        assertEquals("row4", filtered.getValue(2, "ID"));
    }

    @Test
    void FilteredTable_IntPredicate_正常ケース_述語ベースで除外行を指定する_行数と値が正しく返ること() throws Exception {
        ITable delegate = mock(ITable.class);
        ITableMetaData metaData = mock(ITableMetaData.class);
        when(delegate.getTableMetaData()).thenReturn(metaData);
        when(delegate.getValue(0, "ID")).thenReturn("A");
        when(delegate.getValue(2, "ID")).thenReturn("C");

        // 物理行 0,1,2 のうち行インデックス 1 をスキップする述語
        ScenarioDuplicateHandler.FilteredTable filtered =
                new ScenarioDuplicateHandler.FilteredTable(delegate, i -> i == 1, 2);

        assertEquals(2, filtered.getRowCount());
        assertSame(metaData, filtered.getTableMetaData());
        assertEquals("A", filtered.getValue(0, "ID"));
        assertEquals("C", filtered.getValue(1, "ID"));
    }
}
