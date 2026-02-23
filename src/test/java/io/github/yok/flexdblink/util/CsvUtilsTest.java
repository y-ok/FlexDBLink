
package io.github.yok.flexdblink.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CsvUtilsTest {

    @Test
    void writeCsvUtf8_正常ケース_ヘッダと複数行が正しく書き込まれること(@TempDir File tmpDir) throws Exception {
        File csvFile = new File(tmpDir, "out.csv");
        String[] headers = {"id", "name", "note"};
        List<List<String>> rows = Arrays.asList(Arrays.asList("1", "Alice", "Hello,World"), // カンマを含む
                Arrays.asList("2", "Bob", "Path\\to\\file"), // バックスラッシュを含む
                Arrays.asList("3", "Carol", " spaced ") // 前後にスペース
        );

        // 実行
        CsvUtils.writeCsvUtf8(csvFile, headers, rows);

        // 内容を読み取って検証
        String content = Files.readString(csvFile.toPath());

        // ヘッダが含まれる
        assertTrue(content.contains("id,name,note"));
        // カンマを含むセルは引用符で囲まれる
        assertTrue(content.contains("\"Hello,World\""));
        // バックスラッシュはエスケープされる
        assertTrue(content.contains("Path\\\\to\\\\file"));
        // スペース付きは引用符で囲まれる
        assertTrue(content.contains("\" spaced \""));
    }

    @Test
    void writeCsvUtf8_異常ケース_ディレクトリ指定でIOExceptionが送出されること(@TempDir File tmpDir) {
        File dirAsFile = tmpDir; // ディレクトリをそのまま渡す
        String[] headers = {"h1"};
        List<List<String>> rows = List.of(List.of("x"));

        assertThrows(IOException.class, () -> CsvUtils.writeCsvUtf8(dirAsFile, headers, rows));
    }

    @Test
    void constructor_正常ケース_privateコンストラクタが存在しインスタンス化できること() throws Exception {
        Constructor<CsvUtils> cons = CsvUtils.class.getDeclaredConstructor();
        assertTrue((cons.getModifiers() & java.lang.reflect.Modifier.PRIVATE) != 0,
                "constructor must be private");
        cons.setAccessible(true);
        Object instance = cons.newInstance();
        assertNotNull(instance);
    }

    // -------------------------------------------------------------------------
    // buildSortIndices
    // -------------------------------------------------------------------------

    @Test
    void buildSortIndices_正常ケース_PKがない_先頭インデックス0が返ること() {
        String[] headers = {"A", "B", "C"};
        List<Integer> result = CsvUtils.buildSortIndices(headers, List.of());
        assertEquals(List.of(0), result);
    }

    @Test
    void buildSortIndices_正常ケース_PK1列_対応インデックスが返ること() {
        String[] headers = {"A", "B", "C"};
        List<Integer> result = CsvUtils.buildSortIndices(headers, List.of("B"));
        assertEquals(List.of(1), result);
    }

    @Test
    void buildSortIndices_正常ケース_PK複数列_順序どおりのインデックスが返ること() {
        String[] headers = {"A", "B", "C"};
        List<Integer> result = CsvUtils.buildSortIndices(headers, List.of("C", "A"));
        assertEquals(List.of(2, 0), result);
    }

    // -------------------------------------------------------------------------
    // rowComparator
    // -------------------------------------------------------------------------

    @Test
    void rowComparator_正常ケース_数値比較で正しく順序付けされること() {
        Comparator<List<String>> cmp = CsvUtils.rowComparator(List.of(0));
        List<String> a = List.of("10");
        List<String> b = List.of("2");
        assertTrue(cmp.compare(a, b) > 0, "10 should be greater than 2 numerically");
        assertTrue(cmp.compare(b, a) < 0);
    }

    @Test
    void rowComparator_正常ケース_文字列比較で正しく順序付けされること() {
        Comparator<List<String>> cmp = CsvUtils.rowComparator(List.of(0));
        List<String> a = List.of("abc");
        List<String> b = List.of("abd");
        assertTrue(cmp.compare(a, b) < 0, "'abc' < 'abd' lexicographically");
        assertTrue(cmp.compare(b, a) > 0);
    }

    @Test
    void rowComparator_正常ケース_同値のとき0が返ること() {
        Comparator<List<String>> cmp = CsvUtils.rowComparator(List.of(0));
        List<String> a = List.of("A");
        List<String> b = List.of("A");
        assertEquals(0, cmp.compare(a, b));
    }

    @Test
    void rowComparator_正常ケース_複数インデックスで第1キー同値_第2キーで決まること() {
        Comparator<List<String>> cmp = CsvUtils.rowComparator(List.of(0, 1));
        List<String> a = List.of("1", "z");
        List<String> b = List.of("1", "a");
        assertTrue(cmp.compare(a, b) > 0, "second key 'z' > 'a'");
    }

    // -------------------------------------------------------------------------
    // resolveTemporalValue
    // -------------------------------------------------------------------------

    @Test
    void resolveTemporalValue_正常ケース_DATE型でgetDateが非null_typed値が返ること() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        java.sql.Date typed = java.sql.Date.valueOf("2026-01-15");
        when(rs.getDate(1)).thenReturn(typed);
        Object result = CsvUtils.resolveTemporalValue(rs, 1, "raw", Types.DATE, "DATE");
        assertEquals(typed, result);
    }

    @Test
    void resolveTemporalValue_正常ケース_DATE型でgetDateがnull_raw値が返ること() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        Object raw = new Object();
        when(rs.getDate(1)).thenReturn(null);
        Object result = CsvUtils.resolveTemporalValue(rs, 1, raw, Types.DATE, "DATE");
        assertEquals(raw, result);
    }

    @Test
    void resolveTemporalValue_正常ケース_TIME型でgetTimeが非null_typed値が返ること() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        java.sql.Time typed = java.sql.Time.valueOf("12:34:56");
        when(rs.getTime(1)).thenReturn(typed);
        Object result = CsvUtils.resolveTemporalValue(rs, 1, "raw", Types.TIME, "TIME");
        assertEquals(typed, result);
    }

    @Test
    void resolveTemporalValue_正常ケース_TIME型でgetTimeがnull_raw値が返ること() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        Object raw = new Object();
        when(rs.getTime(1)).thenReturn(null);
        Object result = CsvUtils.resolveTemporalValue(rs, 1, raw, Types.TIME, "TIME");
        assertEquals(raw, result);
    }

    @Test
    void resolveTemporalValue_正常ケース_TIMESTAMP型でgetTimestampが非null_typed値が返ること()
            throws Exception {
        ResultSet rs = mock(ResultSet.class);
        java.sql.Timestamp typed = java.sql.Timestamp.valueOf("2026-01-15 12:34:56");
        when(rs.getTimestamp(1)).thenReturn(typed);
        Object result =
                CsvUtils.resolveTemporalValue(rs, 1, "raw", Types.TIMESTAMP, "TIMESTAMP");
        assertEquals(typed, result);
    }

    @Test
    void resolveTemporalValue_正常ケース_TIMESTAMP型でgetTimestampがnull_raw値が返ること() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        Object raw = new Object();
        when(rs.getTimestamp(1)).thenReturn(null);
        Object result = CsvUtils.resolveTemporalValue(rs, 1, raw, Types.TIMESTAMP, "TIMESTAMP");
        assertEquals(raw, result);
    }

    @Test
    void resolveTemporalValue_正常ケース_VARCHAR型_raw値がそのまま返ること() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        Object raw = "someString";
        Object result = CsvUtils.resolveTemporalValue(rs, 1, raw, Types.VARCHAR, "VARCHAR2");
        assertEquals(raw, result);
    }

    @Test
    void resolveTemporalValue_正常ケース_DATE型名で一致_typed値が返ること() throws Exception {
        // typeName "DATE" but sqlType != Types.DATE (e.g., Oracle DATE mapped to OTHER)
        ResultSet rs = mock(ResultSet.class);
        java.sql.Date typed = java.sql.Date.valueOf("2026-01-01");
        when(rs.getDate(1)).thenReturn(typed);
        Object result = CsvUtils.resolveTemporalValue(rs, 1, "raw", Types.OTHER, "DATE");
        assertEquals(typed, result);
    }

    // -------------------------------------------------------------------------
    // fetchPrimaryKeyColumns
    // -------------------------------------------------------------------------

    @Test
    void fetchPrimaryKeyColumns_正常ケース_主キーが1列である_列名が返ること() throws Exception {
        Connection conn = mock(Connection.class);
        when(conn.getCatalog()).thenReturn("cat");
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet pkRs = mock(ResultSet.class);
        when(meta.getPrimaryKeys("cat", "APP", "T1")).thenReturn(pkRs);
        when(pkRs.next()).thenReturn(true, false);
        when(pkRs.getString("COLUMN_NAME")).thenReturn("ID");

        List<String> result = CsvUtils.fetchPrimaryKeyColumns(conn, "APP", "T1");
        assertEquals(List.of("ID"), result);
    }

    @Test
    void fetchPrimaryKeyColumns_正常ケース_主キーがない_空リストが返ること() throws Exception {
        Connection conn = mock(Connection.class);
        when(conn.getCatalog()).thenReturn(null);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
        ResultSet pkRs = mock(ResultSet.class);
        when(meta.getPrimaryKeys(null, "APP", "T1")).thenReturn(pkRs);
        when(pkRs.next()).thenReturn(false);

        List<String> result = CsvUtils.fetchPrimaryKeyColumns(conn, "APP", "T1");
        assertEquals(List.of(), result);
    }
}
