package io.github.yok.flexdblink.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TableDependencyResolverTest {

    private Connection conn;
    private DatabaseMetaData meta;

    @BeforeEach
    void setUp() throws SQLException {
        conn = mock(Connection.class);
        meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);
    }

    /**
     * Creates a mock {@code ResultSet} that simulates a {@code getImportedKeys} response with no
     * rows, representing a table that has no foreign key dependencies.
     *
     * <ul>
     * <li>{@code rs.next()} always returns {@code false}.</li>
     * </ul>
     *
     * <p>
     * <b>Note:</b> Call this method outside of Mockito {@code when()} stub chains (i.e. not as an
     * argument to {@code thenReturn()}), to avoid unintended nested stubbing.
     * </p>
     *
     * @return a mock {@code ResultSet} that returns no FK rows
     * @throws SQLException never thrown; declared for Mockito compatibility
     */
    private static ResultSet buildEmptyRs() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(false);
        return rs;
    }

    /**
     * Creates a mock {@code ResultSet} that simulates a {@code getImportedKeys} response with a
     * single row, representing a table that has a foreign key to exactly one parent table.
     *
     * <ul>
     * <li>{@code rs.next()} returns {@code true} on the first call, then {@code false}.</li>
     * <li>{@code rs.getString("PKTABLE_NAME")} returns {@code pkTableName}.</li>
     * </ul>
     *
     * @param pkTableName the value to return for the {@code PKTABLE_NAME} column
     * @return a mock {@code ResultSet} that returns one FK metadata row
     * @throws SQLException never thrown; declared for Mockito compatibility
     */
    private static ResultSet buildSingleParentRs(String pkTableName) throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true, false);
        when(rs.getString("PKTABLE_NAME")).thenReturn(pkTableName);
        return rs;
    }

    /**
     * Creates a mock {@code ResultSet} that simulates a {@code getImportedKeys} response with two
     * rows, used for the following scenarios:
     *
     * <ul>
     * <li>A table that has foreign keys to two distinct parent tables (e.g. diamond
     * dependency).</li>
     * <li>A composite foreign key where {@code getImportedKeys} returns two rows for the same
     * parent table (used to verify that duplicate edges are ignored).</li>
     * </ul>
     *
     * <p>
     * Stubbing behaviour:
     * </p>
     * <ul>
     * <li>{@code rs.next()} returns {@code true} on the first and second calls, then
     * {@code false}.</li>
     * <li>{@code rs.getString("PKTABLE_NAME")} returns {@code pkTable1} on the first call and
     * {@code pkTable2} on the second.</li>
     * </ul>
     *
     * @param pkTable1 the {@code PKTABLE_NAME} value for the first row
     * @param pkTable2 the {@code PKTABLE_NAME} value for the second row
     * @return a mock {@code ResultSet} that returns two FK metadata rows
     * @throws SQLException never thrown; declared for Mockito compatibility
     */
    private static ResultSet buildTwoParentsRs(String pkTable1, String pkTable2)
            throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getString("PKTABLE_NAME")).thenReturn(pkTable1, pkTable2);
        return rs;
    }

    @Test
    void resolveLoadOrder_空リスト_空リストが返ること() throws SQLException {
        List<String> result =
                TableDependencyResolver.resolveLoadOrder(conn, null, "APP", List.of());
        assertTrue(result.isEmpty());
    }

    @Test
    void resolveLoadOrder_1テーブルのみ_そのまま返ること() throws SQLException {
        List<String> result =
                TableDependencyResolver.resolveLoadOrder(conn, null, "APP", List.of("ORDERS"));
        assertEquals(List.of("ORDERS"), result);
    }

    @Test
    void resolveLoadOrder_FK依存なし_アルファベット順が返ること() throws SQLException {
        ResultSet rs = buildEmptyRs();
        when(meta.getImportedKeys(any(), any(), any())).thenReturn(rs);

        List<String> result = TableDependencyResolver.resolveLoadOrder(conn, null, "APP",
                List.of("C_TABLE", "A_TABLE", "B_TABLE"));

        assertEquals(List.of("A_TABLE", "B_TABLE", "C_TABLE"), result);
    }

    @Test
    void resolveLoadOrder_単純チェーン_親テーブルが先に並ぶこと() throws SQLException {
        // ORDERS -> CUSTOMERS -> REGIONS の依存チェーン
        // ORDERS は CUSTOMERS に依存、CUSTOMERS は REGIONS に依存
        ResultSet rsRegions = buildEmptyRs();
        ResultSet rsCustomers = buildSingleParentRs("REGIONS");
        ResultSet rsOrders = buildSingleParentRs("CUSTOMERS");
        when(meta.getImportedKeys(any(), any(), eq("REGIONS"))).thenReturn(rsRegions);
        when(meta.getImportedKeys(any(), any(), eq("CUSTOMERS"))).thenReturn(rsCustomers);
        when(meta.getImportedKeys(any(), any(), eq("ORDERS"))).thenReturn(rsOrders);

        List<String> result = TableDependencyResolver.resolveLoadOrder(conn, null, "APP",
                List.of("ORDERS", "CUSTOMERS", "REGIONS"));

        // REGIONS が最初、ORDERS が最後
        assertEquals(0, result.indexOf("REGIONS"));
        assertEquals(1, result.indexOf("CUSTOMERS"));
        assertEquals(2, result.indexOf("ORDERS"));
    }

    @Test
    void resolveLoadOrder_ダイヤモンド型依存_親が先で末端が最後になること() throws SQLException {
        // DETAIL は HEADER_A と HEADER_B に依存
        // HEADER_A と HEADER_B は MASTER に依存
        ResultSet rsMaster = buildEmptyRs();
        ResultSet rsHeaderA = buildSingleParentRs("MASTER");
        ResultSet rsHeaderB = buildSingleParentRs("MASTER");
        ResultSet rsDetail = buildTwoParentsRs("HEADER_A", "HEADER_B");
        when(meta.getImportedKeys(any(), any(), eq("MASTER"))).thenReturn(rsMaster);
        when(meta.getImportedKeys(any(), any(), eq("HEADER_A"))).thenReturn(rsHeaderA);
        when(meta.getImportedKeys(any(), any(), eq("HEADER_B"))).thenReturn(rsHeaderB);
        when(meta.getImportedKeys(any(), any(), eq("DETAIL"))).thenReturn(rsDetail);

        List<String> result = TableDependencyResolver.resolveLoadOrder(conn, null, "APP",
                List.of("DETAIL", "HEADER_B", "HEADER_A", "MASTER"));

        assertEquals(4, result.size());
        // MASTER が最初
        assertEquals("MASTER", result.get(0));
        // DETAIL が最後
        assertEquals("DETAIL", result.get(3));
        // HEADER_A と HEADER_B は MASTER の後、DETAIL の前
        assertTrue(result.indexOf("HEADER_A") > result.indexOf("MASTER"));
        assertTrue(result.indexOf("HEADER_B") > result.indexOf("MASTER"));
        assertTrue(result.indexOf("HEADER_A") < result.indexOf("DETAIL"));
        assertTrue(result.indexOf("HEADER_B") < result.indexOf("DETAIL"));
    }

    @Test
    void resolveLoadOrder_循環参照_警告後アルファベット順で全テーブル返ること() throws SQLException {
        // ALPHA と BETA が互いに参照し合う循環
        ResultSet rsAlpha = buildSingleParentRs("BETA");
        ResultSet rsBeta = buildSingleParentRs("ALPHA");
        when(meta.getImportedKeys(any(), any(), eq("ALPHA"))).thenReturn(rsAlpha);
        when(meta.getImportedKeys(any(), any(), eq("BETA"))).thenReturn(rsBeta);

        List<String> result = TableDependencyResolver.resolveLoadOrder(conn, null, "APP",
                List.of("BETA", "ALPHA"));

        // 2テーブル全てが返ること
        assertEquals(2, result.size());
        assertTrue(result.contains("ALPHA"));
        assertTrue(result.contains("BETA"));
        // 循環ノードはアルファベット順で返る
        assertEquals("ALPHA", result.get(0));
        assertEquals("BETA", result.get(1));
    }

    @Test
    void resolveLoadOrder_循環参照と非循環参照の混在_非循環が先で循環がアルファベット順で続くこと() throws SQLException {
        ResultSet rsMaster = buildEmptyRs();
        ResultSet rsCycleA = buildSingleParentRs("CYCLE_B");
        ResultSet rsCycleB = buildSingleParentRs("CYCLE_A");
        when(meta.getImportedKeys(any(), any(), eq("MASTER"))).thenReturn(rsMaster);
        when(meta.getImportedKeys(any(), any(), eq("CYCLE_A"))).thenReturn(rsCycleA);
        when(meta.getImportedKeys(any(), any(), eq("CYCLE_B"))).thenReturn(rsCycleB);

        List<String> result = TableDependencyResolver.resolveLoadOrder(conn, null, "APP",
                List.of("CYCLE_B", "MASTER", "CYCLE_A"));

        assertEquals(3, result.size());
        // 非循環の MASTER が最初
        assertEquals("MASTER", result.get(0));
        // 循環テーブルはアルファベット順で末尾に追加
        assertEquals("CYCLE_A", result.get(1));
        assertEquals("CYCLE_B", result.get(2));
    }

    @Test
    void resolveLoadOrder_テーブルセット外FK_無視してアルファベット順になること() throws SQLException {
        // ORDERS は EXTERNAL_TABLE（リスト外）を参照しているが無視される
        ResultSet rsCustomers = buildEmptyRs();
        ResultSet rsOrders = buildSingleParentRs("EXTERNAL_TABLE");
        when(meta.getImportedKeys(any(), any(), eq("CUSTOMERS"))).thenReturn(rsCustomers);
        when(meta.getImportedKeys(any(), any(), eq("ORDERS"))).thenReturn(rsOrders);

        List<String> result = TableDependencyResolver.resolveLoadOrder(conn, null, "APP",
                List.of("ORDERS", "CUSTOMERS"));

        // 外部参照は無視されるため両テーブルは入次数0 → アルファベット順
        assertEquals(List.of("CUSTOMERS", "ORDERS"), result);
    }

    @Test
    void resolveLoadOrder_大文字小文字混在_正しく照合されること() throws SQLException {
        // getImportedKeys が大文字で "CUSTOMERS" を返す（テーブルリストは小文字 "customers"）
        ResultSet rsCustomers = buildEmptyRs();
        ResultSet rsOrders = buildSingleParentRs("CUSTOMERS"); // 大文字で返る
        when(meta.getImportedKeys(any(), any(), eq("customers"))).thenReturn(rsCustomers);
        when(meta.getImportedKeys(any(), any(), eq("orders"))).thenReturn(rsOrders);

        List<String> result = TableDependencyResolver.resolveLoadOrder(conn, null, "app",
                List.of("orders", "customers"));

        // customers が先（orders が依存している）
        assertEquals("customers", result.get(0));
        assertEquals("orders", result.get(1));
    }

    @Test
    void resolveLoadOrder_自己参照FK_無視されること() throws SQLException {
        // TREE テーブルが自分自身を参照（親子関係を持つテーブル）
        ResultSet rsTree = buildSingleParentRs("TREE"); // 自己参照
        ResultSet rsLeaf = buildSingleParentRs("TREE");
        when(meta.getImportedKeys(any(), any(), eq("TREE"))).thenReturn(rsTree);
        when(meta.getImportedKeys(any(), any(), eq("LEAF"))).thenReturn(rsLeaf);

        List<String> result = TableDependencyResolver.resolveLoadOrder(conn, null, "APP",
                List.of("LEAF", "TREE"));

        // 自己参照は無視 → TREE は入次数0 → TREE が先
        assertEquals("TREE", result.get(0));
        assertEquals("LEAF", result.get(1));
    }

    @Test
    void resolveLoadOrder_catalog引数null_正常に動作すること() throws SQLException {
        ResultSet rs = buildEmptyRs();
        when(meta.getImportedKeys(isNull(), any(), any())).thenReturn(rs);

        List<String> result =
                TableDependencyResolver.resolveLoadOrder(conn, null, "APP", List.of("T1", "T2"));

        assertEquals(2, result.size());
    }

    @Test
    void resolveLoadOrder_nullリスト_空リストが返ること() throws SQLException {
        List<String> result = TableDependencyResolver.resolveLoadOrder(conn, null, "APP", null);
        assertTrue(result.isEmpty());
    }

    @Test
    void resolveLoadOrder_大文字小文字重複テーブル名_警告ログ出力後1件として扱われること() throws SQLException {
        ResultSet rsOrders = buildEmptyRs();
        ResultSet rsOrdersDup = buildEmptyRs();
        when(meta.getImportedKeys(any(), any(), eq("ORDERS"))).thenReturn(rsOrders);
        when(meta.getImportedKeys(any(), any(), eq("orders"))).thenReturn(rsOrdersDup);

        List<String> result = TableDependencyResolver.resolveLoadOrder(conn, null, "APP",
                List.of("ORDERS", "orders"));

        // 重複分は排除されて 1 件になること
        assertEquals(1, result.size());
        assertEquals("ORDERS", result.get(0));
    }

    @Test
    void resolveLoadOrder_PKTABLE_NAMEがnull_スキップされること() throws SQLException {
        ResultSet rsA = mock(ResultSet.class);
        when(rsA.next()).thenReturn(true, false);
        when(rsA.getString("PKTABLE_NAME")).thenReturn(null); // null 返却

        ResultSet rsB = buildEmptyRs();
        when(meta.getImportedKeys(any(), any(), eq("A"))).thenReturn(rsA);
        when(meta.getImportedKeys(any(), any(), eq("B"))).thenReturn(rsB);

        List<String> result =
                TableDependencyResolver.resolveLoadOrder(conn, null, "APP", List.of("A", "B"));

        // null PKTABLE_NAME はスキップ → FK なしと同じでアルファベット順
        assertEquals(List.of("A", "B"), result);
    }

    @Test
    void resolveLoadOrder_複合FK同一親に複数列_重複エッジは無視されること() throws SQLException {
        ResultSet rsMaster = buildEmptyRs();
        // DETAIL -> MASTER の FK が 2 列（複合 FK）で返る
        ResultSet rsDetail = buildTwoParentsRs("MASTER", "MASTER");
        when(meta.getImportedKeys(any(), any(), eq("MASTER"))).thenReturn(rsMaster);
        when(meta.getImportedKeys(any(), any(), eq("DETAIL"))).thenReturn(rsDetail);

        List<String> result = TableDependencyResolver.resolveLoadOrder(conn, null, "APP",
                List.of("DETAIL", "MASTER"));

        // MASTER が先、DETAIL が後（重複カウントされず inDegree=1 のまま）
        assertEquals("MASTER", result.get(0));
        assertEquals("DETAIL", result.get(1));
    }

    @Test
    void constructor_異常ケース_リフレクションで呼び出す_例外が送出されること() throws Exception {
        Constructor<TableDependencyResolver> cons =
                TableDependencyResolver.class.getDeclaredConstructor();
        assertTrue((cons.getModifiers() & java.lang.reflect.Modifier.PRIVATE) != 0,
                "constructor must be private");
        cons.setAccessible(true);
        assertThrows(Exception.class, cons::newInstance);
    }

    @Test
    public void resolveLoadOrder_異常ケース_connがnull_例外がスローされること() {
        assertThrows(IllegalArgumentException.class,
                () -> TableDependencyResolver.resolveLoadOrder(null, null, "APP", List.of("T1")));
    }

    @Test
    public void resolveLoadOrder_異常ケース_tablesに空白が含まれる_例外がスローされること() {
        assertThrows(IllegalArgumentException.class, () -> TableDependencyResolver
                .resolveLoadOrder(conn, null, "APP", List.of("T1", "  ")));
    }

    @Test
    public void resolveLoadOrder_正常ケース_小文字格納DB_PostgreSQL想定_schemaとtableが小文字で検索され依存が解決されること()
            throws SQLException {

        when(meta.storesLowerCaseIdentifiers()).thenReturn(true);
        when(meta.storesUpperCaseIdentifiers()).thenReturn(false);

        ResultSet defaultRs = buildEmptyRs();
        when(meta.getImportedKeys(any(), any(), any())).thenReturn(defaultRs);

        ResultSet rsParent = buildEmptyRs();
        ResultSet rsChild = buildSingleParentRs("z_parent");

        when(meta.getImportedKeys(isNull(), eq("app"), eq("z_parent"))).thenReturn(rsParent);
        when(meta.getImportedKeys(isNull(), eq("app"), eq("a_child"))).thenReturn(rsChild);

        List<String> result = TableDependencyResolver.resolveLoadOrder(conn, null, "APP",
                List.of("A_CHILD", "Z_PARENT"));

        assertEquals(List.of("Z_PARENT", "A_CHILD"), result);
    }

    @Test
    public void resolveLoadOrder_正常ケース_大文字格納DB_Oracle想定_schemaとtableが大文字で検索され依存が解決されること()
            throws SQLException {

        when(meta.storesLowerCaseIdentifiers()).thenReturn(false);
        when(meta.storesUpperCaseIdentifiers()).thenReturn(true);

        ResultSet defaultRs = buildEmptyRs();
        when(meta.getImportedKeys(any(), any(), any())).thenReturn(defaultRs);

        ResultSet rsParent = buildEmptyRs();
        ResultSet rsChild = buildSingleParentRs("Z_PARENT");

        when(meta.getImportedKeys(isNull(), eq("APP"), eq("Z_PARENT"))).thenReturn(rsParent);
        when(meta.getImportedKeys(isNull(), eq("APP"), eq("A_CHILD"))).thenReturn(rsChild);

        List<String> result = TableDependencyResolver.resolveLoadOrder(conn, null, "app",
                List.of("a_child", "z_parent"));

        assertEquals(List.of("z_parent", "a_child"), result);
    }

    @Test
    public void resolveLoadOrder_正常ケース_catalog指定_最初0件ならcatalogNullでリトライして依存が解決されること()
            throws SQLException {

        ResultSet defaultRs = buildEmptyRs();
        when(meta.getImportedKeys(any(), any(), any())).thenReturn(defaultRs);

        ResultSet rsParentCatalog = buildEmptyRs();
        ResultSet rsParentNullCatalog = buildEmptyRs();
        when(meta.getImportedKeys(eq("CAT"), any(), eq("Z_PARENT"))).thenReturn(rsParentCatalog);
        when(meta.getImportedKeys(isNull(), any(), eq("Z_PARENT"))).thenReturn(rsParentNullCatalog);

        ResultSet rsChildCatalogEmpty = buildEmptyRs();
        ResultSet rsChildRetry = buildSingleParentRs("Z_PARENT");
        when(meta.getImportedKeys(eq("CAT"), any(), eq("A_CHILD"))).thenReturn(rsChildCatalogEmpty);
        when(meta.getImportedKeys(isNull(), any(), eq("A_CHILD"))).thenReturn(rsChildRetry);

        List<String> result = TableDependencyResolver.resolveLoadOrder(conn, "CAT", "APP",
                List.of("A_CHILD", "Z_PARENT"));

        assertEquals(List.of("Z_PARENT", "A_CHILD"), result);
    }

    @Test
    void resolveLoadOrder_正常ケース_schemaがnullを指定する_getImportedKeysがschemaNullで呼び出されること()
            throws SQLException {

        ResultSet rs = buildEmptyRs();
        when(meta.getImportedKeys(isNull(), isNull(), any())).thenReturn(rs);

        List<String> result = TableDependencyResolver.resolveLoadOrder(conn, null, null,
                List.of("B_TABLE", "A_TABLE"));

        assertEquals(List.of("A_TABLE", "B_TABLE"), result);

        verify(meta, times(2)).getImportedKeys(isNull(), isNull(), any());
    }
}
