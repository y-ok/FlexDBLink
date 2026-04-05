package io.github.yok.flexdblink.junit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.db.DbDialectHandler;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * Unit tests for {@link FlexAssert}.
 */
class FlexAssertTest {

    private static final String DB_NAME = "db1";
    private static FlexAssert stackFlexAssert;

    @LoadData(scenario = "stack-method", dbNames = {"db1"})
    static class StackMethodCaller {
        void execute() {
            stackFlexAssert.assertTable(DB_NAME, "FA_STACK_METHOD");
        }
    }

    @LoadData(scenario = "stack-class", dbNames = {"db1"})
    static class StackClassCaller {
        void execute() {
            stackFlexAssert.assertTable(DB_NAME, "FA_STACK_CLASS");
        }
    }

    static class PlainMethodCaller {
        void execute() {
            stackFlexAssert.assertTable(DB_NAME, "FA_STACK_METHOD");
        }
    }

    static class AnnotatedMethodCaller {
        @LoadData(scenario = "stack-annotated-method", dbNames = {"db1"})
        void execute() {
            stackFlexAssert.assertTable(DB_NAME, "FA_STACK_ANNOTATED_METHOD");
        }
    }

    @AfterEach
    void tearDown() throws IOException {
        LoadDataExtension.CURRENT_TEST_CLASS.remove();
        LoadDataExtension.CURRENT_SCENARIO.remove();
        LoadDataExtension.clearCurrentDataSources();
        DataSourceRegistry.clear();
        stackFlexAssert = null;
        deleteClassRoot(FlexAssertTest.class);
        deleteClassRoot(StackMethodCaller.class);
        deleteClassRoot(StackClassCaller.class);
        deleteClassRoot(PlainMethodCaller.class);
        deleteClassRoot(AnnotatedMethodCaller.class);
    }

    @Test
    void globalExcludeColumns_正常ケース_除外カラムを指定_グローバル除外が設定されること() {
        FlexAssert fa = FlexAssert.globalExcludeColumns("COL_A", "COL_B");
        Set<String> excludes = fa.resolveExcludeColumns("ANY_TABLE");
        assertEquals(2, excludes.size());
        assertTrue(excludes.contains("COL_A"));
        assertTrue(excludes.contains("COL_B"));
    }

    @Test
    void globalExcludeColumns_正常ケース_引数なし_空の除外セットであること() {
        FlexAssert fa = FlexAssert.globalExcludeColumns();
        Set<String> excludes = fa.resolveExcludeColumns("ANY_TABLE");
        assertTrue(excludes.isEmpty());
    }

    @Test
    void withDefaults_正常ケース_除外カラムなし_空の除外セットであること() {
        FlexAssert fa = FlexAssert.withDefaults();
        Set<String> excludes = fa.resolveExcludeColumns("ANY_TABLE");
        assertTrue(excludes.isEmpty());
    }

    @Test
    void table_excludeColumns_正常ケース_テーブル個別除外を指定_該当テーブルのみ除外されること() {
        FlexAssert fa = FlexAssert.withDefaults().table("USERS").excludeColumns("ID", "PASSWORD");

        Set<String> usersExcludes = fa.resolveExcludeColumns("USERS");
        assertEquals(2, usersExcludes.size());
        assertTrue(usersExcludes.contains("ID"));
        assertTrue(usersExcludes.contains("PASSWORD"));

        Set<String> ordersExcludes = fa.resolveExcludeColumns("ORDERS");
        assertTrue(ordersExcludes.isEmpty());
    }

    @Test
    void table_excludeColumns_正常ケース_複数テーブル個別除外を指定_各テーブルに除外が設定されること() {
        FlexAssert fa = FlexAssert.withDefaults().table("USERS").excludeColumns("ID")
                .table("ORDERS").excludeColumns("SEQ");

        Set<String> usersExcludes = fa.resolveExcludeColumns("USERS");
        assertEquals(1, usersExcludes.size());
        assertTrue(usersExcludes.contains("ID"));

        Set<String> ordersExcludes = fa.resolveExcludeColumns("ORDERS");
        assertEquals(1, ordersExcludes.size());
        assertTrue(ordersExcludes.contains("SEQ"));
    }

    @Test
    void resolveExcludeColumns_正常ケース_グローバルとテーブル個別を併用_マージされること() {
        FlexAssert fa = FlexAssert.globalExcludeColumns("CREATED_AT", "UPDATED_AT").table("USERS")
                .excludeColumns("ID");

        Set<String> usersExcludes = fa.resolveExcludeColumns("USERS");
        assertEquals(3, usersExcludes.size());
        assertTrue(usersExcludes.contains("CREATED_AT"));
        assertTrue(usersExcludes.contains("UPDATED_AT"));
        assertTrue(usersExcludes.contains("ID"));

        Set<String> ordersExcludes = fa.resolveExcludeColumns("ORDERS");
        assertEquals(2, ordersExcludes.size());
        assertTrue(ordersExcludes.contains("CREATED_AT"));
        assertTrue(ordersExcludes.contains("UPDATED_AT"));
    }

    @Test
    void resolveExcludeColumns_正常ケース_グローバルとテーブルで重複指定_重複が除去されること() {
        FlexAssert fa = FlexAssert.globalExcludeColumns("COL_A").table("T1").excludeColumns("COL_A",
                "COL_B");

        Set<String> excludes = fa.resolveExcludeColumns("T1");
        assertEquals(2, excludes.size());
        assertTrue(excludes.contains("COL_A"));
        assertTrue(excludes.contains("COL_B"));
    }

    @Test
    void table_excludeColumns_正常ケース_チェーン呼び出し_元のインスタンスに影響しないこと() {
        FlexAssert original = FlexAssert.globalExcludeColumns("COL_A");
        FlexAssert withTable = original.table("T1").excludeColumns("COL_B");

        Set<String> originalExcludes = original.resolveExcludeColumns("T1");
        assertEquals(1, originalExcludes.size());
        assertTrue(originalExcludes.contains("COL_A"));

        Set<String> withTableExcludes = withTable.resolveExcludeColumns("T1");
        assertEquals(2, withTableExcludes.size());
    }

    @Test
    void assertTables_異常ケース_LoadDataアノテーションなし_IllegalStateExceptionが送出されること() {
        FlexAssert fa = FlexAssert.withDefaults();
        IllegalStateException ex =
                assertThrows(IllegalStateException.class, () -> fa.assertTables("DB1"));
        assertTrue(ex.getMessage().contains("@LoadData"));
    }

    @Test
    void assertTables_異常ケース_expectedのtableOrderingが存在しない_RuntimeExceptionが送出されること()
            throws Exception {
        LoadDataExtension.CURRENT_TEST_CLASS.set(FlexAssertTest.class);
        LoadDataExtension.CURRENT_SCENARIO.set("missing-ordering");
        Path dir = expectedDir(FlexAssertTest.class, "missing-ordering", DB_NAME);
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("FA_MISSING_ORDER.csv"), "ID,NAME\n1,test\n",
                StandardCharsets.UTF_8);

        DriverManagerDataSource dataSource = createDriverManagerDataSource();
        DataSourceRegistry.register(DB_NAME, dataSource);
        Connection connection = mockConnection("APP_USER",
                tableSpec("FA_MISSING_ORDER", columns("ID", "NAME"),
                        sqlTypes(Types.INTEGER, Types.VARCHAR), typeNames("INTEGER", "VARCHAR"),
                        row(1, "test")));

        try (MockedStatic<DataSourceUtils> dataSourceUtils =
                Mockito.mockStatic(DataSourceUtils.class)) {
            dataSourceUtils.when(() -> DataSourceUtils.getConnection(dataSource))
                    .thenReturn(connection);
            RuntimeException ex = assertThrows(RuntimeException.class,
                    () -> createFlexAssert().assertTables(DB_NAME));
            assertTrue(ex.getMessage().contains("Assertion failed due to unexpected error."));
        }
    }

    @Test
    void assertTable_異常ケース_LoadDataアノテーションなし_IllegalStateExceptionが送出されること() {
        FlexAssert fa = FlexAssert.withDefaults();
        IllegalStateException ex =
                assertThrows(IllegalStateException.class, () -> fa.assertTable("DB1", "USERS"));
        assertTrue(ex.getMessage().contains("@LoadData"));
    }

    @Test
    void assertTable_正常ケース_数値時刻LOB値を正規化して比較する_一致すること() throws Exception {
        LoadDataExtension.CURRENT_TEST_CLASS.set(FlexAssertTest.class);
        LoadDataExtension.CURRENT_SCENARIO.set("normalized");
        writeExpectedCsv(FlexAssertTest.class, "normalized", DB_NAME, "FA_NORMALIZED",
                "ID,FLAG,TIME_ONLY_COL,BIN_COL,TEXT_COL\n"
                        + "1,true,14:05:30,file:files/bin.dat,file:files/text.txt\n");
        writeExpectedFile(FlexAssertTest.class, "normalized", DB_NAME, "files/bin.dat",
                new byte[] {0x0A, 0x0B});
        writeExpectedTextFile(FlexAssertTest.class, "normalized", DB_NAME, "files/text.txt",
                "hello-lob");

        DriverManagerDataSource dataSource = createDriverManagerDataSource();
        DataSourceRegistry.register(DB_NAME, dataSource);
        Connection connection = mockConnection("APP_USER",
                tableSpec("FA_NORMALIZED",
                        columns("ID", "FLAG", "TIME_ONLY_COL", "BIN_COL", "TEXT_COL"),
                        sqlTypes(Types.INTEGER, Types.NUMERIC, Types.TIMESTAMP, Types.BLOB,
                                Types.CLOB),
                        typeNames("INTEGER", "NUMERIC", "TIMESTAMP", "BLOB", "CLOB"),
                        row(1, Integer.valueOf(1),
                                Timestamp.valueOf(LocalDate.ofEpochDay(0).atTime(14, 5, 30)),
                                new byte[] {0x0A, 0x0B}, "hello-lob")));

        try (MockedStatic<DataSourceUtils> dataSourceUtils =
                Mockito.mockStatic(DataSourceUtils.class)) {
            dataSourceUtils.when(() -> DataSourceUtils.getConnection(dataSource))
                    .thenReturn(connection);
            createFlexAssert().assertTable(DB_NAME, "FA_NORMALIZED");
        }
    }

    @Test
    void assertTables_異常ケース_一部テーブルで値不一致が発生する_集約AssertionErrorが送出されること() throws Exception {
        LoadDataExtension.CURRENT_TEST_CLASS.set(FlexAssertTest.class);
        LoadDataExtension.CURRENT_SCENARIO.set("aggregate-failure");
        writeExpectedCsv(FlexAssertTest.class, "aggregate-failure", DB_NAME, "FA_OK",
                "ID,NAME\n1,ok\n");
        writeExpectedCsv(FlexAssertTest.class, "aggregate-failure", DB_NAME, "FA_NG",
                "ID,NAME\n1,expected\n");

        DriverManagerDataSource dataSource = createDriverManagerDataSource();
        DataSourceRegistry.register(DB_NAME, dataSource);
        Connection connection = mockConnection("APP_USER",
                tableSpec("FA_OK", columns("ID", "NAME"), sqlTypes(Types.INTEGER, Types.VARCHAR),
                        typeNames("INTEGER", "VARCHAR"), row(1, "ok")),
                tableSpec("FA_NG", columns("ID", "NAME"), sqlTypes(Types.INTEGER, Types.VARCHAR),
                        typeNames("INTEGER", "VARCHAR"), row(1, "actual")));

        try (MockedStatic<DataSourceUtils> dataSourceUtils =
                Mockito.mockStatic(DataSourceUtils.class)) {
            dataSourceUtils.when(() -> DataSourceUtils.getConnection(dataSource))
                    .thenReturn(connection);
            AssertionError ex = assertThrows(AssertionError.class,
                    () -> createFlexAssert().assertTables(DB_NAME));
            assertTrue(ex.getMessage().contains("1 of 2 tables failed"));
            assertTrue(ex.getMessage().contains("[FA_NG]"));
        }
    }

    @Test
    void assertTable_異常ケース_実テーブルに期待カラムが存在しない_IllegalStateExceptionが送出されること() throws Exception {
        LoadDataExtension.CURRENT_TEST_CLASS.set(FlexAssertTest.class);
        LoadDataExtension.CURRENT_SCENARIO.set("missing-column");
        writeExpectedCsv(FlexAssertTest.class, "missing-column", DB_NAME, "FA_MISSING_COL",
                "ID,UNKNOWN_COL\n1,value\n");

        DriverManagerDataSource dataSource = createDriverManagerDataSource();
        DataSourceRegistry.register(DB_NAME, dataSource);
        Connection connection = mockConnection("APP_USER", tableSpec("FA_MISSING_COL",
                columns("ID"), sqlTypes(Types.INTEGER), typeNames("INTEGER"), row(1)));

        try (MockedStatic<DataSourceUtils> dataSourceUtils =
                Mockito.mockStatic(DataSourceUtils.class)) {
            dataSourceUtils.when(() -> DataSourceUtils.getConnection(dataSource))
                    .thenReturn(connection);
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> createFlexAssert().assertTable(DB_NAME, "FA_MISSING_COL"));
            assertTrue(ex.getMessage().contains("Column not found in actual table"));
        }
    }

    @Test
    void assertTable_異常ケース_接続情報の構築に失敗する_IllegalStateExceptionが送出されること() throws Exception {
        LoadDataExtension.CURRENT_TEST_CLASS.set(FlexAssertTest.class);
        LoadDataExtension.CURRENT_SCENARIO.set("entry-failure");
        writeExpectedCsv(FlexAssertTest.class, "entry-failure", DB_NAME, "FA_ENTRY", "ID\n1\n");

        DataSource dataSource = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        when(connection.getMetaData()).thenThrow(new java.sql.SQLException("meta-fail"));
        DataSourceRegistry.register(DB_NAME, dataSource);

        try (MockedStatic<DataSourceUtils> dataSourceUtils =
                Mockito.mockStatic(DataSourceUtils.class)) {
            dataSourceUtils.when(() -> DataSourceUtils.getConnection(dataSource))
                    .thenReturn(connection);
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> createFlexAssert().assertTable(DB_NAME, "FA_ENTRY"));
            assertTrue(ex.getMessage().contains("Failed to build connection entry"));
        }
    }

    @Test
    void assertTable_異常ケース_srcテストリソースへフォールバックしたがexpectedが存在しない_IllegalStateExceptionが送出されること() {
        LoadDataExtension.CURRENT_TEST_CLASS.set(StackMethodCaller.class);
        LoadDataExtension.CURRENT_SCENARIO.set("missing-expected");
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> FlexAssert.withDefaults().assertTable(DB_NAME, "ANY_TABLE"));
        assertTrue(ex.getMessage().contains("Expected directory not found"));
    }

    @Test
    void assertTable_正常ケース_DriverManagerDataSourceの未設定値をJDBCメタデータで補完する_接続情報が解決されること()
            throws Exception {
        LoadDataExtension.CURRENT_TEST_CLASS.set(FlexAssertTest.class);
        LoadDataExtension.CURRENT_SCENARIO.set("metadata-fallback");
        writeExpectedCsv(FlexAssertTest.class, "metadata-fallback", DB_NAME, "FA_META",
                "ID,NAME\n1,meta\n");

        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setUrl(null);
        dataSource.setUsername(null);
        dataSource.setPassword("secret");
        DataSourceRegistry.register(DB_NAME, dataSource);

        Connection connection = mockConnection("META_USER",
                tableSpec("FA_META", columns("ID", "NAME"), sqlTypes(Types.INTEGER, Types.VARCHAR),
                        typeNames("INTEGER", "VARCHAR"), row(1, "meta")));
        when(connection.getMetaData().getURL()).thenReturn("jdbc:postgresql://meta-host/meta-db");

        ConnectionConfig.Entry[] captured = new ConnectionConfig.Entry[1];
        DbDialectHandler dialectHandler = createDialectHandlerStub();
        try (MockedStatic<DataSourceUtils> dataSourceUtils =
                Mockito.mockStatic(DataSourceUtils.class)) {
            dataSourceUtils.when(() -> DataSourceUtils.getConnection(dataSource))
                    .thenReturn(connection);
            FlexAssert flexAssert = new FlexAssert(Collections.emptySet(), Collections.emptyMap(),
                    (classRoot, entry, conn) -> {
                        captured[0] = entry;
                        return dialectHandler;
                    });
            flexAssert.assertTable(DB_NAME, "FA_META");
        }

        assertEquals("jdbc:postgresql://meta-host/meta-db", captured[0].getUrl());
        assertEquals("META_USER", captured[0].getUser());
        assertEquals("secret", captured[0].getPassword());
    }

    @Test
    void assertTable_正常ケース_時刻系と数値系の追加正規化分岐を通す_一致すること() throws Exception {
        LoadDataExtension.CURRENT_TEST_CLASS.set(FlexAssertTest.class);
        LoadDataExtension.CURRENT_SCENARIO.set("extra-normalized");
        writeExpectedCsv(FlexAssertTest.class, "extra-normalized", DB_NAME, "FA_EXTRA",
                "ID,FLOAT_COL,SMALLINT_COL,EVENT_TIME,TS_TZ_COL,TZ_TIME_COL,TEXT_LOB,NULLABLE_COL\n"
                        + "1,1.0,f,14:05:30,2026-02-15 01:02:03,09:10:11,payload,\n");
        writeExpectedTextFile(FlexAssertTest.class, "extra-normalized", DB_NAME,
                "files/payload.txt", "payload");
        Path csv = expectedDir(FlexAssertTest.class, "extra-normalized", DB_NAME)
                .resolve("FA_EXTRA.csv");
        String content = Files.readString(csv, StandardCharsets.UTF_8);
        Files.writeString(csv, content.replace("payload", "file:files/payload.txt"),
                StandardCharsets.UTF_8);

        DriverManagerDataSource dataSource = createDriverManagerDataSource();
        DataSourceRegistry.register(DB_NAME, dataSource);
        Connection connection = mockConnection("APP_USER", tableSpec("FA_EXTRA",
                columns("ID", "FLOAT_COL", "SMALLINT_COL", "EVENT_TIME", "TS_TZ_COL", "TZ_TIME_COL",
                        "TEXT_LOB", "NULLABLE_COL"),
                sqlTypes(Types.INTEGER, Types.FLOAT, Types.SMALLINT, Types.TIMESTAMP,
                        Types.TIMESTAMP_WITH_TIMEZONE, Types.TIME_WITH_TIMEZONE, Types.CLOB,
                        Types.VARCHAR),
                typeNames("INTEGER", "FLOAT", "SMALLINT", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE",
                        "TIME WITH TIME ZONE", "CLOB", "VARCHAR"),
                List.of("MISSING_PK"),
                row(1, Double.valueOf(1.0D), Integer.valueOf(0),
                        Timestamp.valueOf("1970-01-01 14:05:30"), "2026-02-15 01:02:03.0",
                        "09:10:11", "payload", null)));

        try (MockedStatic<DataSourceUtils> dataSourceUtils =
                Mockito.mockStatic(DataSourceUtils.class)) {
            dataSourceUtils.when(() -> DataSourceUtils.getConnection(dataSource))
                    .thenReturn(connection);
            createFlexAssert().assertTable(DB_NAME, "FA_EXTRA");
        }
    }

    @Test
    void assertTable_正常ケース_スレッドローカルが片側だけ設定されてもスタックトレースへフォールバックする_一致すること() throws Exception {
        LoadDataExtension.CURRENT_TEST_CLASS.set(FlexAssertTest.class);
        writeExpectedCsv(StackMethodCaller.class, "stack-method", DB_NAME, "FA_STACK_METHOD",
                "ID,NAME\n1,stack-method\n");
        DriverManagerDataSource dataSource = createDriverManagerDataSource();
        DataSourceRegistry.register(DB_NAME, dataSource);
        Connection connection = mockConnection("APP_USER",
                tableSpec("FA_STACK_METHOD", columns("ID", "NAME"),
                        sqlTypes(Types.INTEGER, Types.VARCHAR), typeNames("INTEGER", "VARCHAR"),
                        row(1, "stack-method")));

        try (MockedStatic<DataSourceUtils> dataSourceUtils =
                Mockito.mockStatic(DataSourceUtils.class)) {
            dataSourceUtils.when(() -> DataSourceUtils.getConnection(dataSource))
                    .thenReturn(connection);
            stackFlexAssert = createFlexAssert();
            new StackMethodCaller().execute();
        }
    }

    @Test
    void assertTable_正常ケース_スタックトレースからメソッドアノテーションを解決する_一致すること() throws Exception {
        writeExpectedCsv(StackMethodCaller.class, "stack-method", DB_NAME, "FA_STACK_METHOD",
                "ID,NAME\n1,stack-method\n");
        DriverManagerDataSource dataSource = createDriverManagerDataSource();
        DataSourceRegistry.register(DB_NAME, dataSource);
        Connection connection = mockConnection("APP_USER",
                tableSpec("FA_STACK_METHOD", columns("ID", "NAME"),
                        sqlTypes(Types.INTEGER, Types.VARCHAR), typeNames("INTEGER", "VARCHAR"),
                        row(1, "stack-method")));

        try (MockedStatic<DataSourceUtils> dataSourceUtils =
                Mockito.mockStatic(DataSourceUtils.class)) {
            dataSourceUtils.when(() -> DataSourceUtils.getConnection(dataSource))
                    .thenReturn(connection);
            stackFlexAssert = createFlexAssert();
            new StackMethodCaller().execute();
        }
    }

    @Test
    void assertTable_正常ケース_スタックトレースからクラスアノテーションを解決する_一致すること() throws Exception {
        writeExpectedCsv(StackClassCaller.class, "stack-class", DB_NAME, "FA_STACK_CLASS",
                "ID,NAME\n1,stack-class\n");
        DriverManagerDataSource dataSource = createDriverManagerDataSource();
        DataSourceRegistry.register(DB_NAME, dataSource);
        Connection connection = mockConnection("APP_USER",
                tableSpec("FA_STACK_CLASS", columns("ID", "NAME"),
                        sqlTypes(Types.INTEGER, Types.VARCHAR), typeNames("INTEGER", "VARCHAR"),
                        row(1, "stack-class")));

        try (MockedStatic<DataSourceUtils> dataSourceUtils =
                Mockito.mockStatic(DataSourceUtils.class)) {
            dataSourceUtils.when(() -> DataSourceUtils.getConnection(dataSource))
                    .thenReturn(connection);
            stackFlexAssert = createFlexAssert();
            new StackClassCaller().execute();
        }
    }

    @Test
    void assertTable_正常ケース_スタックトレースからメソッドアノテーションを解決する別経路でも一致すること() throws Exception {
        writeExpectedCsv(AnnotatedMethodCaller.class, "stack-annotated-method", DB_NAME,
                "FA_STACK_ANNOTATED_METHOD", "ID,NAME\n1,annotated-method\n");
        DriverManagerDataSource dataSource = createDriverManagerDataSource();
        DataSourceRegistry.register(DB_NAME, dataSource);
        Connection connection = mockConnection("APP_USER",
                tableSpec("FA_STACK_ANNOTATED_METHOD", columns("ID", "NAME"),
                        sqlTypes(Types.INTEGER, Types.VARCHAR), typeNames("INTEGER", "VARCHAR"),
                        row(1, "annotated-method")));

        try (MockedStatic<DataSourceUtils> dataSourceUtils =
                Mockito.mockStatic(DataSourceUtils.class)) {
            dataSourceUtils.when(() -> DataSourceUtils.getConnection(dataSource))
                    .thenReturn(connection);
            stackFlexAssert = createFlexAssert();
            new AnnotatedMethodCaller().execute();
        }
    }

    @Test
    void assertTable_正常ケース_スレッドローカルがシナリオだけ設定されてもスタックトレースへフォールバックする_一致すること() throws Exception {
        LoadDataExtension.CURRENT_SCENARIO.set("ignored-scenario");
        writeExpectedCsv(StackMethodCaller.class, "stack-method", DB_NAME, "FA_STACK_METHOD",
                "ID,NAME\n1,stack-method\n");
        DriverManagerDataSource dataSource = createDriverManagerDataSource();
        DataSourceRegistry.register(DB_NAME, dataSource);
        Connection connection = mockConnection("APP_USER",
                tableSpec("FA_STACK_METHOD", columns("ID", "NAME"),
                        sqlTypes(Types.INTEGER, Types.VARCHAR), typeNames("INTEGER", "VARCHAR"),
                        row(1, "stack-method")));

        try (MockedStatic<DataSourceUtils> dataSourceUtils =
                Mockito.mockStatic(DataSourceUtils.class)) {
            dataSourceUtils.when(() -> DataSourceUtils.getConnection(dataSource))
                    .thenReturn(connection);
            stackFlexAssert = createFlexAssert();
            new StackMethodCaller().execute();
        }
    }

    @Test
    void assertTable_正常ケース_追加の数値LOB空文字分岐を通す_一致すること() throws Exception {
        LoadDataExtension.CURRENT_TEST_CLASS.set(FlexAssertTest.class);
        LoadDataExtension.CURRENT_SCENARIO.set("numeric-binary-empty");
        writeExpectedCsv(FlexAssertTest.class, "numeric-binary-empty", DB_NAME, "FA_COMPLEX",
                "ID,DECIMAL_COL,BIGINT_COL,TINYINT_COL,REAL_COL,DOUBLE_COL,CREATED_AT,BINARY_COL,EMPTY_TEXT\n"
                        + "1,t,1,0,1.5,2.5,14:05:30,file:files/payload.dat,\"\"\n");
        writeExpectedBinaryFile(FlexAssertTest.class, "numeric-binary-empty", DB_NAME,
                "files/payload.dat", new byte[] {0x01, 0x2A});

        DriverManagerDataSource dataSource = createDriverManagerDataSource();
        DataSourceRegistry.register(DB_NAME, dataSource);
        Connection connection = mockConnection("APP_USER", tableSpec("FA_COMPLEX",
                columns("ID", "DECIMAL_COL", "BIGINT_COL", "TINYINT_COL", "REAL_COL", "DOUBLE_COL",
                        "CREATED_AT", "BINARY_COL", "EMPTY_TEXT"),
                sqlTypes(Types.INTEGER, Types.DECIMAL, Types.BIGINT, Types.TINYINT, Types.REAL,
                        Types.DOUBLE, Types.TIMESTAMP, Types.BLOB, Types.VARCHAR),
                typeNames("INTEGER", "DECIMAL", "BIGINT", "TINYINT", "REAL", "DOUBLE", "TIMESTAMP",
                        "BLOB", "VARCHAR"),
                row(1, Integer.valueOf(1), Long.valueOf(1L), Integer.valueOf(0),
                        Float.valueOf(1.5F), Double.valueOf(2.5D), "14:05:30",
                        new byte[] {0x01, 0x2A}, "")));

        DbDialectHandler dialectHandler = createDialectHandlerStub();
        when(dialectHandler.isDateTimeTypeForDump(Types.TIMESTAMP, "TIMESTAMP")).thenReturn(false);

        try (MockedStatic<DataSourceUtils> dataSourceUtils =
                Mockito.mockStatic(DataSourceUtils.class)) {
            dataSourceUtils.when(() -> DataSourceUtils.getConnection(dataSource))
                    .thenReturn(connection);
            createFlexAssert(dialectHandler).assertTable(DB_NAME, "FA_COMPLEX");
        }
    }

    @Test
    void assertTable_正常ケース_expectedのnull値とLOBのnull解決を比較する_一致すること() throws Exception {
        LoadDataExtension.CURRENT_TEST_CLASS.set(FlexAssertTest.class);
        LoadDataExtension.CURRENT_SCENARIO.set("null-and-lob-null");
        writeExpectedCsv(FlexAssertTest.class, "null-and-lob-null", DB_NAME, "FA_NULL_CASE",
                "ID,NULL_FROM_CSV,LOB_NULL\n1\n");

        DriverManagerDataSource dataSource = createDriverManagerDataSource();
        DataSourceRegistry.register(DB_NAME, dataSource);
        Connection connection = mockConnection("APP_USER",
                tableSpec("FA_NULL_CASE", columns("ID", "NULL_FROM_CSV", "LOB_NULL"),
                        sqlTypes(Types.INTEGER, Types.VARCHAR, Types.BLOB),
                        typeNames("INTEGER", "VARCHAR", "BLOB"), row(1, "", "")));

        DbDialectHandler dialectHandler = createDialectHandlerStub();
        Mockito.doReturn(null).when(dialectHandler).readLobFile(anyString(), anyString(),
                anyString(), any());

        Path csv = expectedDir(FlexAssertTest.class, "null-and-lob-null", DB_NAME)
                .resolve("FA_NULL_CASE.csv");
        Files.writeString(csv, "ID,NULL_FROM_CSV,LOB_NULL\n1,,file:files/missing.dat\n",
                StandardCharsets.UTF_8);

        try (MockedStatic<DataSourceUtils> dataSourceUtils = Mockito.mockStatic(
                DataSourceUtils.class)) {
            dataSourceUtils.when(() -> DataSourceUtils.getConnection(dataSource))
                    .thenReturn(connection);
            createFlexAssert(dialectHandler).assertTable(DB_NAME, "FA_NULL_CASE");
        }
    }

    @Test
    void assertTable_正常ケース_数値型へ非数値文字列が入っても文字列比較へフォールバックする_一致すること() throws Exception {
        LoadDataExtension.CURRENT_TEST_CLASS.set(FlexAssertTest.class);
        LoadDataExtension.CURRENT_SCENARIO.set("non-numeric-fallback");
        writeExpectedCsv(FlexAssertTest.class, "non-numeric-fallback", DB_NAME, "FA_NON_NUMERIC",
                "ID,DECIMAL_TEXT\n1,abc\n");

        DriverManagerDataSource dataSource = createDriverManagerDataSource();
        DataSourceRegistry.register(DB_NAME, dataSource);
        Connection connection = mockConnection("APP_USER",
                tableSpec("FA_NON_NUMERIC", columns("ID", "DECIMAL_TEXT"),
                        sqlTypes(Types.INTEGER, Types.DECIMAL), typeNames("INTEGER", "DECIMAL"),
                        row(1, "abc")));

        try (MockedStatic<DataSourceUtils> dataSourceUtils = Mockito.mockStatic(
                DataSourceUtils.class)) {
            dataSourceUtils.when(() -> DataSourceUtils.getConnection(dataSource))
                    .thenReturn(connection);
            createFlexAssert().assertTable(DB_NAME, "FA_NON_NUMERIC");
        }
    }

    @Test
    void assertTable_正常ケース_TIMESTAMP列名がTIMEを含まない場合は時刻専用扱いしない_一致すること() throws Exception {
        LoadDataExtension.CURRENT_TEST_CLASS.set(FlexAssertTest.class);
        LoadDataExtension.CURRENT_SCENARIO.set("timestamp-non-time-column");
        writeExpectedCsv(FlexAssertTest.class, "timestamp-non-time-column", DB_NAME,
                "FA_TIMESTAMP_RAW", "ID,CREATED_AT\n1,14:05:30\n");

        DriverManagerDataSource dataSource = createDriverManagerDataSource();
        DataSourceRegistry.register(DB_NAME, dataSource);
        Connection connection = mockConnection("APP_USER",
                tableSpec("FA_TIMESTAMP_RAW", columns("ID", "CREATED_AT"),
                        sqlTypes(Types.INTEGER, Types.TIMESTAMP),
                        typeNames("INTEGER", "TIMESTAMP"), row(1, "14:05:30")));

        DbDialectHandler dialectHandler = createDialectHandlerStub();
        when(dialectHandler.isDateTimeTypeForDump(Types.TIMESTAMP, "TIMESTAMP")).thenReturn(false);

        try (MockedStatic<DataSourceUtils> dataSourceUtils = Mockito.mockStatic(
                DataSourceUtils.class)) {
            dataSourceUtils.when(() -> DataSourceUtils.getConnection(dataSource))
                    .thenReturn(connection);
            createFlexAssert(dialectHandler).assertTable(DB_NAME, "FA_TIMESTAMP_RAW");
        }
    }

    @Test
    void assertTable_正常ケース_TIMESTAMP列名がTIMESTAMPを含む場合は時刻専用扱いしない_一致すること() throws Exception {
        LoadDataExtension.CURRENT_TEST_CLASS.set(FlexAssertTest.class);
        LoadDataExtension.CURRENT_SCENARIO.set("timestamp-name-column");
        writeExpectedCsv(FlexAssertTest.class, "timestamp-name-column", DB_NAME,
                "FA_TIMESTAMP_NAME", "ID,EVENT_TIMESTAMP\n1,14:05:30\n");

        DriverManagerDataSource dataSource = createDriverManagerDataSource();
        DataSourceRegistry.register(DB_NAME, dataSource);
        Connection connection = mockConnection("APP_USER",
                tableSpec("FA_TIMESTAMP_NAME", columns("ID", "EVENT_TIMESTAMP"),
                        sqlTypes(Types.INTEGER, Types.TIMESTAMP),
                        typeNames("INTEGER", "TIMESTAMP"), row(1, "14:05:30")));

        DbDialectHandler dialectHandler = createDialectHandlerStub();
        when(dialectHandler.isDateTimeTypeForDump(Types.TIMESTAMP, "TIMESTAMP")).thenReturn(false);

        try (MockedStatic<DataSourceUtils> dataSourceUtils = Mockito.mockStatic(
                DataSourceUtils.class)) {
            dataSourceUtils.when(() -> DataSourceUtils.getConnection(dataSource))
                    .thenReturn(connection);
            createFlexAssert(dialectHandler).assertTable(DB_NAME, "FA_TIMESTAMP_NAME");
        }
    }

    private FlexAssert createFlexAssert() throws Exception {
        DbDialectHandler dialectHandler = createDialectHandlerStub();
        return createFlexAssert(dialectHandler);
    }

    private FlexAssert createFlexAssert(DbDialectHandler dialectHandler) {
        return new FlexAssert(Collections.emptySet(), Collections.emptyMap(),
                (classRoot, entry, conn) -> dialectHandler);
    }

    private DbDialectHandler createDialectHandlerStub() throws Exception {
        DbDialectHandler dialectHandler = mock(DbDialectHandler.class);
        when(dialectHandler.resolveSchema(any())).thenReturn("APP_USER");
        when(dialectHandler.isBinaryTypeForDump(anyInt(), anyString())).thenAnswer(invocation -> {
            int sqlType = invocation.getArgument(0);
            return sqlType == Types.BINARY || sqlType == Types.VARBINARY
                    || sqlType == Types.LONGVARBINARY || sqlType == Types.BLOB;
        });
        when(dialectHandler.isDateTimeTypeForDump(anyInt(), anyString())).thenAnswer(invocation -> {
            int sqlType = invocation.getArgument(0);
            return sqlType == Types.DATE || sqlType == Types.TIME
                    || sqlType == Types.TIME_WITH_TIMEZONE || sqlType == Types.TIMESTAMP
                    || sqlType == Types.TIMESTAMP_WITH_TIMEZONE;
        });
        when(dialectHandler.formatDateTimeColumn(anyString(), any(), any()))
                .thenAnswer(invocation -> invocation.getArgument(1).toString());
        when(dialectHandler.formatDbValueForCsv(anyString(), any())).thenAnswer(invocation -> {
            Object value = invocation.getArgument(1, Object.class);
            return Objects.toString(value, null);
        });
        when(dialectHandler.readLobFile(anyString(), anyString(), anyString(), any())).thenAnswer(
                invocation -> readLobFile(invocation.getArgument(0), invocation.getArgument(3)));
        return dialectHandler;
    }

    private Object readLobFile(String fileRef, java.io.File baseDir) throws IOException {
        Path path = baseDir.toPath().resolve(fileRef);
        if (fileRef.endsWith(".dat")) {
            return Files.readAllBytes(path);
        }
        return Files.readString(path, StandardCharsets.UTF_8);
    }

    private DriverManagerDataSource createDriverManagerDataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setUrl("jdbc:postgresql://localhost:5432/flexassert");
        dataSource.setUsername("APP_USER");
        dataSource.setPassword("secret");
        return dataSource;
    }

    private void writeExpectedBinaryFile(Class<?> testClass, String scenario, String dbName,
            String relativePath, byte[] content) throws IOException {
        Path path = expectedDir(testClass, scenario, dbName).resolve(relativePath);
        Files.createDirectories(path.getParent());
        Files.write(path, content);
    }

    private Connection mockConnection(String userName, TableSpec... tableSpecs) throws Exception {
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        when(connection.createStatement()).thenReturn(statement);
        when(connection.getMetaData()).thenReturn(metaData);
        when(connection.getCatalog()).thenReturn(null);
        when(metaData.getURL()).thenReturn("jdbc:postgresql://localhost:5432/flexassert");
        when(metaData.getUserName()).thenReturn(userName);
        when(metaData.getIdentifierQuoteString()).thenReturn("\"");
        when(metaData.storesLowerCaseIdentifiers()).thenReturn(true);
        when(metaData.storesUpperCaseIdentifiers()).thenReturn(false);
        when(metaData.storesMixedCaseIdentifiers()).thenReturn(false);
        when(metaData.supportsMixedCaseIdentifiers()).thenReturn(false);

        Map<String, ResultSet> queryResults = new HashMap<>();
        Map<String, ResultSet> primaryKeys = new HashMap<>();
        for (TableSpec spec : tableSpecs) {
            queryResults.put("SELECT * FROM " + spec.tableName, createRowResultSet(spec));
            primaryKeys.put(spec.tableName, createPrimaryKeyResultSet(spec.primaryKeys));
        }

        when(statement.executeQuery(anyString())).thenAnswer(invocation -> {
            String sql = invocation.getArgument(0);
            ResultSet resultSet = queryResults.get(sql);
            if (resultSet == null) {
                throw new IllegalArgumentException("Unexpected SQL: " + sql);
            }
            return resultSet;
        });
        when(metaData.getPrimaryKeys(any(), any(), anyString())).thenAnswer(invocation -> {
            String tableName = invocation.getArgument(2);
            return primaryKeys.get(tableName);
        });
        return connection;
    }

    private ResultSet createRowResultSet(TableSpec spec) throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        AtomicInteger rowIndex = new AtomicInteger(-1);
        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(spec.columnNames.length);
        for (int i = 0; i < spec.columnNames.length; i++) {
            int index = i + 1;
            int valueIndex = i;
            when(metaData.getColumnLabel(index)).thenReturn(spec.columnNames[i]);
            when(metaData.getColumnType(index)).thenReturn(spec.sqlTypes[i]);
            when(metaData.getColumnTypeName(index)).thenReturn(spec.typeNames[i]);
            when(resultSet.findColumn(spec.columnNames[i])).thenReturn(index);
            when(resultSet.getObject(index))
                    .thenAnswer(invocation -> spec.rows.get(rowIndex.get())[valueIndex]);
            when(resultSet.getBytes(index)).thenAnswer(invocation -> {
                Object value = spec.rows.get(rowIndex.get())[valueIndex];
                if (value instanceof byte[]) {
                    return value;
                }
                return null;
            });
            when(resultSet.getDate(index)).thenAnswer(invocation -> {
                Object value = spec.rows.get(rowIndex.get())[valueIndex];
                if (value instanceof Date) {
                    return value;
                }
                return null;
            });
            when(resultSet.getTime(index)).thenAnswer(invocation -> {
                Object value = spec.rows.get(rowIndex.get())[valueIndex];
                if (value instanceof Time) {
                    return value;
                }
                return null;
            });
            when(resultSet.getTimestamp(index)).thenAnswer(invocation -> {
                Object value = spec.rows.get(rowIndex.get())[valueIndex];
                if (value instanceof Timestamp) {
                    return value;
                }
                return null;
            });
        }
        when(resultSet.next())
                .thenAnswer(invocation -> rowIndex.incrementAndGet() < spec.rows.size());
        return resultSet;
    }

    private ResultSet createPrimaryKeyResultSet(List<String> primaryKeys) throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        AtomicInteger index = new AtomicInteger(-1);
        when(resultSet.next())
                .thenAnswer(invocation -> index.incrementAndGet() < primaryKeys.size());
        when(resultSet.getString("COLUMN_NAME"))
                .thenAnswer(invocation -> primaryKeys.get(index.get()));
        return resultSet;
    }

    private void writeExpectedCsv(Class<?> owner, String scenario, String dbName, String tableName,
            String body) throws IOException {
        Path dir = expectedDir(owner, scenario, dbName);
        Files.createDirectories(dir);
        Files.writeString(dir.resolve(tableName + ".csv"), body, StandardCharsets.UTF_8);
        updateTableOrdering(dir, tableName);
    }

    private void updateTableOrdering(Path dir, String tableName) throws IOException {
        Path orderingFile = dir.resolve("table-ordering.txt");
        List<String> tableNames = new ArrayList<>();
        if (Files.exists(orderingFile)) {
            tableNames.addAll(Files.readAllLines(orderingFile, StandardCharsets.UTF_8));
        }
        if (!tableNames.contains(tableName)) {
            tableNames.add(tableName);
        }
        Files.write(orderingFile, tableNames, StandardCharsets.UTF_8);
    }

    private void writeExpectedFile(Class<?> owner, String scenario, String dbName,
            String relativePath, byte[] content) throws IOException {
        Path path = expectedDir(owner, scenario, dbName).resolve(relativePath);
        Files.createDirectories(path.getParent());
        Files.write(path, content);
    }

    private void writeExpectedTextFile(Class<?> owner, String scenario, String dbName,
            String relativePath, String content) throws IOException {
        Path path = expectedDir(owner, scenario, dbName).resolve(relativePath);
        Files.createDirectories(path.getParent());
        Files.writeString(path, content, StandardCharsets.UTF_8);
    }

    private Path expectedDir(Class<?> owner, String scenario, String dbName) {
        return classRoot(owner).resolve(scenario).resolve("expected").resolve(dbName);
    }

    private Path classRoot(Class<?> owner) {
        String packagePath = owner.getPackage().getName().replace('.', '/');
        return Paths.get("target", "test-classes", packagePath, owner.getSimpleName())
                .toAbsolutePath().normalize();
    }

    private void deleteClassRoot(Class<?> owner) throws IOException {
        Path root = classRoot(owner);
        if (Files.exists(root)) {
            FileUtils.deleteDirectory(root.toFile());
        }
    }

    private TableSpec tableSpec(String tableName, String[] columnNames, int[] sqlTypes,
            String[] typeNames, Object[]... rows) {
        return new TableSpec(tableName, columnNames, sqlTypes, typeNames, List.of("ID"),
                List.of(rows));
    }

    private TableSpec tableSpec(String tableName, String[] columnNames, int[] sqlTypes,
            String[] typeNames, List<String> primaryKeys, Object[]... rows) {
        return new TableSpec(tableName, columnNames, sqlTypes, typeNames, primaryKeys,
                List.of(rows));
    }

    private String[] columns(String... values) {
        return values;
    }

    private int[] sqlTypes(int... values) {
        return values;
    }

    private String[] typeNames(String... values) {
        return values;
    }

    private Object[] row(Object... values) {
        return values;
    }

    private static final class TableSpec {
        private final String tableName;
        private final String[] columnNames;
        private final int[] sqlTypes;
        private final String[] typeNames;
        private final List<String> primaryKeys;
        private final List<Object[]> rows;

        private TableSpec(String tableName, String[] columnNames, int[] sqlTypes,
                String[] typeNames, List<String> primaryKeys, List<Object[]> rows) {
            this.tableName = tableName;
            this.columnNames = columnNames;
            this.sqlTypes = sqlTypes;
            this.typeNames = typeNames;
            this.primaryKeys = primaryKeys;
            this.rows = rows;
        }
    }
}
