package io.github.yok.flexdblink.db.sqlserver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.CsvDateTimeFormatProperties;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.db.DbUnitConfigFactory;
import io.github.yok.flexdblink.util.OracleDateTimeFormatUtil;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.junit.jupiter.api.Test;

class SqlServerDialectHandlerTest {

    @Test
    void prepareConnection_正常ケース_接続初期化を実行する_SQLServer向けSQLが実行されること() throws Exception {
        SqlServerDialectHandler handler = createHandler();
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        when(connection.createStatement()).thenReturn(statement);

        handler.prepareConnection(connection);

        verify(statement).execute("SET LANGUAGE us_english");
        verify(statement).execute("SET DATEFORMAT ymd");
    }

    @Test
    void resolveSchema_正常ケース_接続情報を指定する_dboが返ること() throws Exception {
        SqlServerDialectHandler handler = createHandler();
        assertEquals("dbo", handler.resolveSchema(new ConnectionConfig.Entry()));
    }

    @Test
    void applyPagination_正常ケース_offsetlimitを指定する_OffsetFetch形式が返ること() throws Exception {
        SqlServerDialectHandler handler = createHandler();
        assertEquals("SELECT * FROM T OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY",
                handler.applyPagination("SELECT * FROM T", 5, 10));
    }

    @Test
    void quoteIdentifier_正常ケース_識別子を指定する_角括弧で囲まれること() throws Exception {
        SqlServerDialectHandler handler = createHandler();
        assertEquals("[ID]", handler.quoteIdentifier("ID"));
    }

    @Test
    void booleanLiteral_正常ケース_真偽値リテラルを取得する_10が返ること() throws Exception {
        SqlServerDialectHandler handler = createHandler();
        assertEquals("1", handler.getBooleanTrueLiteral());
        assertEquals("0", handler.getBooleanFalseLiteral());
    }

    @Test
    void getCurrentTimestampFunction_正常ケース_関数を取得する_SYSDATETIMEが返ること() throws Exception {
        SqlServerDialectHandler handler = createHandler();
        assertEquals("SYSDATETIME()", handler.getCurrentTimestampFunction());
    }

    @Test
    void formatDateLiteral_正常ケース_日時を指定する_DATETIME2キャスト文字列が返ること() throws Exception {
        SqlServerDialectHandler handler = createHandler();
        String actual = handler.formatDateLiteral(LocalDateTime.of(2026, 2, 19, 7, 0, 1));
        assertEquals("CAST('2026-02-19 07:00:01' AS DATETIME2)", actual);
    }

    @Test
    void buildUpsertSql_正常ケース_列定義を指定する_MERGE文が返ること() throws Exception {
        SqlServerDialectHandler handler = createHandler();
        String sql = handler.buildUpsertSql("T_MAIN", List.of("ID"), List.of("ID", "NAME"),
                List.of("NAME"));
        assertTrue(sql.startsWith("MERGE INTO [T_MAIN] AS tgt"));
        assertTrue(sql.contains("WHEN MATCHED THEN UPDATE SET"));
        assertTrue(sql.contains("WHEN NOT MATCHED THEN INSERT ([ID], [NAME]) VALUES"));
    }

    @Test
    void getCreateTempTableSql_正常ケース_列定義を指定する_ハッシュ始まりの作成SQLが返ること() throws Exception {
        SqlServerDialectHandler handler = createHandler();
        Map<String, String> columns = new LinkedHashMap<>();
        columns.put("ID", "BIGINT");
        columns.put("NAME", "NVARCHAR(20)");
        String sql = handler.getCreateTempTableSql("TMP_MAIN", columns);
        assertEquals("CREATE TABLE #TMP_MAIN ([ID] BIGINT, [NAME] NVARCHAR(20))", sql);
    }

    @Test
    void applyForUpdate_正常ケース_select文を指定する_ロックヒント付きSQLが返ること() throws Exception {
        SqlServerDialectHandler handler = createHandler();
        assertEquals("SELECT * FROM T WITH (UPDLOCK, ROWLOCK)",
                handler.applyForUpdate("SELECT * FROM T"));
    }

    @Test
    void getDataTypeFactory_正常ケース_型ファクトリを取得する_SQLServer用が返ること() throws Exception {
        SqlServerDialectHandler handler = createHandler();
        assertTrue(handler.getDataTypeFactory() instanceof CustomSqlServerDataTypeFactory);
    }

    /**
     * Creates handler with minimum JDBC/DBUnit mocks.
     *
     * @return SQL Server handler
     * @throws Exception when construction fails
     */
    private SqlServerDialectHandler createHandler() throws Exception {
        DatabaseConnection dbConn = mock(DatabaseConnection.class);
        Connection jdbc = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet tables = mock(ResultSet.class);
        IDataSet dataSet = mock(IDataSet.class);

        when(dbConn.getConnection()).thenReturn(jdbc);
        when(jdbc.getSchema()).thenReturn("dbo");
        when(jdbc.getMetaData()).thenReturn(meta);
        when(meta.getTables(null, "dbo", "%", new String[] {"TABLE"})).thenReturn(tables);
        when(tables.next()).thenReturn(false);
        when(dbConn.createDataSet()).thenReturn(dataSet);

        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        dbUnitConfig.setLobDirName("files");
        DumpConfig dumpConfig = new DumpConfig();
        dumpConfig.setExcludeTables(List.of("flyway_schema_history"));
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(".");

        CsvDateTimeFormatProperties props = new CsvDateTimeFormatProperties();
        props.setDate("yyyy-MM-dd");
        props.setTime("HH:mm:ss");
        props.setDateTime("yyyy-MM-dd HH:mm:ss");
        props.setDateTimeWithMillis("yyyy-MM-dd HH:mm:ss.SSS");
        OracleDateTimeFormatUtil formatter = new OracleDateTimeFormatUtil(props);

        return new SqlServerDialectHandler(dbConn, dumpConfig, dbUnitConfig,
                new DbUnitConfigFactory(), formatter, pathsConfig);
    }
}
