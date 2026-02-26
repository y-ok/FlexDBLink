package io.github.yok.flexdblink.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DataTypeFactoryMode;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.db.mysql.MySqlDialectHandler;
import io.github.yok.flexdblink.db.oracle.OracleDialectHandler;
import io.github.yok.flexdblink.db.postgresql.PostgresqlDialectHandler;
import io.github.yok.flexdblink.db.sqlserver.SqlServerDialectHandler;
import io.github.yok.flexdblink.util.DateTimeFormatUtil;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.datatype.IDataTypeFactory;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

class DbDialectHandlerFactoryTest {

    /**
     * Invokes the private resolveMode method.
     *
     * @param factory target factory
     * @param entry connection entry
     * @return resolved mode
     * @throws Exception when reflection fails
     */
    private DataTypeFactoryMode invokeResolveMode(DbDialectHandlerFactory factory,
            ConnectionConfig.Entry entry) throws Exception {
        Method method = DbDialectHandlerFactory.class.getDeclaredMethod("resolveMode",
                ConnectionConfig.Entry.class);
        method.setAccessible(true);
        return (DataTypeFactoryMode) method.invoke(factory, entry);
    }

    /**
     * Invokes the private resolveModeFromDriverClass method.
     *
     * @param factory target factory
     * @param driverClass driver class name
     * @return resolved mode or {@code null}
     * @throws Exception when reflection fails
     */
    private DataTypeFactoryMode invokeResolveModeFromDriverClass(DbDialectHandlerFactory factory,
            String driverClass) throws Exception {
        Method method = DbDialectHandlerFactory.class
                .getDeclaredMethod("resolveModeFromDriverClass", String.class);
        method.setAccessible(true);
        return (DataTypeFactoryMode) method.invoke(factory, driverClass);
    }

    /**
     * Invokes the private resolveModeFromJdbcUrl method.
     *
     * @param factory target factory
     * @param jdbcUrl JDBC URL
     * @return resolved mode or {@code null}
     * @throws Exception when reflection fails
     */
    private DataTypeFactoryMode invokeResolveModeFromJdbcUrl(DbDialectHandlerFactory factory,
            String jdbcUrl) throws Exception {
        Method method = DbDialectHandlerFactory.class.getDeclaredMethod("resolveModeFromJdbcUrl",
                String.class);
        method.setAccessible(true);
        return (DataTypeFactoryMode) method.invoke(factory, jdbcUrl);
    }

    /**
     * Invokes the private normalizeLower method.
     *
     * @param factory target factory
     * @param value input value
     * @return normalized value or {@code null}
     * @throws Exception when reflection fails
     */
    private String invokeNormalizeLower(DbDialectHandlerFactory factory, String value)
            throws Exception {
        Method method =
                DbDialectHandlerFactory.class.getDeclaredMethod("normalizeLower", String.class);
        method.setAccessible(true);
        return (String) method.invoke(factory, value);
    }

    @Test
    void create_正常ケース_OracleURLを指定する_OracleDialectHandlerが返ること() throws Exception {
        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        DumpConfig dumpConfig = new DumpConfig();
        PathsConfig pathsConfig = new PathsConfig();
        DbUnitConfigFactory configFactory = mock(DbUnitConfigFactory.class);
        DateTimeFormatUtil dateTimeFormatUtil = mock(DateTimeFormatUtil.class);
        DbDialectHandlerFactory factory = new DbDialectHandlerFactory(dbUnitConfig, dumpConfig,
                pathsConfig, dateTimeFormatUtil, configFactory);

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setUrl("jdbc:oracle:thin:@localhost:1521/OPEDB");
        entry.setUser("app");
        entry.setPassword("pw");

        Connection jdbc = mock(Connection.class);
        DatabaseConfig databaseConfig = mock(DatabaseConfig.class);
        IDataTypeFactory dataTypeFactory = mock(IDataTypeFactory.class);

        try (MockedStatic<DriverManager> driverManagerMock = mockStatic(DriverManager.class);
                MockedConstruction<DatabaseConnection> dbConnMock =
                        mockConstruction(DatabaseConnection.class, (mock, context) -> {
                            when(mock.getConfig()).thenReturn(databaseConfig);
                        });
                MockedConstruction<OracleDialectHandler> handlerMock =
                        mockConstruction(OracleDialectHandler.class, (mock, context) -> {
                            when(mock.getDataTypeFactory()).thenReturn(dataTypeFactory);
                        })) {

            driverManagerMock
                    .when(() -> DriverManager
                            .getConnection("jdbc:oracle:thin:@localhost:1521/OPEDB", "app", "pw"))
                    .thenReturn(jdbc);

            DbDialectHandler actual = factory.create(entry);
            OracleDialectHandler created = handlerMock.constructed().get(0);
            assertSame(created, actual);
            verify(created).prepareConnection(jdbc);
            verify(configFactory).configure(eq(databaseConfig), eq(dataTypeFactory));
        }
    }

    @Test
    void create_異常ケース_Oracle接続でSQL例外が発生する_IllegalStateExceptionが送出されること() {
        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        DbDialectHandlerFactory factory =
                new DbDialectHandlerFactory(dbUnitConfig, new DumpConfig(), new PathsConfig(),
                        mock(DateTimeFormatUtil.class), mock(DbUnitConfigFactory.class));

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setUrl("jdbc:mock");
        entry.setUser("app");
        entry.setPassword("pw");
        entry.setDriverClass("oracle.jdbc.OracleDriver");

        try (MockedStatic<DriverManager> driverManagerMock = mockStatic(DriverManager.class)) {
            driverManagerMock.when(() -> DriverManager.getConnection("jdbc:mock", "app", "pw"))
                    .thenAnswer(invocation -> {
                        throw new SQLException("connect failed");
                    });
            assertThrows(IllegalStateException.class, () -> factory.create(entry));
        }
    }

    @Test
    void create_正常ケース_SQLServerURLを指定する_SqlServerDialectHandlerが返ること() throws Exception {
        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        DumpConfig dumpConfig = new DumpConfig();
        PathsConfig pathsConfig = new PathsConfig();
        DbUnitConfigFactory configFactory = mock(DbUnitConfigFactory.class);
        DateTimeFormatUtil dateTimeFormatUtil = mock(DateTimeFormatUtil.class);
        DbDialectHandlerFactory factory = new DbDialectHandlerFactory(dbUnitConfig, dumpConfig,
                pathsConfig, dateTimeFormatUtil, configFactory);

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setUrl("jdbc:sqlserver://localhost:1433;databaseName=testdb");
        entry.setUser("app");
        entry.setPassword("pw");

        Connection jdbc = mock(Connection.class);
        DatabaseConfig databaseConfig = mock(DatabaseConfig.class);
        IDataTypeFactory dataTypeFactory = mock(IDataTypeFactory.class);

        try (MockedStatic<DriverManager> driverManagerMock = mockStatic(DriverManager.class);
                MockedConstruction<DatabaseConnection> dbConnMock =
                        mockConstruction(DatabaseConnection.class, (mock, context) -> {
                            when(mock.getConfig()).thenReturn(databaseConfig);
                        });
                MockedConstruction<SqlServerDialectHandler> handlerMock =
                        mockConstruction(SqlServerDialectHandler.class, (mock, context) -> {
                            when(mock.getDataTypeFactory()).thenReturn(dataTypeFactory);
                        })) {

            driverManagerMock
                    .when(() -> DriverManager.getConnection(
                            "jdbc:sqlserver://localhost:1433;databaseName=testdb", "app", "pw"))
                    .thenReturn(jdbc);

            DbDialectHandler actual = factory.create(entry);
            SqlServerDialectHandler created = handlerMock.constructed().get(0);
            assertSame(created, actual);
            verify(created).prepareConnection(jdbc);
            verify(configFactory).configure(eq(databaseConfig), eq(dataTypeFactory));
        }
    }

    @Test
    void create_正常ケース_MySQLURLを指定する_MySqlDialectHandlerが返ること() throws Exception {
        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        DumpConfig dumpConfig = new DumpConfig();
        PathsConfig pathsConfig = new PathsConfig();
        DbUnitConfigFactory configFactory = mock(DbUnitConfigFactory.class);
        DateTimeFormatUtil dateTimeFormatUtil = mock(DateTimeFormatUtil.class);
        DbDialectHandlerFactory factory = new DbDialectHandlerFactory(dbUnitConfig, dumpConfig,
                pathsConfig, dateTimeFormatUtil, configFactory);

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setUrl("jdbc:mysql://localhost:3306/testdb");
        entry.setUser("app");
        entry.setPassword("pw");

        Connection jdbc = mock(Connection.class);
        DatabaseConfig databaseConfig = mock(DatabaseConfig.class);
        IDataTypeFactory dataTypeFactory = mock(IDataTypeFactory.class);

        try (MockedStatic<DriverManager> driverManagerMock = mockStatic(DriverManager.class);
                MockedConstruction<DatabaseConnection> dbConnMock =
                        mockConstruction(DatabaseConnection.class, (mock, context) -> {
                            when(mock.getConfig()).thenReturn(databaseConfig);
                        });
                MockedConstruction<MySqlDialectHandler> handlerMock =
                        mockConstruction(MySqlDialectHandler.class, (mock, context) -> {
                            when(mock.getDataTypeFactory()).thenReturn(dataTypeFactory);
                        })) {

            driverManagerMock.when(() -> DriverManager
                    .getConnection("jdbc:mysql://localhost:3306/testdb", "app", "pw"))
                    .thenReturn(jdbc);

            DbDialectHandler actual = factory.create(entry);
            MySqlDialectHandler created = handlerMock.constructed().get(0);
            assertSame(created, actual);
            verify(created).prepareConnection(jdbc);
            verify(configFactory).configure(eq(databaseConfig), eq(dataTypeFactory));
        }
    }

    @Test
    void create_正常ケース_PostgreSQLURLを指定する_PostgresqlDialectHandlerが返ること() throws Exception {
        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        DumpConfig dumpConfig = new DumpConfig();
        PathsConfig pathsConfig = new PathsConfig();
        DbUnitConfigFactory configFactory = mock(DbUnitConfigFactory.class);
        DateTimeFormatUtil dateTimeFormatUtil = mock(DateTimeFormatUtil.class);
        DbDialectHandlerFactory factory = new DbDialectHandlerFactory(dbUnitConfig, dumpConfig,
                pathsConfig, dateTimeFormatUtil, configFactory);

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setUrl("jdbc:postgresql://localhost:5432/testdb");
        entry.setUser("app");
        entry.setPassword("pw");

        Connection jdbc = mock(Connection.class);
        DatabaseConfig databaseConfig = mock(DatabaseConfig.class);
        IDataTypeFactory dataTypeFactory = mock(IDataTypeFactory.class);

        try (MockedStatic<DriverManager> driverManagerMock = mockStatic(DriverManager.class);
                MockedConstruction<DatabaseConnection> dbConnMock =
                        mockConstruction(DatabaseConnection.class, (mock, context) -> {
                            when(mock.getConfig()).thenReturn(databaseConfig);
                        });
                MockedConstruction<PostgresqlDialectHandler> handlerMock =
                        mockConstruction(PostgresqlDialectHandler.class, (mock, context) -> {
                            when(mock.getDataTypeFactory()).thenReturn(dataTypeFactory);
                        })) {

            driverManagerMock
                    .when(() -> DriverManager
                            .getConnection("jdbc:postgresql://localhost:5432/testdb", "app", "pw"))
                    .thenReturn(jdbc);

            DbDialectHandler actual = factory.create(entry);
            PostgresqlDialectHandler created = handlerMock.constructed().get(0);
            assertSame(created, actual);
            verify(created).prepareConnection(jdbc);
            verify(configFactory).configure(eq(databaseConfig), eq(dataTypeFactory));
        }
    }

    @Test
    void create_異常ケース_DBUnit初期化でDataSetExceptionが発生する_IllegalStateExceptionが送出されること()
            throws Exception {
        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        DumpConfig dumpConfig = new DumpConfig();
        PathsConfig pathsConfig = new PathsConfig();
        DbUnitConfigFactory configFactory = mock(DbUnitConfigFactory.class);
        DateTimeFormatUtil dateTimeFormatUtil = mock(DateTimeFormatUtil.class);
        DbDialectHandlerFactory factory = new DbDialectHandlerFactory(dbUnitConfig, dumpConfig,
                pathsConfig, dateTimeFormatUtil, configFactory);

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setUrl("jdbc:mock");
        entry.setUser("app");
        entry.setPassword("pw");
        entry.setDriverClass("oracle.jdbc.OracleDriver");

        Connection jdbc = mock(Connection.class);
        DatabaseConfig databaseConfig = mock(DatabaseConfig.class);

        try (MockedStatic<DriverManager> driverManagerMock = mockStatic(DriverManager.class);
                MockedConstruction<DatabaseConnection> dbConnMock =
                        mockConstruction(DatabaseConnection.class, (mock, context) -> {
                            when(mock.getConfig()).thenReturn(databaseConfig);
                        });
                MockedConstruction<OracleDialectHandler> handlerMock =
                        mockConstruction(OracleDialectHandler.class, (mock, context) -> {
                            when(mock.getDataTypeFactory()).thenAnswer(invocation -> {
                                throw new DataSetException("x");
                            });
                        })) {
            driverManagerMock.when(() -> DriverManager.getConnection("jdbc:mock", "app", "pw"))
                    .thenReturn(jdbc);
            IllegalStateException ex =
                    assertThrows(IllegalStateException.class, () -> factory.create(entry));
            assertEquals("Failed to initialize DBUnit dataset", ex.getMessage());
            assertTrue(ex.getCause() instanceof DataSetException);
        }
    }

    @Test
    void create_異常ケース_DBUnit設定時にRuntimeExceptionが発生する_IllegalStateExceptionが送出されること()
            throws Exception {
        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        DumpConfig dumpConfig = new DumpConfig();
        PathsConfig pathsConfig = new PathsConfig();
        DbUnitConfigFactory configFactory = mock(DbUnitConfigFactory.class);
        DateTimeFormatUtil dateTimeFormatUtil = mock(DateTimeFormatUtil.class);
        DbDialectHandlerFactory factory = new DbDialectHandlerFactory(dbUnitConfig, dumpConfig,
                pathsConfig, dateTimeFormatUtil, configFactory);

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setUrl("jdbc:mock");
        entry.setUser("app");
        entry.setPassword("pw");
        entry.setDriverClass("oracle.jdbc.OracleDriver");

        Connection jdbc = mock(Connection.class);
        DatabaseConfig databaseConfig = mock(DatabaseConfig.class);
        IDataTypeFactory dataTypeFactory = mock(IDataTypeFactory.class);
        doThrow(new RuntimeException("config failed")).when(configFactory)
                .configure(eq(databaseConfig), eq(dataTypeFactory));

        try (MockedStatic<DriverManager> driverManagerMock = mockStatic(DriverManager.class);
                MockedConstruction<DatabaseConnection> dbConnMock =
                        mockConstruction(DatabaseConnection.class, (mock, context) -> {
                            when(mock.getConfig()).thenReturn(databaseConfig);
                        });
                MockedConstruction<OracleDialectHandler> handlerMock =
                        mockConstruction(OracleDialectHandler.class, (mock, context) -> {
                            when(mock.getDataTypeFactory()).thenReturn(dataTypeFactory);
                        })) {

            driverManagerMock.when(() -> DriverManager.getConnection("jdbc:mock", "app", "pw"))
                    .thenReturn(jdbc);
            assertThrows(IllegalStateException.class, () -> factory.create(entry));
        }
    }

    @Test
    void private_正常ケース_resolveModeを呼び出す_driverClassがOracleのときORACLEが返ること() throws Exception {
        DbDialectHandlerFactory factory =
                new DbDialectHandlerFactory(new DbUnitConfig(), new DumpConfig(), new PathsConfig(),
                        mock(DateTimeFormatUtil.class), mock(DbUnitConfigFactory.class));

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setDriverClass("oracle.jdbc.OracleDriver");
        entry.setUrl("jdbc:postgresql://localhost:5432/testdb");

        assertEquals(DataTypeFactoryMode.ORACLE, invokeResolveMode(factory, entry));
    }

    @Test
    void private_正常ケース_resolveModeを呼び出す_driverClassがPostgreSQLのときPOSTGRESQLが返ること()
            throws Exception {
        DbDialectHandlerFactory factory =
                new DbDialectHandlerFactory(new DbUnitConfig(), new DumpConfig(), new PathsConfig(),
                        mock(DateTimeFormatUtil.class), mock(DbUnitConfigFactory.class));

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setDriverClass("org.postgresql.Driver");
        entry.setUrl("jdbc:mysql://localhost:3306/testdb");

        assertEquals(DataTypeFactoryMode.POSTGRESQL, invokeResolveMode(factory, entry));
    }

    @Test
    void private_正常ケース_resolveModeを呼び出す_driverClassがMySQLのときMYSQLが返ること() throws Exception {
        DbDialectHandlerFactory factory =
                new DbDialectHandlerFactory(new DbUnitConfig(), new DumpConfig(), new PathsConfig(),
                        mock(DateTimeFormatUtil.class), mock(DbUnitConfigFactory.class));

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setDriverClass("com.mysql.cj.jdbc.Driver");
        entry.setUrl("jdbc:sqlserver://localhost:1433;databaseName=testdb");

        assertEquals(DataTypeFactoryMode.MYSQL, invokeResolveMode(factory, entry));
    }

    @Test
    void private_正常ケース_resolveModeFromDriverClassを呼び出す_旧MySQLドライバを指定する_MYSQLが返ること()
            throws Exception {
        DbDialectHandlerFactory factory =
                new DbDialectHandlerFactory(new DbUnitConfig(), new DumpConfig(), new PathsConfig(),
                        mock(DateTimeFormatUtil.class), mock(DbUnitConfigFactory.class));

        assertEquals(DataTypeFactoryMode.MYSQL,
                invokeResolveModeFromDriverClass(factory, "com.mysql.jdbc.Driver"));
    }

    @Test
    void private_正常ケース_resolveModeを呼び出す_driverClassがSQLServerのときSQLSERVERが返ること() throws Exception {
        DbDialectHandlerFactory factory =
                new DbDialectHandlerFactory(new DbUnitConfig(), new DumpConfig(), new PathsConfig(),
                        mock(DateTimeFormatUtil.class), mock(DbUnitConfigFactory.class));

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setDriverClass("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        entry.setUrl("jdbc:oracle:thin:@localhost:1521/OPEDB");

        assertEquals(DataTypeFactoryMode.SQLSERVER, invokeResolveMode(factory, entry));
    }

    @Test
    void create_異常ケース_接続情報からDB種別を判定できない_IllegalStateExceptionが送出されること() {
        DumpConfig dumpConfig = new DumpConfig();
        PathsConfig pathsConfig = new PathsConfig();
        DbUnitConfigFactory configFactory = mock(DbUnitConfigFactory.class);
        DateTimeFormatUtil dateTimeFormatUtil = mock(DateTimeFormatUtil.class);
        DbDialectHandlerFactory factory = new DbDialectHandlerFactory(new DbUnitConfig(),
                dumpConfig, pathsConfig, dateTimeFormatUtil, configFactory);

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("DBX");
        entry.setUrl("jdbc:unknown://localhost/db");
        entry.setUser("app");
        entry.setPassword("pw");
        entry.setDriverClass("com.example.UnknownDriver");

        IllegalStateException ex =
                assertThrows(IllegalStateException.class, () -> factory.create(entry));
        assertEquals("Failed to create DbDialectHandler", ex.getMessage());
        assertTrue(ex.getCause() instanceof IllegalArgumentException);
        assertTrue(ex.getCause().getMessage().contains("DBX"));
    }

    @Test
    void private_正常ケース_resolveModeFromJdbcUrlを呼び出す_未知URLを指定する_nullが返ること() throws Exception {
        DbDialectHandlerFactory factory =
                new DbDialectHandlerFactory(new DbUnitConfig(), new DumpConfig(), new PathsConfig(),
                        mock(DateTimeFormatUtil.class), mock(DbUnitConfigFactory.class));

        assertNull(invokeResolveModeFromJdbcUrl(factory, "jdbc:unknown://localhost/db"));
    }

    @Test
    void private_正常ケース_resolveModeFromJdbcUrlを呼び出す_nullを指定する_nullが返ること() throws Exception {
        DbDialectHandlerFactory factory =
                new DbDialectHandlerFactory(new DbUnitConfig(), new DumpConfig(), new PathsConfig(),
                        mock(DateTimeFormatUtil.class), mock(DbUnitConfigFactory.class));

        assertNull(invokeResolveModeFromJdbcUrl(factory, null));
    }

    @Test
    void private_正常ケース_normalizeLowerを呼び出す_blank文字列を指定する_nullが返ること() throws Exception {
        DbDialectHandlerFactory factory =
                new DbDialectHandlerFactory(new DbUnitConfig(), new DumpConfig(), new PathsConfig(),
                        mock(DateTimeFormatUtil.class), mock(DbUnitConfigFactory.class));

        assertNull(invokeNormalizeLower(factory, "   "));
    }

    @Test
    void private_正常ケース_resolveMySqlDatabaseを呼び出す_URL形式ごとにDB名が解決されること() throws Exception {
        DbDialectHandlerFactory factory =
                new DbDialectHandlerFactory(new DbUnitConfig(), new DumpConfig(), new PathsConfig(),
                        mock(DateTimeFormatUtil.class), mock(DbUnitConfigFactory.class));

        Method method = DbDialectHandlerFactory.class.getDeclaredMethod("resolveMySqlDatabase",
                String.class);
        method.setAccessible(true);

        assertEquals("testdb", method.invoke(factory, new Object[] {null}));
        assertEquals("testdb", method.invoke(factory, "plain-url"));
        assertEquals("localhost", method.invoke(factory, "jdbc:mysql://localhost"));
        assertEquals("testdb", method.invoke(factory, "jdbc:mysql://localhost/"));
        assertEquals("testdb", method.invoke(factory, "jdbc:mysql://localhost/?useSSL=false"));
        assertEquals("sample", method.invoke(factory, "jdbc:mysql://localhost:3306/sample"));
        assertEquals("sample",
                method.invoke(factory, "jdbc:mysql://localhost:3306/sample?useSSL=false"));
    }

    @Test
    void private_正常ケース_resolveSqlServerSchemaを呼び出す_dboが返ること() throws Exception {
        DbDialectHandlerFactory factory =
                new DbDialectHandlerFactory(new DbUnitConfig(), new DumpConfig(), new PathsConfig(),
                        mock(DateTimeFormatUtil.class), mock(DbUnitConfigFactory.class));

        Method method = DbDialectHandlerFactory.class.getDeclaredMethod("resolveSqlServerSchema",
                String.class);
        method.setAccessible(true);

        assertEquals("dbo", method.invoke(factory, new Object[] {null}));
        assertEquals("dbo", method.invoke(factory, "jdbc:sqlserver://localhost:1433"));
    }
}
