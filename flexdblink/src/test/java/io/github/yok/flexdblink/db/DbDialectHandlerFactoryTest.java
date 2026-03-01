package io.github.yok.flexdblink.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.db.mysql.MySqlDialectHandler;
import io.github.yok.flexdblink.db.oracle.OracleDialectHandler;
import io.github.yok.flexdblink.db.postgresql.PostgresqlDialectHandler;
import io.github.yok.flexdblink.db.sqlserver.SqlServerDialectHandler;
import io.github.yok.flexdblink.util.DateTimeFormatUtil;
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
    void create_正常ケース_レガシーMySQLDriverかつURL未設定を指定する_MySqlDialectHandlerが返ること()
            throws Exception {
        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        DumpConfig dumpConfig = new DumpConfig();
        PathsConfig pathsConfig = new PathsConfig();
        DbUnitConfigFactory configFactory = mock(DbUnitConfigFactory.class);
        DateTimeFormatUtil dateTimeFormatUtil = mock(DateTimeFormatUtil.class);
        DbDialectHandlerFactory factory = new DbDialectHandlerFactory(dbUnitConfig, dumpConfig,
                pathsConfig, dateTimeFormatUtil, configFactory);

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setUrl(null);
        entry.setUser("app");
        entry.setPassword("pw");
        entry.setDriverClass("com.mysql.jdbc.Driver");

        Connection jdbc = mock(Connection.class);
        DatabaseConfig databaseConfig = mock(DatabaseConfig.class);
        IDataTypeFactory dataTypeFactory = mock(IDataTypeFactory.class);
        String[] schemaHolder = new String[1];

        try (MockedStatic<DriverManager> driverManagerMock = mockStatic(DriverManager.class);
                MockedConstruction<DatabaseConnection> dbConnMock =
                        mockConstruction(DatabaseConnection.class, (mock, context) -> {
                            when(mock.getConfig()).thenReturn(databaseConfig);
                            schemaHolder[0] = (String) context.arguments().get(1);
                        });
                MockedConstruction<MySqlDialectHandler> handlerMock =
                        mockConstruction(MySqlDialectHandler.class, (mock, context) -> {
                            when(mock.getDataTypeFactory()).thenReturn(dataTypeFactory);
                        })) {

            driverManagerMock.when(() -> DriverManager.getConnection(null, "app", "pw"))
                    .thenReturn(jdbc);

            DbDialectHandler actual = factory.create(entry);
            MySqlDialectHandler created = handlerMock.constructed().get(0);
            assertSame(created, actual);
            assertEquals("testdb", schemaHolder[0]);
            verify(created).prepareConnection(jdbc);
            verify(configFactory).configure(eq(databaseConfig), eq(dataTypeFactory));
        }
    }

    @Test
    void create_正常ケース_新MySQLDriverかつURL未設定を指定する_MySqlDialectHandlerが返ること()
            throws Exception {
        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        DumpConfig dumpConfig = new DumpConfig();
        PathsConfig pathsConfig = new PathsConfig();
        DbUnitConfigFactory configFactory = mock(DbUnitConfigFactory.class);
        DateTimeFormatUtil dateTimeFormatUtil = mock(DateTimeFormatUtil.class);
        DbDialectHandlerFactory factory = new DbDialectHandlerFactory(dbUnitConfig, dumpConfig,
                pathsConfig, dateTimeFormatUtil, configFactory);

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setUrl(null);
        entry.setUser("app");
        entry.setPassword("pw");
        entry.setDriverClass("com.mysql.cj.jdbc.Driver");

        Connection jdbc = mock(Connection.class);
        DatabaseConfig databaseConfig = mock(DatabaseConfig.class);
        IDataTypeFactory dataTypeFactory = mock(IDataTypeFactory.class);
        String[] schemaHolder = new String[1];

        try (MockedStatic<DriverManager> driverManagerMock = mockStatic(DriverManager.class);
                MockedConstruction<DatabaseConnection> dbConnMock =
                        mockConstruction(DatabaseConnection.class, (mock, context) -> {
                            when(mock.getConfig()).thenReturn(databaseConfig);
                            schemaHolder[0] = (String) context.arguments().get(1);
                        });
                MockedConstruction<MySqlDialectHandler> handlerMock =
                        mockConstruction(MySqlDialectHandler.class, (mock, context) -> {
                            when(mock.getDataTypeFactory()).thenReturn(dataTypeFactory);
                        })) {

            driverManagerMock.when(() -> DriverManager.getConnection(null, "app", "pw"))
                    .thenReturn(jdbc);

            DbDialectHandler actual = factory.create(entry);
            MySqlDialectHandler created = handlerMock.constructed().get(0);
            assertSame(created, actual);
            assertEquals("testdb", schemaHolder[0]);
            verify(created).prepareConnection(jdbc);
            verify(configFactory).configure(eq(databaseConfig), eq(dataTypeFactory));
        }
    }

    @Test
    void create_正常ケース_MySQLURL末尾スラッシュを指定する_デフォルトDB名でMySqlDialectHandlerが返ること()
            throws Exception {
        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        DumpConfig dumpConfig = new DumpConfig();
        PathsConfig pathsConfig = new PathsConfig();
        DbUnitConfigFactory configFactory = mock(DbUnitConfigFactory.class);
        DateTimeFormatUtil dateTimeFormatUtil = mock(DateTimeFormatUtil.class);
        DbDialectHandlerFactory factory = new DbDialectHandlerFactory(dbUnitConfig, dumpConfig,
                pathsConfig, dateTimeFormatUtil, configFactory);

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setUrl("jdbc:mysql://localhost:3306/");
        entry.setUser("app");
        entry.setPassword("pw");

        Connection jdbc = mock(Connection.class);
        DatabaseConfig databaseConfig = mock(DatabaseConfig.class);
        IDataTypeFactory dataTypeFactory = mock(IDataTypeFactory.class);
        String[] schemaHolder = new String[1];

        try (MockedStatic<DriverManager> driverManagerMock = mockStatic(DriverManager.class);
                MockedConstruction<DatabaseConnection> dbConnMock =
                        mockConstruction(DatabaseConnection.class, (mock, context) -> {
                            when(mock.getConfig()).thenReturn(databaseConfig);
                            schemaHolder[0] = (String) context.arguments().get(1);
                        });
                MockedConstruction<MySqlDialectHandler> handlerMock =
                        mockConstruction(MySqlDialectHandler.class, (mock, context) -> {
                            when(mock.getDataTypeFactory()).thenReturn(dataTypeFactory);
                        })) {

            driverManagerMock.when(() -> DriverManager
                    .getConnection("jdbc:mysql://localhost:3306/", "app", "pw"))
                    .thenReturn(jdbc);

            DbDialectHandler actual = factory.create(entry);
            MySqlDialectHandler created = handlerMock.constructed().get(0);
            assertSame(created, actual);
            assertEquals("testdb", schemaHolder[0]);
            verify(created).prepareConnection(jdbc);
            verify(configFactory).configure(eq(databaseConfig), eq(dataTypeFactory));
        }
    }

    @Test
    void create_正常ケース_MySQLURLスラッシュなしを指定する_デフォルトDB名でMySqlDialectHandlerが返ること()
            throws Exception {
        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        DumpConfig dumpConfig = new DumpConfig();
        PathsConfig pathsConfig = new PathsConfig();
        DbUnitConfigFactory configFactory = mock(DbUnitConfigFactory.class);
        DateTimeFormatUtil dateTimeFormatUtil = mock(DateTimeFormatUtil.class);
        DbDialectHandlerFactory factory = new DbDialectHandlerFactory(dbUnitConfig, dumpConfig,
                pathsConfig, dateTimeFormatUtil, configFactory);

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setUrl("jdbc:mysql:testdb");
        entry.setUser("app");
        entry.setPassword("pw");

        Connection jdbc = mock(Connection.class);
        DatabaseConfig databaseConfig = mock(DatabaseConfig.class);
        IDataTypeFactory dataTypeFactory = mock(IDataTypeFactory.class);
        String[] schemaHolder = new String[1];

        try (MockedStatic<DriverManager> driverManagerMock = mockStatic(DriverManager.class);
                MockedConstruction<DatabaseConnection> dbConnMock =
                        mockConstruction(DatabaseConnection.class, (mock, context) -> {
                            when(mock.getConfig()).thenReturn(databaseConfig);
                            schemaHolder[0] = (String) context.arguments().get(1);
                        });
                MockedConstruction<MySqlDialectHandler> handlerMock =
                        mockConstruction(MySqlDialectHandler.class, (mock, context) -> {
                            when(mock.getDataTypeFactory()).thenReturn(dataTypeFactory);
                        })) {

            driverManagerMock.when(() -> DriverManager.getConnection("jdbc:mysql:testdb", "app",
                    "pw")).thenReturn(jdbc);

            DbDialectHandler actual = factory.create(entry);
            MySqlDialectHandler created = handlerMock.constructed().get(0);
            assertSame(created, actual);
            assertEquals("testdb", schemaHolder[0]);
            verify(created).prepareConnection(jdbc);
            verify(configFactory).configure(eq(databaseConfig), eq(dataTypeFactory));
        }
    }

    @Test
    void create_正常ケース_MySQLURLクエリ付きでDB名空を指定する_デフォルトDB名でMySqlDialectHandlerが返ること()
            throws Exception {
        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        DumpConfig dumpConfig = new DumpConfig();
        PathsConfig pathsConfig = new PathsConfig();
        DbUnitConfigFactory configFactory = mock(DbUnitConfigFactory.class);
        DateTimeFormatUtil dateTimeFormatUtil = mock(DateTimeFormatUtil.class);
        DbDialectHandlerFactory factory = new DbDialectHandlerFactory(dbUnitConfig, dumpConfig,
                pathsConfig, dateTimeFormatUtil, configFactory);

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setUrl("jdbc:mysql://localhost:3306/?useSSL=false");
        entry.setUser("app");
        entry.setPassword("pw");

        Connection jdbc = mock(Connection.class);
        DatabaseConfig databaseConfig = mock(DatabaseConfig.class);
        IDataTypeFactory dataTypeFactory = mock(IDataTypeFactory.class);
        String[] schemaHolder = new String[1];

        try (MockedStatic<DriverManager> driverManagerMock = mockStatic(DriverManager.class);
                MockedConstruction<DatabaseConnection> dbConnMock =
                        mockConstruction(DatabaseConnection.class, (mock, context) -> {
                            when(mock.getConfig()).thenReturn(databaseConfig);
                            schemaHolder[0] = (String) context.arguments().get(1);
                        });
                MockedConstruction<MySqlDialectHandler> handlerMock =
                        mockConstruction(MySqlDialectHandler.class, (mock, context) -> {
                            when(mock.getDataTypeFactory()).thenReturn(dataTypeFactory);
                        })) {

            driverManagerMock.when(() -> DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/?useSSL=false", "app", "pw")).thenReturn(jdbc);

            DbDialectHandler actual = factory.create(entry);
            MySqlDialectHandler created = handlerMock.constructed().get(0);
            assertSame(created, actual);
            assertEquals("testdb", schemaHolder[0]);
            verify(created).prepareConnection(jdbc);
            verify(configFactory).configure(eq(databaseConfig), eq(dataTypeFactory));
        }
    }

    @Test
    void create_正常ケース_PostgreSQLDriverClassを指定する_PostgresqlDialectHandlerが返ること()
            throws Exception {
        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        DumpConfig dumpConfig = new DumpConfig();
        PathsConfig pathsConfig = new PathsConfig();
        DbUnitConfigFactory configFactory = mock(DbUnitConfigFactory.class);
        DateTimeFormatUtil dateTimeFormatUtil = mock(DateTimeFormatUtil.class);
        DbDialectHandlerFactory factory = new DbDialectHandlerFactory(dbUnitConfig, dumpConfig,
                pathsConfig, dateTimeFormatUtil, configFactory);

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setUrl(null);
        entry.setUser("app");
        entry.setPassword("pw");
        entry.setDriverClass("org.postgresql.Driver");

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

            driverManagerMock.when(() -> DriverManager.getConnection(null, "app", "pw"))
                    .thenReturn(jdbc);

            DbDialectHandler actual = factory.create(entry);
            PostgresqlDialectHandler created = handlerMock.constructed().get(0);
            assertSame(created, actual);
            verify(created).prepareConnection(jdbc);
            verify(configFactory).configure(eq(databaseConfig), eq(dataTypeFactory));
        }
    }

    @Test
    void create_正常ケース_SQLServerDriverClassを指定する_SqlServerDialectHandlerが返ること()
            throws Exception {
        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        DumpConfig dumpConfig = new DumpConfig();
        PathsConfig pathsConfig = new PathsConfig();
        DbUnitConfigFactory configFactory = mock(DbUnitConfigFactory.class);
        DateTimeFormatUtil dateTimeFormatUtil = mock(DateTimeFormatUtil.class);
        DbDialectHandlerFactory factory = new DbDialectHandlerFactory(dbUnitConfig, dumpConfig,
                pathsConfig, dateTimeFormatUtil, configFactory);

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setUrl(null);
        entry.setUser("app");
        entry.setPassword("pw");
        entry.setDriverClass("com.microsoft.sqlserver.jdbc.SQLServerDriver");

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

            driverManagerMock.when(() -> DriverManager.getConnection(null, "app", "pw"))
                    .thenReturn(jdbc);

            DbDialectHandler actual = factory.create(entry);
            SqlServerDialectHandler created = handlerMock.constructed().get(0);
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
        assertTrue(ex.getMessage().contains("DBX"));
        assertTrue(ex.getCause() instanceof IllegalArgumentException);
        assertTrue(ex.getCause().getMessage().contains("DBX"));
    }

    @Test
    void create_異常ケース_driverClass空白かつURL未設定を指定する_IllegalStateExceptionが送出されること() {
        DumpConfig dumpConfig = new DumpConfig();
        PathsConfig pathsConfig = new PathsConfig();
        DbUnitConfigFactory configFactory = mock(DbUnitConfigFactory.class);
        DateTimeFormatUtil dateTimeFormatUtil = mock(DateTimeFormatUtil.class);
        DbDialectHandlerFactory factory = new DbDialectHandlerFactory(new DbUnitConfig(),
                dumpConfig, pathsConfig, dateTimeFormatUtil, configFactory);

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("DBY");
        entry.setUrl(null);
        entry.setUser("app");
        entry.setPassword("pw");
        entry.setDriverClass("   ");

        IllegalStateException ex =
                assertThrows(IllegalStateException.class, () -> factory.create(entry));
        assertTrue(ex.getMessage().contains("DBY"));
        assertTrue(ex.getCause() instanceof IllegalArgumentException);
        assertTrue(ex.getCause().getMessage().contains("DBY"));
    }

    @Test
    void create_異常ケース_DatabaseConnection生成時に例外が発生する_JDBCコネクションがクローズされること() throws Exception {
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

        try (MockedStatic<DriverManager> driverManagerMock = mockStatic(DriverManager.class);
                MockedConstruction<DatabaseConnection> dbConnMock =
                        mockConstruction(DatabaseConnection.class, (mock, context) -> {
                            throw new SQLException("construction failed");
                        })) {
            driverManagerMock.when(() -> DriverManager.getConnection("jdbc:mock", "app", "pw"))
                    .thenReturn(jdbc);
            assertThrows(IllegalStateException.class, () -> factory.create(entry));
            verify(jdbc).close();
        }
    }
}
