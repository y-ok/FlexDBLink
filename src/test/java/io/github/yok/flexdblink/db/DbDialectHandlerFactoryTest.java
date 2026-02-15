package io.github.yok.flexdblink.db;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import io.github.yok.flexdblink.util.OracleDateTimeFormatUtil;
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
     * Throws any checked exception as-is from lambda expressions.
     *
     * @param throwable exception to throw
     * @param <T> inferred return type of caller context
     * @return never returns
     */
    private static <T> T sneakyThrow(Throwable throwable) {
        DbDialectHandlerFactoryTest.<RuntimeException>throwAny(throwable);
        return null;
    }

    /**
     * Generic throwing helper used by {@link #sneakyThrow(Throwable)}.
     *
     * @param throwable exception to throw
     * @param <E> throwable type
     * @throws E always thrown
     */
    @SuppressWarnings("unchecked")
    private static <E extends Throwable> void throwAny(Throwable throwable) throws E {
        throw (E) throwable;
    }

    @Test
    void create_正常ケース_oracleモードを指定する_OracleDialectHandlerが返ること() throws Exception {
        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        dbUnitConfig.setDataTypeFactoryMode(DataTypeFactoryMode.ORACLE);
        DumpConfig dumpConfig = new DumpConfig();
        PathsConfig pathsConfig = new PathsConfig();
        DbUnitConfigFactory configFactory = mock(DbUnitConfigFactory.class);
        OracleDateTimeFormatUtil dateTimeFormatUtil = mock(OracleDateTimeFormatUtil.class);
        DbDialectHandlerFactory factory = new DbDialectHandlerFactory(dbUnitConfig, dumpConfig,
                pathsConfig, dateTimeFormatUtil, configFactory);

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setUrl("jdbc:mock");
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

            driverManagerMock.when(() -> DriverManager.getConnection("jdbc:mock", "app", "pw"))
                    .thenReturn(jdbc);

            DbDialectHandler actual = factory.create(entry);
            OracleDialectHandler created = handlerMock.constructed().get(0);
            assertSame(created, actual);
            verify(created).prepareConnection(jdbc);
            verify(configFactory).configure(eq(databaseConfig), eq(dataTypeFactory));
        }
    }

    @Test
    void create_異常ケース_oracleモードでSQL例外が発生する_IllegalStateExceptionが送出されること() {
        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        dbUnitConfig.setDataTypeFactoryMode(DataTypeFactoryMode.ORACLE);
        DbDialectHandlerFactory factory =
                new DbDialectHandlerFactory(dbUnitConfig, new DumpConfig(), new PathsConfig(),
                        mock(OracleDateTimeFormatUtil.class), mock(DbUnitConfigFactory.class));

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setUrl("jdbc:mock");
        entry.setUser("app");
        entry.setPassword("pw");

        try (MockedStatic<DriverManager> driverManagerMock = mockStatic(DriverManager.class)) {
            driverManagerMock.when(() -> DriverManager.getConnection("jdbc:mock", "app", "pw"))
                    .thenAnswer(invocation -> {
                        throw new SQLException("connect failed");
                    });
            assertThrows(IllegalStateException.class, () -> factory.create(entry));
        }
    }

    @Test
    void create_異常ケース_未サポートモードを指定する_IllegalStateExceptionが送出されること() {
        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        dbUnitConfig.setDataTypeFactoryMode(DataTypeFactoryMode.MYSQL);
        DbDialectHandlerFactory factory =
                new DbDialectHandlerFactory(dbUnitConfig, new DumpConfig(), new PathsConfig(),
                        mock(OracleDateTimeFormatUtil.class), mock(DbUnitConfigFactory.class));

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setUrl("jdbc:mock");
        entry.setUser("app");
        entry.setPassword("pw");

        assertThrows(IllegalStateException.class, () -> factory.create(entry));
    }

    @Test
    void create_異常ケース_DBUnit初期化でDataSetExceptionが発生する_IllegalStateExceptionが送出されること()
            throws Exception {
        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        dbUnitConfig.setDataTypeFactoryMode(DataTypeFactoryMode.ORACLE);
        DumpConfig dumpConfig = new DumpConfig();
        PathsConfig pathsConfig = new PathsConfig();
        DbUnitConfigFactory configFactory = mock(DbUnitConfigFactory.class);
        OracleDateTimeFormatUtil dateTimeFormatUtil = mock(OracleDateTimeFormatUtil.class);
        DbDialectHandlerFactory factory = new DbDialectHandlerFactory(dbUnitConfig, dumpConfig,
                pathsConfig, dateTimeFormatUtil, configFactory);

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setUrl("jdbc:mock");
        entry.setUser("app");
        entry.setPassword("pw");

        Connection jdbc = mock(Connection.class);
        DatabaseConfig databaseConfig = mock(DatabaseConfig.class);

        try (MockedStatic<DriverManager> driverManagerMock = mockStatic(DriverManager.class);
                MockedConstruction<DatabaseConnection> dbConnMock =
                        mockConstruction(DatabaseConnection.class, (mock, context) -> {
                            when(mock.getConfig()).thenReturn(databaseConfig);
                        });
                MockedConstruction<OracleDialectHandler> handlerMock =
                        mockConstruction(OracleDialectHandler.class, (mock, context) -> {
                            when(mock.getDataTypeFactory())
                                    .thenAnswer(invocation -> sneakyThrow(new DataSetException("x")));
                        })) {
            driverManagerMock.when(() -> DriverManager.getConnection("jdbc:mock", "app", "pw"))
                    .thenReturn(jdbc);
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> factory.create(entry));
            assertEquals("Failed to initialize DBUnit dataset", ex.getMessage());
            assertTrue(ex.getCause() instanceof DataSetException);
        }
    }

    @Test
    void create_異常ケース_DBUnit設定時にRuntimeExceptionが発生する_IllegalStateExceptionが送出されること()
            throws Exception {
        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        dbUnitConfig.setDataTypeFactoryMode(DataTypeFactoryMode.ORACLE);
        DumpConfig dumpConfig = new DumpConfig();
        PathsConfig pathsConfig = new PathsConfig();
        DbUnitConfigFactory configFactory = mock(DbUnitConfigFactory.class);
        OracleDateTimeFormatUtil dateTimeFormatUtil = mock(OracleDateTimeFormatUtil.class);
        DbDialectHandlerFactory factory = new DbDialectHandlerFactory(dbUnitConfig, dumpConfig,
                pathsConfig, dateTimeFormatUtil, configFactory);

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setUrl("jdbc:mock");
        entry.setUser("app");
        entry.setPassword("pw");

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
}
