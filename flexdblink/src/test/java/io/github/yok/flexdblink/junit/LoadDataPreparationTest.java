package io.github.yok.flexdblink.junit;

import static io.github.yok.flexdblink.junit.TestMocks.mockNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.github.yok.flexdblink.core.DataLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import org.springframework.transaction.support.TransactionSynchronizationManager;

class LoadDataPreparationTest {
    @LoadData(scenario = "sample", dbNames = "db1")
    static class Fixture {
    }

    @TempDir
    Path directory;

    @Test
    void beforeTestExecution_正常ケース_同一クラスで繰り返しロードする_設定の読込は初期化ごとに一回であること() throws Exception {
        Files.createDirectories(directory.resolve("sample/input/db1"));
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("spring.datasource.db1.url", "jdbc:postgresql:test");
        connectionProperties.setProperty("spring.datasource.db1.username", "test");
        Properties mappings = new Properties();
        mappings.setProperty("flexdblink.load.datasource.db1", "ds1");
        LoadDataExtension extension = spy(new LoadDataExtension());
        extension.setTestResourceContext(new TestResourceContext(directory, connectionProperties));
        doReturn(mappings).when(extension).loadFlexDbLinkProperties(any());
        ExtensionContext context = mockNonNull(ExtensionContext.class);
        doReturn(Fixture.class).when(context).getRequiredTestClass();
        when(context.getTestMethod()).thenReturn(Optional.empty());
        when(context.getStore(any())).thenReturn(mock(ExtensionContext.Store.class));
        ApplicationContext ac = mock(ApplicationContext.class);
        DataSource ds = mockNonNull(DataSource.class);
        Connection jdbc = mockNonNull(Connection.class);
        when(ac.getBean("ds1", DataSource.class)).thenReturn(ds);
        when(ac.getBeanNamesForType(PlatformTransactionManager.class))
                .thenReturn(new String[] {"tm"});
        when(ac.getBean("tm", PlatformTransactionManager.class))
                .thenReturn(new DataSourceTransactionManager(ds));
        when(ac.getBeansOfType(TransactionInterceptor.class)).thenReturn(Map.of());
        try (MockedStatic<SpringExtension> spring = mockStatic(SpringExtension.class);
                MockedStatic<DataSourceUtils> connections = mockStatic(DataSourceUtils.class);
                MockedStatic<TransactionSynchronizationManager> transactions =
                        mockStatic(TransactionSynchronizationManager.class);
                MockedConstruction<DataLoader> loaders = mockConstruction(DataLoader.class)) {
            spring.when(() -> SpringExtension.getApplicationContext(context)).thenReturn(ac);
            connections.when(() -> DataSourceUtils.getConnection(ds)).thenReturn(jdbc);
            transactions.when(TransactionSynchronizationManager::isActualTransactionActive)
                    .thenReturn(true);
            transactions.when(() -> TransactionSynchronizationManager.hasResource(ds))
                    .thenReturn(true);
            for (int i = 0; i < 2; i++) {
                extension.beforeTestExecution(context);
                extension.afterTestExecution(context);
            }
            assertEquals(1, loaders.constructed().size());
            verify(loaders.constructed().get(0), times(2)).executeWithConnection(any(), any(),
                    any());
            verify(extension).loadFlexDbLinkProperties(any());
            extension.setTestResourceContext(
                    new TestResourceContext(directory, connectionProperties));
            extension.beforeTestExecution(context);
            extension.afterTestExecution(context);
            assertEquals(2, loaders.constructed().size());
            verify(extension, times(2)).loadFlexDbLinkProperties(any());
        }
    }
}
