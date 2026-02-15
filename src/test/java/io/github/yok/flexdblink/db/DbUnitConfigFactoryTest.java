package io.github.yok.flexdblink.db;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import io.github.yok.flexdblink.config.DbUnitConfigProperties;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.dataset.datatype.IDataTypeFactory;
import org.junit.jupiter.api.Test;

class DbUnitConfigFactoryTest {

    @Test
    void コンストラクタ_正常ケース_デフォルトコンストラクタを呼び出す_インスタンス生成できること() {
        DbUnitConfigFactory factory = new DbUnitConfigFactory();
        DatabaseConfig cfg = mock(DatabaseConfig.class);
        IDataTypeFactory dataTypeFactory = mock(IDataTypeFactory.class);
        factory.configure(cfg, dataTypeFactory);
        verify(cfg).setProperty(eq(DatabaseConfig.PROPERTY_DATATYPE_FACTORY), eq(dataTypeFactory));
        verify(cfg).setProperty(eq(DatabaseConfig.PROPERTY_ESCAPE_PATTERN), eq("\"?\""));
    }

    @Test
    void configure_escapepatternと既存設定を適用すること() {
        DbUnitConfigProperties props = new DbUnitConfigProperties();
        props.setAllowEmptyFields(false);
        props.setBatchedStatements(false);
        props.setBatchSize(77);

        DbUnitConfigFactory factory = new DbUnitConfigFactory(props);
        DatabaseConfig cfg = mock(DatabaseConfig.class);
        IDataTypeFactory dataTypeFactory = mock(IDataTypeFactory.class);

        factory.configure(cfg, dataTypeFactory);

        verify(cfg).setProperty(eq(DatabaseConfig.PROPERTY_DATATYPE_FACTORY), eq(dataTypeFactory));
        verify(cfg).setProperty(eq(DatabaseConfig.PROPERTY_ESCAPE_PATTERN), eq("\"?\""));
        verify(cfg).setProperty(eq(DatabaseConfig.FEATURE_ALLOW_EMPTY_FIELDS), eq(false));
        verify(cfg).setProperty(eq(DatabaseConfig.FEATURE_BATCHED_STATEMENTS), eq(false));
        verify(cfg).setProperty(eq(DatabaseConfig.PROPERTY_BATCH_SIZE), eq(77));
    }
}
