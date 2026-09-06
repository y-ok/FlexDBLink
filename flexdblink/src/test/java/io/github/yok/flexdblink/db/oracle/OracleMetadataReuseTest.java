package io.github.yok.flexdblink.db.oracle;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.db.DbUnitConfigFactory;
import io.github.yok.flexdblink.util.DateTimeFormatSupport;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.List;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.datatype.DataType;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

class OracleMetadataReuseTest {
    @Test
    void createDbUnitConnection_正常ケース_接続とスキーマを指定する_同一トランザクション内だけの再利用であること() throws Exception {
        Connection jdbc = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(jdbc.getMetaData()).thenReturn(meta);
        when(jdbc.getSchema()).thenReturn("APP");
        when(meta.getColumns(any(), any(), any(), any())).thenReturn(mock(ResultSet.class));
        DatabaseConnection db = mock(DatabaseConnection.class);
        when(db.getConnection()).thenReturn(jdbc);
        when(db.getSchema()).thenReturn("APP");
        when(db.getConfig()).thenReturn(new DatabaseConfig());
        when(db.createDataSet(new String[] {"T"})).thenReturn(new DefaultDataSet(
                new DefaultTable("T", new Column[] {new Column("ID", DataType.INTEGER)})));
        PathsConfig paths = new PathsConfig();
        paths.setDataPath("target");
        OracleDialectHandler handler = new OracleDialectHandler(db, new DumpConfig(),
                new DbUnitConfig(), new DbUnitConfigFactory(), mock(DateTimeFormatSupport.class),
                paths, List.of("T"));
        assertSame(db, handler.createDbUnitConnection(jdbc, "APP"));
        try (MockedConstruction<DatabaseConnection> construction = mockConstruction(
                DatabaseConnection.class,
                (created, context) -> when(created.getConfig()).thenReturn(new DatabaseConfig()))) {
            assertNotSame(db, handler.createDbUnitConnection(jdbc, "OTHER"));
            assertNotSame(db, handler.createDbUnitConnection(mock(Connection.class), "APP"));
            org.junit.jupiter.api.Assertions.assertEquals(2, construction.constructed().size());
        }
    }
}
