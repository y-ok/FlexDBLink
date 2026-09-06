package io.github.yok.flexdblink.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.util.DateTimeFormatUtil;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.List;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.datatype.DataType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;

class SelectedMetadataTest {
    @ParameterizedTest
    @ValueSource(strings = {"oracle", "postgresql", "mysql", "sqlserver"})
    void create_正常ケース_既存接続と対象テーブルを指定する_対象テーブルだけのメタデータであること(String vendor) throws Exception {
        Connection jdbc = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(jdbc.getMetaData()).thenReturn(meta);
        when(jdbc.getSchema()).thenReturn("APP");
        ResultSet columns = mock(ResultSet.class);
        when(meta.getColumns(any(), any(), any(), any())).thenReturn(columns);
        DefaultTable sample = new DefaultTable("SAMPLE",
                new Column[] {new Column("ID", DataType.INTEGER), new Column("BODY", DataType.BLOB),
                        new Column("TEXT", DataType.CLOB), new Column("UNUSED", DataType.VARCHAR)});
        sample.addRow(new Object[] {1, null, "plain", "plain"});
        sample.addRow(new Object[] {2, "file:body", "file:text", "plain"});
        IDataSet data = new DefaultDataSet(sample);
        PathsConfig paths = new PathsConfig();
        paths.setDataPath("target");
        DbDialectHandlerFactory factory = new DbDialectHandlerFactory(new DbUnitConfig(),
                new DumpConfig(), paths, mock(DateTimeFormatUtil.class), new DbUnitConfigFactory());
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setUrl("jdbc:" + vendor + "://localhost/APP");
        entry.setUser("APP");
        try (MockedConstruction<DatabaseConnection> construction =
                mockConstruction(DatabaseConnection.class, (db, context) -> {
                    when(db.getConnection()).thenReturn(jdbc);
                    when(db.getSchema()).thenReturn("APP");
                    when(db.getConfig()).thenReturn(new DatabaseConfig());
                    when(db.createDataSet(new String[] {"sample"})).thenReturn(data);
                })) {
            DbDialectHandler handler = factory.create(entry, jdbc, List.of("sample"));
            assertNotNull(handler);
            verify(meta, never()).getTables(any(), any(), any(), any());
            verify(meta).getColumns(isNull(), eq("APP"), eq("SAMPLE"), any());
            verify(jdbc, never()).close();
            verify(jdbc, never()).commit();
            verify(jdbc, never()).rollback();
            verify(construction.constructed().get(0), never()).close();
            assertEquals(2, handler.getLobColumns(sample).length);
            DefaultTable noLob =
                    new DefaultTable("SAMPLE", new Column[] {new Column("ID", DataType.INTEGER)});
            noLob.addRow(new Object[] {1});
            assertEquals(0, handler.getLobColumns(noLob).length);
            if (vendor.equals("oracle")) {
                DefaultTable plain = new DefaultTable("SAMPLE",
                        new Column[] {new Column("BODY", DataType.BLOB)});
                plain.addRow(new Object[] {null});
                assertEquals(0, handler.getLobColumns(plain).length);
            }
        }
    }

    @Test
    void getLobColumns_正常ケース_参照とnullと空行を指定する_参照列だけであること() throws Exception {
        DbDialectMetadataOperations operations =
                mock(DbDialectMetadataOperations.class, CALLS_REAL_METHODS);
        DefaultTable table = new DefaultTable("T", new Column[] {new Column("A", DataType.VARCHAR),
                new Column("B", DataType.VARCHAR), new Column("C", DataType.INTEGER)});
        assertEquals(0, operations.getLobColumns(table).length);
        table.addRow(new Object[] {null, "plain", 1});
        table.addRow(new Object[] {"file:blob", "", 2});
        assertEquals("A", operations.getLobColumns(table)[0].getColumnName());
    }

    @Test
    void apply_正常ケース_Functionとしてファクトリを呼び出す_生成したハンドラと同一であること() {
        DbDialectHandlerFactory factory = org.mockito.Mockito.spy(
                new DbDialectHandlerFactory(new DbUnitConfig(), new DumpConfig(), new PathsConfig(),
                        mock(DateTimeFormatUtil.class), new DbUnitConfigFactory()));
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        DbDialectHandler handler = mock(DbDialectHandler.class);
        org.mockito.Mockito.doReturn(handler).when(factory).create(entry);
        assertSame(handler, factory.apply(entry));
    }
}
