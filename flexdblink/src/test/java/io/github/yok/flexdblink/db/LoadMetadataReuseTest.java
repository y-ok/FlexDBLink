package io.github.yok.flexdblink.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.util.DateTimeFormatSupport;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IMetadataHandler;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.datatype.DataType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedConstruction;

class LoadMetadataReuseTest {
    @ParameterizedTest
    @CsvSource({"oracle,\"?\"", "postgresql,\"?\"", "mysql,`?`", "sqlserver,[?]"})
    void create_正常ケース_各DBのロード接続を作成する_メタデータの再利用と既存の識別子エスケープであること(String vendor, String escapePattern)
            throws Exception {
        Connection jdbc = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(jdbc.getMetaData()).thenReturn(meta);
        when(jdbc.getSchema()).thenReturn("APP");
        when(meta.getColumns(any(), any(), any(), any())).thenReturn(columns());
        PathsConfig paths = new PathsConfig();
        paths.setDataPath("target");
        DbDialectHandlerFactory factory =
                new DbDialectHandlerFactory(new DbUnitConfig(), new DumpConfig(), paths,
                        mock(DateTimeFormatSupport.class), new DbUnitConfigFactory());
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setUrl("jdbc:" + vendor + "://localhost/APP");
        entry.setUser("APP");
        DefaultDataSet data = new DefaultDataSet(
                new DefaultTable("T", new Column[] {new Column("ID", DataType.INTEGER)}));
        String metadataSchema = "APP";
        if (vendor.equals("mysql")) {
            metadataSchema = null;
        }
        String expectedSchema = metadataSchema;
        try (MockedConstruction<DatabaseConnection> construction =
                mockConstruction(DatabaseConnection.class, (db, context) -> {
                    when(db.getConnection()).thenReturn(jdbc);
                    when(db.getSchema()).thenReturn(expectedSchema);
                    when(db.getConfig()).thenReturn(new DatabaseConfig());
                    when(db.createDataSet(new String[] {"T"})).thenAnswer(call -> {
                        IMetadataHandler metadata = (IMetadataHandler) db.getConfig()
                                .getProperty(DatabaseConfig.PROPERTY_METADATA_HANDLER);
                        try (ResultSet result = metadata.getColumns(meta, expectedSchema, "T")) {
                            assertTrue(result.next());
                        }
                        return data;
                    });
                })) {
            DbDialectHandler handler = factory.create(entry, jdbc, List.of("T"));
            DatabaseConnection reused = handler.createDbUnitConnection(jdbc, "APP");
            assertEquals(1, construction.constructed().size());
            assertSame(construction.constructed().get(0), reused);
            assertEquals(escapePattern,
                    reused.getConfig().getProperty(DatabaseConfig.PROPERTY_ESCAPE_PATTERN));
            assertTrue(handler.hasNotNullLobColumn(jdbc, "APP", "T",
                    new Column[] {new Column("BODY", DataType.CLOB)}));
            handler.logTableDefinition(jdbc, "APP", "T", "test");
            verify(meta).getColumns(any(), any(), any(), any());
            verify(jdbc, never()).close();
            verify(jdbc, never()).commit();
            verify(jdbc, never()).rollback();
        }
    }

    private CachedRowSet columns() throws SQLException {
        String[] names = {"COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "NULLABLE", "COLUMN_SIZE",
                "CHAR_OCTET_LENGTH", "IS_NULLABLE"};
        int[] types = {Types.VARCHAR, Types.INTEGER, Types.VARCHAR, Types.INTEGER, Types.INTEGER,
                Types.INTEGER, Types.VARCHAR};
        Object[] values =
                {"BODY", Types.CLOB, "CLOB", DatabaseMetaData.columnNoNulls, 100, 100, "NO"};
        RowSetMetaDataImpl metadata = new RowSetMetaDataImpl();
        metadata.setColumnCount(names.length);
        for (int i = 0; i < names.length; i++) {
            metadata.setColumnName(i + 1, names[i]);
            metadata.setColumnType(i + 1, types[i]);
        }
        CachedRowSet rows = RowSetProvider.newFactory().createCachedRowSet();
        rows.setMetaData(metadata);
        rows.moveToInsertRow();
        for (int i = 0; i < values.length; i++) {
            rows.updateObject(i + 1, values[i]);
        }
        rows.insertRow();
        rows.moveToCurrentRow();
        rows.beforeFirst();
        return rows;
    }
}
