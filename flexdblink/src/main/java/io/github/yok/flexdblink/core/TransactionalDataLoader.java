package io.github.yok.flexdblink.core;

import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.db.DbDialectHandler;
import io.github.yok.flexdblink.db.DbDialectHandlerFactory;
import io.github.yok.flexdblink.db.LobResolvingTableWrapper;
import io.github.yok.flexdblink.parser.DatasetFiles;
import io.github.yok.flexdblink.util.TableDependencyResolver;
import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.filter.DefaultColumnFilter;
import org.dbunit.operation.DatabaseOperation;

/**
 * Loads a dataset through a caller-owned transaction with preparation scoped to one load.
 * Connections and parsed data are never retained across tests.
 */
@Slf4j
final class TransactionalDataLoader {
    private final DbDialectHandlerFactory factory;
    private final DumpConfig dumpConfig;

    /**
     * Creates a loader using the built-in dialect factory.
     *
     * @param factory factory supporting caller-owned connections
     * @param dumpConfig table exclusions
     */
    TransactionalDataLoader(DbDialectHandlerFactory factory, DumpConfig dumpConfig) {
        this.factory = factory;
        this.dumpConfig = dumpConfig;
    }

    /**
     * Replaces selected tables without opening or completing a transaction.
     *
     * @param directory dataset directory
     * @param entry dialect settings
     * @param jdbc caller-owned transaction connection
     * @throws Exception if initialization or loading fails
     */
    void execute(File directory, ConnectionConfig.Entry entry, Connection jdbc) throws Exception {
        DatasetFiles files = new DatasetFiles(directory);
        List<String> tables = files.getTableNames();
        List<String> excluded = dumpConfig.getExcludeTables();
        tables.removeIf(table -> excluded.stream().anyMatch(table::equalsIgnoreCase));
        if (tables.isEmpty()) {
            return;
        }
        DbDialectHandler dialect = factory.create(entry, jdbc, tables);
        String schema = dialect.resolveSchema(entry);
        try {
            tables = TableDependencyResolver.resolveLoadOrder(
                    jdbc, jdbc.getCatalog(), schema, tables);
        } catch (SQLException e) {
            log.warn("[{}] FK dependency resolution failed; using alphabetical order. reason={}",
                    entry.getId(), e.getMessage());
        }
        dialect.prepareConnection(jdbc);
        DatabaseConnection db = dialect.createDbUnitConnection(jdbc, schema);
        // Closing this wrapper would close the caller's transaction connection.
        DatabaseOperation insert = DatabaseOperation.CLEAN_INSERT;
        if (tables.size() > 1) {
            List<String> deleteOrder = new ArrayList<>(tables);
            Collections.reverse(deleteOrder);
            for (String table : deleteOrder) {
                DatabaseOperation.DELETE_ALL.execute(db, db.createDataSet(new String[] {table}));
            }
            insert = DatabaseOperation.INSERT;
        }
        for (String table : tables) {
            IDataSet parsed;
            try {
                parsed = files.parse(table);
            } catch (Exception e) {
                log.warn("[{}] Failed to resolve dataset for table={} — skipping: {}",
                        entry.getId(), table, e.getMessage());
                continue;
            }
            if (log.isDebugEnabled()) {
                dialect.logTableDefinition(jdbc, schema, table, entry.getId());
            }
            ITable base = parsed.getTable(table);
            ITable wrapped = new LobResolvingTableWrapper(base, directory, dialect);
            Column[] lobColumns = new Column[0];
            if (files.isCsv(table)) {
                lobColumns = dialect.getLobColumns(base);
            }
            if (lobColumns.length > 0
                    && !dialect.hasNotNullLobColumn(jdbc, schema, table, lobColumns)) {
                insert.execute(db, new DefaultDataSet(
                        DefaultColumnFilter.excludedColumnsTable(base, lobColumns)));
                DatabaseOperation.UPDATE.execute(db, new DefaultDataSet(wrapped));
            } else {
                insert.execute(db, new DefaultDataSet(wrapped));
            }
            log.info("[{}] Table[{}] loaded (target rows={}, loaded rows={})", entry.getId(),
                    table, base.getRowCount(), base.getRowCount());
        }
    }
}
