package io.github.yok.flexdblink.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import java.io.File;
import java.io.Reader;
import java.nio.file.Files;
import java.util.Iterator;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.DefaultTableMetaData;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.datatype.DataType;

/**
 * Implementation of {@link DataParser} that reads YAML dataset files from a directory and produces
 * a {@link IDataSet}.
 *
 * <p>
 * Each YAML file should contain an array of objects, where the file name (without extension) is
 * used as the table name, and the object keys are treated as column names.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
public class YamlDataParser implements DataParser {

    private static final YAMLMapper MAPPER = new YAMLMapper();

    /**
     * {@inheritDoc}
     */
    @Override
    public IDataSet parse(File dir) throws Exception {
        File[] files = dir.listFiles((d, name) -> name.toLowerCase().endsWith(".yaml")
                || name.toLowerCase().endsWith(".yml"));
        if (files == null) {
            throw new DataSetException("No YAML files found in directory: " + dir);
        }

        return parseFiles(files);
    }

    /**
     * Parses one selected file without scanning or reading sibling datasets.
     *
     * @param file selected dataset file
     * @return parsed dataset
     * @throws Exception if parsing fails
     */
    public IDataSet parseFile(File file) throws Exception {
        return parseFiles(new File[] {file});
    }

    /**
     * Parses the supplied files using the same conversion rules for both entry points.
     *
     * @param files files to parse
     * @return parsed dataset
     * @throws Exception if parsing fails
     */
    private IDataSet parseFiles(File[] files) throws Exception {
        DefaultDataSet dataSet = new DefaultDataSet();
        for (File file : files) {
            JsonNode root;
            try (Reader reader = Files.newBufferedReader(file.toPath())) {
                root = MAPPER.readTree(reader);
            }

            if (!root.isArray() || root.size() == 0) {
                // skip empty or invalid
                continue;
            }

            // extract column names
            Iterator<String> colNames = root.get(0).fieldNames();
            String[] cols = new String[root.get(0).size()];
            int idx = 0;
            while (colNames.hasNext()) {
                cols[idx++] = colNames.next();
            }

            Column[] dbunitCols = new Column[cols.length];
            for (int i = 0; i < cols.length; i++) {
                dbunitCols[i] = new Column(cols[i], DataType.VARCHAR);
            }
            String tableName =
                    file.getName().substring(0, file.getName().lastIndexOf('.')).toUpperCase();
            DefaultTableMetaData metaData = new DefaultTableMetaData(tableName, dbunitCols);
            DefaultTable table = new DefaultTable(metaData);

            for (JsonNode row : root) {
                Object[] values = new Object[cols.length];
                for (int i = 0; i < cols.length; i++) {
                    JsonNode val = row.get(cols[i]);
                    if (val != null && !val.isNull()) {
                        values[i] = val.asText();
                    }
                }
                table.addRow(values);
            }

            dataSet.addTable(table);
        }

        return dataSet;
    }
}
