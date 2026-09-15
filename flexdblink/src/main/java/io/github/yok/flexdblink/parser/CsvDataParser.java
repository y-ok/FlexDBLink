package io.github.yok.flexdblink.parser;

import io.github.yok.flexdblink.util.CsvUtils;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.csv.CsvDataSet;
import org.dbunit.dataset.csv.CsvProducer;
import org.dbunit.dataset.datatype.DataType;

/**
 * Reads CSV datasets, distinguishing unquoted empty fields from quoted empty strings.
 *
 * @author Yasuharu.Okawauchi
 */
public class CsvDataParser implements DataParser {

    /** {@inheritDoc} */
    @Override
    public IDataSet parse(File dir) throws Exception {
        DefaultDataSet dataSet = new DefaultDataSet();
        for (Object table : CsvProducer.getTables(dir.toURI().toURL(),
                CsvDataSet.TABLE_ORDERING_FILE)) {
            IDataSet parsed = parseFile(new File(dir, table + ".csv"));
            dataSet.addTable(parsed.getTable(table.toString()));
        }
        return dataSet;
    }

    /**
     * Reads one CSV file without requiring a table-ordering file.
     *
     * @param file selected CSV file
     * @return dataset named after the file, with unquoted empty fields converted to null
     * @throws IOException if the CSV file cannot be read
     * @throws DataSetException if the parsed rows cannot form a table
     */
    public IDataSet parseFile(File file) throws IOException, DataSetException {
        try (CSVParser parser = CSVParser.parse(file, StandardCharsets.UTF_8, CsvUtils.FORMAT)) {
            List<CSVRecord> rows = parser.getRecords();
            CSVRecord header = rows.get(0);
            Column[] columns = new Column[header.size()];
            for (int i = 0; i < columns.length; i++) {
                columns[i] = new Column(header.get(i).trim(), DataType.UNKNOWN);
            }
            DefaultTable table =
                    new DefaultTable(FilenameUtils.getBaseName(file.getName()), columns);
            for (int row = 1; row < rows.size(); row++) {
                table.addRow(rows.get(row).values());
            }
            return new DefaultDataSet(table);
        }
    }
}
