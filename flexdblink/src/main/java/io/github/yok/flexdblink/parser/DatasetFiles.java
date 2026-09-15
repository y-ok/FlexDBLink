package io.github.yok.flexdblink.parser;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.io.FilenameUtils;
import org.dbunit.dataset.IDataSet;

/**
 * Indexes one load directory once and parses only the selected file for each table. The index
 * expires with the load so subsequent tests observe changed dataset files.
 */
public final class DatasetFiles {
    private final Map<String, File> files = new LinkedHashMap<>();
    private final JsonDataParser json = new JsonDataParser();
    private final YamlDataParser yaml = new YamlDataParser();

    /**
     * Captures supported files with CSV, JSON, YAML, XML precedence.
     *
     * @param directory dataset directory
     * @throws IOException if the directory cannot be listed
     */
    public DatasetFiles(File directory) throws IOException {
        File[] candidates = directory.listFiles();
        if (candidates == null) {
            throw new IOException("Cannot list dataset directory: " + directory);
        }
        Arrays.sort(candidates);
        for (DataFormat format : DataFormat.values()) {
            for (File file : candidates) {
                String extension =
                        FilenameUtils.getExtension(file.getName()).toLowerCase(Locale.ROOT);
                if (format.matches(extension)) {
                    String table = FilenameUtils.getBaseName(file.getName());
                    files.putIfAbsent(table.toLowerCase(Locale.ROOT), file);
                }
            }
        }
    }

    /**
     * Returns selected table names in deterministic file order.
     *
     * @return table names preserving file-name case
     */
    public List<String> getTableNames() {
        List<String> names = new ArrayList<>();
        for (File file : files.values()) {
            names.add(FilenameUtils.getBaseName(file.getName()));
        }
        names.sort(String.CASE_INSENSITIVE_ORDER);
        return names;
    }

    /**
     * Reports whether the chosen source uses CSV-specific LOB loading rules.
     *
     * @param table selected table name
     * @return whether the selected file is CSV
     */
    public boolean isCsv(String table) {
        return DataFormat.CSV.matches(
                FilenameUtils.getExtension(files.get(table.toLowerCase(Locale.ROOT)).getName())
                        .toLowerCase(Locale.ROOT));
    }

    /**
     * Parses a selected file without reading any sibling datasets.
     *
     * @param table selected table name
     * @return dataset containing the table
     * @throws Exception if parsing fails
     */
    public IDataSet parse(String table) throws Exception {
        File file = files.get(table.toLowerCase(Locale.ROOT));
        String extension = FilenameUtils.getExtension(file.getName()).toLowerCase(Locale.ROOT);
        if (DataFormat.CSV.matches(extension)) {
            return new CsvDataParser().parseFile(file);
        }
        if (DataFormat.JSON.matches(extension)) {
            return json.parseFile(file);
        }
        if (DataFormat.YAML.matches(extension)) {
            return yaml.parseFile(file);
        }
        return new XmlDataParser().parseFile(file);
    }

}
