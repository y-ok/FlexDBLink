package io.github.yok.flexdblink.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;

/**
 * Utility class for reading and writing CSV files.
 *
 * <p>
 * This class currently provides a helper to write CSV files in UTF-8 using Apache Commons CSV with
 * minimal quoting. Records are separated using the platform's default line separator, and the
 * backslash character ({@code \}) is used as the escape character.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
public final class CsvUtils {

    private CsvUtils() {
        // Utility class; do not instantiate.
    }

    /**
     * Builds the list of column indices to use for sorting, given the header array and the primary
     * key column list.
     *
     * <p>
     * When {@code pkColumns} is empty, returns {@code [0]} (sort by first column). When duplicated
     * header names exist, the first occurrence wins.
     * </p>
     *
     * @param headers CSV header names
     * @param pkColumns PK column names (empty list means no primary key)
     * @return list of zero-based column indices to sort by, in PK order
     */
    public static List<Integer> buildSortIndices(String[] headers, List<String> pkColumns) {
        Map<String, Integer> headerIndex = IntStream.range(0, headers.length).boxed().collect(
                Collectors.toMap(i -> headers[i], i -> i, (a, b) -> a, LinkedHashMap::new));
        if (pkColumns.isEmpty()) {
            return List.of(0);
        }
        return pkColumns.stream().map(headerIndex::get).collect(Collectors.toList());
    }

    /**
     * Writes the given header and row data to a CSV file encoded in UTF-8.
     *
     * <p>
     * The CSV is written with:
     * </p>
     * <ul>
     * <li>Header row provided by {@code headers}</li>
     * <li>Quote mode: {@link QuoteMode#MINIMAL}</li>
     * <li>Escape character: backslash ({@code \})</li>
     * <li>Record separator: {@link System#lineSeparator()}</li>
     * </ul>
     *
     * @param csvFile the destination CSV file (will be created or overwritten)
     * @param headers the header columns to write as the first record
     * @param rows the data rows; each inner list represents one CSV record
     * @throws IOException if an I/O error occurs while writing the file
     */
    public static void writeCsvUtf8(File csvFile, String[] headers, List<List<String>> rows)
            throws IOException {
        CSVFormat fmt =
                CSVFormat.DEFAULT.builder().setHeader(headers).setQuoteMode(QuoteMode.MINIMAL)
                        .setEscape('\\').setRecordSeparator(System.lineSeparator()).get();
        try (Writer w =
                new OutputStreamWriter(new FileOutputStream(csvFile), StandardCharsets.UTF_8);
                CSVPrinter printer = new CSVPrinter(w, fmt)) {
            for (List<String> row : rows) {
                printer.printRecord(row);
            }
        }
    }
}
