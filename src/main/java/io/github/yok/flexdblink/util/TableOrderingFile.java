package io.github.yok.flexdblink.util;

import io.github.yok.flexdblink.parser.DataFormat;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.ArrayUtils;

/**
 * Utility class for managing the {@code table-ordering.txt} temporary file.
 *
 * <p>
 * {@code table-ordering.txt} is a temporary working file placed under the per-DB output directory.
 * It records the ordered list of table names used during a Dump or Load operation.
 * </p>
 *
 * <ul>
 * <li>{@link #write(File, List)} - writes a pre-computed table list (used by DataDumper).</li>
 * <li>{@link #ensure(File)} - regenerates the file from dataset files present in a directory (used
 * by DataLoader).</li>
 * <li>{@link #delete(File)} - deletes the file after the operation completes.</li>
 * </ul>
 *
 * @author Yasuharu.Okawauchi
 */
@Slf4j
public final class TableOrderingFile {

    public static final String FILE_NAME = "table-ordering.txt";

    private TableOrderingFile() {
        // Utility class; do not instantiate.
    }

    /**
     * Writes the given table list to {@code table-ordering.txt} under {@code dir}.
     *
     * <p>
     * The file is created (or overwritten) with one table name per line, in the order provided.
     * </p>
     *
     * @param dir target directory
     * @param tables ordered list of table names
     */
    public static void write(File dir, List<String> tables) {
        File orderFile = new File(dir, FILE_NAME);
        try {
            String content = String.join(System.lineSeparator(), tables);
            FileUtils.writeStringToFile(orderFile, content, StandardCharsets.UTF_8);
            log.info("Generated table-ordering.txt: {}", orderFile.getAbsolutePath());
        } catch (IOException e) {
            log.error("Failed to create table-ordering.txt: {}", e.getMessage(), e);
            ErrorHandler.errorAndExit("Failed to create table-ordering.txt", e);
        }
    }

    /**
     * Ensures {@code table-ordering.txt} exists under {@code dir} by scanning for supported dataset
     * files (CSV / JSON / YAML / XML) and writing their base names (sorted) as the ordering.
     *
     * <p>
     * Any existing {@code table-ordering.txt} is deleted before regeneration. If no dataset files
     * are found, no file is created.
     * </p>
     *
     * @param dir directory to scan for dataset files
     */
    public static void ensure(File dir) {
        File orderFile = new File(dir, FILE_NAME);

        File[] dataFiles = dir.listFiles((d, name) -> {
            String ext = FilenameUtils.getExtension(name).toLowerCase(Locale.ROOT);
            return DataFormat.CSV.matches(ext) || DataFormat.JSON.matches(ext)
                    || DataFormat.YAML.matches(ext) || DataFormat.XML.matches(ext);
        });
        int fileCount = ArrayUtils.getLength(dataFiles);

        FileUtils.deleteQuietly(orderFile);

        if (fileCount == 0) {
            log.info("No dataset files found → ordering file not generated");
            return;
        }

        try {
            String content =
                    Arrays.stream(dataFiles).map(f -> FilenameUtils.getBaseName(f.getName()))
                            .sorted().collect(Collectors.joining(System.lineSeparator()));
            FileUtils.writeStringToFile(orderFile, content, StandardCharsets.UTF_8);
            log.info("Generated table-ordering.txt: {}", orderFile.getAbsolutePath());
        } catch (IOException e) {
            log.error("Failed to create table-ordering.txt: {}", e.getMessage(), e);
            ErrorHandler.errorAndExit("Failed to create table-ordering.txt", e);
        }
    }

    /**
     * Deletes {@code table-ordering.txt} from {@code dir} if it exists. Silently ignores the case
     * where the file does not exist.
     *
     * @param dir directory containing {@code table-ordering.txt}
     */
    public static void delete(File dir) {
        File orderFile = new File(dir, FILE_NAME);
        if (FileUtils.deleteQuietly(orderFile)) {
            log.info("Deleted table-ordering.txt: {}", orderFile.getAbsolutePath());
        }
    }
}
