package io.github.yok.flexdblink.parser;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.dbunit.dataset.ITable;

/**
 * Loads fixtures and shares CSV and sparse-row expectations between dataset-loading entry points.
 */
final class DataParserTestSupport {

    private DataParserTestSupport() {}

    static void copySparseDataset(Path directory, String extension) throws Exception {
        String fixtureExtension = extension;
        if ("yml".equals(extension)) {
            fixtureExtension = "yaml";
        }
        copyFixture(directory, "sparse/EMP." + fixtureExtension, "EMP." + extension);
    }

    static void copyFixture(Path directory, String resourceName, String fileName) throws Exception {
        try (InputStream input = Objects.requireNonNull(
                DataParserTestSupport.class.getResourceAsStream("datasets/" + resourceName),
                resourceName)) {
            Files.copy(input, directory.resolve(fileName));
        }
    }

    static void assertCsvValues(ITable table) throws Exception {
        assertEquals(6, table.getRowCount());
        assertAll(() -> assertEquals("1", table.getValue(0, "ID")),
                () -> assertEquals("a,b", table.getValue(0, "NAME")),
                () -> assertNull(table.getValue(0, "NOTE")),
                () -> assertEquals("quoted\"text", table.getValue(1, "NAME")),
                () -> assertEquals("", table.getValue(1, "NOTE")),
                () -> assertEquals("null", table.getValue(2, "NAME")),
                () -> assertNull(table.getValue(2, "NOTE")),
                () -> assertEquals(" A ", table.getValue(3, "NAME")),
                () -> assertEquals("日本語 😀", table.getValue(3, "NOTE")),
                () -> assertEquals("Path\\to\\file", table.getValue(4, "NAME")),
                () -> assertEquals("null", table.getValue(4, "NOTE")),
                () -> assertEquals("null", table.getValue(5, "NAME")),
                () -> assertEquals("", table.getValue(5, "NOTE")));
    }

    static void assertSparseTable(ITable table) throws Exception {
        assertEquals(3, table.getRowCount());
        assertEquals(Set.of("ID", "NAME", "NOTE"),
                Arrays.stream(table.getTableMetaData().getColumns())
                        .map(column -> column.getColumnName()).collect(Collectors.toSet()),
                "Columns introduced after the first row must not be silently discarded.");
        assertAll(() -> assertEquals("1", table.getValue(0, "ID")),
                () -> assertEquals("2", table.getValue(1, "ID")),
                () -> assertEquals("3", table.getValue(2, "ID")),
                () -> assertNull(table.getValue(0, "NAME")),
                () -> assertNull(table.getValue(0, "NOTE")),
                () -> assertEquals("Alice", table.getValue(1, "NAME")),
                () -> assertNull(table.getValue(1, "NOTE")),
                () -> assertNull(table.getValue(2, "NAME")),
                () -> assertEquals("late", table.getValue(2, "NOTE")));
    }
}
