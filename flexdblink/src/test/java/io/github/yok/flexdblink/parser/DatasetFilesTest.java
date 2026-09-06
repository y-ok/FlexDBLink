package io.github.yok.flexdblink.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.csv.CsvDataSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DatasetFilesTest {
    @TempDir
    Path directory;

    @Test
    void parse_正常ケース_CSVの引用符とnullと空文字を読み込む_従来と同一であること() throws Exception {
        Files.writeString(directory.resolve("sample.csv"),
                " ID ,NAME,NOTE\n1,\"a,b\",null\n2,\"quoted\"\"text\",\"\"\n");
        Files.writeString(directory.resolve("table-ordering.txt"), "sample\n");
        ITable expected = new CsvDataSet(directory.toFile()).getTable("sample");
        DatasetFiles files = new DatasetFiles(directory.toFile());
        ITable actual = files.parse("sample").getTable("sample");
        assertEquals(expected.getRowCount(), actual.getRowCount());
        for (int row = 0; row < actual.getRowCount(); row++) {
            for (String column : List.of("ID", "NAME", "NOTE")) {
                assertEquals(expected.getValue(row, column), actual.getValue(row, column));
            }
        }
        assertTrue(files.isCsv("SAMPLE"));
        assertEquals("sample\n", Files.readString(directory.resolve("table-ordering.txt")));
    }

    @Test
    void parse_正常ケース_混在形式と同名ファイルを指定する_優先形式だけの値が返ること() throws Exception {
        Files.writeString(directory.resolve("a.csv"), "ID\n1\n");
        Files.writeString(directory.resolve("a.json"), "invalid sibling");
        Files.writeString(directory.resolve("b.json"), "[{\"ID\":2,\"VALUE\":null},{\"ID\":3}]");
        Files.writeString(directory.resolve("c.yaml"), "- ID: 4\n  VALUE: null\n- ID: 5\n");
        Files.writeString(directory.resolve("d.yml"), "- ID: 6\n");
        Files.writeString(directory.resolve("e.xml"), "<dataset><e ID=\"7\"/></dataset>");
        Files.writeString(directory.resolve("f.xml"), "<dataset><f ID=\"8\"/></dataset>");
        Files.writeString(directory.resolve("readme.txt"), "ignored");
        DatasetFiles files = new DatasetFiles(directory.toFile());
        assertEquals(List.of("a", "b", "c", "d", "e", "f"), files.getTableNames());
        for (String table : files.getTableNames()) {
            assertEquals(1, files.parse(table).getTableNames().length);
        }
        assertEquals("2", files.parse("b").getTable("b").getValue(0, "ID"));
        assertFalse(files.isCsv("b"));
        assertFalse(Files.exists(directory.resolve("table-ordering.txt")));
    }

    @Test
    void parse_正常ケース_空配列と非配列を指定する_空データセットであること() throws Exception {
        Files.writeString(directory.resolve("a.json"), "[]");
        Files.writeString(directory.resolve("b.json"), "{}");
        Files.writeString(directory.resolve("c.yaml"), "[]");
        Files.writeString(directory.resolve("d.yaml"), "{}");
        DatasetFiles files = new DatasetFiles(directory.toFile());
        for (String table : files.getTableNames()) {
            IDataSet parsed = files.parse(table);
            assertEquals(0, parsed.getTableNames().length);
        }
    }

    @Test
    void parse_正常ケース_次のロードまでにファイルを変更する_最新の値であること() throws Exception {
        Path file = directory.resolve("a.csv");
        Files.writeString(file, "ID\n1\n");
        assertEquals("1",
                new DatasetFiles(directory.toFile()).parse("a").getTable("a").getValue(0, "ID"));
        Files.writeString(file, "ID\n2\n");
        assertEquals("2",
                new DatasetFiles(directory.toFile()).parse("a").getTable("a").getValue(0, "ID"));
    }

    @Test
    void constructor_異常ケース_存在しないディレクトリを指定する_IOExceptionであること() {
        assertThrows(IOException.class,
                () -> new DatasetFiles(directory.resolve("missing").toFile()));
    }
}
