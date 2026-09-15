package io.github.yok.flexdblink.parser;

import static io.github.yok.flexdblink.parser.DataParserTestSupport.copyFixture;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.github.yok.flexdblink.util.CsvUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

class DatasetFilesTest {
    @TempDir
    Path directory;

    @ParameterizedTest(name = "table={0}")
    @ValueSource(strings = {"A", "B"})
    void parse_正常ケース_複数XMLを索引から選択する_選択ファイルの行が返る結果であること(String table) throws Exception {
        copyFixture(directory, "xml-selection/A.xml", "A.xml");
        copyFixture(directory, "xml-selection/B.xml", "B.xml");

        IDataSet dataSet = new DatasetFiles(directory.toFile()).parse(table);

        assertArrayEquals(new String[] {table}, dataSet.getTableNames());
        assertEquals(1, dataSet.getTable(table).getRowCount());
        assertEquals(table + "-row", dataSet.getTable(table).getValue(0, "ID"));
    }

    @ParameterizedTest(name = "value={0}")
    @NullSource
    @ValueSource(strings = {"", "null", "NULL", "Null", "nullable", "a,b", "a\"b", "C:\\temp\\new",
            "a\nb", "a\r\nb"})
    void parse_正常ケース_NULLと空文字と特殊文字をCSV出力して再読込する_元の値が保持される結果であること(String value) throws Exception {
        CsvUtils.writeCsvUtf8(directory.resolve("sample.csv").toFile(),
                new String[] {"ID", "VALUE"}, List.of(Arrays.asList("1", value)));
        IDataSet parsed = new DatasetFiles(directory.toFile()).parse("sample");

        ITable table = parsed.getTable("sample");
        assertEquals(1, table.getRowCount());
        assertEquals("1", table.getValue(0, "ID"));
        assertEquals(value, table.getValue(0, "VALUE"),
                "A literal string must survive a CSV write/read round trip without becoming null.");
    }

    @ParameterizedTest(name = "format={0}")
    @ValueSource(strings = {"json", "yaml", "yml"})
    void parse_正常ケース_後続行でカラムを追加する_全カラムと欠落値が保持される結果であること(String extension) throws Exception {
        DataParserTestSupport.copySparseDataset(directory, extension);

        IDataSet parsed = new DatasetFiles(directory.toFile()).parse("EMP");

        DataParserTestSupport.assertSparseTable(parsed.getTable("EMP"));
    }

    @Test
    void parse_正常ケース_CSVの引用符とnullと空文字を読み込む_各値が保持される結果であること() throws Exception {
        copyFixture(directory, "csv-values/TBL.csv", "TBL.csv");
        DatasetFiles files = new DatasetFiles(directory.toFile());

        DataParserTestSupport.assertCsvValues(files.parse("TBL").getTable("TBL"));

        assertTrue(files.isCsv("tbl"));
        assertFalse(Files.exists(directory.resolve("table-ordering.txt")));
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
