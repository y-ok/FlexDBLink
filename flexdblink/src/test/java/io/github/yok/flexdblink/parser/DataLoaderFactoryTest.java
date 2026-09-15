package io.github.yok.flexdblink.parser;

import static io.github.yok.flexdblink.parser.DataParserTestSupport.copyFixture;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.github.yok.flexdblink.util.CsvUtils;
import java.io.File;
import java.io.FilenameFilter;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

class DataLoaderFactoryTest {

    @TempDir
    Path tempDir;

    @ParameterizedTest(name = "format={0}")
    @CsvSource({"csv,csv", "json,json", "yaml,yaml", "yml,yaml", "xml,xml"})
    void create_正常ケース_対応形式のファイルを指定する_対象テーブルの行が返る結果であること(String extension, String expected)
            throws Exception {
        copyDataset("valid", extension);

        IDataSet parsed = DataLoaderFactory.create(tempDir.toFile(), "TBL");

        ITable table = parsed.getTable("TBL");
        assertEquals(1, table.getRowCount());
        assertEquals("1", table.getValue(0, "ID"));
        assertEquals(expected, table.getValue(0, "NAME"));
    }

    @ParameterizedTest(name = "format={0}")
    @CsvSource({"csv,csv", "json,json", "yaml,yaml", "yml,yaml", "xml,xml"})
    void create_正常ケース_対象名の大文字小文字を変える_同じテーブルが返る結果であること(String extension, String expected)
            throws Exception {
        copyDataset("valid", extension);

        IDataSet parsed = DataLoaderFactory.create(tempDir.toFile(), "tBl");

        assertEquals(expected, parsed.getTable("TBL").getValue(0, "NAME"));
    }

    @ParameterizedTest(name = "files={0}, selected={1}")
    @CsvSource({"'csv,json,yaml,yml,xml',csv", "'json,yaml,yml,xml',json", "'yaml,xml',yaml",
            "'yml,xml',yaml"})
    void create_正常ケース_同名の複数形式を配置する_優先順位が最上位のファイルの値であること(String extensions, String expected)
            throws Exception {
        for (String extension : extensions.split(",")) {
            copyDataset("valid", extension);
        }

        IDataSet parsed = DataLoaderFactory.create(tempDir.toFile(), "TBL");

        assertEquals(expected, parsed.getTable("TBL").getValue(0, "NAME"));
    }

    @Test
    void create_正常ケース_CSVの引用符とnullと空文字を読み込む_各値が保持される結果であること() throws Exception {
        copyFixture(tempDir, "csv-values/TBL.csv", "TBL.csv");
        copyFixture(tempDir, "valid/table-ordering.txt", "table-ordering.txt");

        DataParserTestSupport
                .assertCsvValues(DataLoaderFactory.create(tempDir.toFile(), "TBL").getTable("TBL"));
    }

    @Test
    void create_正常ケース_別名の高優先ファイルを配置する_対象名に一致するファイルの値であること() throws Exception {
        copyFixture(tempDir, "valid/TBL.csv", "OTHER.csv");
        copyFixture(tempDir, "unsupported/TBL.txt", "TBL.txt");
        copyDataset("valid", "json");

        IDataSet parsed = DataLoaderFactory.create(tempDir.toFile(), "TBL");

        assertEquals("json", parsed.getTable("TBL").getValue(0, "NAME"));
    }

    @Test
    void create_異常ケース_対象ファイルを配置しない_対象名を含む例外通知であること() {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> DataLoaderFactory.create(tempDir.toFile(), "MISSING"));
        assertEquals("No dataset file found for table: MISSING", failure.getMessage());
    }

    @Test
    void create_異常ケース_未対応形式だけを配置する_対象名を含む例外通知であること() throws Exception {
        copyFixture(tempDir, "unsupported/TBL.txt", "TBL.txt");

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> DataLoaderFactory.create(tempDir.toFile(), "TBL"));

        assertEquals("No dataset file found for table: TBL", failure.getMessage());
    }

    @Test
    void create_異常ケース_ディレクトリに通常ファイルを指定する_対象名を含む例外通知であること() throws Exception {
        Path file = tempDir.resolve("not-directory.txt");
        copyFixture(tempDir, "unsupported/TBL.txt", file.getFileName().toString());

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> DataLoaderFactory.create(file.toFile(), "TBL"));

        assertEquals("No dataset file found for table: TBL", failure.getMessage());
    }

    @ParameterizedTest(name = "format={0}")
    @ValueSource(strings = {"csv", "json", "yaml", "yml", "xml"})
    void create_異常ケース_選択ファイルの解析を失敗させる_呼び出し元への例外通知であること(String extension) throws Exception {
        copyDataset("malformed", extension);
        // A valid lower-priority file must not hide corruption in the selected file.
        if (!"xml".equals(extension)) {
            copyDataset("valid", "xml");
        }

        assertThrows(Exception.class, () -> DataLoaderFactory.create(tempDir.toFile(), "TBL"));
    }

    @ParameterizedTest(name = "value={0}")
    @NullSource
    @ValueSource(strings = {"", "null", "NULL", "Null", "nullable", "a,b", "a\"b", "C:\\temp\\new",
            "a\nb", "a\r\nb"})
    void create_正常ケース_NULLと空文字と特殊文字をCSV出力して再読込する_元の値が保持される結果であること(String value) throws Exception {
        CsvUtils.writeCsvUtf8(tempDir.resolve("TBL.csv").toFile(), new String[] {"ID", "VALUE"},
                List.of(Arrays.asList("1", value)));
        copyFixture(tempDir, "valid/table-ordering.txt", "table-ordering.txt");

        ITable table = DataLoaderFactory.create(tempDir.toFile(), "TBL").getTable("TBL");

        assertEquals(1, table.getRowCount());
        assertEquals("1", table.getValue(0, "ID"));
        assertEquals(value, table.getValue(0, "VALUE"),
                "A literal string must survive a CSV write/read round trip without becoming null.");
    }

    @ParameterizedTest(name = "format={0}")
    @ValueSource(strings = {"json", "yaml", "yml"})
    void create_正常ケース_後続行でカラムを追加する_全カラムと欠落値が保持される結果であること(String extension) throws Exception {
        DataParserTestSupport.copySparseDataset(tempDir, extension);

        IDataSet parsed = DataLoaderFactory.create(tempDir.toFile(), "EMP");

        DataParserTestSupport.assertSparseTable(parsed.getTable("EMP"));
    }

    @Test
    void constructor_正常ケース_直接生成する_インスタンスが生成される結果であること() {
        assertNotNull(new DataLoaderFactory());
    }

    @Test
    void createParser_異常ケース_null形式を指定する_未対応形式の例外通知であること() {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> DataLoaderFactory.createParser(null));
        assertEquals("Unsupported format: null", failure.getMessage());
    }

    @Test
    void createParser_正常ケース_各形式を指定する_対応するパーサであること() {
        assertTrue(DataLoaderFactory.createParser(DataFormat.CSV) instanceof CsvDataParser);
        assertTrue(DataLoaderFactory.createParser(DataFormat.JSON) instanceof JsonDataParser);
        assertTrue(DataLoaderFactory.createParser(DataFormat.YAML) instanceof YamlDataParser);
        assertTrue(DataLoaderFactory.createParser(DataFormat.XML) instanceof XmlDataParser);
    }

    private void copyDataset(String category, String extension) throws Exception {
        String fixtureExtension = extension;
        if ("yml".equals(extension)) {
            fixtureExtension = "yaml";
        }
        copyFixture(tempDir, category + "/TBL." + fixtureExtension, "TBL." + extension);
        if ("csv".equals(extension)) {
            copyFixture(tempDir, "valid/table-ordering.txt", "table-ordering.txt");
        }
    }

    @ParameterizedTest(name = "table={0}")
    @ValueSource(strings = {"A", "B"})
    void create_正常ケース_複数XMLから対象テーブルを選択する_選択ファイルの行が返る結果であること(String table) throws Exception {
        copyFixture(tempDir, "xml-selection/A.xml", "A.xml");
        copyFixture(tempDir, "xml-selection/B.xml", "B.xml");

        File orderedDir = new OrderedDir(tempDir.toFile());
        IDataSet dataSet = DataLoaderFactory.create(orderedDir, table);

        assertArrayEquals(new String[] {table}, dataSet.getTableNames());
        assertEquals(1, dataSet.getTable(table).getRowCount());
        assertEquals(table + "-row", dataSet.getTable(table).getValue(0, "ID"));
    }

    private static class OrderedDir extends File {
        private static final long serialVersionUID = 1L;

        OrderedDir(File dir) {
            super(dir.getPath());
        }

        @Override
        public File[] listFiles(FilenameFilter filter) {
            File[] files = super.listFiles(filter);
            if (files == null) {
                return null;
            }
            Arrays.sort(files, (a, b) -> a.getName().compareToIgnoreCase(b.getName()));
            return files;
        }
    }

}
