package io.github.yok.flexdblink.parser;

import static io.github.yok.flexdblink.parser.DataParserTestSupport.copyFixture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.dbunit.dataset.IDataSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

class DataParserSuiteTest {

    @TempDir
    Path tempDir;

    @Test
    void parse_正常ケース_CsvDataParserでCSVを読む_テーブル行が取得できること() throws Exception {
        Path csv = tempDir.resolve("BOOK.csv");
        Files.writeString(csv, "ID,NAME\n1,Java\n", StandardCharsets.UTF_8);
        Files.writeString(tempDir.resolve("table-ordering.txt"), "BOOK\n", StandardCharsets.UTF_8);

        CsvDataParser parser = new CsvDataParser();
        IDataSet dataSet = parser.parse(tempDir.toFile());

        assertEquals(1, dataSet.getTable("BOOK").getRowCount());
        assertEquals("Java", dataSet.getTable("BOOK").getValue(0, "NAME"));
    }

    @Test
    void parse_正常ケース_JsonDataParserでJSONを読む_テーブル行が取得できること() throws Exception {
        Path json = tempDir.resolve("EMP.json");
        Files.writeString(json, "[{\"ID\":\"1\",\"NAME\":\"Alice\"},{\"ID\":\"2\",\"NAME\":null}]",
                StandardCharsets.UTF_8);
        JsonDataParser parser = new JsonDataParser();
        IDataSet dataSet = parser.parse(tempDir.toFile());
        assertEquals(2, dataSet.getTable("EMP").getRowCount());
        assertEquals("Alice", dataSet.getTable("EMP").getValue(0, "NAME"));
    }

    @Test
    void parse_正常ケース_YamlDataParserでYAMLを読む_テーブル行が取得できること() throws Exception {
        Path yaml = tempDir.resolve("DEPT.yaml");
        Files.writeString(yaml, "- ID: \"10\"\n  NAME: \"Sales\"\n", StandardCharsets.UTF_8);
        YamlDataParser parser = new YamlDataParser();
        IDataSet dataSet = parser.parse(tempDir.toFile());
        assertEquals(1, dataSet.getTable("DEPT").getRowCount());
    }

    @Test
    void parse_正常ケース_XmlDataParserでXMLを読む_テーブル行が取得できる結果であること() throws Exception {
        copyFixture(tempDir, "valid/TBL.xml", "TBL.xml");
        copyFixture(tempDir, "unsupported/TBL.txt", "TBL.txt");

        IDataSet dataSet = new XmlDataParser().parse(tempDir.toFile());

        assertEquals(1, dataSet.getTable("TBL").getRowCount());
        assertEquals("xml", dataSet.getTable("TBL").getValue(0, "NAME"));
    }

    @Test
    void parse_異常ケース_CsvDataParserにファイルを渡す_DataSetExceptionが送出されること() throws Exception {
        CsvDataParser parser = new CsvDataParser();
        Path notDirectory = tempDir.resolve("single.csv");
        Files.writeString(notDirectory, "ID\n1\n", StandardCharsets.UTF_8);
        assertThrows(Exception.class, () -> parser.parse(notDirectory.toFile()));
    }

    @Test
    void parse_異常ケース_JsonDataParserにファイルを渡す_DataSetExceptionが送出されること() throws Exception {
        JsonDataParser parser = new JsonDataParser();
        Path notDirectory = tempDir.resolve("single.json");
        Files.writeString(notDirectory, "[]", StandardCharsets.UTF_8);
        assertThrows(Exception.class, () -> parser.parse(notDirectory.toFile()));
    }

    @Test
    void parse_正常ケース_JsonDataParserで空配列を読む_テーブル数が0であること() throws Exception {
        Files.writeString(tempDir.resolve("EMPTY.json"), "[]", StandardCharsets.UTF_8);
        JsonDataParser parser = new JsonDataParser();
        IDataSet dataSet = parser.parse(tempDir.toFile());
        assertEquals(0, dataSet.getTableNames().length);
    }

    @Test
    void parse_正常ケース_JsonDataParserで非配列ルートを読む_テーブルが作成されないこと() throws Exception {
        Files.writeString(tempDir.resolve("OBJ.json"), "{\"ID\":\"1\",\"NAME\":\"obj\"}",
                StandardCharsets.UTF_8);
        JsonDataParser parser = new JsonDataParser();
        IDataSet dataSet = parser.parse(tempDir.toFile());
        assertEquals(0, dataSet.getTableNames().length);
    }

    @Test
    void parse_正常ケース_JsonDataParserで行の列欠落を読む_欠落列がnullとして格納されること() throws Exception {
        Files.writeString(tempDir.resolve("MISS.json"),
                "[{\"ID\":\"1\",\"NAME\":\"A\"},{\"ID\":\"2\"}]", StandardCharsets.UTF_8);
        JsonDataParser parser = new JsonDataParser();
        IDataSet dataSet = parser.parse(tempDir.toFile());
        assertEquals(2, dataSet.getTable("MISS").getRowCount());
        assertNull(dataSet.getTable("MISS").getValue(1, "NAME"));
    }

    @Test
    void parse_正常ケース_YamlDataParserでyml拡張子を読む_テーブル行が取得できること() throws Exception {
        Files.writeString(tempDir.resolve("DEPT2.yml"), "- ID: \"20\"\n  NAME: \"HR\"\n",
                StandardCharsets.UTF_8);
        YamlDataParser parser = new YamlDataParser();
        IDataSet dataSet = parser.parse(tempDir.toFile());
        assertEquals("HR", dataSet.getTable("DEPT2").getValue(0, "NAME"));
    }

    @Test
    void parse_異常ケース_YamlDataParserにファイルを渡す_DataSetExceptionが送出されること() throws Exception {
        YamlDataParser parser = new YamlDataParser();
        Path notDirectory = tempDir.resolve("single.yaml");
        Files.writeString(notDirectory, "- ID: \"1\"\n", StandardCharsets.UTF_8);
        assertThrows(Exception.class, () -> parser.parse(notDirectory.toFile()));
    }

    @Test
    void parse_異常ケース_XmlDataParserにファイルを渡す_DataSetExceptionが送出されること() throws Exception {
        XmlDataParser parser = new XmlDataParser();
        Path notDirectory = tempDir.resolve("single.xml");
        Files.writeString(notDirectory, "<dataset/>", StandardCharsets.UTF_8);
        assertThrows(Exception.class, () -> parser.parse(notDirectory.toFile()));
    }

    @Test
    void parse_異常ケース_XmlDataParserに空ディレクトリを渡す_DataSetExceptionが送出されること() throws Exception {
        XmlDataParser parser = new XmlDataParser();
        assertThrows(Exception.class, () -> parser.parse(tempDir.toFile()));
    }

    @Test
    void parse_正常ケース_YamlDataParserでyml以外拡張子を含む_対象外ファイルは読み飛ばされること() throws Exception {
        Files.writeString(tempDir.resolve("IGNORE.txt"), "x", StandardCharsets.UTF_8);
        Files.writeString(tempDir.resolve("OK.yaml"), "- ID: \"1\"\n  NAME: \"ok\"\n",
                StandardCharsets.UTF_8);
        YamlDataParser parser = new YamlDataParser();
        IDataSet dataSet = parser.parse(tempDir.toFile());
        assertEquals(1, dataSet.getTable("OK").getRowCount());
    }

    @Test
    void parse_正常ケース_YamlDataParserで非配列ルートを読む_テーブルが作成されないこと() throws Exception {
        Files.writeString(tempDir.resolve("OBJ.yaml"), "ID: \"1\"\nNAME: \"x\"\n",
                StandardCharsets.UTF_8);
        YamlDataParser parser = new YamlDataParser();
        IDataSet dataSet = parser.parse(tempDir.toFile());
        assertEquals(0, dataSet.getTableNames().length);
    }

    @Test
    void parse_正常ケース_YamlDataParserで空配列を読む_テーブルが作成されないこと() throws Exception {
        Files.writeString(tempDir.resolve("EMPTY.yaml"), "[]\n", StandardCharsets.UTF_8);
        YamlDataParser parser = new YamlDataParser();
        IDataSet dataSet = parser.parse(tempDir.toFile());
        assertEquals(0, dataSet.getTableNames().length);
    }

    @Test
    void parse_正常ケース_YamlDataParserで列欠落とnull値を読む_nullとして格納されること() throws Exception {
        Files.writeString(tempDir.resolve("MISSY.yaml"), "- ID: \"1\"\n  NAME: null\n- ID: \"2\"\n",
                StandardCharsets.UTF_8);
        YamlDataParser parser = new YamlDataParser();
        IDataSet dataSet = parser.parse(tempDir.toFile());
        assertEquals(2, dataSet.getTable("MISSY").getRowCount());
        assertNull(dataSet.getTable("MISSY").getValue(0, "NAME"));
        assertNull(dataSet.getTable("MISSY").getValue(1, "NAME"));
    }

    @Test
    void parse_異常ケース_XmlDataParserで読み取り不可ファイルを読む_DataSetExceptionが送出されること() throws Exception {
        Path xml = tempDir.resolve("LOCK.xml");
        Files.writeString(xml, "<dataset><LOCK ID=\"1\"/></dataset>", StandardCharsets.UTF_8);
        XmlDataParser parser = new XmlDataParser();

        try (MockedStatic<Files> files = mockStatic(Files.class, CALLS_REAL_METHODS)) {
            files.when(() -> Files.isReadable(xml)).thenReturn(false);
            assertThrows(Exception.class, () -> parser.parse(tempDir.toFile()));
        }
    }

}
