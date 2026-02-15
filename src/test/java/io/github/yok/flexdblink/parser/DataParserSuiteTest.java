package io.github.yok.flexdblink.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
    void create_正常ケース_同名で複数形式がある_CSV優先で解決されること() throws Exception {
        Files.writeString(tempDir.resolve("TBL.csv"), "ID,NAME\n1,A\n", StandardCharsets.UTF_8);
        Files.writeString(tempDir.resolve("table-ordering.txt"), "TBL\n", StandardCharsets.UTF_8);

        IDataSet dataSet = DataLoaderFactory.create(tempDir.toFile(), "TBL");
        assertEquals(1, dataSet.getTable("TBL").getRowCount());
        assertEquals("A", dataSet.getTable("TBL").getValue(0, "NAME"));
    }

    @Test
    void create_正常ケース_XMLのみ存在する_XMLパーサで解決されること() throws Exception {
        Files.writeString(tempDir.resolve("T4.xml"),
                "<dataset><T4 ID=\"1\" NAME=\"Xml\"/></dataset>", StandardCharsets.UTF_8);
        IDataSet dataSet = DataLoaderFactory.create(tempDir.toFile(), "T4");
        assertEquals("Xml", dataSet.getTable("T4").getValue(0, "NAME"));
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
    void create_正常ケース_JSONのみ存在する_JSONパーサで解決されること() throws Exception {
        Files.writeString(tempDir.resolve("T2.json"), "[{\"ID\":\"1\",\"NAME\":\"Json\"}]",
                StandardCharsets.UTF_8);
        IDataSet dataSet = DataLoaderFactory.create(tempDir.toFile(), "t2");
        assertEquals("Json", dataSet.getTable("T2").getValue(0, "NAME"));
    }

    @Test
    void create_正常ケース_YAMLのみ存在する_YAMLパーサで解決されること() throws Exception {
        Files.writeString(tempDir.resolve("T3.yaml"), "- ID: \"1\"\n  NAME: \"Yaml\"\n",
                StandardCharsets.UTF_8);
        IDataSet dataSet = DataLoaderFactory.create(tempDir.toFile(), "T3");
        assertEquals("Yaml", dataSet.getTable("T3").getValue(0, "NAME"));
    }

    @Test
    void create_異常ケース_対象テーブルファイルが存在しない_IllegalArgumentExceptionが送出されること() throws Exception {
        assertThrows(IllegalArgumentException.class,
                () -> DataLoaderFactory.create(tempDir.toFile(), "MISSING"));
    }

    @Test
    void create_異常ケース_シナリオディレクトリがファイルである_IllegalArgumentExceptionが送出されること() throws Exception {
        Path notDirectory = tempDir.resolve("not-directory.txt");
        Files.writeString(notDirectory, "x", StandardCharsets.UTF_8);
        assertThrows(IllegalArgumentException.class,
                () -> DataLoaderFactory.create(notDirectory.toFile(), "MISSING"));
    }

    @Test
    void コンストラクタ_正常ケース_リフレクションで生成する_インスタンスが生成されること() throws Exception {
        Constructor<DataLoaderFactory> ctor = DataLoaderFactory.class.getDeclaredConstructor();
        ctor.setAccessible(true);
        Object instance = ctor.newInstance();
        assertNotNull(instance);
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
    void createParser_異常ケース_nullフォーマットを指定する_IllegalArgumentExceptionが送出されること() throws Exception {
        java.lang.reflect.Method method =
                DataLoaderFactory.class.getDeclaredMethod("createParser", DataFormat.class);
        method.setAccessible(true);
        InvocationTargetException ex = assertThrows(InvocationTargetException.class,
                () -> method.invoke(null, new Object[] {null}));
        assertTrue(ex.getCause() instanceof IllegalArgumentException);
        assertEquals("Unsupported format: null", ex.getCause().getMessage());
    }

    @Test
    void createParser_正常ケース_各フォーマットを指定する_対応パーサが返ること() throws Exception {
        Method method = DataLoaderFactory.class.getDeclaredMethod("createParser", DataFormat.class);
        method.setAccessible(true);

        Object csv = method.invoke(null, DataFormat.CSV);
        Object json = method.invoke(null, DataFormat.JSON);
        Object yaml = method.invoke(null, DataFormat.YAML);
        Object xml = method.invoke(null, DataFormat.XML);

        assertTrue(csv instanceof CsvDataParser);
        assertTrue(json instanceof JsonDataParser);
        assertTrue(yaml instanceof YamlDataParser);
        assertTrue(xml instanceof XmlDataParser);
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
