package io.github.yok.flexdblink.parser;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class XmlDataParserKnownIssueTest {

    @TempDir
    Path tempDir;

    @Disabled("Known issue: XmlDataParser reads only the first XML file in the directory.")
    @Test
    void dataLoaderFactory_xmlMayIgnoreResolvedFile() throws Exception {
        writeXml(tempDir.resolve("A.xml"), "A");
        writeXml(tempDir.resolve("B.xml"), "B");

        IDataSet dataSet = DataLoaderFactory.create(tempDir.toFile(), "B");

        assertThrows(DataSetException.class, () -> dataSet.getTable("B"));
    }

    private static void writeXml(Path path, String table) throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                + "<dataset>\n"
                + "  <" + table + " ID=\"1\" />\n"
                + "</dataset>\n";
        Files.writeString(path, xml);
    }
}
