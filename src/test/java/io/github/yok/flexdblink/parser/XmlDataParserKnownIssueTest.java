package io.github.yok.flexdblink.parser;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.FilenameFilter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class XmlDataParserKnownIssueTest {

    @TempDir
    Path tempDir;

    @Test
    void dataLoaderFactory_xmlMayIgnoreResolvedFile() throws Exception {
        writeXml(tempDir.resolve("A.xml"), "A");
        writeXml(tempDir.resolve("B.xml"), "B");

        File orderedDir = new OrderedDir(tempDir.toFile());
        IDataSet dataSet = DataLoaderFactory.create(orderedDir, "B");

        assertThrows(DataSetException.class, () -> dataSet.getTable("B"));
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

    private static void writeXml(Path path, String table) throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                + "<dataset>\n"
                + "  <" + table + " ID=\"1\" />\n"
                + "</dataset>\n";
        Files.writeString(path, xml);
    }
}
