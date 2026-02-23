package io.github.yok.flexdblink.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class TableOrderingFileTest {

    @TempDir
    Path tempDir;

    // -------------------------------------------------------------------------
    // write
    // -------------------------------------------------------------------------

    @Test
    void write_正常ケース_テーブルリストを指定する_ファイルが生成されること() throws Exception {
        TableOrderingFile.write(tempDir.toFile(), List.of("T1", "T2", "T3"));

        Path ordering = tempDir.resolve(TableOrderingFile.FILE_NAME);
        assertTrue(Files.exists(ordering));
        String content = Files.readString(ordering, StandardCharsets.UTF_8);
        assertEquals("T1" + System.lineSeparator() + "T2" + System.lineSeparator() + "T3", content);
    }

    @Test
    void write_異常ケース_IOExceptionが発生する_ErrorHandlerが呼ばれること() {
        IOException io = new IOException("write fail");

        try (MockedStatic<FileUtils> fileUtils = Mockito.mockStatic(FileUtils.class);
                MockedStatic<ErrorHandler> handler = Mockito.mockStatic(ErrorHandler.class)) {

            fileUtils.when(() -> FileUtils.writeStringToFile(any(File.class), anyString(),
                    eq(StandardCharsets.UTF_8))).thenThrow(io);
            handler.when(() -> ErrorHandler.errorAndExit(anyString(), any()))
                    .thenAnswer(inv -> null);

            TableOrderingFile.write(tempDir.toFile(), List.of("T1"));

            handler.verify(
                    () -> ErrorHandler.errorAndExit("Failed to create table-ordering.txt", io));
        }
    }

    // -------------------------------------------------------------------------
    // delete
    // -------------------------------------------------------------------------

    @Test
    void delete_正常ケース_ファイルが存在する_削除されること() throws Exception {
        Path ordering = tempDir.resolve(TableOrderingFile.FILE_NAME);
        Files.writeString(ordering, "T1", StandardCharsets.UTF_8);
        assertTrue(Files.exists(ordering));

        TableOrderingFile.delete(tempDir.toFile());

        assertFalse(Files.exists(ordering));
    }

    @Test
    void delete_正常ケース_ファイルが存在しない_ログなしで正常終了すること() {
        // table-ordering.txt が存在しない → deleteQuietly が false を返す (false ブランチ)
        TableOrderingFile.delete(tempDir.toFile());
        // 例外が発生しないことで false ブランチの通過を確認
    }

    // -------------------------------------------------------------------------
    // ensure
    // -------------------------------------------------------------------------

    @Test
    void ensure_正常ケース_csv_json_yamlファイルがある_アルファベット順でorderingが生成されること() throws Exception {
        Path dir = tempDir.resolve("case1");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("B.csv"), "ID\n1\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("A.json"), "[]", StandardCharsets.UTF_8);

        TableOrderingFile.ensure(dir.toFile());

        Path ordering = dir.resolve(TableOrderingFile.FILE_NAME);
        String actual = Files.readString(ordering, StandardCharsets.UTF_8);
        assertEquals("A" + System.lineSeparator() + "B", actual);
    }

    @Test
    void ensure_正常ケース_xml拡張子を指定する_orderingが生成されること() throws Exception {
        Path dir = tempDir.resolve("case_xml");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("X.xml"), "<dataset/>", StandardCharsets.UTF_8);

        TableOrderingFile.ensure(dir.toFile());

        assertTrue(Files.exists(dir.resolve(TableOrderingFile.FILE_NAME)));
    }

    @Test
    void ensure_正常ケース_orderingが既存であっても_常に再生成されること() throws Exception {
        Path dir = tempDir.resolve("case_existing");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("B.csv"), "ID\n1\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("A.yaml"), "[]", StandardCharsets.UTF_8);
        Path ordering = dir.resolve(TableOrderingFile.FILE_NAME);
        Files.writeString(ordering, "EXISTING1\nEXISTING2\n", StandardCharsets.UTF_8);

        TableOrderingFile.ensure(dir.toFile());

        String actual = Files.readString(ordering, StandardCharsets.UTF_8);
        assertEquals("A" + System.lineSeparator() + "B", actual);
    }

    @Test
    void ensure_正常ケース_データセットファイルがない_orderingが生成されないこと() throws Exception {
        Path dir = tempDir.resolve("case_empty");
        Files.createDirectories(dir);

        TableOrderingFile.ensure(dir.toFile());

        assertFalse(Files.exists(dir.resolve(TableOrderingFile.FILE_NAME)));
    }

    @Test
    void ensure_正常ケース_dirがファイルである_orderingを生成せず元ファイルが残ること() throws Exception {
        Path file = tempDir.resolve("not_a_dir.txt");
        Files.writeString(file, "x", StandardCharsets.UTF_8);

        // listFiles() が null を返す → fileCount == 0 → 早期リターン
        TableOrderingFile.ensure(file.toFile());

        assertTrue(Files.exists(file));
    }

    @Test
    void ensure_正常ケース_行数一致でも常に再生成されること() throws Exception {
        Path dir = tempDir.resolve("case_always_regen");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("T1.csv"), "ID\n1\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("T2.csv"), "ID\n2\n", StandardCharsets.UTF_8);
        Path ordering = dir.resolve(TableOrderingFile.FILE_NAME);
        Files.writeString(ordering, "CUSTOM1\nCUSTOM2", StandardCharsets.UTF_8);
        long lastModified = ordering.toFile().lastModified();
        Thread.sleep(50);

        TableOrderingFile.ensure(dir.toFile());

        assertTrue(ordering.toFile().lastModified() > lastModified,
                "table-ordering.txt should always be regenerated");
        List<String> lines = Files.readAllLines(ordering, StandardCharsets.UTF_8);
        assertEquals(List.of("T1", "T2"), lines);
    }

    @Test
    void ensure_異常ケース_書き込み失敗が発生する_ErrorHandlerが呼ばれること() throws Exception {
        Path dir = tempDir.resolve("case_write_fail");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("A.csv"), "ID\n1\n", StandardCharsets.UTF_8);

        try (MockedStatic<FileUtils> fileUtils = Mockito.mockStatic(FileUtils.class);
                MockedStatic<ErrorHandler> errorHandler = Mockito.mockStatic(ErrorHandler.class)) {

            fileUtils.when(() -> FileUtils.writeStringToFile(any(File.class), anyString(),
                    eq(StandardCharsets.UTF_8))).thenThrow(new IOException("write fail"));
            errorHandler.when(() -> ErrorHandler.errorAndExit(anyString(), any(Throwable.class)))
                    .thenAnswer(inv -> null);

            TableOrderingFile.ensure(dir.toFile());

            errorHandler.verify(() -> ErrorHandler
                    .errorAndExit(eq("Failed to create table-ordering.txt"), any(Throwable.class)),
                    times(1));
        }
    }

    @Test
    void ensure_異常ケース_disableExitで書き込み失敗が発生する_IllegalStateExceptionが送出されること() throws Exception {
        Path dir = tempDir.resolve("case_write_fail_exit");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("A.csv"), "ID\n1\n", StandardCharsets.UTF_8);

        ErrorHandler.disableExitForCurrentThread();
        try (MockedStatic<FileUtils> fileUtils = Mockito.mockStatic(FileUtils.class)) {
            fileUtils.when(() -> FileUtils.writeStringToFile(any(File.class), anyString(),
                    eq(StandardCharsets.UTF_8))).thenThrow(new IOException("write fail"));
            assertThrows(IllegalStateException.class, () -> TableOrderingFile.ensure(dir.toFile()));
        } finally {
            ErrorHandler.restoreExitForCurrentThread();
        }
    }
}
