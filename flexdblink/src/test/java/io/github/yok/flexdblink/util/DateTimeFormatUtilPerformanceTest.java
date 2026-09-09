package io.github.yok.flexdblink.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.github.yok.flexdblink.config.CsvDateTimeFormatProperties;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.stream.Collectors;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Observes caught parsing exceptions through JFR without accessing implementation internals.
 */
class DateTimeFormatUtilPerformanceTest {

    @TempDir
    Path directory;

    @ParameterizedTest
    @ValueSource(strings = {"2026-09-10 12:34:56", "2026-09-10 12:34:56.000"})
    void parseConfiguredTimestamp_正常ケース_設定書式の日時を変換する_内部の解析例外生成がゼロであること(String value)
            throws Exception {
        CsvDateTimeFormatProperties formats = new CsvDateTimeFormatProperties();
        formats.setDate("yyyy-MM-dd");
        formats.setTime("HH:mm:ss");
        formats.setDateTime("yyyy-MM-dd HH:mm:ss");
        formats.setDateTimeWithMillis("yyyy-MM-dd HH:mm:ss.SSS");
        DateTimeFormatUtil util = new DateTimeFormatUtil(formats);
        Path recordingFile = directory.resolve("timestamp-parsing.jfr");
        long threadId = Thread.currentThread().getId();
        try (Recording recording = new Recording()) {
            recording.enable("jdk.JavaExceptionThrow").withStackTrace();
            recording.start();
            // Confirm that observation works even though the exception is caught.
            assertThrows(DateTimeParseException.class, () -> LocalDateTime.parse("invalid"));
            assertEquals(LocalDateTime.of(2026, 9, 10, 12, 34, 56),
                    util.parseConfiguredTimestamp(value));
            recording.stop();
            recording.dump(recordingFile);
        }
        List<RecordedEvent> parsingExceptions = RecordingFile.readAllEvents(recordingFile).stream()
                .filter(event -> event.getEventType().getName().equals("jdk.JavaExceptionThrow"))
                .filter(event -> event.getThread().getJavaThreadId() == threadId)
                .filter(event -> event.getClass("thrownClass").getName()
                        .equals(DateTimeParseException.class.getName()))
                .collect(Collectors.toList());
        assertTrue(parsingExceptions.stream().anyMatch(event -> !isUtilityCall(event)),
                "JFR must capture the control exception before checking the production call.");
        long actual = parsingExceptions.stream().filter(this::isUtilityCall).count();
        assertEquals(0, actual,
                "A valid configured timestamp must not generate a caught DateTimeParseException.");
    }

    private boolean isUtilityCall(RecordedEvent event) {
        return event.getStackTrace().getFrames().stream().anyMatch(frame -> frame.getMethod()
                .getType().getName().equals(DateTimeFormatUtil.class.getName()));
    }
}
