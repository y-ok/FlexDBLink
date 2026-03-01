package io.github.yok.flexdblink.db;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Locale;
import lombok.Generated;

/**
 * Shared flexible {@link DateTimeFormatter} constants used by all dialect handlers for CSV load
 * parsing.
 *
 * <p>
 * These parsers are intentionally permissive: optional seconds, optional fractional seconds, and
 * multiple date formats allow robust parsing of varied CSV input while remaining deterministic.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
public final class FlexibleDateTimeParsers {

    /**
     * Flexible local-time parser accepting {@code HH:mm}, {@code HH:mm:ss}, and
     * {@code HH:mm:ss.fraction}.
     */
    public static final DateTimeFormatter FLEXIBLE_LOCAL_TIME_PARSER_COLON =
            new DateTimeFormatterBuilder().appendPattern("HH:mm").optionalStart()
                    .appendPattern(":ss").optionalEnd().optionalStart()
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd()
                    .toFormatter();

    /**
     * Flexible local-time parser accepting {@code HHmm}, {@code HHmmss}, and
     * {@code HHmmss.fraction} (no colon separators).
     */
    public static final DateTimeFormatter FLEXIBLE_LOCAL_TIME_PARSER_NO_COLON =
            new DateTimeFormatterBuilder().appendPattern("HHmm").optionalStart().appendPattern("ss")
                    .optionalEnd().optionalStart()
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd()
                    .toFormatter();

    /**
     * Date-only formatters tried in order.
     * <ol>
     * <li>{@code yyyy-MM-dd} (ISO)</li>
     * <li>{@code yyyy/MM/dd}</li>
     * <li>{@code yyyyMMdd} (basic ISO)</li>
     * <li>{@code yyyy.MM.dd}</li>
     * <li>{@code yyyy年M月d日} (Japanese)</li>
     * </ol>
     */
    public static final DateTimeFormatter[] DATE_ONLY_FORMATTERS =
            {DateTimeFormatter.ISO_LOCAL_DATE, DateTimeFormatter.ofPattern("yyyy/MM/dd"),
                    DateTimeFormatter.BASIC_ISO_DATE, DateTimeFormatter.ofPattern("yyyy.MM.dd"),
                    DateTimeFormatter.ofPattern("yyyy年M月d日", Locale.JAPANESE)};

    /**
     * Prevents instantiation of this utility class.
     */
    @Generated
    private FlexibleDateTimeParsers() {}
}
