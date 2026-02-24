package io.github.yok.flexdblink.util;

import io.github.yok.flexdblink.config.CsvDateTimeFormatProperties;
import java.sql.Connection;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Utility for normalizing JDBC date/time/interval values into CSV-friendly strings.
 *
 * <p>
 * This class formats standard JDBC temporal types ({@link java.sql.Date}, {@link java.sql.Time},
 * {@link java.sql.Timestamp}) and, when present, vendor-specific temporal objects obtained through
 * JDBC drivers. It produces stable, locale-independent strings suitable for deterministic CSV
 * exports.
 * </p>
 *
 * <p>
 * Formatting patterns are provided via {@link CsvDateTimeFormatProperties} at construction time.
 * The class is stateless and thread-safe once constructed.
 * </p>
 *
 * <h3>Special handling</h3>
 * <ul>
 * <li><strong>INTERVAL YEAR TO MONTH</strong>: normalized to {@code [+|-]YY-MM} (zero-padded).</li>
 * <li><strong>INTERVAL DAY TO SECOND</strong>: normalized to {@code [+|-]DD HH:MM:SS} (zero-padded;
 * fractional seconds dropped).</li>
 * <li>Trailing redundant fractional part like {@code .0} is removed where appropriate.</li>
 * </ul>
 *
 * <p>
 * Column-specific tweaks are triggered by column names. For example, {@code TIMESTAMP_LTZ_COL}
 * drops timezone/region tails, while {@code TIMESTAMP_COL} and {@code DATE_COL} drop redundant
 * trailing {@code .0}. These names are illustrative and can be adapted to your schema conventions.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 * @since 1.0
 */
@Slf4j
@Component
public class DateTimeFormatUtil implements DateTimeFormatSupport {

    // Date format (e.g. {@code yyyy-MM-dd})
    private final DateTimeFormatter dateFormatter;
    // Time format (e.g. {@code HH:mm:ss})
    private final DateTimeFormatter timeFormatter;
    // Timestamp format without milliseconds (e.g. {@code yyyy-MM-dd HH:mm:ss})
    private final DateTimeFormatter dateTimeFormatter;
    // Timestamp format with milliseconds (e.g. {@code yyyy-MM-dd HH:mm:ss.SSS})
    private final DateTimeFormatter dateTimeMillisFormatter;

    /**
     * Creates a new formatter using the provided CSV date/time patterns.
     *
     * @param props format patterns for date, time, timestamp(with/without millis)
     * @throws IllegalArgumentException if any pattern is invalid
     */
    public DateTimeFormatUtil(CsvDateTimeFormatProperties props) {
        this.dateFormatter = DateTimeFormatter.ofPattern(props.getDate());
        this.timeFormatter = DateTimeFormatter.ofPattern(props.getTime());
        this.dateTimeFormatter = DateTimeFormatter.ofPattern(props.getDateTime());
        this.dateTimeMillisFormatter = DateTimeFormatter.ofPattern(props.getDateTimeWithMillis());
    }

    /**
     * Formats a JDBC value into a CSV-friendly string using column-aware normalization.
     *
     * <p>
     * Behaviors:
     * </p>
     * <ul>
     * <li>{@link Timestamp}: formatted with {@link #dateTimeMillisFormatter}.</li>
     * <li>{@link Date}: formatted with {@link #dateFormatter}.</li>
     * <li>{@link Time}: formatted with {@link #timeFormatter}.</li>
     * <li>{@link String}: returned as is (then subject to column-specific cleanup).</li>
     * <li>Other objects: {@code toString()} fallback.</li>
     * </ul>
     *
     * <p>
     * Column-specific post-processing:
     * </p>
     * <ul>
     * <li><code>TIMESTAMP_LTZ_COL</code>: drop region/zone tail and colon in offset; remove
     * trailing <code>.0</code>.</li>
     * <li><code>TIMESTAMP_COL</code>, <code>DATE_COL</code>: remove trailing <code>.0</code>.</li>
     * <li><code>INTERVAL_YM_COL</code>: normalize using {@link #normalizeIntervalYm(String)}.</li>
     * <li><code>INTERVAL_DS_COL</code>: normalize using {@link #normalizeIntervalDs(String)}.</li>
     * </ul>
     *
     * @param colName column name (case-insensitive; used for special handling)
     * @param value JDBC temporal value
     * @param conn JDBC connection (reserved for future extension)
     * @return normalized string for CSV output, or {@code null} if {@code value} is {@code null}
     */
    public String formatJdbcDateTime(String colName, Object value, Connection conn) {
        if (value == null) {
            return null;
        }

        String normalized;
        try {
            if (value instanceof Timestamp) {
                normalized = ((Timestamp) value).toLocalDateTime().format(dateTimeMillisFormatter);

            } else if (value instanceof Date) {
                normalized = ((Date) value).toLocalDate().format(dateFormatter);

            } else if (value instanceof Time) {
                normalized = ((Time) value).toLocalTime().format(timeFormatter);

            } else if (value instanceof String) {
                normalized = (String) value;

            } else {
                normalized = value.toString();
            }

            // Column-specific cleanup/normalization
            switch (colName.toUpperCase()) {
                case "TIMESTAMP_LTZ_COL":
                    normalized = normalized.replaceAll(" [A-Za-z0-9/_+\\-]+$", "")
                            .replaceAll("([+-]\\d{2}):(\\d{2})$", "") // remove colon in +HH:MM
                            .replaceAll("\\.0+$", ""); // trim redundant .0
                    break;
                case "TIMESTAMP_COL":
                case "DATE_COL":
                    normalized = normalized.replaceAll("\\.0+$", "");
                    break;
                case "INTERVAL_YM_COL":
                    normalized = normalizeIntervalYm(normalized);
                    break;
                case "INTERVAL_DS_COL":
                    normalized = normalizeIntervalDs(normalized);
                    break;
                default:
                    // e.g. TIMESTAMP_TZ_COL — keep as produced above
                    break;
            }

            return normalized;

        } catch (Exception ex) {
            log.warn("Failed to normalize temporal value: colName={}, value={}", colName, value,
                    ex);
            return value.toString();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LocalDate parseConfiguredDate(String value) {
        try {
            return LocalDate.parse(value, dateFormatter);
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LocalTime parseConfiguredTime(String value) {
        try {
            return LocalTime.parse(value, timeFormatter);
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LocalDateTime parseConfiguredTimestamp(String value) {
        try {
            return LocalDateTime.parse(value, dateTimeMillisFormatter);
        } catch (DateTimeParseException e) {
            // fall through to dateTime format
        }
        try {
            return LocalDateTime.parse(value, dateTimeFormatter);
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    /**
     * Normalizes INTERVAL YEAR TO MONTH to {@code [+|-]YY-MM} with zero-padding.
     *
     * @param input raw interval string (e.g., {@code "1-6"}, {@code "-2-03"})
     * @return normalized interval string; returns {@code input} unchanged if it doesn't match the
     *         pattern
     */
    private String normalizeIntervalYm(String input) {
        String s = input.replaceAll("[\\s\u3000]+", "");
        Matcher m = Pattern.compile("^(-?)(\\d+)-(\\d+)$").matcher(s);
        if (m.matches()) {
            String sign = m.group(1).isEmpty() ? "+" : "-";
            int years = Integer.parseInt(m.group(2));
            int months = Integer.parseInt(m.group(3));
            return String.format("%s%02d-%02d", sign, years, months);
        }
        return input;
    }

    /**
     * Normalizes INTERVAL DAY TO SECOND to {@code [+|-]DD HH:MM:SS} with zero-padding and without
     * fractional seconds.
     *
     * @param input raw interval string (e.g., {@code "0 5:0:0.0"}, {@code "-2 10:02:03"})
     * @return normalized interval string; returns {@code input} unchanged if it doesn't match the
     *         pattern
     */
    private String normalizeIntervalDs(String input) {
        String s = input.replaceAll("\\.\\d+$", "").replaceAll("[\\s\u3000]+", " ");
        Matcher m = Pattern.compile("^(-?)(\\d+) (\\d+):(\\d+):(\\d+)$").matcher(s);
        if (m.matches()) {
            String sign = m.group(1).isEmpty() ? "+" : "-";
            int days = Integer.parseInt(m.group(2));
            int hours = Integer.parseInt(m.group(3));
            int minutes = Integer.parseInt(m.group(4));
            int seconds = Integer.parseInt(m.group(5));
            return String.format("%s%02d %02d:%02d:%02d", sign, days, hours, minutes, seconds);
        }
        return input;
    }
}
