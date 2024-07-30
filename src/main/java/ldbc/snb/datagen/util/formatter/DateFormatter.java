package ldbc.snb.datagen.util.formatter;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class DateFormatter {
    private DateTimeFormatter gmtDateTimeFormatter;
    private DateTimeFormatter gmtDateFormatter;

    private static ZoneId GMT = ZoneId.of("GMT");

    public DateFormatter() {
        String formatDateTimeString = "yyyy-MM-dd'T'HH:mm:ss.SSS+00:00";
        gmtDateTimeFormatter = DateTimeFormatter.ofPattern(formatDateTimeString).withZone(GMT);
        String formatDateString = "yyyy-MM-dd";
        gmtDateFormatter = DateTimeFormatter.ofPattern(formatDateString).withZone(GMT);
    }

    public String formatDateTime(long date) {
        return gmtDateTimeFormatter.format(Instant.ofEpochMilli(date));
    }

    public String formatDate(long date) {
        return gmtDateFormatter.format(Instant.ofEpochMilli(date));
    }

}
