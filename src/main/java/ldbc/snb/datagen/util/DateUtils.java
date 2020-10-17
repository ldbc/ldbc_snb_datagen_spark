package ldbc.snb.datagen.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;

public class DateUtils {
    public static ZoneId UTC = ZoneId.of("UTC");

    public static long toEpochMilli(LocalDate ld) {
        return ld.atStartOfDay(UTC).toInstant().toEpochMilli();
    }

    public static LocalDate utcDateOfEpochMilli(long epochMilli) {
        return Instant.ofEpochMilli(epochMilli).atZone(UTC).toLocalDate();
    }

    public static String formatYear(long epochMilli) {
        LocalDate date = utcDateOfEpochMilli(epochMilli);
        return Integer.toString(date.getYear());
    }

    public static boolean isTravelSeason(long epochMilli) {
        LocalDate date = utcDateOfEpochMilli(epochMilli);

        int day = date.getDayOfMonth();
        int month = date.getMonthValue();

        if ((month > 4) && (month < 7)) {
            return true;
        }
        return ((month == 11) && (day > 23));
    }

    public static int getNumberOfMonths(long epochMilli, int startMonth, int startYear) {
        LocalDate date = utcDateOfEpochMilli(epochMilli);
        int month = date.getMonthValue();
        int year = date.getYear();
        return (year - startYear) * 12 + month - startMonth;
    }

    public static int getYear(long epochMilli) {
        LocalDate date = utcDateOfEpochMilli(epochMilli);
        return date.getYear();
    }

    public static Month getMonth(long epochMilli) {
        LocalDate date = utcDateOfEpochMilli(epochMilli);
        return date.getMonth();
    }
}
