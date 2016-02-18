package ldbc.snb.datagen.serializer.formatter;

import org.apache.hadoop.conf.Configuration;
import sun.util.calendar.Gregorian;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * Created by aprat on 14/01/16.
 */
public class StringDateFormatter implements DateFormatter{

    private String formatDateTimeString_ = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    private String formatDateString_ = "yyyy-MM-dd";

    private SimpleDateFormat gmtDateTimeFormatter_;
    private SimpleDateFormat gmtDateFormatter_;
    private Date date_;
    public void initialize(Configuration conf) {

        formatDateTimeString_ = conf.get("ldbc.snb.datagen.serializer.formatter.StringDateFormatter.dateTimeFormat", formatDateTimeString_);
        gmtDateTimeFormatter_ = new SimpleDateFormat(formatDateTimeString_);
        gmtDateTimeFormatter_.setTimeZone(TimeZone.getTimeZone("GMT"));
        formatDateString_ = conf.get("ldbc.snb.datagen.serializer.formatter.StringDateFormatter.dateFormat", formatDateString_);
        gmtDateFormatter_ = new SimpleDateFormat(formatDateString_);
        gmtDateFormatter_.setTimeZone(TimeZone.getTimeZone("GMT"));
        date_ = new Date();
    }

    public String formatDateTime(long date) {
        date_.setTime(date);
        return gmtDateTimeFormatter_.format(date_);
    }

    public String formatDate(long date) {
        date_.setTime(date);
        return gmtDateFormatter_.format(date_);
    }

}
