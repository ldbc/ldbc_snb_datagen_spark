package ldbc.snb.datagen.serializer.formatter;

import org.apache.hadoop.conf.Configuration;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * Created by aprat on 14/01/16.
 */
public class LongDateFormatter implements DateFormatter {
    private Date date_;
    private GregorianCalendar calendar_;
    public void initialize(Configuration config) {
        date_ = new Date();
        calendar_ = new GregorianCalendar(TimeZone.getTimeZone("GMT"));

    }

    public String formatDate(long date) {
        date_.setTime(date);
        calendar_.setTime(date_);
        int year = calendar_.get(Calendar.YEAR);
        int month = calendar_.get(Calendar.MONTH);
        int day = calendar_.get(Calendar.DAY_OF_MONTH);
        calendar_.clear();
        calendar_.set(year, month, day,0,0,0);
        return Long.toString(calendar_.getTime().getTime());
    }

    public String formatDateTime(long date) {
        return Long.toString(date);
    }
}
