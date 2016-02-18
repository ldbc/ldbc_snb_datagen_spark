package ldbc.snb.datagen.serializer.formatter;

import org.apache.hadoop.conf.Configuration;

/**
 * Created by aprat on 14/01/16.
 */

public interface DateFormatter {
    public void initialize(Configuration config);
    public String formatDate(long date);
    public String formatDateTime(long date);
}
