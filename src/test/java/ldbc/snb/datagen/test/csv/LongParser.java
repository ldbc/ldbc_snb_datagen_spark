package ldbc.snb.datagen.test.csv;

/**
 * Created by aprat on 23/12/15.
 */
public class LongParser extends Parser<Long> {

    @Override
    public Long parse(String s) {
        return Long.parseLong(s);
    }
}
