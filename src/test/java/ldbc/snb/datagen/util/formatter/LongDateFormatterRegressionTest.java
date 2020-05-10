package ldbc.snb.datagen.util.formatter;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertTrue;

public class LongDateFormatterRegressionTest {
    @Test
    public void testLongDateFormatter() {
        Random rnd = new Random();
        rnd.setSeed(42);

        int n = 100;

        LongDateFormatter ldf = new LongDateFormatter();
        ldf.initialize(null);
        OldLongDateFormatter oldf = new OldLongDateFormatter();
        oldf.initialize(null);

        for (int i = 0; i < n; ++i) {
            float r = rnd.nextFloat();
            long date = (long) (r * 1e13);

            long expected = Long.parseLong(oldf.formatDate(date));
            long actual = Long.parseLong(ldf.formatDate(date));

            assertTrue((expected - 43200000 == actual) || (expected == actual));
        }
    }

}
