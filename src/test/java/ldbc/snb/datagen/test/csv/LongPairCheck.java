package ldbc.snb.datagen.test.csv;

/**
 * Created by aprat on 30/03/16.
 */

public class LongPairCheck extends NumericPairCheck<Long> {

    public LongPairCheck(Parser<Long> parser, String name, Integer columnA, Integer columnB, NumericCheckType type) {
        super(parser, name, columnA, columnB, type);
    }

    @Override
    public boolean greater(Long val1, Long val2) {
        return val1 > val2;
    }

    @Override
    public boolean greaterEqual(Long val1, Long val2) {
        return val1 >= val2;
    }

    @Override
    public boolean less(Long val1, Long val2) {
        return val1 < val2;
    }

    @Override
    public boolean lessEqual(Long val1, Long val2) {
        return val1 <= val2;
    }

    @Override
    public boolean equals(Long val1, Long val2) {
        return val1 == val2;
    }

    @Override
    public boolean nonEquals(Long val1, Long val2) {
        return val1 != val2;
    }
}
