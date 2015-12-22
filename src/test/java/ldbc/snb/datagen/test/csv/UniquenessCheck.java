package ldbc.snb.datagen.test.csv;

import java.util.List;

/**
 * Created by aprat on 18/12/15.
 */
public class UniquenessCheck<T extends Number > extends Check {



    public UniquenessCheck(List<Integer> columns) {
        super(columns);
    }

    @Override
    public boolean check(List<String> values) {
        T l = T.valueOf(values.get(0));
        return false;
    }
}
