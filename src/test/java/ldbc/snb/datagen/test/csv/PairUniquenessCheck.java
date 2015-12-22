package ldbc.snb.datagen.test.csv;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Created by aprat on 18/12/15.
 */
public class PairUniquenessCheck extends Check {


    HashSet<> values = null;

    public PairUniquenessCheck(int columnA, int columnB) {
        super( (new ArrayList<Integer>()));
        this.getColumns().add(columnA);
        values = new HashSet<String>();
    }

    @Override
    public boolean check(List<String> vals) {
        for(String value : vals) {
            if(values.contains(value)) return false;
            values.add(value);
        }
        return true;
    }
}
