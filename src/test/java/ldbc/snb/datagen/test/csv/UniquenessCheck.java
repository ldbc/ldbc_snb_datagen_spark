package ldbc.snb.datagen.test.csv;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Created by aprat on 18/12/15.
 */
public class UniquenessCheck extends Check {

    HashSet<String> values = null;

    public UniquenessCheck(int column) {
        super( "Uniqueness check", (new ArrayList<Integer>()));
        this.getColumns().add(column);
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
