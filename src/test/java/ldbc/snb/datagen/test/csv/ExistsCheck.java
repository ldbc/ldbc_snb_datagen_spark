package ldbc.snb.datagen.test.csv;

import java.util.List;
import java.util.Set;

/**
 * Created by aprat on 21/12/15.
 */
public class ExistsCheck extends Check {

    protected List<Set<String>> refColumns = null;

    public ExistsCheck(List<Integer> indexes, List<Set<String>> refColumns) {
        super("Exists Check", indexes);
        this.refColumns = refColumns;
    }

    @Override
    public boolean check(List<String> values) {
        for(String val : values) {
            boolean found = false;
            for( Set<String> column : refColumns) {
                if(column.contains(val)) {
                    found = true;
                    break;
                }
            }
            if(!found) return false;
        }
        return true;
    }
}
