package ldbc.snb.datagen.test.csv;

import java.util.*;

/**
 * Created by aprat on 18/12/15.
 */
public class PairUniquenessCheck extends Check {


    HashMap< String,Set<String>> values = null;

    public PairUniquenessCheck(int columnA, int columnB) {
        super( "Pair Uniqueness Check", (new ArrayList<Integer>()));
        this.getColumns().add(columnA);
        this.getColumns().add(columnB);
        values = new HashMap<String, Set<String>>();
    }

    @Override
    public boolean check(List<String> vals) {
        String valA = vals.get(0);
        String valB = vals.get(1);
        Set<String> others = values.get(valA);
        if(others == null) {
            others = new HashSet<String>();
            others.add(valB);
            values.put(valA,others);
        } else {
            if(others.contains(valB)) {
                System.err.println(valA+" "+valB+" already exists");
                return false;
            }
            others.add(valB);
        }
        return true;
    }
}
