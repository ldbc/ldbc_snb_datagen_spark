package ldbc.snb.datagen.test.csv;

import java.util.*;

/**
 * Created by aprat on 18/12/15.
 */
public class PairUniquenessCheck<T,S> extends Check {


    protected HashMap< T,Set<S>> values = null;
    protected Parser<T> parserA = null;
    protected Parser<S> parserB = null;


    public PairUniquenessCheck(Parser<T> parserA, Parser<S> parserB, int columnA, int columnB) {
        super( "Pair Uniqueness Check", (new ArrayList<Integer>()));
        this.parserA = parserA;
        this.parserB = parserB;
        this.getColumns().add(columnA);
        this.getColumns().add(columnB);
        values = new HashMap<T, Set<S>>();
    }

    @Override
    public boolean check(List<String> vals) {
        T valA = parserA.parse(vals.get(0));
        S valB = parserB.parse(vals.get(1));
        Set<S> others = values.get(valA);
        if(others == null) {
            others = new HashSet<S>();
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
