package ldbc.socialnet.dbgen.util;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by aprat on 11/17/14.
 */

public class ComposedKeyGroupComparator extends WritableComparator {

    protected ComposedKeyGroupComparator() {
        super(ComposedKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //return a.compareTo(b);
        ComposedKey keyA = (ComposedKey)a;
        ComposedKey keyB = (ComposedKey)b;
        if (keyA.block < keyB.block) return -1;
        if (keyA.block > keyB.block) return 1;
        return 0;
    }
}
