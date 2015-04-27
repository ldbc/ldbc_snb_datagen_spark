package ldbc.snb.datagen.hadoop;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by aprat on 11/17/14.
 */

public class BlockKeyGroupComparator extends WritableComparator {

    protected BlockKeyGroupComparator() {
        super(BlockKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //return a.compareTo(b);
        BlockKey keyA = (BlockKey)a;
        BlockKey keyB = (BlockKey)b;
        if (keyA.block < keyB.block) return -1;
        if (keyA.block > keyB.block) return 1;
        return 0;
    }
}
