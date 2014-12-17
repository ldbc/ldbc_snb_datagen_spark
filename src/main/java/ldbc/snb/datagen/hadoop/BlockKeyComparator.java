package ldbc.snb.datagen.hadoop;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by aprat on 11/9/14.
 */
public class BlockKeyComparator extends WritableComparator {

    protected BlockKeyComparator() {
        super(BlockKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //return a.compareTo(b);
        BlockKey keyA = (BlockKey)a;
        BlockKey keyB = (BlockKey)b;
        if (keyA.block < keyB.block) return -1;
        if (keyA.block > keyB.block) return 1;
        return keyA.tk.compareTo(keyB.tk);
    }
}
