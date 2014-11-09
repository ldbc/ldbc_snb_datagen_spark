package ldbc.snb.datagen.hadoop;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by aprat on 11/9/14.
 */
public class ComposedKeyComparator extends WritableComparator {

    protected ComposedKeyComparator() {
        super(ComposedKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //return a.compareTo(b);
        ComposedKey keyA = (ComposedKey)a;
        ComposedKey keyB = (ComposedKey)b;
        if (keyA.block < keyB.block) return -1;
        if (keyA.block > keyB.block) return 1;
        if (keyA.key < keyB.key) return -1;
        if (keyA.key > keyB.key) return 1;
        return 0;
    }
}
