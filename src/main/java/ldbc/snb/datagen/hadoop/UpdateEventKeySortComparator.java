package ldbc.snb.datagen.hadoop;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by aprat on 11/17/14.
 */

public class UpdateEventKeySortComparator extends WritableComparator {

    protected UpdateEventKeySortComparator() {
        super(UpdateEventKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        UpdateEventKey keyA = (UpdateEventKey)a;
        UpdateEventKey keyB = (UpdateEventKey)b;
        if (keyA.reducerId != keyB.reducerId) return keyA.reducerId - keyB.reducerId;
        if (keyA.partition != keyB.partition) return keyA.partition - keyB.partition;
        if( keyA.date < keyB.date) return -1;
        if( keyA.date > keyB.date) return 1;
        return 0;
    }
}
