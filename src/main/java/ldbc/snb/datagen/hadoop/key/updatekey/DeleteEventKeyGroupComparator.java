package ldbc.snb.datagen.hadoop.key.updatekey;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DeleteEventKeyGroupComparator extends WritableComparator {

    protected DeleteEventKeyGroupComparator() {
        super(DeleteEventKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        DeleteEventKey keyA = (DeleteEventKey) a;
        DeleteEventKey keyB = (DeleteEventKey) b;
        if (keyA.reducerId != keyB.reducerId) return keyA.reducerId - keyB.reducerId;
        if (keyA.partition != keyB.partition) return keyA.partition - keyB.partition;
        return 0;
    }
}
