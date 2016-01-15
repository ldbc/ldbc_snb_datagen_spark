package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.objects.UpdateEvent;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by aprat on 5/01/16.
 */
public class UpdateEventKey implements WritableComparable<UpdateEventKey> {

    public long date;
    public int reducerId;
    public int partition;

    public UpdateEventKey( ) {
    }

    public UpdateEventKey(UpdateEventKey key) {
        this.date = key.date;
        this.reducerId = key.reducerId;
        this.partition = key.partition;
    }

    public UpdateEventKey(  long date, int reducerId, int partition) {

        this.date = date;
        this.reducerId = reducerId;
        this.partition = partition;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(date);
        out.writeInt(reducerId);
        out.writeInt(partition);
    }

    public void readFields(DataInput in) throws IOException {
        date = in.readLong();
        reducerId = in.readInt();
        partition = in.readInt();
    }

    public int compareTo( UpdateEventKey key) {
        if (reducerId != key.reducerId) return reducerId - key.reducerId;
        if (partition != key.partition) return partition - key.partition;
        if( date < key.date) return -1;
        if( date > key.date) return 1;
        return 0;
    }
}
