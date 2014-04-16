package ldbc.socialnet.dbgen.objects;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class UpdateEvent implements Serializable, Writable {

    public enum UpdateEventType {
        ADD_PERSON,
        ADD_LIKE_POST,
        ADD_LIKE_COMMENT,
        ADD_FORUM,
        ADD_FORUM_MEMBERSHIP,
        ADD_POST,
        ADD_COMMENT,
        ADD_FRIENDSHIP,
        NO_EVENT
    }

    public long    date;
    public String eventData;
    public UpdateEventType type;

    public UpdateEvent( long date, UpdateEventType type, String eventData) {
        this.date = date;
        this.type = type;
        this.eventData = eventData;
    }

    public void readObject(java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException{
       this.date = stream.readLong();
       this.type = UpdateEventType.values()[stream.readInt()];
       this.eventData = stream.readUTF();
    }

    public void writeObject(java.io.ObjectOutputStream stream)
            throws IOException{
        stream.writeLong(this.date);
        stream.writeInt(type.ordinal());
        stream.writeUTF(eventData);
    }

    public void readFields(DataInput arg0) throws IOException {
        this.date = arg0.readLong();
        this.type = UpdateEventType.values()[arg0.readInt()];
        this.eventData = arg0.readUTF();
    }

    public void write(DataOutput arg0) throws IOException {
        arg0.writeLong(date);
        arg0.writeInt(type.ordinal());
        arg0.writeUTF(eventData);
    }
}
