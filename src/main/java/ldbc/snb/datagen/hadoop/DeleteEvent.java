package ldbc.snb.datagen.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This class represents a delete event
 */
public class DeleteEvent implements Writable {

    public enum DeleteEventType {
        REMOVE_PERSON(8),
        REMOVE_LIKE_POST(9),
        REMOVE_LIKE_COMMENT(10),
        REMOVE_FORUM(11),
        REMOVE_FORUM_MEMBERSHIP(12),
        REMOVE_POST(13),
        REMOVE_COMMENT(14),
        REMOVE_FRIENDSHIP(15),
        NO_EVENT(100);

        private final int type;

        DeleteEventType(int type) {
            this.type = type;
        }

        public int getType() {
            return type;
        }
    }

    private long eventDate;
    private long dependantOnDate;
    private String eventData;
    private DeleteEventType deleteEventType;

    public DeleteEvent(long eventDate, long dependantOnDate,  DeleteEventType deleteEventType,String eventData) {
        this.eventDate = eventDate;
        this.dependantOnDate = dependantOnDate;
        this.eventData = eventData;
        this.deleteEventType = deleteEventType;
    }

    public long getEventDate() {
        return eventDate;
    }

    public long getDependantOnDate() {
        return dependantOnDate;
    }

    public String getEventData() {
        return eventData;
    }

    public DeleteEventType getDeleteEventType() {
        return deleteEventType;
    }

    public void setEventDate(long eventDate) {
        this.eventDate = eventDate;
    }

    public void setDependantOnDate(long dependantOnDate) {
        this.dependantOnDate = dependantOnDate;
    }

    public void setEventData(String eventData) {
        this.eventData = eventData;
    }

    public void setDeleteEventType(DeleteEventType deleteEventType) {
        this.deleteEventType = deleteEventType;
    }

    public void readFields(DataInput arg0) throws IOException {
        this.eventDate = arg0.readLong();
        this.dependantOnDate = arg0.readLong();
        this.deleteEventType = DeleteEventType.values()[arg0.readInt()];
        this.eventData = arg0.readUTF();
    }

    public void write(DataOutput arg0) throws IOException {
        arg0.writeLong(eventDate);
        arg0.writeLong(dependantOnDate);
        arg0.writeInt(deleteEventType.getType());
        arg0.writeUTF(eventData);
    }
}
