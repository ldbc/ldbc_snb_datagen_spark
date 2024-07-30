
package ldbc.snb.datagen.entities.dynamic.relations;

import ldbc.snb.datagen.entities.dynamic.DynamicActivity;

public class Like implements DynamicActivity {
    public enum LikeType {
        POST,
        COMMENT,
        PHOTO
    }

    private boolean isExplicitlyDeleted;
    private long person;
    private long personCreationDate;
    private long messageId;
    private long creationDate;
    private long deletionDate;
    private LikeType type;

    public boolean isExplicitlyDeleted() {
        return isExplicitlyDeleted;
    }

    public void setExplicitlyDeleted(boolean explicitlyDeleted) {
        isExplicitlyDeleted = explicitlyDeleted;
    }

    public long getPerson() {
        return person;
    }

    public long getPersonCreationDate() {
        return personCreationDate;
    }

    public long getMessageId() {
        return messageId;
    }

    public long getCreationDate() {
        return creationDate;
    }

    public long getDeletionDate() {
        return deletionDate;
    }

    public LikeType getType() {
        return type;
    }

    public void setPerson(long person) {
        this.person = person;
    }

    public void setPersonCreationDate(long personCreationDate) {
        this.personCreationDate = personCreationDate;
    }

    public void setMessageId(long messageId) {
        this.messageId = messageId;
    }

    public void setCreationDate(long creationDate) {
        this.creationDate = creationDate;
    }

    public void setDeletionDate(long deletionDate) {
        this.deletionDate = deletionDate;
    }

    public void setType(LikeType type) {
        this.type = type;
    }
}
