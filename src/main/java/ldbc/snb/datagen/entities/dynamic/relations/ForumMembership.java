package ldbc.snb.datagen.entities.dynamic.relations;

import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.entities.dynamic.DynamicActivity;
import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.person.PersonSummary;

/**
 * This class represents a hasMember edge between a Person and a Forum
 */
public class ForumMembership implements DynamicActivity {

    private boolean isExplicitlyDeleted;
    private long forumId;
    private long creationDate;
    private long deletionDate;
    private PersonSummary person;
    private Forum.ForumType forumType;

    public ForumMembership(long forumId, long creationDate, long deletionDate, PersonSummary p, Forum.ForumType forumType, boolean isExplicitlyDeleted) {
        assert (p.getCreationDate() + DatagenParams.delta) <= creationDate : "Person creation date is larger than membership";
        this.forumId = forumId;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.forumType = forumType;
        person = new PersonSummary(p);
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public boolean isExplicitlyDeleted() {
        return isExplicitlyDeleted;
    }

    public void setExplicitlyDeleted(boolean explicitlyDeleted) {
        isExplicitlyDeleted = explicitlyDeleted;
    }

    public long getForumId() {
        return forumId;
    }

    public void setForumId(long forumId) {
        this.forumId = forumId;
    }

    public long getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(long creationDate) {
        this.creationDate = creationDate;
    }

    public long getDeletionDate() {
        return deletionDate;
    }

    public void setDeletionDate(long deletionDate) {
        this.deletionDate = deletionDate;
    }

    public PersonSummary getPerson() {
        return person;
    }

    public void setPerson(PersonSummary p) {
        person = p;
    }

    public Forum.ForumType getForumType() {
        return forumType;
    }

    public void setForumType(Forum.ForumType forumType) {
        this.forumType = forumType;
    }
}
