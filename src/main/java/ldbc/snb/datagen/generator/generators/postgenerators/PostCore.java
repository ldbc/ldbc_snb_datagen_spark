package ldbc.snb.datagen.generator.generators.postgenerators;

import java.util.TreeSet;

class PostCore {

    private TreeSet<Integer> tags;
    private long creationDate;
    private long deletionDate;

    PostCore() {
        this.tags = new TreeSet<>();
    }

    public TreeSet<Integer> getTags() {
        return tags;
    }

    public void setTags(TreeSet<Integer> tags) {
        this.tags = tags;
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
}
