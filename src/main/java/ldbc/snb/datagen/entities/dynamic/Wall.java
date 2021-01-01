package ldbc.snb.datagen.entities.dynamic;

import ldbc.snb.datagen.entities.Triplet;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;

import java.io.Serializable;
import java.util.List;

public class Wall<T> implements Serializable {
    public List<Triplet<Forum, List<ForumMembership>, List<T>>> getInner() {
        return inner;
    }

    public void setInner(List<Triplet<Forum, List<ForumMembership>, List<T>>> inner) {
        this.inner = inner;
    }

    public List<Triplet<Forum, List<ForumMembership>, List<T>>> inner;

    public Wall(List<Triplet<
            Forum,
            List<ForumMembership>,
            List<T>
            >> inner) {
        this.inner = inner;
    }
}
