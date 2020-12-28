package ldbc.snb.datagen.generator.generators;

import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import org.javatuples.Triplet;

import java.io.Serializable;
import java.util.List;

public class GenWall<T> implements Serializable {
    public List<Triplet<Forum, List<ForumMembership>, List<T>>> inner;

    public GenWall(List<Triplet<
            Forum,
            List<ForumMembership>,
            List<T>
            >> inner) {
        this.inner = inner;
    }
}
