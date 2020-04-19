package ldbc.snb.datagen.generator.generators;

import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import org.javatuples.Triplet;

import java.util.Iterator;

public class GenWall<T> implements Iterator<Triplet<
        Forum,
        Iterator<ForumMembership>,
        Iterator<T>
        >> {
    public Iterator<Triplet<
                Forum,
                Iterator<ForumMembership>,
                Iterator<T>
                >> inner;

    public GenWall(Iterator<Triplet<
            Forum,
            Iterator<ForumMembership>,
            Iterator<T>
            >> inner) {
        this.inner = inner;
    }

    @Override
    public boolean hasNext() {
        return inner.hasNext();
    }

    @Override
    public Triplet<Forum, Iterator<ForumMembership>, Iterator<T>> next() {
        return inner.next();
    }
}
