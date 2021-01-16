package ldbc.snb.datagen.generator.generators;

import ldbc.snb.datagen.spark.generation.entities.dynamic.Forum;
import ldbc.snb.datagen.spark.generation.entities.dynamic.relations.ForumMembership;
import org.javatuples.Triplet;

import java.util.stream.Stream;

public class GenWall<T> {
    public Stream<Triplet<
                Forum,
                Stream<ForumMembership>,
                Stream<T>
                >> inner;

    public GenWall(Stream<Triplet<
            Forum,
            Stream<ForumMembership>,
            Stream<T>
            >> inner) {
        this.inner = inner;
    }
}
