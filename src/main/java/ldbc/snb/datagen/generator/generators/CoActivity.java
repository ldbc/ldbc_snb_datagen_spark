package ldbc.snb.datagen.generator.generators;

import ldbc.snb.datagen.entities.dynamic.messages.Comment;
import ldbc.snb.datagen.entities.dynamic.messages.Photo;
import ldbc.snb.datagen.entities.dynamic.messages.Post;
import ldbc.snb.datagen.entities.dynamic.relations.Like;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.util.Iterator;

public class CoActivity {
    public final CoWall<Triplet<Post, Iterator<Like>, Iterator<Pair<Comment, Iterator<Like>>>>> coWall;
    public final Iterator<CoWall<Triplet<Post, Iterator<Like>, Iterator<Pair<Comment, Iterator<Like>>>>>> coGroups;
    public final CoWall<Pair<Photo, Iterator<Like>>> coAlbums;

    public CoActivity(CoWall<Triplet<Post, Iterator<Like>, Iterator<Pair<Comment, Iterator<Like>>>>> coWall, Iterator<CoWall<Triplet<Post, Iterator<Like>, Iterator<Pair<Comment, Iterator<Like>>>>>> coGroups, CoWall<Pair<Photo, Iterator<Like>>> coAlbums) {
        this.coWall = coWall;
        this.coGroups = coGroups;
        this.coAlbums = coAlbums;
    }
}
