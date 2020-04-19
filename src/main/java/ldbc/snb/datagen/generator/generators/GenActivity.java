package ldbc.snb.datagen.generator.generators;

import ldbc.snb.datagen.entities.dynamic.messages.Comment;
import ldbc.snb.datagen.entities.dynamic.messages.Photo;
import ldbc.snb.datagen.entities.dynamic.messages.Post;
import ldbc.snb.datagen.entities.dynamic.relations.Like;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.util.Iterator;

public class GenActivity {
    public final GenWall<Triplet<Post, Iterator<Like>, Iterator<Pair<Comment, Iterator<Like>>>>> genWall;
    public final Iterator<GenWall<Triplet<Post, Iterator<Like>, Iterator<Pair<Comment, Iterator<Like>>>>>> genGroups;
    public final GenWall<Pair<Photo, Iterator<Like>>> genAlbums;

    public GenActivity(
            GenWall<Triplet<Post, Iterator<Like>, Iterator<Pair<Comment, Iterator<Like>>>>> genWall,
            Iterator<GenWall<Triplet<Post, Iterator<Like>, Iterator<Pair<Comment, Iterator<Like>>>>>> genGroups,
            GenWall<Pair<Photo, Iterator<Like>>> genAlbums
    ) {
        this.genWall = genWall;
        this.genGroups = genGroups;
        this.genAlbums = genAlbums;
    }
}
