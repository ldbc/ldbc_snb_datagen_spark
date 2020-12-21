package ldbc.snb.datagen.generator.generators;

import ldbc.snb.datagen.entities.dynamic.messages.Comment;
import ldbc.snb.datagen.entities.dynamic.messages.Photo;
import ldbc.snb.datagen.entities.dynamic.messages.Post;
import ldbc.snb.datagen.entities.dynamic.relations.Like;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.util.List;

public class GenActivity {
    public final GenWall<Triplet<Post, List<Like>, List<Pair<Comment, List<Like>>>>> genWall;
    public final List<GenWall<Triplet<Post, List<Like>, List<Pair<Comment, List<Like>>>>>> genGroups;
    public final GenWall<Pair<Photo, List<Like>>> genAlbums;

    public GenActivity(
            GenWall<Triplet<Post, List<Like>, List<Pair<Comment, List<Like>>>>> genWall,
            List<GenWall<Triplet<Post, List<Like>, List<Pair<Comment, List<Like>>>>>> genGroups,
            GenWall<Pair<Photo, List<Like>>> genAlbums
    ) {
        this.genWall = genWall;
        this.genGroups = genGroups;
        this.genAlbums = genAlbums;
    }
}
