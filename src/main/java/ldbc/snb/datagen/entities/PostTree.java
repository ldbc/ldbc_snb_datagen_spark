package ldbc.snb.datagen.entities;

import ldbc.snb.datagen.entities.dynamic.messages.Comment;
import ldbc.snb.datagen.entities.dynamic.messages.Post;
import ldbc.snb.datagen.entities.dynamic.relations.Like;

import java.util.List;

public class PostTree extends Triplet<Post, List<Like>, List<Pair<Comment, List<Like>>>> {
    public PostTree(Post value0, List<Like> value1, List<Pair<Comment, List<Like>>> value2) {
        super(value0, value1, value2);
    }
}
