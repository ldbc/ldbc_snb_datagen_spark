package ldbc.snb.datagen.serializer;

import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.messages.Comment;
import ldbc.snb.datagen.entities.dynamic.messages.Photo;
import ldbc.snb.datagen.entities.dynamic.messages.Post;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.dynamic.relations.Knows;
import ldbc.snb.datagen.entities.dynamic.relations.Like;

import java.io.IOException;

public interface AbstractDeleteEventSerializer {
    void changePartition();

    void close() throws IOException;

    void export(Person person) throws IOException;

    void export(Person p, Knows k) throws IOException;

    void export(Post post) throws IOException;

    void export(Like like) throws IOException;

    void export(Photo photo) throws IOException;

    void export(Comment comment) throws IOException;

    void export(Forum forum) throws IOException;

    void export(ForumMembership membership) throws IOException;
}
