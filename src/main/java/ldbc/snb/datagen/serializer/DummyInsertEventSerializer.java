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

// FIXME delete this when refactoring insert event serialization
public class DummyInsertEventSerializer implements AbstractInsertEventSerializer {
    @Override
    public void changePartition() {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void export(Person person) throws IOException {

    }

    @Override
    public void export(Person p, Knows k) throws IOException {

    }

    @Override
    public void export(Post post) throws IOException {

    }

    @Override
    public void export(Like like) throws IOException {

    }

    @Override
    public void export(Photo photo) throws IOException {

    }

    @Override
    public void export(Comment comment) throws IOException {

    }

    @Override
    public void export(Forum forum) throws IOException {

    }

    @Override
    public void export(ForumMembership membership) throws IOException {

    }
}
