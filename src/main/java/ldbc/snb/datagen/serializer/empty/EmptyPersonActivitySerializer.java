package ldbc.snb.datagen.serializer.empty;

import ldbc.snb.datagen.objects.*;
import ldbc.snb.datagen.serializer.PersonActivitySerializer;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Created by aprat on 30/01/15.
 */
public class EmptyPersonActivitySerializer extends PersonActivitySerializer {

    @Override
    public void initialize(Configuration conf, int reducerId) throws IOException {
        //This is left intentionally blank
    }

    @Override
    public void close() {
        //This is left intentionally blank
    }

    @Override
    protected void serialize(final  Forum forum ) {
        //This is left intentionally blank
    }

    @Override
    protected void serialize(final  Post post ) {
        //This is left intentionally blank
    }

    @Override
    protected void serialize(final  Comment comment ) {
        //This is left intentionally blank
    }

    @Override
    protected void serialize(final  Photo photo ) {
        //This is left intentionally blank
    }

    @Override
    protected void serialize(final  ForumMembership membership ) {
        //This is left intentionally blank
    }

    @Override
    protected void serialize(final  Like like ) {
        //This is left intentionally blank
    }

    @Override
    public void reset() {
        //This is left intentionally blank
    }
}
