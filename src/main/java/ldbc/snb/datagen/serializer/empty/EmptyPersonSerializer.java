package ldbc.snb.datagen.serializer.empty;

import ldbc.snb.datagen.objects.Knows;
import ldbc.snb.datagen.objects.Person;
import ldbc.snb.datagen.objects.StudyAt;
import ldbc.snb.datagen.objects.WorkAt;
import ldbc.snb.datagen.serializer.PersonSerializer;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Created by aprat on 30/01/15.
 */
public class EmptyPersonSerializer extends PersonSerializer {

    @Override
    public void initialize(Configuration conf, int reducerId) throws IOException {
        //Intentionally left empty
    }

    @Override
    public void close() {
        //Intentionally left empty
    }

    @Override
    protected void serialize(final Person p) {
        //Intentionally left empty
    }

    @Override
    protected void serialize(final StudyAt studyAt) {
        //Intentionally left empty
    }

    @Override
    protected void serialize(final WorkAt workAt) {
        //Intentionally left empty
    }

    @Override
    protected void serialize(final Person p,final  Knows knows) {
        //Intentionally left empty
    }

    @Override
    public void reset() {
        //Intentionally left empty
    }

}
