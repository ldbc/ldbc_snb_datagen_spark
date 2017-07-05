package ldbc.snb.datagen.serializer.empty;

import ldbc.snb.datagen.objects.Organization;
import ldbc.snb.datagen.objects.Place;
import ldbc.snb.datagen.objects.Tag;
import ldbc.snb.datagen.objects.TagClass;
import ldbc.snb.datagen.serializer.InvariantSerializer;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by aprat on 30/01/15.
 */
public class EmptyInvariantSerializer extends InvariantSerializer {

    @Override
    public void initialize(Configuration conf, int reducerId) {
        //Intentionally left empty
    }

    @Override
    public void close() {
        //Intentionally left empty

    }

    @Override
    protected void serialize(final Place place) {
        //Intentionally left empty

    }

    @Override
    protected void serialize(final Organization organization) {
        //Intentionally left empty

    }

    @Override
    protected void serialize(final TagClass tagClass) {
        //Intentionally left empty

    }

    @Override
    protected void serialize(final Tag tag) {
        //Intentionally left empty

    }

    @Override
    public void reset() {
        //Intentionally left empty

    }
}
