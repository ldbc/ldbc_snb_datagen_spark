package ldbc.snb.datagen.serializer;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.objects.Organization;
import ldbc.snb.datagen.objects.Place;
import ldbc.snb.datagen.objects.Tag;
import ldbc.snb.datagen.objects.TagClass;
import org.apache.hadoop.conf.Configuration;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by aprat on 12/17/14.
 */
abstract public class InvariantSerializer {

    public InvariantSerializer() {
    }


    abstract public void reset();

    public void export(final TagClass tagclass) {
        serialize(tagclass);
    }

    public void export(final Place place) {
        serialize(place);
    }

    public void export(final Organization organization) {
       serialize(organization);
    }

    public void export(final Tag tag) {
        serialize(tag);
    }

    abstract public void initialize(Configuration conf, int reducerId);

    abstract public void close();

    abstract protected void serialize(final Place place);

    abstract protected void serialize(final Organization organization);

    abstract protected void serialize(final TagClass tagClass);

    abstract protected void serialize(final Tag tag);
}
