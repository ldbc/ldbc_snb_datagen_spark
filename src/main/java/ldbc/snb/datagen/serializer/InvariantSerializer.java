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



    public void export(TagClass tagclass) {
        serialize(tagclass);
    }

    public void export(Place place) {
        serialize(place);
    }

    public void export(Organization organization) {
       serialize(organization);
    }

    public void export(Tag tag) {
        serialize(tag);
    }

    abstract public void initialize(Configuration conf, int reducerId);

    abstract public void close();

    abstract protected void serialize(Place place);

    abstract protected void serialize(Organization organization);

    abstract protected void serialize(TagClass tagClass);

    abstract protected void serialize(Tag tag);
}
