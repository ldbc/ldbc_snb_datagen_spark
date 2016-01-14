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

     public void initialize(Configuration conf, int reducerId) {

     }

     public void close() {

     }

     protected void serialize(final Place place) {

     }

     protected void serialize(final Organization organization) {

     }

     protected void serialize(final TagClass tagClass) {

     }

     protected void serialize(final Tag tag) {

     }

 public void reset() {

 }
}
