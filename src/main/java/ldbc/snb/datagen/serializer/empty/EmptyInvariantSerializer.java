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

     protected void serialize(Place place) {

     }

     protected void serialize(Organization organization) {

     }

     protected void serialize(TagClass tagClass) {

     }

     protected void serialize(Tag tag) {

     }

 public void reset() {

 }
}
