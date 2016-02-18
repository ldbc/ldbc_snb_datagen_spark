package ldbc.snb.datagen.serializer.empty;

import ldbc.snb.datagen.objects.*;
import ldbc.snb.datagen.serializer.PersonActivitySerializer;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by aprat on 30/01/15.
 */
public class EmptyPersonActivitySerializer extends PersonActivitySerializer {

     public void initialize(Configuration conf, int reducerId) {

     }

     public void close() {

     }

     protected void serialize(final  Forum forum ) {

     }

     protected void serialize(final  Post post ) {

     }

     protected void serialize(final  Comment comment ) {

     }

     protected void serialize(final  Photo photo ) {

     }

     protected void serialize(final  ForumMembership membership ) {

     }

     protected void serialize(final  Like like ) {

     }

     public void reset() {

     }

}
