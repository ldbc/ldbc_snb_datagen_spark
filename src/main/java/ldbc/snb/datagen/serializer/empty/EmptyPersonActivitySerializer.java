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

     protected void serialize( Forum forum ) {

     }

     protected void serialize( Post post ) {

     }

     protected void serialize( Comment comment ) {

     }

     protected void serialize( Photo photo ) {

     }

     protected void serialize( ForumMembership membership ) {

     }

     protected void serialize( Like like ) {

     }

     public void reset() {

     }

}
