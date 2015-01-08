package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.serializer.InvariantSerializer;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by aprat on 12/17/14.
 */
public class HadoopInvariantSerializer {

    private InvariantSerializer invariantSerializer_;

    private Configuration conf;

    public HadoopInvariantSerializer( Configuration conf ) {
        this.conf = new Configuration(conf);
    }

    public void run() throws Exception {
        try {
            invariantSerializer_ = (InvariantSerializer) Class.forName(conf.get("ldbc.snb.datagen.serializer.invariantSerializer")).newInstance();
            invariantSerializer_.initialize(conf,0);
        } catch( Exception e ) {
            System.err.println(e.getMessage());
        }
        invariantSerializer_.exportTags();
        invariantSerializer_.exportOrganizations();
        invariantSerializer_.exportPlaces();
        invariantSerializer_.close();
    }
}
