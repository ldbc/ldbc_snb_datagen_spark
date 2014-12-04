package ldbc.socialnet.dbgen.util;

import ldbc.socialnet.dbgen.objects.ReducedUserProfile;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by aprat on 11/17/14.
 */
public class HadoopBlockPartitioner extends Partitioner<ComposedKey, ReducedUserProfile> {

    public HadoopBlockPartitioner() {
        super();
    }

    @Override
    public int getPartition(ComposedKey key, ReducedUserProfile person, int numReduceTasks) {
        return (int)(key.block % numReduceTasks);
    }
}
