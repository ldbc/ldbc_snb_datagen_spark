package ldbc.snb.datagen.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by aprat on 25/08/15.
 */
public class HadoopUpdateEventKeyPartitioner extends Partitioner<UpdateEventKey, Text> {

    @Override
    public int getPartition(UpdateEventKey key, Text text, int numReduceTasks) {
        return (key.reducerId);
    }
}

