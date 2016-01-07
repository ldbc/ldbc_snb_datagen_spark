package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.objects.Person;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by aprat on 25/08/15.
 */
public class HadoopUpdateEventKeyPartitioner extends Partitioner<UpdateEventKey, Text> {

    public HadoopUpdateEventKeyPartitioner() {
        super();
    }

    @Override
    public int getPartition(UpdateEventKey key, Text text, int numReduceTasks) {
        return (key.reducerId);
    }
}
