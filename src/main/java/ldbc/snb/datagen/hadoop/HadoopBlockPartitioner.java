package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.objects.Person;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by aprat on 11/17/14.
 */
public class HadoopBlockPartitioner extends Partitioner<ComposedKey, Person> {

    public HadoopBlockPartitioner() {
        super();
    }

    @Override
    public int getPartition(ComposedKey key, Person person, int numReduceTasks) {
        return (int)(key.block % numReduceTasks);
    }
}
