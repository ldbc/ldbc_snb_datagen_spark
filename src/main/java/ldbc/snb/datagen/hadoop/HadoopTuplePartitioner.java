package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.objects.Person;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by aprat on 25/08/15.
 */
public class HadoopTuplePartitioner extends Partitioner<TupleKey, Person> {

    public HadoopTuplePartitioner() {
        super();
    }

    @Override
    public int getPartition(TupleKey key, Person person, int numReduceTasks) {
        return (int)(key.key % numReduceTasks);
    }
}
