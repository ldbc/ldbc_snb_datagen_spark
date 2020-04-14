package ldbc.snb.datagen.hadoop.generator;

import ldbc.snb.datagen.hadoop.key.updatekey.DeleteEventKey;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class HadoopDeleteEventKeyPartitioner extends Partitioner<DeleteEventKey, Text> {
    @Override
    public int getPartition(DeleteEventKey key, Text text, int numReduceTasks) {
        return (key.reducerId);
    }
}




