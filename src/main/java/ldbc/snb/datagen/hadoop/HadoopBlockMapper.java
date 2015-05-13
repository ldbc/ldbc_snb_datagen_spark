package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.objects.Person;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by aprat on 11/17/14.
 */
public class HadoopBlockMapper extends Mapper<LongWritable, Person, BlockKey, Person> {
    int mapId;
    int blockSize = 0;

    @Override
    public void setup(Mapper.Context context) {
        Configuration conf = context.getConfiguration();
        mapId = context.getTaskAttemptID().getId();
        blockSize = conf.getInt("ldbc.snb.datagen.generator.blockSize", 10000);
    }

    @Override
    public void map(LongWritable key, Person value, Mapper.Context context)
            throws IOException, InterruptedException {
        context.write(new BlockKey(key.get() / blockSize, new TupleKey(key.get(),value.accountId())), value);
    }
}
