package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.generator.DatagenParams;
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

    @Override
    public void setup(Mapper.Context context) {
        Configuration conf = context.getConfiguration();
        DatagenParams.readConf(conf);
        mapId = context.getTaskAttemptID().getId();
    }

    @Override
    public void map(LongWritable key, Person value, Mapper.Context context)
            throws IOException, InterruptedException {
        context.write(new BlockKey(key.get() / DatagenParams.blockSize, new TupleKey(key.get(),value.accountId)), value);
    }
}
