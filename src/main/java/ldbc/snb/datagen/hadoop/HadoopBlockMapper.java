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
public class HadoopBlockMapper extends Mapper<LongWritable, Person, ComposedKey, Person> {
    int mapId;

    @Override
    public void setup(Mapper.Context context) {
        Configuration conf = context.getConfiguration();
        DatagenParams.readConf(conf);
        DatagenParams.readParameters("/params.ini");
        mapId = context.getTaskAttemptID().getId();
    }

    @Override
    public void map(LongWritable key, Person value, Mapper.Context context)
            throws IOException, InterruptedException {
        context.write(new ComposedKey(key.get() / DatagenParams.blockSize, key.get()), value);
    }
}
