package ldbc.socialnet.dbgen.util;

import ldbc.socialnet.dbgen.objects.ReducedUserProfile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by aprat on 11/17/14.
 */
public class HadoopBlockMapper extends Mapper<LongWritable, ReducedUserProfile, ComposedKey, ReducedUserProfile> {
    int mapId;

    @Override
    public void setup(Mapper.Context context) {
        Configuration conf = context.getConfiguration();
        mapId = context.getTaskAttemptID().getId();
    }

    @Override
    public void map(LongWritable key, ReducedUserProfile value, Mapper.Context context)
            throws IOException, InterruptedException {
        context.write(new ComposedKey(key.get() / 10000, key.get(),value.getAccountId()), value);
    }
}
