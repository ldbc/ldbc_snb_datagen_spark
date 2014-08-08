package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.objects.ReducedUserProfile;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by aprat on 8/8/14.
 */
public class ForwardMapper extends Mapper<MapReduceKey, ReducedUserProfile, MapReduceKey, ReducedUserProfile> {
    @Override
    public void map(MapReduceKey key, ReducedUserProfile value,
                    Context context)
            throws IOException, InterruptedException {
        context.write(key, value);
    }
}

