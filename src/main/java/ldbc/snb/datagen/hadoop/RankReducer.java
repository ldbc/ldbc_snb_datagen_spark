package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.generator.ScalableGenerator;
import ldbc.snb.datagen.objects.ReducedUserProfile;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by aprat on 8/8/14.
 */
public class RankReducer extends Reducer<MapReduceKey, ReducedUserProfile, MapReduceKey, ReducedUserProfile> {

    @Override
    protected void setup(Context context) {
    }

    @Override
    public void reduce(MapReduceKey key, Iterable<ReducedUserProfile> valueSet,
                       Context context) throws IOException, InterruptedException {

        int numUser = 0;
        int blockSize = ScalableGenerator.blockSize;
        for (ReducedUserProfile user : valueSet) {
            context.write(new MapReduceKey(numUser / blockSize, key.key, key.id), user);
            numUser++;
        }
    }
}

