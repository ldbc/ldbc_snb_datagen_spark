package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.generator.ScalableGenerator;
import ldbc.snb.datagen.objects.ReducedUserProfile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by aprat on 8/8/14.
 */

public class UserActivityReducer extends Reducer<MapReduceKey, ReducedUserProfile, MapReduceKey, ReducedUserProfile> {

    public static ScalableGenerator friendGenerator;
    private int attempTaskId;
    private int totalObjects;

    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        String strTaskId = context.getTaskAttemptID().getTaskID().toString();
        attempTaskId = Integer.parseInt(strTaskId.substring(strTaskId.length() - 3));
        friendGenerator = new ScalableGenerator(attempTaskId, conf);
        //friendGenerator.init();
        friendGenerator.openSerializer();
        totalObjects = 0;
    }

    @Override
    public void reduce(MapReduceKey key, Iterable<ReducedUserProfile> valueSet,
                       Context context) throws IOException, InterruptedException {

        friendGenerator.resetState(key.block);
        for (ReducedUserProfile user : valueSet) {
            friendGenerator.generateUserActivity(user, context);
            totalObjects++;
        }
    }

    @Override
    protected void cleanup(Context context) {
        friendGenerator.closeSerializer();
        System.out.println("Number of users serialized by reducer " + attempTaskId + ": " + totalObjects);
    }
}
