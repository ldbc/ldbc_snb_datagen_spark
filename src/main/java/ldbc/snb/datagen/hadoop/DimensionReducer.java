package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.generator.ScalableGenerator;
import ldbc.snb.datagen.objects.ReducedUserProfile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by aprat on 8/8/14.
 */
public class DimensionReducer extends Reducer<MapReduceKey, ReducedUserProfile, MapReduceKey, ReducedUserProfile> {

    public static ScalableGenerator friendGenerator;
    private int attempTaskId;
    private int dimension;
    private int pass;

    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        dimension = Integer.parseInt(conf.get("dimension"));
        pass = Integer.parseInt(conf.get("pass"));

        String strTaskId = context.getTaskAttemptID().getTaskID().toString();
        attempTaskId = Integer.parseInt(strTaskId.substring(strTaskId.length() - 3));
        friendGenerator = new ScalableGenerator(attempTaskId, conf);
        friendGenerator.init();
    }

    @Override
    public void reduce(MapReduceKey key, Iterable<ReducedUserProfile> valueSet,
                       Context context) throws IOException, InterruptedException {
        friendGenerator.resetState(key.block);
        int counter = 0;
        System.out.println("Start University group: " + key.block);
        for (ReducedUserProfile user : valueSet) {
            //                 System.out.println(user.getAccountId());
            friendGenerator.pushUserProfile(user, pass, dimension, context);
            counter++;
        }
        friendGenerator.pushAllRemainingUser(pass, dimension, context);
        System.out.println("End group with size: " + counter);
    }

    @Override
    protected void cleanup(Context context) {
        System.out.println("Summary for reducer" + attempTaskId);
        System.out.println("Number of user profile read " + friendGenerator.totalNumUserProfilesRead);
        System.out.println("Number of exact user profile out " + friendGenerator.exactOutput);
        System.out.println("Number of exact friend added " + friendGenerator.friendshipNum);
    }
}

