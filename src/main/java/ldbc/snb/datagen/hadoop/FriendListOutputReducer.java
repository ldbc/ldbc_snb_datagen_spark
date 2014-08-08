package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.objects.ReducedUserProfile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeSet;

/**
 * Created by aprat on 8/8/14.
 */
public class FriendListOutputReducer extends Reducer<MapReduceKey, ReducedUserProfile, MapReduceKey, ReducedUserProfile> {
    FSDataOutputStream out;
    private int numUsers;

    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        numUsers = 0;
        try {
            FileSystem fs = FileSystem.get(conf);
            String strTaskId = context.getTaskAttemptID().getTaskID().toString();
            int attempTaskId = Integer.parseInt(strTaskId.substring(strTaskId.length() - 3));
            Path outFile = new Path(context.getConfiguration().get("outputDir") + "/social_network/m0friendList" + attempTaskId + ".csv");
            out = fs.create(outFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void reduce(MapReduceKey key, Iterable<ReducedUserProfile> valueSet,
                       Context context) throws IOException, InterruptedException {
        for (ReducedUserProfile user : valueSet) {
            numUsers++;
            StringBuffer strbuf = new StringBuffer();
            strbuf.append(user.getAccountId());
            TreeSet<Long> ids = user.getFriendIds();
            for (Long id : ids) {
                strbuf.append(",");
                strbuf.append(id);
            }
            strbuf.append("\n");
            out.write(strbuf.toString().getBytes());
        }
    }

    @Override
    protected void cleanup(Context context) {
        try {
            System.out.println("Number of user friends lists reduced " + numUsers);
            out.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
