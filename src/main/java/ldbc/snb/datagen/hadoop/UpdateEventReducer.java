package ldbc.snb.datagen.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Created by aprat on 8/8/14.
 */

public class UpdateEventReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

    OutputStream out;
    OutputStream properties;
    private int numEvents = 0;
    private long min = Long.MAX_VALUE;
    private long max = Long.MIN_VALUE;

    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        try {
            FileSystem fs = FileSystem.get(conf);
            String strTaskId = context.getTaskAttemptID().getTaskID().toString();
            int attempTaskId = Integer.parseInt(strTaskId.substring(strTaskId.length() - 3));
            if (Boolean.parseBoolean(conf.get("compressed")) == true) {
                Path outFile = new Path(context.getConfiguration().get("outputDir") + "/social_network/updateStream_" + attempTaskId + ".csv.gz");
                out = new GZIPOutputStream(fs.create(outFile));
            } else {
                Path outFile = new Path(context.getConfiguration().get("outputDir") + "/social_network/updateStream_" + attempTaskId + ".csv");
                out = fs.create(outFile);
            }
            properties = fs.create(new Path(context.getConfiguration().get("outputDir") + "/social_network/updateStream_" + attempTaskId + ".properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void reduce(LongWritable key, Iterable<Text> valueSet,
                       Context context) throws IOException, InterruptedException {
        for (Text event : valueSet) {
            min = min > key.get() ? key.get() : min;
            max = max < key.get() ? key.get() : max;
            out.write(event.toString().getBytes("UTF8"));
            numEvents++;
        }
    }

    @Override
    protected void cleanup(Context context) {
        try {
            System.out.println("Number of events reduced " + numEvents);
            String propertiesStr = new String("gctdeltaduration:" + context.getConfiguration().get("deltaTime") + "\nmin_write_event_start_time:" + min + "\nmax_write_event_start_time:" + max);
            properties.write(propertiesStr.getBytes("UTF8"));
            properties.flush();
            properties.close();
            out.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
