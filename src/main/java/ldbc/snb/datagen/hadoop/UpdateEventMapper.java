package ldbc.snb.datagen.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by aprat on 8/8/14.
 */

public class UpdateEventMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    private int numEvents = 0;

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        context.write(key, value);
        numEvents++;
    }

    @Override
    protected void cleanup(Context context) {
        System.out.println("Number of events mapped: " + numEvents);
    }

}
