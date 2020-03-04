package ldbc.snb.datagen.hadoop.sorting;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<LongWritable, Text,Text,NullWritable> {
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text value:values) { //for all values at this time
            context.write(value,NullWritable.get()); //just output
        }
    }
}