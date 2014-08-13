package ldbc.snb.datagen.generator;

import ldbc.snb.datagen.hadoop.UpdateEventMapper;
import ldbc.snb.datagen.hadoop.UpdateEventPartitioner;
import ldbc.snb.datagen.hadoop.UpdateEventReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * Created by aprat on 8/11/14.
 */
public class UpdateStreamSorter {

    public UpdateStreamSorter() {

    }

    void run( Configuration conf ) throws Exception {

        FileSystem fs = FileSystem.get(conf);
        String hadoopDir = new String(conf.get("outputDir") + "/hadoop");
        String socialNetDir = new String(conf.get("outputDir") + "/social_network");
        int numThreads = Integer.parseInt(conf.get("numThreads"));

        /// --------------- Fifth job: Sort update streams ----------------
        conf.setInt("mapred.line.input.format.linespermap", 1000000);
        Job job = new Job(conf, "Soring update streams");
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setJarByClass(UpdateEventMapper.class);
        job.setMapperClass(UpdateEventMapper.class);
        job.setReducerClass(UpdateEventReducer.class);
        job.setNumReduceTasks(1);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setPartitionerClass(UpdateEventPartitioner.class);

        for (int i = 0; i < numThreads; ++i) {
            FileInputFormat.addInputPath(job, new Path(socialNetDir + "/temp_updateStream_" + i + ".csv"));
        }
        FileOutputFormat.setOutputPath(job, new Path(hadoopDir + "/sibEnd"));
        job.waitForCompletion(true);
        fs.delete(new Path(hadoopDir + "/sibEnd"), true);
        for (int i = 0; i < numThreads; ++i) {
            fs.delete(new Path(socialNetDir + "/temp_updateStream_" + i + ".csv"), false);
        }
    }
}
