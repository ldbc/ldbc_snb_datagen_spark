package ldbc.snb.datagen.generator;

import ldbc.snb.datagen.hadoop.*;
import ldbc.snb.datagen.objects.ReducedUserProfile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * Created by aprat on 8/8/14.
 */
public class PersonGenerator {

    public PersonGenerator() {

    }

    void run( Configuration conf ) throws Exception {

        conf.set("pass", Integer.toString(0));
        String hadoopDir = new String(conf.get("outputDir") + "/hadoop");
        String socialNetDir = new String(conf.get("outputDir") + "/social_network");
        int numThreads = Integer.parseInt(conf.get("numThreads"));
        conf.set("dimension", Integer.toString(1));
        Job job = new Job(conf, "SIB Generate Users & 1st Dimension");
        job.setMapOutputKeyClass(MapReduceKey.class);
        job.setMapOutputValueClass(ReducedUserProfile.class);
        job.setOutputKeyClass(MapReduceKey.class);
        job.setOutputValueClass(ReducedUserProfile.class);
        job.setJarByClass(GenerateUsersMapper.class);
        job.setMapperClass(GenerateUsersMapper.class);
        job.setReducerClass(DimensionReducer.class);
        job.setNumReduceTasks(numThreads);
        job.setInputFormatClass(NLineInputFormat.class);
        conf.setInt("mapred.line.input.format.linespermap", 1);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setPartitionerClass(MapReduceKeyPartitioner.class);
        job.setSortComparatorClass(MapReduceKeyComparator.class);
        job.setGroupingComparatorClass(MapReduceKeyGroupKeyComparator.class);
        FileInputFormat.setInputPaths(job, new Path(hadoopDir) + "/mrInputFile");
        FileOutputFormat.setOutputPath(job, new Path(hadoopDir + "/sib"));

        int res = job.waitForCompletion(true) ? 0 : 1;
    }
}
