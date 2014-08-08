package ldbc.snb.datagen.generator;

import ldbc.snb.datagen.hadoop.*;
import ldbc.snb.datagen.objects.ReducedUserProfile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * Created by aprat on 8/8/14.
 */
public class FriendshipGenerator {

    public FriendshipGenerator() {

    }

    void run( Configuration conf, String inputFile, String outputFile, int pass, int dimension ) throws Exception {


        FileSystem fs = FileSystem.get(conf);
        conf.set("pass", Integer.toString(0));
        String hadoopDir = new String(conf.get("outputDir") + "/hadoop");
        String socialNetDir = new String(conf.get("outputDir") + "/social_network");

        int numThreads = Integer.parseInt(conf.get("numThreads"));
        Job jobSorting = new Job(conf, "Sorting phase to create blocks");
        jobSorting.setMapOutputKeyClass(MapReduceKey.class);
        jobSorting.setMapOutputValueClass(ReducedUserProfile.class);
        jobSorting.setOutputKeyClass(MapReduceKey.class);
        jobSorting.setOutputValueClass(ReducedUserProfile.class);
        jobSorting.setJarByClass(RankMapper.class);
        jobSorting.setMapperClass(RankMapper.class);
        jobSorting.setReducerClass(RankReducer.class);
        jobSorting.setNumReduceTasks(1);
        jobSorting.setInputFormatClass(SequenceFileInputFormat.class);
        jobSorting.setOutputFormatClass(SequenceFileOutputFormat.class);
        jobSorting.setPartitionerClass(MapReduceKeyPartitioner.class);
        jobSorting.setSortComparatorClass(MapReduceKeyComparator.class);
        jobSorting.setGroupingComparatorClass(MapReduceKeyGroupKeyComparator.class);
        FileInputFormat.setInputPaths(jobSorting, new Path(inputFile));
        FileOutputFormat.setOutputPath(jobSorting, new Path(hadoopDir + "/sibSorting"));

        conf.set("pass", Integer.toString(pass));
        conf.set("dimension", Integer.toString(dimension));
        Job job2 = new Job(conf, "SIB Generate Friendship - Interest");
        job2.setMapOutputKeyClass(MapReduceKey.class);
        job2.setMapOutputValueClass(ReducedUserProfile.class);
        job2.setOutputKeyClass(MapReduceKey.class);
        job2.setOutputValueClass(ReducedUserProfile.class);
        job2.setJarByClass(ForwardMapper.class);
        job2.setMapperClass(ForwardMapper.class);
        job2.setReducerClass(DimensionReducer.class);
        job2.setNumReduceTasks(numThreads);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        job2.setPartitionerClass(MapReduceKeyPartitioner.class);
        job2.setSortComparatorClass(MapReduceKeyComparator.class);
        job2.setGroupingComparatorClass(MapReduceKeyGroupKeyComparator.class);
        FileInputFormat.setInputPaths(job2, new Path(hadoopDir + "/sibSorting"));
        FileOutputFormat.setOutputPath(job2, new Path(outputFile));

        jobSorting.waitForCompletion(true);
        job2.waitForCompletion(true);
        fs.delete(new Path(hadoopDir + "/sibSorting"), true);
    }
}
