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
 * Created by aprat on 8/11/14.
 */
public class ActivityGenerator {

    public ActivityGenerator () {

    }

    void run( Configuration conf, String inputFile, String updateStreamsPrefix ) throws Exception {

        FileSystem fs = FileSystem.get(conf);
        String hadoopDir = new String(conf.get("outputDir") + "/hadoop");
        String socialNetDir = new String(conf.get("outputDir") + "/social_network");
        int numThreads = Integer.parseInt(conf.get("numThreads"));

        Job sorting = new Job(conf, "Sorting phase 3 to create blocks");
        sorting.setMapOutputKeyClass(MapReduceKey.class);
        sorting.setMapOutputValueClass(ReducedUserProfile.class);
        sorting.setOutputKeyClass(MapReduceKey.class);
        sorting.setOutputValueClass(ReducedUserProfile.class);
        sorting.setJarByClass(RankMapper.class);
        sorting.setMapperClass(RankMapper.class);
        sorting.setReducerClass(RankReducer.class);
        sorting.setNumReduceTasks(1);
        sorting.setInputFormatClass(SequenceFileInputFormat.class);
        sorting.setOutputFormatClass(SequenceFileOutputFormat.class);
        sorting.setPartitionerClass(MapReduceKeyPartitioner.class);
        sorting.setSortComparatorClass(MapReduceKeyComparator.class);
        sorting.setGroupingComparatorClass(MapReduceKeyGroupKeyComparator.class);
        FileInputFormat.setInputPaths(sorting, new Path(inputFile));
        FileOutputFormat.setOutputPath(sorting, new Path(hadoopDir + "/sibSorting"));

        /// --------------- Fourth job: Serialize static network ----------------

        Job job = new Job(conf, "Generate user activity");
        job.setMapOutputKeyClass(MapReduceKey.class);
        job.setMapOutputValueClass(ReducedUserProfile.class);
        job.setOutputKeyClass(MapReduceKey.class);
        job.setOutputValueClass(ReducedUserProfile.class);
        job.setJarByClass(ForwardMapper.class);
        job.setMapperClass(ForwardMapper.class);
        job.setReducerClass(UserActivityReducer.class);
        job.setNumReduceTasks(numThreads);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setPartitionerClass(MapReduceKeyPartitioner.class);
        job.setSortComparatorClass(MapReduceKeyComparator.class);
        job.setGroupingComparatorClass(MapReduceKeyGroupKeyComparator.class);

        Job dump = new Job(conf, "Dump the friends lists");
        dump.setMapOutputKeyClass(MapReduceKey.class);
        dump.setMapOutputValueClass(ReducedUserProfile.class);
        dump.setOutputKeyClass(MapReduceKey.class);
        dump.setOutputValueClass(ReducedUserProfile.class);
        dump.setJarByClass(ForwardMapper.class);
        dump.setMapperClass(ForwardMapper.class);
        dump.setReducerClass(FriendListOutputReducer.class);
        dump.setNumReduceTasks(numThreads);
        dump.setInputFormatClass(SequenceFileInputFormat.class);
        dump.setOutputFormatClass(SequenceFileOutputFormat.class);
        dump.setPartitionerClass(MapReduceKeyPartitioner.class);
        dump.setSortComparatorClass(MapReduceKeyComparator.class);
        dump.setGroupingComparatorClass(MapReduceKeyGroupKeyComparator.class);
        FileInputFormat.setInputPaths(dump, new Path(hadoopDir + "/sibSorting"));
        FileOutputFormat.setOutputPath(dump, new Path(hadoopDir + "/sibAux"));

        FileInputFormat.setInputPaths(job, new Path(hadoopDir + "/sibSorting"));
        FileOutputFormat.setOutputPath(job, new Path(hadoopDir + "/sibAux"));
        sorting.waitForCompletion(true);
        job.waitForCompletion(true);
        fs.delete(new Path(hadoopDir + "/sibAux"), true);
        dump.waitForCompletion(true);
        fs.delete(new Path(hadoopDir + "/sibSorting"), true);
        fs.delete(new Path(hadoopDir + "/sibAux"), true);

    }
}
