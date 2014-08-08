/*
 * Copyright (c) 2013 LDBC
 * Linked Data Benchmark Council (http://ldbc.eu)
 *
 * This file is part of ldbc_socialnet_dbgen.
 *
 * ldbc_socialnet_dbgen is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ldbc_socialnet_dbgen is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ldbc_socialnet_dbgen.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2011 OpenLink Software <bdsmt@openlinksw.com>
 * All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation;  only Version 2 of the License dated
 * June 1991.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package ldbc.snb.datagen.generator;

import ldbc.snb.datagen.hadoop.*;
import ldbc.snb.datagen.objects.ReducedUserProfile;
import ldbc.snb.datagen.util.ConfigParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.io.OutputStream;


public class LDBCDatagen {

    private void printProgress(String message) {
        System.out.println("************************************************");
        System.out.println("* " + message + " *");
        System.out.println("************************************************");
    }

    public int runGenerateJob(Configuration conf) throws Exception {
        FileSystem fs = FileSystem.get(conf);
        String hadoopDir = new String(conf.get("outputDir") + "/hadoop");
        String socialNetDir = new String(conf.get("outputDir") + "/social_network");
        int numThreads = Integer.parseInt(conf.get("numThreads"));
        System.out.println("NUMBER OF THREADS " + numThreads);

        /// --------------- Sorting phase 3--------------

        Job jobSorting3 = new Job(conf, "Sorting phase 3 to create blocks");
        jobSorting3.setMapOutputKeyClass(MapReduceKey.class);
        jobSorting3.setMapOutputValueClass(ReducedUserProfile.class);
        jobSorting3.setOutputKeyClass(MapReduceKey.class);
        jobSorting3.setOutputValueClass(ReducedUserProfile.class);
        jobSorting3.setJarByClass(RankMapper.class);
        jobSorting3.setMapperClass(RankMapper.class);
        jobSorting3.setReducerClass(RankReducer.class);
        jobSorting3.setNumReduceTasks(1);
        jobSorting3.setInputFormatClass(SequenceFileInputFormat.class);
        jobSorting3.setOutputFormatClass(SequenceFileOutputFormat.class);
        jobSorting3.setPartitionerClass(MapReduceKeyPartitioner.class);
        jobSorting3.setSortComparatorClass(MapReduceKeyComparator.class);
        jobSorting3.setGroupingComparatorClass(MapReduceKeyGroupKeyComparator.class);
        FileInputFormat.setInputPaths(jobSorting3, new Path(hadoopDir + "/sib3"));
        FileOutputFormat.setOutputPath(jobSorting3, new Path(hadoopDir + "/sibSorting3"));

        /// --------------- Fourth job: Serialize static network ----------------

        Job job4 = new Job(conf, "Generate user activity");
        job4.setMapOutputKeyClass(MapReduceKey.class);
        job4.setMapOutputValueClass(ReducedUserProfile.class);
        job4.setOutputKeyClass(MapReduceKey.class);
        job4.setOutputValueClass(ReducedUserProfile.class);
        job4.setJarByClass(ForwardMapper.class);
        job4.setMapperClass(ForwardMapper.class);
        job4.setReducerClass(UserActivityReducer.class);
        job4.setNumReduceTasks(numThreads);
        job4.setInputFormatClass(SequenceFileInputFormat.class);
        job4.setOutputFormatClass(SequenceFileOutputFormat.class);
        job4.setPartitionerClass(MapReduceKeyPartitioner.class);
        job4.setSortComparatorClass(MapReduceKeyComparator.class);
        job4.setGroupingComparatorClass(MapReduceKeyGroupKeyComparator.class);

        FileInputFormat.setInputPaths(job4, new Path(hadoopDir + "/sibSorting3"));
        FileOutputFormat.setOutputPath(job4, new Path(hadoopDir + "/sib4"));


        /// --------------- Fifth job: Sort update streams ----------------
        conf.setInt("mapred.line.input.format.linespermap", 1000000);
        Job job5 = new Job(conf, "Soring update streams");
        job5.setMapOutputKeyClass(LongWritable.class);
        job5.setMapOutputValueClass(Text.class);
        job5.setOutputKeyClass(LongWritable.class);
        job5.setOutputValueClass(Text.class);
        job5.setJarByClass(UpdateEventMapper.class);
        job5.setMapperClass(UpdateEventMapper.class);
        job5.setReducerClass(UpdateEventReducer.class);
        job5.setNumReduceTasks(1);
        job5.setInputFormatClass(SequenceFileInputFormat.class);
        job5.setOutputFormatClass(SequenceFileOutputFormat.class);
        job5.setPartitionerClass(UpdateEventPartitioner.class);

        for (int i = 0; i < numThreads; ++i) {
            FileInputFormat.addInputPath(job5, new Path(socialNetDir + "/temp_updateStream_" + i + ".csv"));
        }
        FileOutputFormat.setOutputPath(job5, new Path(hadoopDir + "/sibEnd"));

        /// --------------- Sixth job: Materialize the friends lists ----------------
        Job job6 = new Job(conf, "Dump the friends lists");
        job6.setMapOutputKeyClass(MapReduceKey.class);
        job6.setMapOutputValueClass(ReducedUserProfile.class);
        job6.setOutputKeyClass(MapReduceKey.class);
        job6.setOutputValueClass(ReducedUserProfile.class);
        job6.setJarByClass(ForwardMapper.class);
        job6.setMapperClass(ForwardMapper.class);
        job6.setReducerClass(FriendListOutputReducer.class);
        job6.setNumReduceTasks(numThreads);
        job6.setInputFormatClass(SequenceFileInputFormat.class);
        job6.setOutputFormatClass(SequenceFileOutputFormat.class);
        job6.setPartitionerClass(MapReduceKeyPartitioner.class);
        job6.setSortComparatorClass(MapReduceKeyComparator.class);
        job6.setGroupingComparatorClass(MapReduceKeyGroupKeyComparator.class);
        FileInputFormat.setInputPaths(job6, new Path(hadoopDir + "/sibSorting3"));
        FileOutputFormat.setOutputPath(job6, new Path(hadoopDir + "/job6"));


        /// --------- Execute Jobs ------

        long start = System.currentTimeMillis();

        printProgress("Starting: Person generation and friendship generation 1");
        PersonGenerator personGenerator = new PersonGenerator();
        personGenerator.run(conf);

        printProgress("Starting: Friendship generation 2");
        FriendshipGenerator friendGenerator = new FriendshipGenerator();
        friendGenerator.run(conf,hadoopDir + "/sib",hadoopDir + "/sib2",1,2);
        fs.delete(new Path(hadoopDir + "/sib"), true);

        printProgress("Starting: Friendship generation 3");
        friendGenerator.run(conf,hadoopDir + "/sib2",hadoopDir + "/sib3",2,2);
        fs.delete(new Path(hadoopDir + "/sib2"), true);

        printProgress("Starting: Generating person activity");
        int resSorting3 = jobSorting3.waitForCompletion(true) ? 0 : 1;
        fs.delete(new Path(hadoopDir + "/sib3"), true);
        int resUpdateStreams = job4.waitForCompletion(true) ? 0 : 1;
        fs.delete(new Path(hadoopDir + "/sib4"), true);

        printProgress("Starting: Sorting update streams");
        int sortUpdateStreams = job5.waitForCompletion(true) ? 0 : 1;

        for (int i = 0; i < numThreads; ++i) {
            fs.delete(new Path(socialNetDir + "/temp_updateStream_" + i + ".csv"), false);
        }
        fs.delete(new Path(hadoopDir + "/sibEnd"), true);

        printProgress("Starting: Materialize friends for substitution parameters");
        int resMaterializeFriends = job6.waitForCompletion(true) ? 0 : 1;
        fs.delete(new Path(hadoopDir + "/sibSorting3"), true);

        long end = System.currentTimeMillis();
        System.out.println(((end - start) / 1000)
                + " total seconds");
        for (int i = 0; i < numThreads; ++i) {
            fs.copyToLocalFile(new Path(socialNetDir + "/m" + i + "factors.txt"), new Path("./"));
            fs.copyToLocalFile(new Path(socialNetDir + "/m0friendList" + i + ".csv"), new Path("./"));
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = ConfigParser.GetConfig(args[0]);
        ConfigParser.PringConfig(conf);


        // Deleting exisging files
        FileSystem dfs = FileSystem.get(conf);
        dfs.delete(new Path(conf.get("outputDir") + "/hadoop"), true);
        dfs.delete(new Path(conf.get("outputDir") + "/social_network"), true);

        // Create input text file in HDFS
        writeToOutputFile(conf.get("outputDir") + "/hadoop/mrInputFile", Integer.parseInt(conf.get("numThreads")), conf);
        LDBCDatagen datagen = new LDBCDatagen();
        datagen.runGenerateJob(conf);

    }

    public static void writeToOutputFile(String filename, int numMaps, Configuration conf) {
        try {
            FileSystem dfs = FileSystem.get(conf);
            OutputStream output = dfs.create(new Path(filename));
            for (int i = 0; i < numMaps; i++)
                output.write((new String(i + "\n").getBytes()));
            output.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
