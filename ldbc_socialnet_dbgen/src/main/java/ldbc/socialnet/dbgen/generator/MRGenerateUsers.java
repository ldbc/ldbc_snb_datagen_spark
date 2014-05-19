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
package ldbc.socialnet.dbgen.generator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Writer;
import java.util.TreeSet;

import ldbc.socialnet.dbgen.objects.ReducedUserProfile;
import ldbc.socialnet.dbgen.objects.UpdateEvent;
import ldbc.socialnet.dbgen.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;



public class MRGenerateUsers{

    static String hadoopDir;
    static String outputDir;
    static int    numThreads;
    static String dbgenDir;


    public static class UpdateEventMapper extends Mapper <LongWritable, Text, LongWritable, Text> {

        private int numEvents = 0;
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            context.write(key,value);
            numEvents++;
        }

        @Override
        protected void cleanup(Context context) {
           System.out.println("Number of events mapped: "+numEvents);
        }

    }

    public static class UpdateEventReducer extends Reducer<LongWritable, Text, LongWritable, Text>{

        FSDataOutputStream out;
        private int numEvents = 0;
        @Override
        protected void setup(Context context){
            Configuration conf = new Configuration();
            try {
                FileSystem fs = FileSystem.get(conf);
                String strTaskId = context.getTaskAttemptID().getTaskID().toString();
                int attempTaskId = Integer.parseInt(strTaskId.substring(strTaskId.length() - 3));
                Path outFile = new Path(context.getConfiguration().get("sibOutputDir")+"/updateStream_"+attempTaskId+".csv");
                out = fs.create(outFile);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void reduce(LongWritable key, Iterable<Text> valueSet,
                           Context context) throws IOException, InterruptedException{
            for( Text event : valueSet ) {
                out.write(event.toString().getBytes("UTF8"));
                numEvents++;
            }
        }
        @Override
        protected void cleanup(Context context){
            try {
                System.out.println("Number of events reduced "+numEvents);
                out.flush();
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

	public static class GenerateUsersMapper extends Mapper <LongWritable, Text, MapReduceKey, ReducedUserProfile> {
			
		private String outputDir; 
		private String homeDir; 
		private int numMappers;
		private int fileIdx;

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			homeDir = conf.get("sibHomeDir").toString();
			outputDir = conf.get("sibOutputDir").toString();
			numMappers = Integer.parseInt(conf.get("numMappers"));
			fileIdx = Integer.parseInt(value.toString());
			System.out.println("Generating user at mapper " + fileIdx);
			ScalableGenerator generator;
			generator = new ScalableGenerator(fileIdx, outputDir, homeDir);
			System.out.println("Successfully init Generator object");
			generator.init(numMappers, fileIdx);
			int pass = 0;
			generator.mrGenerateUserInfo(pass, context, fileIdx);
		}
		
	}

    public static class ForwardMapper extends  Mapper <MapReduceKey, ReducedUserProfile, MapReduceKey, ReducedUserProfile> {
        @Override
        public void map(MapReduceKey key, ReducedUserProfile value,
                        Context context)
                throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class DimensionReducer extends Reducer<MapReduceKey, ReducedUserProfile, MapReduceKey, ReducedUserProfile>{

        public static ScalableGenerator friendGenerator;
        private String outputDir;
        private String homeDir;
        private int numReducer;
        private int attempTaskId;
        private int dimension;
        private int pass;

        @Override
        protected void setup(Context context){
            Configuration conf = context.getConfiguration();
            homeDir = conf.get("sibHomeDir").toString();
            outputDir = conf.get("sibOutputDir").toString();
            numReducer = Integer.parseInt(conf.get("numMappers"));
            dimension = Integer.parseInt(conf.get("dimension"));
            pass = Integer.parseInt(conf.get("pass"));

            String strTaskId = context.getTaskAttemptID().getTaskID().toString();
            attempTaskId = Integer.parseInt(strTaskId.substring(strTaskId.length() - 3));
            friendGenerator = new ScalableGenerator(attempTaskId, outputDir, homeDir);
            friendGenerator.init(numReducer, attempTaskId);
        }

        @Override
        public void reduce(MapReduceKey key, Iterable<ReducedUserProfile> valueSet,
                           Context context) throws IOException, InterruptedException{
            friendGenerator.resetState(key.block);
            int counter = 0;
            System.out.println("Start University group: "+key.block);
            for (ReducedUserProfile user:valueSet){
                //                 System.out.println(user.getAccountId());
                friendGenerator.pushUserProfile(user, pass, dimension, context);
                counter++;
            }
            friendGenerator.pushAllRemainingUser(pass, dimension, context);
            System.out.println("End group with size: "+counter);
        }
        @Override
        protected void cleanup(Context context){
            System.out.println("Summary for reducer" + attempTaskId);
            System.out.println("Number of user profile read " + friendGenerator.totalNumUserProfilesRead);
            System.out.println("Number of exact user profile out " + friendGenerator.exactOutput);
            System.out.println("Number of exact friend added " + friendGenerator.friendshipNum);
        }
    }

    public static class RankMapper extends  Mapper <MapReduceKey, ReducedUserProfile, MapReduceKey, ReducedUserProfile> {
        @Override
        public void map(MapReduceKey key, ReducedUserProfile value,
                        Context context)
                throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class RankReducer extends Reducer<MapReduceKey, ReducedUserProfile, MapReduceKey, ReducedUserProfile>{

        @Override
        protected void setup(Context context){
        }

        @Override
        public void reduce(MapReduceKey key, Iterable<ReducedUserProfile> valueSet,
                           Context context) throws IOException, InterruptedException{

            int numUser = 0;
            int blockSize = ScalableGenerator.blockSize;
            for (ReducedUserProfile user:valueSet){
                context.write(new MapReduceKey(numUser / blockSize, key.key, key.id), user);
                numUser++;
            }
        }
    }

    public static class UserActivityReducer extends Reducer <MapReduceKey, ReducedUserProfile, MapReduceKey, ReducedUserProfile>{

        public static ScalableGenerator friendGenerator;
        private String outputDir;
        private String homeDir;
        private int numReducer;
        private int attempTaskId;
        private int	totalObjects;

        @Override
        protected void setup(Context context){
            Configuration conf = context.getConfiguration();
            homeDir = conf.get("sibHomeDir").toString();
            outputDir = conf.get("sibOutputDir").toString();
            numReducer = Integer.parseInt(conf.get("numMappers"));

            String strTaskId = context.getTaskAttemptID().getTaskID().toString();
            attempTaskId = Integer.parseInt(strTaskId.substring(strTaskId.length() - 3));
            friendGenerator = new ScalableGenerator(attempTaskId, outputDir, homeDir);
            friendGenerator.init(numReducer, attempTaskId);
            friendGenerator.openSerializer();
            totalObjects = 0;
        }

        @Override
        public void reduce(MapReduceKey key, Iterable<ReducedUserProfile> valueSet,
                           Context context) throws IOException, InterruptedException{

            friendGenerator.resetState(key.block);
            for (ReducedUserProfile user: valueSet){
                friendGenerator.generateUserActivity(user, context);
            	System.out.println("Number of work places: " + user.getNumOfWorkPlaces());
                totalObjects++;
            }
        }

        @Override
        protected void cleanup(Context context){
            friendGenerator.closeSerializer();
            System.out.println("Number of users serialized by reducer "+attempTaskId+": "+totalObjects);
        }
    }

    public static class FriendListOutputReducer extends Reducer<MapReduceKey, ReducedUserProfile, MapReduceKey, ReducedUserProfile> {
        FSDataOutputStream out;
        private int numUsers;
        @Override
        protected void setup(Context context){
            Configuration conf = new Configuration();
            numUsers = 0;
            try {
                FileSystem fs = FileSystem.get(conf);
                String strTaskId = context.getTaskAttemptID().getTaskID().toString();
                int attempTaskId = Integer.parseInt(strTaskId.substring(strTaskId.length() - 3));
                Path outFile = new Path(context.getConfiguration().get("sibOutputDir")+"/m0friendList"+attempTaskId+".csv");
                out = fs.create(outFile);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void reduce(MapReduceKey key, Iterable<ReducedUserProfile> valueSet,
                           Context context) throws IOException, InterruptedException{
            for (ReducedUserProfile user:valueSet){
            	numUsers++;
            	StringBuffer strbuf = new StringBuffer();
            	strbuf.append(user.getAccountId());
            	TreeSet<Long>  ids = user.getFriendIds();
            	for (Long id: ids){
            		strbuf.append(",");
            		strbuf.append(id);
            	}
            	strbuf.append("\n");
            	out.write(strbuf.toString().getBytes());
            }
        }
        @Override
        protected void cleanup(Context context){
            try {
                System.out.println("Number of user friends lists reduced "+numUsers);
                out.flush();
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    	
    }
    
	public int runGenerateJob(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		// Set parameter
		conf.set("numMappers", Integer.toString(numThreads));
		conf.set("sibHomeDir", dbgenDir);
		conf.set("sibOutputDir", outputDir );

        /// --------------- First job Generating users and friendships----------------

        conf.set("pass",Integer.toString(0));
        conf.set("dimension",Integer.toString(1));
		Job job = new Job(conf,"SIB Generate Users & 1st Dimension");
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
	    FileInputFormat.setInputPaths(job, new Path(hadoopDir)+"/mrInputFile");
	    FileOutputFormat.setOutputPath(job, new Path(hadoopDir+"/sib"));

        /// --------------- Sorting phase --------------

        Job jobSorting = new Job(conf,"Sorting phase to create blocks");
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
        FileInputFormat.setInputPaths(jobSorting, new Path(hadoopDir+"/sib"));
        FileOutputFormat.setOutputPath(jobSorting, new Path(hadoopDir+"/sibSorting"));

	    /// --------------- Second job Generating Friendships  ----------------

        conf.set("pass",Integer.toString(1));
        conf.set("dimension",Integer.toString(2));
		Job job2 = new Job(conf,"SIB Generate Friendship - Interest");
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
	    FileInputFormat.setInputPaths(job2, new Path(hadoopDir+"/sibSorting"));
	    FileOutputFormat.setOutputPath(job2, new Path(hadoopDir + "/sib2") );

        /// --------------- Sorting phase 2--------------

        Job jobSorting2 = new Job(conf,"Sorting phase 2 to create blocks");
        jobSorting2.setMapOutputKeyClass(MapReduceKey.class);
        jobSorting2.setMapOutputValueClass(ReducedUserProfile.class);
        jobSorting2.setOutputKeyClass(MapReduceKey.class);
        jobSorting2.setOutputValueClass(ReducedUserProfile.class);
        jobSorting2.setJarByClass(RankMapper.class);
        jobSorting2.setMapperClass(RankMapper.class);
        jobSorting2.setReducerClass(RankReducer.class);
        jobSorting2.setNumReduceTasks(1);
        jobSorting2.setInputFormatClass(SequenceFileInputFormat.class);
        jobSorting2.setOutputFormatClass(SequenceFileOutputFormat.class);
        jobSorting2.setPartitionerClass(MapReduceKeyPartitioner.class);
        jobSorting2.setSortComparatorClass(MapReduceKeyComparator.class);
        jobSorting2.setGroupingComparatorClass(MapReduceKeyGroupKeyComparator.class);
        FileInputFormat.setInputPaths(jobSorting2, new Path(hadoopDir+"/sib2"));
        FileOutputFormat.setOutputPath(jobSorting2, new Path(hadoopDir+"/sibSorting2"));

	    
	    /// --------------- Third job Generating Friendships----------------

        conf.set("pass",Integer.toString(2));
        conf.set("dimension",Integer.toString(2));
		Job job3 = new Job(conf,"SIB Generate Friendship - Random");
		job3.setMapOutputKeyClass(MapReduceKey.class);
		job3.setMapOutputValueClass(ReducedUserProfile.class);
		job3.setOutputKeyClass(MapReduceKey.class);
		job3.setOutputValueClass(ReducedUserProfile.class);
		job3.setJarByClass(ForwardMapper.class);
		job3.setMapperClass(ForwardMapper.class);
		job3.setReducerClass(DimensionReducer.class);
		job3.setNumReduceTasks(numThreads);
		job3.setInputFormatClass(SequenceFileInputFormat.class);
		job3.setOutputFormatClass(SequenceFileOutputFormat.class);
        job3.setPartitionerClass(MapReduceKeyPartitioner.class);
        job3.setSortComparatorClass(MapReduceKeyComparator.class);
        job3.setGroupingComparatorClass(MapReduceKeyGroupKeyComparator.class);
		
	    FileInputFormat.setInputPaths(job3, new Path(hadoopDir + "/sibSorting2"));
	    FileOutputFormat.setOutputPath(job3, new Path(hadoopDir + "/sib3") );

        /// --------------- Sorting phase 3--------------

        Job jobSorting3 = new Job(conf,"Sorting phase 3 to create blocks");
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
        FileInputFormat.setInputPaths(jobSorting3, new Path(hadoopDir+"/sib3"));
        FileOutputFormat.setOutputPath(jobSorting3, new Path(hadoopDir+"/sibSorting3"));

        /// --------------- Fourth job: Serialize static network ----------------

        Job job4 = new Job(conf,"Generate user activity");
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
        FileOutputFormat.setOutputPath(job4, new Path(hadoopDir + "/sib4") );

        

        /// --------------- Fifth job: Sort update streams ----------------

        conf.setInt("mapred.line.input.format.linespermap", 1000000);
        Job job5 = new Job(conf,"Soring update streams");
        job5.setMapOutputKeyClass(LongWritable.class);
        job5.setMapOutputValueClass(Text.class);
        job5.setOutputKeyClass(LongWritable.class);
        job5.setOutputValueClass(Text.class);
        job5.setJarByClass(UpdateEventMapper.class);
        job5.setMapperClass(UpdateEventMapper.class);
        job5.setReducerClass(UpdateEventReducer.class);
        job5.setNumReduceTasks(numThreads);
        job5.setInputFormatClass(SequenceFileInputFormat.class);
        job5.setOutputFormatClass(SequenceFileOutputFormat.class);
        job5.setPartitionerClass(UpdateEventPartitioner.class);

        /// --------------- Sixth job: Materialize the friends lists ----------------
        Job job6 = new Job(conf,"Dump the friends lists");
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
        FileOutputFormat.setOutputPath(job6, new Path(hadoopDir + "/job6") );

        for( int i =0; i < numThreads; ++i ) {
            FileInputFormat.addInputPath(job5, new Path(outputDir + "/temp_updateStream_"+i+".csv"));
        }
        FileOutputFormat.setOutputPath(job5, new Path(hadoopDir + "/sibEnd") );



	    /// --------- Execute Jobs ------

	    long start = System.currentTimeMillis();

	    int res = job.waitForCompletion(true) ? 0 : 1;

        int resSorting = jobSorting.waitForCompletion(true) ? 0 : 1;

	    int res2 = job2.waitForCompletion(true) ? 0 : 1;

        int resSorting2 = jobSorting2.waitForCompletion(true) ? 0 : 1;

	    int res3= job3.waitForCompletion(true) ? 0 : 1;

        int resSorting3 = jobSorting3.waitForCompletion(true) ? 0 : 1;

        int resUpdateStreams = job4.waitForCompletion(true) ? 0 : 1;
        
        int sortUpdateStreams= job5.waitForCompletion(true) ? 0 : 1;

        int resMaterializeFriends = job6.waitForCompletion(true) ? 0 : 1;

        FileSystem fs = FileSystem.get(conf);
        for( int i =0; i < numThreads; ++i ) {
            fs.delete(new Path(outputDir + "/temp_updateStream_"+i+".csv"),false);
        }

	    long end = System.currentTimeMillis();
	    System.out.println(((end - start) / 1000)
	                    + " total seconds");
	    return res;
	}

	public static void main(String[] args)  throws Exception{

        hadoopDir = args[0];
        dbgenDir = args[2];
        outputDir = args[3];
        numThreads = Integer.parseInt(args[1]);

        System.out.println("Hadoop tmp file: "+hadoopDir);
        System.out.println("Number of threads: "+numThreads);
        System.out.println("DBGEN working dir: "+dbgenDir);
        System.out.println("Data output dir: "+outputDir);
        // Deleting exisging files

        FileSystem dfs = FileSystem.get(new Configuration());
        dfs.delete(new Path(hadoopDir), true);
        dfs.delete(new Path(outputDir), true);

		// Create input text file in HDFS
		writeToOutputFile("mrInputFile", numThreads);
		dfs.copyFromLocalFile(new Path("mrInputFile"),new Path(hadoopDir+"/mrInputFile") );
		MRGenerateUsers mrGenerator = new MRGenerateUsers();
		mrGenerator.runGenerateJob(args);

	}

	public static void writeToOutputFile(String filename, int numMaps){
	 	Writer output = null;
	 	File file = new File(filename);
	 	try {
			output = new BufferedWriter(new FileWriter(file));
			for (int i = 0; i < numMaps; i++)
				output.write(i + "\n");
			output.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
