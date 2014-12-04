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

import java.io.*;
import java.util.Properties;
import java.util.TreeSet;
import java.util.zip.GZIPOutputStream;

import ldbc.socialnet.dbgen.objects.ReducedUserProfile;
import ldbc.socialnet.dbgen.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;



public class MRGenerateUsers{

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

        OutputStream out;
        OutputStream properties;
        private int numEvents = 0;
        private long min = Long.MAX_VALUE;
        private long max = Long.MIN_VALUE;
        @Override
        protected void setup(Context context){
            Configuration conf = context.getConfiguration();
            try {
                FileSystem fs = FileSystem.get(conf);
                String strTaskId = context.getTaskAttemptID().getTaskID().toString();
                int attempTaskId = Integer.parseInt(strTaskId.substring(strTaskId.length() - 3));
                int reducerId = conf.getInt("reducerId",0);
                int partitionId = conf.getInt("partitionId",0);
                String streamType = conf.get("streamType");
                if( Boolean.parseBoolean(conf.get("compressed")) == true ) {
                    Path outFile = new Path(context.getConfiguration().get("outputDir")+"/social_network/updateStream_"+reducerId+"_"+partitionId+"_"+streamType+".csv.gz");
                    out = new GZIPOutputStream( fs.create(outFile));
                } else {
                    Path outFile = new Path(context.getConfiguration().get("outputDir")+"/social_network/updateStream_"+reducerId+"_"+partitionId+"_"+streamType+".csv");
                    out = fs.create(outFile);
                }
                if (conf.getBoolean("updateStreams",false)) {
                    properties = fs.create(new Path(context.getConfiguration().get("outputDir")+"/social_network/updateStream_"+reducerId+"_"+partitionId+"_"+streamType+".properties"));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void reduce(LongWritable key, Iterable<Text> valueSet,
                           Context context) throws IOException, InterruptedException{
            for( Text event : valueSet ) {
                min = min > key.get() ? key.get() : min;
                max = max < key.get() ? key.get() : max;
                out.write(event.toString().getBytes("UTF8"));
                numEvents++;
            }
        }

        @Override
        protected void cleanup(Context context){
            try {
                System.out.println("Number of events reduced "+numEvents);
                if (numEvents > 0) {
                    long updateDistance = (max-min)/numEvents;
                    String propertiesStr = new String("gctdeltaduration:"+context.getConfiguration().get("deltaTime")+
                                                      "\nmin_write_event_start_time:"+min+
                                                      "\nmax_write_event_start_time:"+max+
                                                      "\nupdate_interleave:"+updateDistance+
                                                      "\nnum_events:"+numEvents);
                    properties.write(propertiesStr.getBytes("UTF8"));
                    properties.flush();
                    properties.close();
                }
                out.flush();
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

	public static class GenerateUsersMapper extends Mapper <LongWritable, Text, LongWritable, ReducedUserProfile> {
			
		private int fileIdx;

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			fileIdx = Integer.parseInt(value.toString());
			System.out.println("Generating user at mapper " + fileIdx);
			ScalableGenerator generator;
			generator = new ScalableGenerator(fileIdx, conf);
			System.out.println("Successfully init Generator object");
			generator.init();
			int pass = 0;
			generator.mrGenerateUserInfo(pass, context);
		}
		
	}

    public static class DimensionReducer extends Reducer<ComposedKey, ReducedUserProfile, LongWritable, ReducedUserProfile>{

        public static ScalableGenerator friendGenerator;
        private int attempTaskId;
        private int dimension;
        private int pass;

        @Override
        protected void setup(Context context){
            Configuration conf = context.getConfiguration();
            dimension = Integer.parseInt(conf.get("dimension"));
            pass = Integer.parseInt(conf.get("pass"));

            String strTaskId = context.getTaskAttemptID().getTaskID().toString();
            attempTaskId = Integer.parseInt(strTaskId.substring(strTaskId.length() - 3));
            friendGenerator = new ScalableGenerator(attempTaskId, conf);
            friendGenerator.init();
        }

        @Override
        public void reduce(ComposedKey key, Iterable<ReducedUserProfile> valueSet,
                           Context context) throws IOException, InterruptedException{
            friendGenerator.resetState((int)key.block);
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

    public static class UserActivityReducer extends Reducer <ComposedKey, ReducedUserProfile, LongWritable, ReducedUserProfile>{

        public static ScalableGenerator friendGenerator;
        private int attempTaskId;
        private int	totalObjects;

        @Override
        protected void setup(Context context){
            Configuration conf = context.getConfiguration();
            String strTaskId = context.getTaskAttemptID().getTaskID().toString();
            attempTaskId = Integer.parseInt(strTaskId.substring(strTaskId.length() - 3));
            friendGenerator = new ScalableGenerator(attempTaskId, conf);
            friendGenerator.init();
            friendGenerator.openSerializer();
            totalObjects = 0;
        }

        @Override
        public void reduce(ComposedKey key, Iterable<ReducedUserProfile> valueSet,
                           Context context) throws IOException, InterruptedException{

            friendGenerator.resetState((int)key.block);
            for (ReducedUserProfile user: valueSet){
                friendGenerator.generateUserActivity(user, context);
                totalObjects++;
            }
        }

        @Override
        protected void cleanup(Context context){
            friendGenerator.closeSerializer();
            System.out.println("Number of users serialized by reducer "+attempTaskId+": "+totalObjects);
        }
    }

    public static class FriendListOutputReducer extends Reducer<ComposedKey, ReducedUserProfile, LongWritable, ReducedUserProfile> {
        FSDataOutputStream out;
        private int numUsers;
        @Override
        protected void setup(Context context){
            Configuration conf = context.getConfiguration();
            numUsers = 0;
            try {
                FileSystem fs = FileSystem.get(conf);
                String strTaskId = context.getTaskAttemptID().getTaskID().toString();
                int attempTaskId = Integer.parseInt(strTaskId.substring(strTaskId.length() - 3));
                Path outFile = new Path(context.getConfiguration().get("outputDir")+"/social_network/m0friendList"+attempTaskId+".csv");
                out = fs.create(outFile);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void reduce(ComposedKey key, Iterable<ReducedUserProfile> valueSet,
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

    private void printProgress( String message ) {
        System.out.println("************************************************");
        System.out.println("* "+message+" *");
        System.out.println("************************************************");
    }
    
	public int runGenerateJob(Configuration conf) throws Exception {
        FileSystem fs = FileSystem.get(conf);
        String hadoopDir = new String(conf.get("outputDir")+"/hadoop");
        String socialNetDir = new String(conf.get("outputDir")+"/social_network");
        int numThreads = Integer.parseInt(conf.get("numThreads"));
    	System.out.println("NUMBER OF THREADS "+numThreads);

        /// --------- Execute Jobs ------
        long start = System.currentTimeMillis();

        /// --------------- First job Generating users----------------
        printProgress("Starting: Person generation");
        conf.set("pass",Integer.toString(0));
		Job job = new Job(conf,"SIB Generate Users & 1st Dimension");
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(ReducedUserProfile.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(ReducedUserProfile.class);
		job.setJarByClass(GenerateUsersMapper.class);
		job.setMapperClass(GenerateUsersMapper.class);
		job.setNumReduceTasks(numThreads);
		job.setInputFormatClass(NLineInputFormat.class);
		conf.setInt("mapred.line.input.format.linespermap", 1);	
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    FileInputFormat.setInputPaths(job, new Path(hadoopDir)+"/mrInputFile");
	    FileOutputFormat.setOutputPath(job, new Path(hadoopDir+"/sib"));
        job.waitForCompletion(true);

        /// --------------- Sorting by first dimension  ----------------
        printProgress("Starting: Sorting by first dimension");
        HadoopFileRanker fileRanker = new HadoopFileRanker(conf,LongWritable.class,ReducedUserProfile.class);
        fileRanker.run(hadoopDir+"/sib",hadoopDir+"/sibSorting");
        fs.delete(new Path(hadoopDir + "/sib"),true);

        /// --------------- job Generating First dimension Friendships  ----------------
        printProgress("Starting: Friendship generation 1.");
        conf.set("pass",Integer.toString(0));
        conf.set("dimension",Integer.toString(1));
        job = new Job(conf,"SIB Generate Friendship - Interest");
        job.setMapOutputKeyClass(ComposedKey.class);
        job.setMapOutputValueClass(ReducedUserProfile.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(ReducedUserProfile.class);
        job.setJarByClass(HadoopBlockMapper.class);
        job.setMapperClass(HadoopBlockMapper.class);
        job.setReducerClass(DimensionReducer.class);
        job.setNumReduceTasks(numThreads);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setPartitionerClass(HadoopBlockPartitioner.class);
        job.setSortComparatorClass(ComposedKeyComparator.class);
        job.setGroupingComparatorClass(ComposedKeyGroupComparator.class);

        FileInputFormat.setInputPaths(job, new Path(hadoopDir+"/sibSorting"));
        FileOutputFormat.setOutputPath(job, new Path(hadoopDir + "/sib2") );
        job.waitForCompletion(true);
        fs.delete(new Path(hadoopDir + "/sibSorting"),true);

        /// --------------- Sorting phase 2  ----------------
        printProgress("Starting: Sorting by second dimension");
        fileRanker = new HadoopFileRanker(conf,LongWritable.class,ReducedUserProfile.class);
        fileRanker.run(hadoopDir+"/sib2",hadoopDir+"/sibSorting2");
        fs.delete(new Path(hadoopDir + "/sib2"),true);

	    /// --------------- Second job Generating Friendships  ----------------
        printProgress("Starting: Friendship generation 2.");
        conf.set("pass",Integer.toString(1));
        conf.set("dimension",Integer.toString(2));
		job = new Job(conf,"SIB Generate Friendship - Interest");
        job.setMapOutputKeyClass(ComposedKey.class);
		job.setMapOutputValueClass(ReducedUserProfile.class);
        job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(ReducedUserProfile.class);
		job.setJarByClass(HadoopBlockMapper.class);
        job.setMapperClass(HadoopBlockMapper.class);
		job.setReducerClass(DimensionReducer.class);
		job.setNumReduceTasks(numThreads);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setPartitionerClass(HadoopBlockPartitioner.class);
        job.setSortComparatorClass(ComposedKeyComparator.class);
        job.setGroupingComparatorClass(ComposedKeyGroupComparator.class);
	    FileInputFormat.setInputPaths(job, new Path(hadoopDir+"/sibSorting2"));
	    FileOutputFormat.setOutputPath(job, new Path(hadoopDir + "/sib3") );
        job.waitForCompletion(true);
        fs.delete(new Path(hadoopDir + "/sibSorting2"),true);

        /// --------------- Sorting phase 3--------------
        printProgress("Starting: Sorting by third dimension");
        fileRanker = new HadoopFileRanker(conf,LongWritable.class,ReducedUserProfile.class);
        fileRanker.run(hadoopDir+"/sib3",hadoopDir+"/sibSorting3");
        fs.delete(new Path(hadoopDir + "/sib3"),true);

	    /// --------------- Third job Generating Friendships----------------
        printProgress("Starting: Friendship generation 3.");
        conf.set("pass",Integer.toString(2));
        conf.set("dimension",Integer.toString(2));
		job = new Job(conf,"SIB Generate Friendship - Random");
        job.setMapOutputKeyClass(ComposedKey.class);
		job.setMapOutputValueClass(ReducedUserProfile.class);
        job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(ReducedUserProfile.class);
		job.setJarByClass(HadoopBlockMapper.class);
		job.setMapperClass(HadoopBlockMapper.class);
		job.setReducerClass(DimensionReducer.class);
		job.setNumReduceTasks(numThreads);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setPartitionerClass(HadoopBlockPartitioner.class);
        job.setSortComparatorClass(ComposedKeyComparator.class);
        job.setGroupingComparatorClass(ComposedKeyGroupComparator.class);
	    FileInputFormat.setInputPaths(job, new Path(hadoopDir + "/sibSorting3"));
	    FileOutputFormat.setOutputPath(job, new Path(hadoopDir + "/sib4") );
        job.waitForCompletion(true);
        fs.delete(new Path(hadoopDir + "/sibSorting3"),true);

        /// --------------- Sorting phase 3--------------

        printProgress("Starting: Sorting by third dimension (for activity generation)");
        fileRanker = new HadoopFileRanker(conf,LongWritable.class,ReducedUserProfile.class);
        fileRanker.run(hadoopDir+"/sib4",hadoopDir+"/sibSorting4");
        fs.delete(new Path(hadoopDir + "/sib4"),true);


        /// --------------- Fourth job: Serialize static network ----------------

        printProgress("Starting: Generating person activity");
        job = new Job(conf,"Generate user activity");
        job.setMapOutputKeyClass(ComposedKey.class);
        job.setMapOutputValueClass(ReducedUserProfile.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(ReducedUserProfile.class);
        job.setJarByClass(HadoopBlockMapper.class);
        job.setMapperClass(HadoopBlockMapper.class);
        job.setReducerClass(UserActivityReducer.class);
        job.setNumReduceTasks(numThreads);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setPartitionerClass(HadoopBlockPartitioner.class);
        job.setSortComparatorClass(ComposedKeyComparator.class);
        job.setGroupingComparatorClass(ComposedKeyGroupComparator.class);
        FileInputFormat.setInputPaths(job, new Path(hadoopDir + "/sibSorting4"));
        FileOutputFormat.setOutputPath(job, new Path(hadoopDir + "/sib5") );
        job.waitForCompletion(true);
        fs.delete(new Path(hadoopDir + "/sib5"),true);

        int numEvents = 0;
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;

        if(conf.getBoolean("updateStreams",false)) {
            for (int i = 0; i < numThreads; ++i) {
                int numPartitions = conf.getInt("numUpdatePartitions", 1);
                for (int j = 0; j < numPartitions; ++j) {
                    /// --------------- Fifth job: Sort update streams ----------------
                    conf.setInt("mapred.line.input.format.linespermap", 1000000);
                    conf.setInt("reducerId", i);
                    conf.setInt("partitionId", j);
                    conf.set("streamType", "forum");
                    Job jobForum = new Job(conf, "Soring update streams " + j + " of reducer " + i);
                    jobForum.setMapOutputKeyClass(LongWritable.class);
                    jobForum.setMapOutputValueClass(Text.class);
                    jobForum.setOutputKeyClass(LongWritable.class);
                    jobForum.setOutputValueClass(Text.class);
                    jobForum.setJarByClass(UpdateEventMapper.class);
                    jobForum.setMapperClass(UpdateEventMapper.class);
                    jobForum.setReducerClass(UpdateEventReducer.class);
                    jobForum.setNumReduceTasks(1);
                    jobForum.setInputFormatClass(SequenceFileInputFormat.class);
                    jobForum.setOutputFormatClass(SequenceFileOutputFormat.class);
                    jobForum.setPartitionerClass(UpdateEventPartitioner.class);
                    FileInputFormat.addInputPath(jobForum, new Path(socialNetDir + "/temp_updateStream_" + i + "_" + j + "_forum"));
                    FileOutputFormat.setOutputPath(jobForum, new Path(hadoopDir + "/sibEnd"));
                    printProgress("Starting: Sorting update streams");
                    jobForum.waitForCompletion(true);
                    fs.delete(new Path(socialNetDir + "/temp_updateStream_" + i + "_" + j + "_forum"), false);
                    fs.delete(new Path(hadoopDir + "/sibEnd"), true);

                    conf.setInt("mapred.line.input.format.linespermap", 1000000);
                    conf.setInt("reducerId", i);
                    conf.setInt("partitionId", j);
                    conf.set("streamType", "person");
                    Job jobPerson = new Job(conf, "Soring update streams " + j + " of reducer " + i);
                    jobPerson.setMapOutputKeyClass(LongWritable.class);
                    jobPerson.setMapOutputValueClass(Text.class);
                    jobPerson.setOutputKeyClass(LongWritable.class);
                    jobPerson.setOutputValueClass(Text.class);
                    jobPerson.setJarByClass(UpdateEventMapper.class);
                    jobPerson.setMapperClass(UpdateEventMapper.class);
                    jobPerson.setReducerClass(UpdateEventReducer.class);
                    jobPerson.setNumReduceTasks(1);
                    jobPerson.setInputFormatClass(SequenceFileInputFormat.class);
                    jobPerson.setOutputFormatClass(SequenceFileOutputFormat.class);
                    jobPerson.setPartitionerClass(UpdateEventPartitioner.class);
                    FileInputFormat.addInputPath(jobPerson, new Path(socialNetDir + "/temp_updateStream_" + i + "_" + j + "_person"));
                    FileOutputFormat.setOutputPath(jobPerson, new Path(hadoopDir + "/sibEnd"));
                    printProgress("Starting: Sorting update streams");
                    jobPerson.waitForCompletion(true);
                    fs.delete(new Path(socialNetDir + "/temp_updateStream_" + i + "_" + j + "_person"), false);
                    fs.delete(new Path(hadoopDir + "/sibEnd"), true);

                    if (conf.getBoolean("updateStreams", false)) {
                        Properties properties = new Properties();
                        FSDataInputStream file = fs.open(new Path(conf.get("outputDir") + "/social_network/updateStream_" + i + "_" + j + "_person.properties"));
                        properties.load(file);
                        if (properties.getProperty("min_write_event_start_time") != null) {
                            Long auxMin = Long.parseLong(properties.getProperty("min_write_event_start_time"));
                            min = auxMin < min ? auxMin : min;
                            Long auxMax = Long.parseLong(properties.getProperty("max_write_event_start_time"));
                            max = auxMax > max ? auxMax : max;
                            numEvents += Long.parseLong(properties.getProperty("num_events"));
                        }
                        file.close();
                        file = fs.open(new Path(conf.get("outputDir") + "/social_network/updateStream_" + i + "_" + j + "_forum.properties"));
                        properties.load(file);
                        if (properties.getProperty("min_write_event_start_time") != null) {
                            Long auxMin = Long.parseLong(properties.getProperty("min_write_event_start_time"));
                            min = auxMin < min ? auxMin : min;
                            Long auxMax = Long.parseLong(properties.getProperty("max_write_event_start_time"));
                            max = auxMax > max ? auxMax : max;
                            numEvents += Long.parseLong(properties.getProperty("num_events"));
                        }
                        file.close();
                        fs.delete(new Path(conf.get("outputDir") + "/social_network/updateStream_" + i + "_" + j + "_person.properties"), true);
                        fs.delete(new Path(conf.get("outputDir") + "/social_network/updateStream_" + i + "_" + j + "_forum.properties"), true);
                    }
                }
            }

            if (conf.getBoolean("updateStreams", false)) {
                OutputStream output = fs.create(new Path(conf.get("outputDir") + "/social_network/updateStream.properties"));
                output.write(new String("ldbc.snb.interactive.gct_delta_duration:" + conf.get("deltaTime") + "\n").getBytes());
                output.write(new String("ldbc.snb.interactive.min_write_event_start_time:" + min + "\n").getBytes());
                output.write(new String("ldbc.snb.interactive.max_write_event_start_time:" + max + "\n").getBytes());
                output.write(new String("ldbc.snb.interactive.update_interleave:" + (max - min) / numEvents + "\n").getBytes());
                output.write(new String("ldbc.snb.interactive.num_events:" + numEvents).getBytes());
                output.close();
            }
        }

        /// --------------- Sixth job: Materialize the friends lists ----------------
        Job job6 = new Job(conf,"Dump the friends lists");
        job6.setMapOutputKeyClass(ComposedKey.class);
        job6.setMapOutputValueClass(ReducedUserProfile.class);
        job6.setOutputKeyClass(LongWritable.class);
        job6.setOutputValueClass(ReducedUserProfile.class);
        job6.setJarByClass(HadoopBlockMapper.class);
        job6.setMapperClass(HadoopBlockMapper.class);
        job6.setReducerClass(FriendListOutputReducer.class);
        job6.setNumReduceTasks(numThreads);
        job6.setInputFormatClass(SequenceFileInputFormat.class);
        job6.setOutputFormatClass(SequenceFileOutputFormat.class);
        job6.setPartitionerClass(HadoopBlockPartitioner.class);
        job6.setSortComparatorClass(ComposedKeyComparator.class);
        job6.setGroupingComparatorClass(ComposedKeyGroupComparator.class);
        FileInputFormat.setInputPaths(job6, new Path(hadoopDir + "/sibSorting4"));
        FileOutputFormat.setOutputPath(job6, new Path(hadoopDir + "/job6") );


        printProgress("Starting: Materialize friends for substitution parameters");
        int resMaterializeFriends = job6.waitForCompletion(true) ? 0 : 1;
        fs.delete(new Path(hadoopDir + "/sibSorting3"),true);


	    long end = System.currentTimeMillis();
	    System.out.println(((end - start) / 1000)
	                    + " total seconds");
        for( int i = 0; i < numThreads; ++i ) {
            fs.copyToLocalFile(new Path(socialNetDir + "/m"+i+"factors.txt"), new Path("./"));
            fs.copyToLocalFile(new Path(socialNetDir + "/m0friendList"+i+".csv"), new Path("./"));
        }
        return 0;
	}

	public static void main(String[] args)  throws Exception{

        Configuration conf = ConfigParser.GetConfig(args[0]);
        ConfigParser.PringConfig(conf);


        // Deleting exisging files
        FileSystem dfs = FileSystem.get(conf);
        dfs.delete(new Path(conf.get("outputDir")+"/hadoop"), true);
        dfs.delete(new Path(conf.get("outputDir")+"/social_network"), true);

		// Create input text file in HDFS
		writeToOutputFile(conf.get("outputDir") + "/hadoop/mrInputFile", Integer.parseInt(conf.get("numThreads")), conf);
		MRGenerateUsers mrGenerator = new MRGenerateUsers();
		mrGenerator.runGenerateJob(conf);

	}

	public static void writeToOutputFile(String filename, int numMaps, Configuration conf){
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
