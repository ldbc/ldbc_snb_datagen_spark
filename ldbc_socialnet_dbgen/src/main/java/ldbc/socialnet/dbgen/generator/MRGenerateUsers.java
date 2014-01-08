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

import ldbc.socialnet.dbgen.objects.ReducedUserProfile;

import org.apache.hadoop.conf.Configuration;
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

	public static class GenerateUsersMapper extends Mapper <LongWritable, Text, IntWritable, ReducedUserProfile> {
			
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
			numMappers = Integer.parseInt(conf.get("numberMappers"));

			// Read the fileid from mapreduce input file
			fileIdx = Integer.parseInt(value.toString());
			System.out.println("Generating user at mapper " + fileIdx);

			ScalableGenerator generator;
			generator = new ScalableGenerator(fileIdx, outputDir, homeDir);
			System.out.println("Successfully init Generator object");
			
			generator.numMaps = numMappers;
			String[] inputParams = new String[0]; 
			generator.init(numMappers, fileIdx);
			
			//Generate all the users 
			int pass = 0; 
			int numCorrelations = 3;
			
			// Generate user information
			generator.mrGenerateUserInfo(pass, context, fileIdx);
			
			System.out.println("Total friendship number from " + fileIdx + " : " + generator.friendshipNum);
		}
		
	}
	
	public static class UniversityReducer extends Reducer<IntWritable, ReducedUserProfile, IntWritable, ReducedUserProfile>{
		
		public static ScalableGenerator friendGenerator; 
		private String outputDir; 
		private String homeDir; 
		private int numReducer;
		private int attempTaskId; 
		
		@Override
		protected void setup(Context context){
			Configuration conf = context.getConfiguration();
			homeDir = conf.get("sibHomeDir").toString();
			outputDir = conf.get("sibOutputDir").toString();
			numReducer = Integer.parseInt(conf.get("numberMappers"));
			
			String strTaskId = context.getTaskAttemptID().getTaskID().toString();
			attempTaskId = Integer.parseInt(strTaskId.substring(strTaskId.length() - 3));
			System.out.println("Task Id from setup: " + attempTaskId);
			
			friendGenerator = new ScalableGenerator(attempTaskId, outputDir, homeDir);
			
			friendGenerator.numMaps = numReducer; 
			String[] inputParams = new String[0]; 
			friendGenerator.init(numReducer, 0);
			
			System.out.println("Cell size = " + friendGenerator.getCellSize());
		}
		
		@Override
		public void reduce(IntWritable key, Iterable<ReducedUserProfile> valueSet, 
				Context context) throws IOException, InterruptedException{	
				for (ReducedUserProfile user:valueSet){
					friendGenerator.pushUserProfile(user, 0, context, true, null);
				}
		}
		@Override
		protected void cleanup(Context context){
			System.out.println("Number of user profile already read " + friendGenerator.numUserProfilesRead);
			System.out.println("Start putting remaining users ");
			friendGenerator.pushAllRemainingUser(0, context, true, null);
			System.out.println("Summary for " + attempTaskId);
			System.out.println("Number of user profile read " + friendGenerator.numUserProfilesRead);
			System.out.println("Number of exact user profile out " + friendGenerator.exactOutput);
			System.out.println("Number of exact friend added " + friendGenerator.friendshipNum);
		}
	}
	
	// Parition for second Id of interest
	
	public static class UniversityPartitioner extends Partitioner<IntWritable, ReducedUserProfile> {
		//private int	numDifKey = 8000;  

		double[] interestKey;
		
		public UniversityPartitioner(){
			super(); 
	
		}

		@Override
		public int getPartition(IntWritable key, ReducedUserProfile value,
				int numReduceTasks) {
				return -1; 
		}
	}
	
	public static class InterestMapper extends  Mapper <IntWritable, ReducedUserProfile, IntWritable, ReducedUserProfile> {
		
		@Override
		public void map(IntWritable key, ReducedUserProfile value, 
				Context context)
				throws IOException, InterruptedException {
			//System.out.println("Call Map function");
			//System.out.println("Key:" + value.getDicElementId(1));
			context.write(new IntWritable(value.getDicElementId(1)), value);
		}
	}
	
	public static class InterestReducer extends Reducer<IntWritable, ReducedUserProfile, IntWritable, ReducedUserProfile>{
		
		public static ScalableGenerator friendGenerator; 
		private String outputDir; 
		private String homeDir; 
		private int numReducer;
		private int attempTaskId; 
		
		@Override
		protected void setup(Context context){
			Configuration conf = context.getConfiguration();
			homeDir = conf.get("sibHomeDir").toString();
			outputDir = conf.get("sibOutputDir").toString();
			numReducer = Integer.parseInt(conf.get("numberMappers"));
			
			String strTaskId = context.getTaskAttemptID().getTaskID().toString();
			attempTaskId = Integer.parseInt(strTaskId.substring(strTaskId.length() - 3));
			System.out.println("Task Id from setup: " + attempTaskId);
			
			friendGenerator = new ScalableGenerator(attempTaskId, outputDir, homeDir);
			
			friendGenerator.numMaps = numReducer; 
			String[] inputParams = new String[0]; 
			friendGenerator.init(numReducer, 0);
			
			System.out.println("Cell size = " + friendGenerator.getCellSize());
		}
		
		@Override
		public void reduce(IntWritable key, Iterable<ReducedUserProfile> valueSet, 
				Context context) throws IOException, InterruptedException{	
				int numInterestKey = 0; 
				for (ReducedUserProfile user:valueSet){
					friendGenerator.pushUserProfile(user, 1, context, true, null);
					numInterestKey++;
				}
		}
		@Override
		protected void cleanup(Context context){
			System.out.println("Number of user profile already read " + friendGenerator.numUserProfilesRead);
			System.out.println("Start putting remaining users ");
			friendGenerator.pushAllRemainingUser(1, context, true, null);
			System.out.println("Summary for " + attempTaskId);
			System.out.println("Number of user profile read " + friendGenerator.numUserProfilesRead);
			System.out.println("Number of exact user profile out " + friendGenerator.exactOutput);
			System.out.println("Number of exact friend added " + friendGenerator.friendshipNum);
		}
	}
	
	public static class RandomMapper extends  Mapper <IntWritable, ReducedUserProfile, IntWritable, ReducedUserProfile> {
		
		@Override
		public void map(IntWritable key, ReducedUserProfile value, 
				Context context)
				throws IOException, InterruptedException {
			context.write(new IntWritable(value.getDicElementId(2)), value);
		}
	}
	public static class RandomReducer extends Reducer <IntWritable, ReducedUserProfile, IntWritable, ReducedUserProfile>{
		
		public static ScalableGenerator friendGenerator; 
		private String outputDir; 
		private String homeDir; 
		private int numReducer;
		private int attempTaskId;
		
		private	String	outputFileName;
		
		private FileOutputStream fos;
		private ObjectOutputStream oos; 
		private static int	numObject; 
		
		@Override
		protected void setup(Context context){
			Configuration conf = context.getConfiguration();
			homeDir = conf.get("sibHomeDir").toString();
			outputDir = conf.get("sibOutputDir").toString();
			numReducer = Integer.parseInt(conf.get("numberMappers"));
			
			String strTaskId = context.getTaskAttemptID().getTaskID().toString();
			attempTaskId = Integer.parseInt(strTaskId.substring(strTaskId.length() - 3));
			System.out.println("Task Id from setup: " + attempTaskId);
			
			outputFileName =  "_userProf_" + attempTaskId;
			
			try {
				fos = new FileOutputStream(outputDir + outputFileName);
				oos = new ObjectOutputStream(fos);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
				
			friendGenerator = new ScalableGenerator(attempTaskId, outputDir, homeDir);
			friendGenerator.numMaps = numReducer; 
			String[] inputParams = new String[0]; 
			friendGenerator.init(numReducer, attempTaskId);
			
			numObject = 0; 
		}
		
		@Override
		public void reduce(IntWritable key, Iterable<ReducedUserProfile> valueSet, 
			Context context) throws IOException, InterruptedException{
			
			for (ReducedUserProfile user:valueSet){
				friendGenerator.pushUserProfile(user, 2, context, false, oos);
				numObject++;
			}
		}
		
		@Override
		protected void cleanup(Context context){
			System.out.println("Number of output object is: " + numObject);
			friendGenerator.pushAllRemainingUser(2, context, false, oos);
			try {
				oos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			int numofCell = numObject / friendGenerator.getCellSize();
			friendGenerator.generateUserActivity(outputFileName, numofCell);
		}
	}
	
	public static class RandomPartitioner extends Partitioner<IntWritable, ReducedUserProfile> {
		private int	numDifKey = 100;  // InterestId from 0 to 33 
		
		@Override
		public int getPartition(IntWritable key, ReducedUserProfile value,
				int numReduceTasks) {
			int numItemPerReduce;
			int extra = numDifKey % numReduceTasks;
			
			numItemPerReduce = (numDifKey )/numReduceTasks;
			
			int dividePortion = extra * (numItemPerReduce+1);
			
			int i = key.get();
			if (i < dividePortion)
				return (i/(numItemPerReduce+1));  //Return the year mod number of reduce tasks as the partitioner number to send the record to.
			else
				return ((i-dividePortion)/numItemPerReduce + extra);
		}
	}
	public int runGenerateJob(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		
		int numMachines = Integer.parseInt(args[2]);
		// Set parameter 
		conf.set("numberMappers", args[2]);
		conf.set("sibHomeDir", args[3]);
		conf.set("sibOutputDir", args[4]);
		
		Job job = new Job(conf,"SIB Generate Users & 1st Dimension");
	
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(ReducedUserProfile.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(ReducedUserProfile.class);
		
		
		job.setJarByClass(GenerateUsersMapper.class);
		
		job.setMapperClass(GenerateUsersMapper.class);
		job.setReducerClass(UniversityReducer.class);
		job.setNumReduceTasks(numMachines);
		
		job.setInputFormatClass(NLineInputFormat.class);
		conf.setInt("mapred.line.input.format.linespermap", 1);	
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    
	    /// --------------- second job ----------------
	    
		Job job2 = new Job(conf,"SIB Generate Friendship - Interest");
		
		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(ReducedUserProfile.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(ReducedUserProfile.class);
		
		
		job2.setJarByClass(InterestMapper.class);
		
		job2.setMapperClass(InterestMapper.class);

		job2.setReducerClass(InterestReducer.class);
		job2.setNumReduceTasks(numMachines);
		
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		
	    FileInputFormat.setInputPaths(job2, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job2, new Path(args[1] + "2") );
	    
	    
	    /// --------------- Third job ----------------
	    
		Job job3 = new Job(conf,"SIB Generate Friendship - Random");
		
		job3.setMapOutputKeyClass(IntWritable.class);
		job3.setMapOutputValueClass(ReducedUserProfile.class);
		job3.setOutputKeyClass(IntWritable.class);
		job3.setOutputValueClass(ReducedUserProfile.class);
		
		
		job3.setJarByClass(RandomMapper.class);
		
		job3.setMapperClass(RandomMapper.class);

		job3.setReducerClass(RandomReducer.class);
		job3.setNumReduceTasks(numMachines);
		
		job3.setInputFormatClass(SequenceFileInputFormat.class);
		job3.setOutputFormatClass(SequenceFileOutputFormat.class);
		job3.setPartitionerClass(RandomPartitioner.class);
		
	    FileInputFormat.setInputPaths(job3, new Path(args[1] + "2"));
	    FileOutputFormat.setOutputPath(job3, new Path(args[1] + "3") );
	    /// --------- Execute Jobs ------
	    
	    long start = System.currentTimeMillis();
	
	    int res = job.waitForCompletion(true) ? 0 : 1;
	    
	    int res2 = job2.waitForCompletion(true) ? 0 : 1;
	    
	    int res3 = job3.waitForCompletion(true) ? 0 : 1;
	    
	    long end = System.currentTimeMillis();
	    System.out.println(((end - start) / 1000)
	                    + " total seconds");
	    return res; 
	}
	
	public static void main(String[] args)  throws Exception{
		
		int numMapers;
		// Create input text file in HDFS
		numMapers = Integer.parseInt(args[2]); 
		
		String mapperInputFile = "mrInputFile.txt";
		
		FileSystem dfs = FileSystem.get(new Configuration());
		
		writeToOutputFile(mapperInputFile, numMapers);
		
		Path src = new Path(mapperInputFile);

		Path dst = new Path(dfs.getWorkingDirectory()+"/input/sib/");

		System.out.println("DFS Working directory is " + dfs.getWorkingDirectory());
		
		dfs.copyFromLocalFile(src, dst);
		
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
