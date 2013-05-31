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
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapReduceGenerator extends Configured implements Tool {
	 
	public static class SIBMapper extends MapReduceBase 
		implements Mapper<LongWritable, Text, Text, LongWritable> {
		
		private String outputDir; 
		private String homeDir; 
		private int numMappers; 
		
		@Override
		public void configure(JobConf job) {

			homeDir = job.get("sibHomeDir").toString();
			outputDir = job.get("sibOutputDir").toString();
			numMappers = Integer.parseInt(job.get("numberMappers"));
			
		}
		@Override
		public void map(LongWritable key, Text value, 
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {

			
			// Read the fileid from mapreduce input file
			int fileIdx; 

			fileIdx = Integer.parseInt(value.toString());

			ScalableGenerator generator;
			System.out.println("File idx for the generator is: " + fileIdx);
			System.out.println("Number of mappers: " + numMappers);
			
			String currentDir = new File(".").getAbsolutePath();
			System.out.println("Mapper default directory is " + currentDir);
			
			System.setProperty("user.dir", homeDir); 
			
			currentDir = new File(".").getAbsolutePath();
			System.out.println("Set current directory is " + currentDir);
			
			generator = new ScalableGenerator(fileIdx, outputDir, homeDir);
			System.out.println("Successfully init Generator object");
			
			generator.numMaps = numMappers;
			String[] inputParams = new String[0]; 
			String[] filenames = generator.prepareMRInput(inputParams, numMappers, "mr"+fileIdx+"_"+"fileList.txt");

			
			int numOfCell; 
			if (fileIdx == (numMappers -1) )
				numOfCell = generator.numCellInLastFile;
			else
				numOfCell = generator.numCellPerfile;
			
			generator.mapreduceTask(filenames[fileIdx], numOfCell);
			
			output.collect(value, key);
			
			// Call for generator function
			
		}
		
	}
	public static class SIBReducer extends MapReduceBase 
		implements Reducer<Text, LongWritable, Text, LongWritable>{ 
		public void reduce(Text key, Iterator<LongWritable> valueSet, 
				OutputCollector<Text, LongWritable> arg2, Reporter arg3) throws IOException{
		
		}
		
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		JobConf job = new JobConf(conf);
		job.setJobName("SIB Generator");

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setJarByClass(MapReduceGenerator.class);
		job.setMapperClass(SIBMapper.class);
		//job.setCombinerClass(SIBReducer.class);
		job.setReducerClass(IdentityReducer.class);

		 
		job.setInputFormat(NLineInputFormat.class);
		job.setInt("mapred.line.input.format.linespermap", 1);	
		
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.set("numberMappers", args[2]);
		job.set("sibHomeDir", args[3]);
		job.set("sibOutputDir", args[4]);
		
        
        long start = System.currentTimeMillis();
        JobClient.runJob(job);

        long end = System.currentTimeMillis();
        System.out.println(((end - start) / 1000)
                        + " total seconds");
        return 0; 
	}
	
	public static void main(String[] args) throws Exception
	{
		
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
		
		
		int res = ToolRunner.run(new Configuration(), new MapReduceGenerator(), args);
		System.exit(res);
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

