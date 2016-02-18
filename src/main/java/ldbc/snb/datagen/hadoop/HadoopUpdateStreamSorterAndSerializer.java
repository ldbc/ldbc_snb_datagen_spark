package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.serializer.PersonSerializer;
import ldbc.snb.datagen.serializer.UpdateEventSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * Created by aprat on 10/15/14.
 */
public class HadoopUpdateStreamSorterAndSerializer {

	public static class HadoopUpdateStreamSorterAndSerializerReducer  extends Reducer<UpdateEventKey, Text, UpdateEventKey, Text> {

		private boolean compressed = false;
		private Configuration conf;
		private String streamType;

		protected void setup(Context context) {
			conf = context.getConfiguration();
			streamType = conf.get("streamType");
			try {
				compressed = Boolean.parseBoolean(conf.get("ldbc.snb.datagen.serializer.compressed"));
			} catch( Exception e) {
				System.err.println(e.getMessage());
			}
		}

		@Override
		public void reduce(UpdateEventKey key, Iterable<Text> valueSet,Context context)
				throws IOException, InterruptedException {
			OutputStream out;
			try {
				FileSystem fs = FileSystem.get(conf);
				if(  compressed ) {
					Path outFile = new Path(context.getConfiguration().get("ldbc.snb.datagen.serializer.socialNetworkDir")+"/updateStream_"+key.reducerId+"_"+key.partition+"_"+streamType+".csv.gz");
					out = new GZIPOutputStream( fs.create(outFile));
				} else {
					Path outFile = new Path(context.getConfiguration().get("ldbc.snb.datagen.serializer.socialNetworkDir")+"/updateStream_"+key.reducerId+"_"+key.partition+"_"+streamType+".csv");
					out = fs.create(outFile);
				}
				int counter = 0;
				for( Text t : valueSet ) {
					counter++;
					out.write(t.toString().getBytes("UTF8"));
				}
				out.close();
			} catch( Exception e ) {
				System.err.println(e.getMessage());
			}
		}
		protected void cleanup(Context context){
			try {
			} catch( Exception e ) {
				System.err.println(e.getMessage());
			}
		}
	}


	private Configuration conf;

	public HadoopUpdateStreamSorterAndSerializer(Configuration conf ) {
		this.conf = new Configuration(conf);
	}
	
	public void run(List<String> inputFileNames, String type ) throws Exception {

		int numThreads = conf.getInt("ldbc.snb.datagen.generator.numThreads",1);
		conf.set("streamType", type);
		
		Job job = Job.getInstance(conf, "Update Stream Serializer");
		job.setMapOutputKeyClass(UpdateEventKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(UpdateEventKey.class);
		job.setOutputValueClass(Text.class);
        job.setJarByClass(HadoopUpdateStreamSorterAndSerializerReducer.class);
		job.setReducerClass(HadoopUpdateStreamSorterAndSerializerReducer.class);
		job.setNumReduceTasks(numThreads);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setPartitionerClass(HadoopUpdateEventKeyPartitioner.class);
		job.setGroupingComparatorClass(UpdateEventKeyGroupComparator.class);
		//job.setSortComparatorClass(UpdateEventKeySortComparator.class);

		for(String s : inputFileNames) {
			FileInputFormat.addInputPath(job, new Path(s));
		}
		FileOutputFormat.setOutputPath(job, new Path(conf.get("ldbc.snb.datagen.serializer.hadoopDir")+"/aux"));
		if(!job.waitForCompletion(true)) {
            throw new Exception();
        }
		
		
		try{
			FileSystem fs = FileSystem.get(conf);
			fs.delete(new Path(conf.get("ldbc.snb.datagen.serializer.hadoopDir")+"/aux"),true);
		} catch(IOException e) {
			System.err.println(e.getMessage());
		}
	}
}

