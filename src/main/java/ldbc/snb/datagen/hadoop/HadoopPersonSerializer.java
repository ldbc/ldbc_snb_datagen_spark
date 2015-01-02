package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.objects.Person;
import ldbc.snb.datagen.serializer.PersonSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;

/**
 * Created by aprat on 10/15/14.
 */
public class HadoopPersonSerializer {
	
	public static class HadoopPersonSerializerReducer  extends Reducer<BlockKey, Person, LongWritable, Person> {
		
		private int reducerId;                          /** The id of the reducer.**/
		private PersonSerializer personSerializer_;   /** The person serializer **/
		
		protected void setup(Context context) {
			Configuration conf = context.getConfiguration();
			reducerId = context.getTaskAttemptID().getTaskID().getId();
			try {
				personSerializer_ = (PersonSerializer) Class.forName(conf.get("ldbc.snb.datagen.serializer.personSerializer")).newInstance();
				personSerializer_.initialize(conf,reducerId);
			} catch( Exception e ) {
				System.err.println(e.getMessage());
			}
		}
		
		@Override
		public void reduce(BlockKey key, Iterable<Person> valueSet,Context context)
			throws IOException, InterruptedException {
			for( Person p : valueSet ) {
				personSerializer_.export(p);
			}
			
		}
		protected void cleanup(Context context){
			personSerializer_.close();
		}
	}
	
	
	private Configuration conf;
	
	public HadoopPersonSerializer( Configuration conf ) {
		this.conf = new Configuration(conf);
	}
	
	public void run( String inputFileName ) throws Exception {
		
		FileSystem fs = FileSystem.get(conf);
		String keyChangedFileName = conf.get("ldbc.snb.datagen.serializer.hadoopDir") + "/key_changed";
		HadoopFileKeyChanger keyChanger = new HadoopFileKeyChanger(conf, LongWritable.class,Person.class,"ldbc.snb.datagen.hadoop.RandomKeySetter");
		keyChanger.run(inputFileName,keyChangedFileName);
		
		
		String rankedFileName = conf.get("ldbc.snb.datagen.serializer.hadoopDir") + "/ranked";
		HadoopFileRanker hadoopFileRanker = new HadoopFileRanker( conf, TupleKey.class, Person.class );
		hadoopFileRanker.run(keyChangedFileName,rankedFileName);
		fs.delete(new Path(keyChangedFileName), true);
		
		int numThreads = Integer.parseInt(conf.get("ldbc.snb.datagen.generator.numThreads"));
		Job job = Job.getInstance(conf, "Person Serializer");
		job.setMapOutputKeyClass(BlockKey.class);
		job.setMapOutputValueClass(Person.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Person.class);
		job.setJarByClass(HadoopBlockMapper.class);
		job.setMapperClass(HadoopBlockMapper.class);
		job.setReducerClass(HadoopPersonSerializerReducer.class);
		job.setNumReduceTasks(numThreads);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setSortComparatorClass(BlockKeyComparator.class);
		job.setGroupingComparatorClass(BlockKeyGroupComparator.class);
		job.setPartitionerClass(HadoopBlockPartitioner.class);
		
		FileInputFormat.setInputPaths(job, new Path(rankedFileName));
		FileOutputFormat.setOutputPath(job, new Path(conf.get("ldbc.snb.datagen.serializer.hadoopDir")+"/aux"));
		job.waitForCompletion(true);
		
		
		try{
			fs.delete(new Path(rankedFileName), true);
			fs.delete(new Path(conf.get("ldbc.snb.datagen.serializer.hadoopDir")+"/aux"),true);
		} catch(IOException e) {
			System.err.println(e.getMessage());
		}
	}
}
