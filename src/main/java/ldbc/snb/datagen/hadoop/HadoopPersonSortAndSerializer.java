package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.generator.LDBCDatagen;
import ldbc.snb.datagen.objects.Knows;
import ldbc.snb.datagen.objects.Person;
import ldbc.snb.datagen.serializer.PersonSerializer;
import ldbc.snb.datagen.serializer.UpdateEventSerializer;
import ldbc.snb.datagen.vocabulary.SN;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * Created by aprat on 10/15/14.
 */
public class HadoopPersonSortAndSerializer {

	public static class HadoopPersonSerializerReducer  extends Reducer<BlockKey, Person, LongWritable, Person> {

		private int reducerId;                          /** The id of the reducer.**/
		private PersonSerializer personSerializer_;   /** The person serializer **/
		private UpdateEventSerializer updateSerializer_;

		@Override
		protected void setup(Context context) {
			Configuration conf = context.getConfiguration();
			reducerId = context.getTaskAttemptID().getTaskID().getId();
            LDBCDatagen.init(conf);
			try {
				personSerializer_ = (PersonSerializer) Class.forName(conf.get("ldbc.snb.datagen.serializer.personSerializer")).newInstance();
				personSerializer_.initialize(conf,reducerId);
				if (DatagenParams.updateStreams) {
					updateSerializer_ = new UpdateEventSerializer(conf, DatagenParams.hadoopDir + "/temp_updateStream_person_" + reducerId, reducerId, DatagenParams.numUpdatePartitions);
				}
			} catch( Exception e ) {
				System.err.println(e.getMessage());
				e.printStackTrace();
			}
		}

		@Override
		public void reduce(BlockKey key, Iterable<Person> valueSet,Context context)
			throws IOException, InterruptedException {
			SN.machineId = key.block;
			personSerializer_.reset();
			for( Person p : valueSet ) {
				if(p.creationDate()< Dictionaries.dates.getUpdateThreshold() || !DatagenParams.updateStreams  ) {
					personSerializer_.export(p);
				} else {
					updateSerializer_.export(p);
                    updateSerializer_.changePartition();
				}

				for( Knows k : p.knows() ) {
					if( k.creationDate() < Dictionaries.dates.getUpdateThreshold() || !DatagenParams.updateStreams ) {
						personSerializer_.export(p, k);
					}
				}
			}

		}
		@Override
		protected void cleanup(Context context){
			personSerializer_.close();
			if (DatagenParams.updateStreams) {
				updateSerializer_.close();
			}
		}
	}


	private Configuration conf;

	public HadoopPersonSortAndSerializer(Configuration conf ) {
		this.conf = new Configuration(conf);
	}
	
	public void run( String inputFileName ) throws Exception {
		
		FileSystem fs = FileSystem.get(conf);

		String rankedFileName = conf.get("ldbc.snb.datagen.serializer.hadoopDir") + "/ranked";
		HadoopFileRanker hadoopFileRanker = new HadoopFileRanker( conf, TupleKey.class, Person.class, null );
        hadoopFileRanker.run(inputFileName,rankedFileName);

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

		job.setPartitionerClass(HadoopTuplePartitioner.class);

		job.setSortComparatorClass(BlockKeyComparator.class);
		job.setGroupingComparatorClass(BlockKeyGroupComparator.class);
		job.setPartitionerClass(HadoopBlockPartitioner.class);

		FileInputFormat.setInputPaths(job, new Path(rankedFileName));
		FileOutputFormat.setOutputPath(job, new Path(conf.get("ldbc.snb.datagen.serializer.hadoopDir")+"/aux"));
		if(!job.waitForCompletion(true)) {
            throw new Exception();
        }
		
		
		try{
			fs.delete(new Path(rankedFileName), true);
			fs.delete(new Path(conf.get("ldbc.snb.datagen.serializer.hadoopDir")+"/aux"),true);
		} catch(IOException e) {
			System.err.println(e.getMessage());
		}
	}
}
