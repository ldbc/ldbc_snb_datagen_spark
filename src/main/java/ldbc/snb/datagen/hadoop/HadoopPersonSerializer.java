package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.objects.Person;
import ldbc.snb.datagen.objects.ReducedUserProfile;
import ldbc.snb.datagen.serializer.NewDataExporter;
import ldbc.snb.datagen.serializer.snb.SNBPersonSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * Created by aprat on 10/15/14.
 */
public class HadoopPersonSerializer {

    public static class HadoopPersonSerializerMapper  extends Mapper<LongWritable, Person, LongWritable, Person> {
        int mapId;
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            String strTaskId = context.getTaskAttemptID().getTaskID().toString();
            DatagenParams.readConf(conf);
            DatagenParams.readParameters("params.ini");
        }

        @Override
        public void map(LongWritable key, Person value, Mapper.Context context)
                throws IOException, InterruptedException {
            context.write(new LongWritable(key.get()/DatagenParams.blockSize),value);
        }
    }

    public static class HadoopPersonSerializerReducer  extends Reducer<LongWritable, Person, LongWritable, Person> {
        private int reducerId;
        private SNBPersonSerializer personSerializer;
        private NewDataExporter dataExporter;
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            String strTaskId = context.getTaskAttemptID().getTaskID().toString();
            reducerId = Integer.parseInt(strTaskId.substring(strTaskId.length() - 3));
            personSerializer = new SNBPersonSerializer(conf.get("outputDir")+"/social_network",
                                                       Integer.toString(reducerId),
                                                       conf.getInt("numPartitions",1),
                                                       conf.getBoolean("compressed",false));
            dataExporter = new NewDataExporter(personSerializer);
        }

        @Override
        public void reduce(LongWritable key, Iterable<Person> valueSet,Context context)
                throws IOException, InterruptedException {
            for( Person p : valueSet ) {
                dataExporter.export(p);
            }

        }
        protected void cleanup(Context context){
           personSerializer.close();
        }
    }

    public static class HadoopPersonSerializerPartitioner extends Partitioner<LongWritable, Person> {

        public HadoopPersonSerializerPartitioner() {
            super();

        }
        @Override
        public int getPartition(LongWritable key, Person person, int numReduceTasks) {
            return (int)(key.get() % numReduceTasks);
        }
    }

    private Configuration conf;

    public HadoopPersonSerializer( Configuration conf ) {
        this.conf = new Configuration(conf);
    }

    public void run( String inputFileName ) throws Exception {

        int numThreads = Integer.parseInt(conf.get("numThreads"));
        Job job = new Job(conf, "Person Serializer");
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Person.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Person.class);
        job.setJarByClass(HadoopPersonSerializerMapper.class);
        job.setMapperClass(HadoopPersonSerializerMapper.class);
        job.setReducerClass(HadoopPersonSerializerReducer.class);
        job.setNumReduceTasks(numThreads);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setPartitionerClass(HadoopPersonSerializerPartitioner.class);
        //job.setSortComparatorClass(MapReduceKeyComparator.class);
        //job.setGroupingComparatorClass(MapReduceKeyGroupKeyComparator.class);
        FileInputFormat.setInputPaths(job, new Path(inputFileName));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("outputDir")+"/hadoop/aux"));
        job.waitForCompletion(true);
    }
}
