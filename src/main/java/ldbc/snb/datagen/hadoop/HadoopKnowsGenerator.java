package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.generator.KnowsGenerator;
import ldbc.snb.datagen.objects.Person;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by aprat on 11/17/14.
 */
public class HadoopKnowsGenerator {

    public static class HadoopKnowsGeneratorReducer  extends Reducer<BlockKey, Person, LongWritable, Person> {

        private KnowsGenerator knowsGenerator;   /** The person serializer **/
        private Configuration conf;

        protected void setup(Context context) {
            this.knowsGenerator = new KnowsGenerator();
            this.conf = context.getConfiguration();
            DatagenParams.readConf(conf);
        }

        @Override
        public void reduce(BlockKey key, Iterable<Person> valueSet,Context context)
                throws IOException, InterruptedException {
            ArrayList<Person> persons = new ArrayList<Person>();
            for( Person p : valueSet ) {
                persons.add(new Person(p));
            }
            this.knowsGenerator.generateKnows(persons, (int)key.block, conf.getFloat("upperBound", 0.1f));
            for( Person p : persons ) {
                context.write(new LongWritable(p.accountId), p);
            }
        }
    }

    private Configuration conf;
    private double upperBound;
    private String keySetterName;


    public HadoopKnowsGenerator( Configuration conf, String keySetterName, float upperBound ) {
        this.conf = conf;
        this.upperBound = upperBound;
        this.keySetterName = keySetterName;
    }

    public void run( String inputFileName, String outputFileName ) throws Exception {


        FileSystem fs = FileSystem.get(conf);

        String keyChangedFileName = conf.get("ldbc.snb.datagen.serializer.hadoopDir") + "/key_changed";
        HadoopFileKeyChanger keyChanger = new HadoopFileKeyChanger(conf, LongWritable.class,Person.class,keySetterName);
        keyChanger.run(inputFileName,keyChangedFileName);


        String rankedFileName = conf.get("ldbc.snb.datagen.serializer.hadoopDir") + "/ranked";
        HadoopFileRanker hadoopFileRanker = new HadoopFileRanker( conf, TupleKey.class, Person.class );
        hadoopFileRanker.run(keyChangedFileName,rankedFileName);
        fs.delete(new Path(keyChangedFileName), true);

        conf.set("upperBound",Double.toString(upperBound));
        int numThreads = Integer.parseInt(conf.get("ldbc.snb.datagen.numThreads"));
        Job job = new Job(conf, "Knows generator");
        job.setMapOutputKeyClass(BlockKey.class);
        job.setMapOutputValueClass(Person.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Person.class);
        job.setJarByClass(HadoopBlockMapper.class);
        job.setMapperClass(HadoopBlockMapper.class);
        job.setReducerClass(HadoopKnowsGeneratorReducer.class);
        job.setNumReduceTasks(numThreads);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setSortComparatorClass(BlockKeyComparator.class);
        job.setGroupingComparatorClass(BlockKeyGroupComparator.class);
        job.setPartitionerClass(HadoopBlockPartitioner.class);

        FileInputFormat.setInputPaths(job, new Path(rankedFileName));
        FileOutputFormat.setOutputPath(job, new Path(outputFileName));
        job.waitForCompletion(true);
        fs.delete(new Path(rankedFileName), true);
    }
}
