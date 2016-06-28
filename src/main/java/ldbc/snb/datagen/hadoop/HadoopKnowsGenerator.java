package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.generator.ClusteringKnowsGenerator;
import ldbc.snb.datagen.generator.DistanceKnowsGenerator;
import ldbc.snb.datagen.generator.KnowsGenerator;
import ldbc.snb.datagen.generator.LDBCDatagen;
import ldbc.snb.datagen.objects.Person;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

    public static class HadoopKnowsGeneratorReducer  extends Reducer<BlockKey, Person, TupleKey, Person> {

        private KnowsGenerator knowsGenerator;   /** The person serializer **/
        private Configuration conf;
        private HadoopFileKeyChanger.KeySetter<TupleKey> keySetter = null;
        private ArrayList<Float> percentages;
        private int step_index;
        private int numGeneratedEdges = 0;

        protected void setup(Context context) {
            //this.knowsGenerator = new DistanceKnowsGenerator();
            this.conf = context.getConfiguration();
            LDBCDatagen.init(conf);
            try {
                this.knowsGenerator = (KnowsGenerator) Class.forName(conf.get("knowsGeneratorName")).newInstance();
                this.knowsGenerator.initialize(conf);
            }catch(Exception e) {
                System.out.println(e.getMessage());
            }
            this.percentages = new ArrayList<Float>();
            this.step_index = conf.getInt("stepIndex",0);
            float p = conf.getFloat("percentage0",0.0f);
            int index = 1;
            while(p != 0.0f) {
                this.percentages.add(p);
                p = conf.getFloat("percentage"+index,0.0f);
                ++index;
            }
            try {
                this.keySetter = (HadoopFileKeyChanger.KeySetter) Class.forName(conf.get("postKeySetterName")).newInstance();
            }catch(Exception e) {
                System.out.println(e.getMessage());
                e.printStackTrace();
            }
        }

        @Override
        public void reduce(BlockKey key, Iterable<Person> valueSet,Context context)
                throws IOException, InterruptedException {
            ArrayList<Person> persons = new ArrayList<Person>();
            for( Person p : valueSet ) {
                persons.add(new Person(p));
            }
            this.knowsGenerator.generateKnows(persons, (int)key.block, percentages, step_index);
            for( Person p : persons ) {
                context.write(keySetter.getKey(p), p);
                numGeneratedEdges+=p.knows().size();
            }
        }

        @Override
        public void cleanup(Context context) {
            System.out.println("Number of generated edges: "+numGeneratedEdges/2);
        }
    }

    private Configuration conf;
    private String preKeySetterName;
    private String postKeySetterName;
    private String knowsGeneratorName;
    private ArrayList<Float> percentages;
    private int step_index;


    public HadoopKnowsGenerator( Configuration conf, String preKeySetterName, String postKeySetterName, ArrayList<Float> percentages, int step_index, String knowsGeneratorName  ) {
        this.conf = new Configuration(conf);
        this.preKeySetterName = preKeySetterName;
        this.postKeySetterName = postKeySetterName;
        this.percentages = percentages;
        this.step_index = step_index;
        this.knowsGeneratorName = knowsGeneratorName;
    }

    public void run( String inputFileName, String outputFileName ) throws Exception {


        FileSystem fs = FileSystem.get(conf);

        /*String keyChangedFileName = inputFileName;
        if(preKeySetterName != null) {
            System.out.println("Changing key of persons");
            long start = System.currentTimeMillis();
            keyChangedFileName = conf.get("ldbc.snb.datagen.serializer.hadoopDir") + "/key_changed";
            HadoopFileKeyChanger keyChanger = new HadoopFileKeyChanger(conf, TupleKey.class, Person.class, preKeySetterName);
            keyChanger.run(inputFileName, keyChangedFileName);
            System.out.println("... Time to change keys: "+ (System.currentTimeMillis() - start)+" ms");
        }
        */

        System.out.println("Ranking persons");
        long start = System.currentTimeMillis();
        String rankedFileName = conf.get("ldbc.snb.datagen.serializer.hadoopDir") + "/ranked";
        HadoopFileRanker hadoopFileRanker = new HadoopFileRanker( conf, TupleKey.class, Person.class , preKeySetterName);
        hadoopFileRanker.run(inputFileName,rankedFileName);
/*        if(preKeySetterName != null ) {
            fs.delete(new Path(keyChangedFileName), true);
        }
        */
        System.out.println("... Time to rank persons: " + (System.currentTimeMillis() - start) + " ms");

        conf.setInt("stepIndex", step_index);
        int index = 0;
        for( float p : percentages ) {
            conf.setFloat("percentage"+index, p);
            ++index;
        }
        conf.set("postKeySetterName",postKeySetterName);
        conf.set("knowsGeneratorName", knowsGeneratorName);
        int numThreads = Integer.parseInt(conf.get("ldbc.snb.datagen.generator.numThreads"));
        Job job = Job.getInstance(conf, "Knows generator");
        job.setMapOutputKeyClass(BlockKey.class);
        job.setMapOutputValueClass(Person.class);
        job.setOutputKeyClass(TupleKey.class);
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

        System.out.println("Generating knows relations");
        start = System.currentTimeMillis();
        if(!job.waitForCompletion(true) ){
            throw new Exception();
        }
        System.out.println("... Time to generate knows relations: "+ (System.currentTimeMillis() - start)+" ms");

        fs.delete(new Path(rankedFileName), true);
    }
}
