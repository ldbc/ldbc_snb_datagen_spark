/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.generator.LDBCDatagen;
import ldbc.snb.datagen.generator.PersonActivityGenerator;
import ldbc.snb.datagen.objects.Knows;
import ldbc.snb.datagen.objects.Person;
import ldbc.snb.datagen.serializer.PersonActivitySerializer;
import ldbc.snb.datagen.serializer.UpdateEventSerializer;
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
import java.io.OutputStream;
import java.util.ArrayList;

/**
 *
 * @author aprat
 */
public class HadoopPersonActivityGenerator {

    public static class HadoopPersonActivityGeneratorReducer  extends Reducer<BlockKey, Person, LongWritable, Person> {

        private int reducerId;                          /** The id of the reducer.**/
        private PersonActivitySerializer personActivitySerializer_;
        private PersonActivityGenerator personActivityGenerator_;
        private UpdateEventSerializer updateSerializer_;
        private OutputStream factors_;
        private OutputStream friends_;
        private FileSystem fs_;

        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            reducerId = context.getTaskAttemptID().getTaskID().getId();
            LDBCDatagen.init(conf);
            try {
                personActivitySerializer_ = (PersonActivitySerializer) Class.forName(conf.get("ldbc.snb.datagen.serializer.personActivitySerializer")).newInstance();
                personActivitySerializer_.initialize(conf,reducerId);
                updateSerializer_ = new UpdateEventSerializer(conf, DatagenParams.hadoopDir+"/temp_updateStream_forum_"+reducerId, DatagenParams.numPartitions);
                personActivityGenerator_ = new PersonActivityGenerator(personActivitySerializer_, updateSerializer_);

                fs_ = FileSystem.get(context.getConfiguration());
                factors_ = fs_.create(new Path(DatagenParams.hadoopDir+"/"+ "m" + reducerId + DatagenParams.PARAM_COUNT_FILE));
                friends_ = fs_.create(new Path(DatagenParams.hadoopDir+"/"+ "m0friendList" + reducerId +".csv"));

            } catch( Exception e ) {
                System.err.println(e.getMessage());
            }
        }

        @Override
        public void reduce(BlockKey key, Iterable<Person> valueSet,Context context)
                throws IOException, InterruptedException {
            ArrayList<Person> persons = new ArrayList<Person>();
            for( Person p : valueSet ) {
                persons.add(new Person(p));

                StringBuffer strbuf = new StringBuffer();
                strbuf.append(p.accountId());
                for( Knows k : p.knows() ) {
                    strbuf.append(",");
                    strbuf.append(k.to().accountId());
                    if( k.creationDate() > Dictionaries.dates.getUpdateThreshold() && DatagenParams.updateStreams ) {
                        updateSerializer_.export(k);
                    }
                }
                strbuf.append("\n");
                friends_.write(strbuf.toString().getBytes("UTF8"));
            }
            personActivityGenerator_.generateActivityForBlock((int)key.block, persons, context );
        }
        protected void cleanup(Context context){
            try {
                personActivityGenerator_.writeFactors(factors_);
                factors_.close();
                friends_.close();
                fs_.copyToLocalFile(false, new Path(DatagenParams.hadoopDir + "/m"+reducerId+"factors.txt"), new Path("./"));
                fs_.copyToLocalFile(false, new Path(DatagenParams.hadoopDir + "/m0friendList"+reducerId+".csv"), new Path("./"));
            } catch (IOException e) {
                e.printStackTrace();
            }
            personActivitySerializer_.close();
            updateSerializer_.close();
        }
    }


    private Configuration conf;

    public HadoopPersonActivityGenerator(Configuration conf) {
        this.conf = conf;
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
        Job job = Job.getInstance(conf, "Person Activity Generator/Serializer");
        job.setMapOutputKeyClass(BlockKey.class);
        job.setMapOutputValueClass(Person.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Person.class);
        job.setJarByClass(HadoopBlockMapper.class);
        job.setMapperClass(HadoopBlockMapper.class);
        job.setReducerClass(HadoopPersonActivityGeneratorReducer.class);
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
