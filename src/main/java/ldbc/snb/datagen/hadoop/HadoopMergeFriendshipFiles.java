package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.generator.KnowsGenerator;
import ldbc.snb.datagen.generator.LDBCDatagen;
import ldbc.snb.datagen.objects.Knows;
import ldbc.snb.datagen.objects.Person;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * Created by aprat on 29/07/15.
 */
public class HadoopMergeFriendshipFiles {


    public static class HadoopMergeFriendshipFilesReducer  extends Reducer<TupleKey, Person, TupleKey, Person> {

        private Configuration conf;
        private HadoopFileKeyChanger.KeySetter<TupleKey> keySetter = null;

        protected void setup(Context context) {
            this.conf = context.getConfiguration();
            try {
                this.keySetter = (HadoopFileKeyChanger.KeySetter) Class.forName(conf.get("postKeySetterName")).newInstance();
            }catch(Exception e) {
                System.out.println(e.getMessage());
            }
            LDBCDatagen.init(conf);
        }

        @Override
        public void reduce(TupleKey key, Iterable<Person> valueSet,Context context)
                throws IOException, InterruptedException {

            ArrayList<Knows> knows = new ArrayList<Knows>();
            Person person = null;
            int index = 0;
            for ( Person p : valueSet) {
                if( index == 0 ) {
                    person = new Person(p);
                }
                for(Knows k : p.knows()) {
                    knows.add(k);
                }
                index++;
            }
            person.knows().clear();
            Collections.sort(knows);
            if(knows.size() > 0 ) {
                long currentTo = knows.get(0).to().accountId();
                person.knows().add(knows.get(0));
                for (index = 1; index < knows.size(); ++index) {
                    Knows nextKnows = knows.get(index);
                    if(currentTo != knows.get(index).to().accountId()) {
                        person.knows().add(nextKnows);
                        currentTo = nextKnows.to().accountId();
                    }
                }
            }

            //System.out.println("Num persons "+index);
            context.write(keySetter.getKey(person),person);
        }
    }

    private Configuration conf;
    private String postKeySetterName;

    public HadoopMergeFriendshipFiles( Configuration conf,  String postKeySetterName ) {

        this.conf = new Configuration(conf);
        this.postKeySetterName = postKeySetterName;
    }

    public void run( String outputFileName, ArrayList<String> friendshipFileNames ) throws Exception {

        conf.set("postKeySetterName",postKeySetterName);
        int numThreads = Integer.parseInt(conf.get("ldbc.snb.datagen.generator.numThreads"));
        Job job = Job.getInstance(conf, "Edges merger generator");
        job.setMapOutputKeyClass(TupleKey.class);
        job.setMapOutputValueClass(Person.class);
        job.setOutputKeyClass(TupleKey.class);
        job.setOutputValueClass(Person.class);
        job.setJarByClass(HadoopBlockMapper.class);
        //job.setMapperClass(HadoopBlockMapper.class);
        job.setReducerClass(HadoopMergeFriendshipFilesReducer.class);
        job.setNumReduceTasks(numThreads);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setPartitionerClass(HadoopTuplePartitioner.class);

        for ( String s : friendshipFileNames ) {
            FileInputFormat.addInputPath(job, new Path(s));
        }
        FileOutputFormat.setOutputPath(job, new Path(outputFileName));

        System.out.println("Merging edges");
        long start = System.currentTimeMillis();
        if(!job.waitForCompletion(true) ){
            throw new Exception();
        }
        System.out.println("... time to merge edges: "+ (System.currentTimeMillis() - start)+" ms");


    }
}
