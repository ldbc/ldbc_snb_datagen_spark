package ldbc.snb.datagen.hadoop;
import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.objects.Person;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.io.*;

/**
 * Created by aprat on 11/7/14.
 */
public class HadoopFileRanker {
    private Configuration conf;
    private Class<?> K;
    private Class<?> V;

    /**
     *
     * @param conf The configuration object.
     * @param K The Key class of the hadoop sequence file.
     * @param V The Value class of the hadoop sequence file.
     */
    public HadoopFileRanker( Configuration conf, Class<?> K, Class<?> V) {
        this.conf  = new Configuration(conf);
        this.K = K;
        this.V = V;
    }

    public static class HadoopFileRankerSortReducer<K, V, T extends IntWritable>  extends Reducer<K, V, IntWritable, V> {

        private int reducerId;
        private long counter = 0;
        @Override
        public void setup( Context context )  {
            reducerId = context.getTaskAttemptID().getId();
        }

        @Override
        public void reduce(K key, Iterable<V> valueSet,
                           Context context) throws IOException, InterruptedException {

            for( V v : valueSet ) {
                context.write(new IntWritable(reducerId), v);
                counter++;
            }
        }

        public void cleanup(Context context) {
            Configuration conf = context.getConfiguration();
            try {
                FileSystem fs = FileSystem.get(conf);
                OutputStream output = fs.create(new Path(conf.get("hadoopDir")+"/rank_"+reducerId));
                output.write((new String(Long.toString(counter)).getBytes()));
                output.close();
            } catch(IOException e) {
                System.err.println(e.getMessage());
            }
        }
    }

    public static class HadoopFileRankerPartitioner<V> extends Partitioner<IntWritable, V> {

        public HadoopFileRankerPartitioner() {
            super();

        }

        @Override
        public int getPartition(IntWritable key, V value,
                                int numReduceTasks) {
            return key.get();
        }
    }

    public static class HadoopFileRankerFinalReducer<K, V, T extends LongWritable>  extends Reducer<K, V, LongWritable, V> {

        private int reducerId;
        private int numReduceTasks;
        private long counters[];

        @Override
        public void setup( Context context )  {
            Configuration conf = context.getConfiguration();
            reducerId = context.getTaskAttemptID().getId();
            numReduceTasks = context.getNumReduceTasks();
            counters = new long[numReduceTasks];
            DatagenParams.readConf(conf);
            DatagenParams.readParameters("/params.ini");
            try{
                counters[0] = 0;
                FileSystem fs = FileSystem.get(conf);
                for(int i = 0; i < numReduceTasks-1;++i ) {
                    DataInputStream inputFile = fs.open(new Path(conf.get("hadoopDir")+"/rank_"+i));
                    counters[i+1] = inputFile.readLong();
                }
            } catch(IOException e) {
                System.err.println(e.getMessage());
            }

            for(int i = 1; i < numReduceTasks;++i ) {
                counters[i] += counters[i-1];
            }
        }

        @Override
        public void reduce(K key, Iterable<V> valueSet,
                           Context context) throws IOException, InterruptedException {

            int i = 0;
            for( V v : valueSet ) {
                context.write(new LongWritable(counters[reducerId]+(i++)), v);
            }
        }
    }


    /** Sorts a hadoop sequence file
     *
     * @param inputFileName The name of the file to sort.
     * @param outputFileName The name of the sorted file.
     * @throws Exception
     */
    public void run( String inputFileName, String outputFileName ) throws Exception {
        int numThreads = conf.getInt("numThreads",1);
        Job jobSort = new Job(conf, "Sorting "+inputFileName);

        FileInputFormat.setInputPaths(jobSort, new Path(inputFileName));
        FileOutputFormat.setOutputPath(jobSort, new Path(conf.get("hadoopDir")+"/rankIntermediate"));

        jobSort.setMapOutputKeyClass(K);
        jobSort.setMapOutputValueClass(V);
        jobSort.setOutputKeyClass(IntWritable.class);
        jobSort.setOutputValueClass(V);
        jobSort.setNumReduceTasks(numThreads);
        jobSort.setReducerClass(HadoopFileRankerSortReducer.class);
        jobSort.setJarByClass(V);
        jobSort.setInputFormatClass(SequenceFileInputFormat.class);
        jobSort.setOutputFormatClass(SequenceFileOutputFormat.class);
        InputSampler.Sampler sampler = new InputSampler.RandomSampler(0.1, 1000);
        TotalOrderPartitioner.setPartitionFile(jobSort.getConfiguration(), new Path(inputFileName + "_partition.lst"));
        InputSampler.writePartitionFile(jobSort, sampler);
        jobSort.setPartitionerClass(TotalOrderPartitioner.class);
        jobSort.waitForCompletion(true);

        Job jobRank = new Job(conf, "Sorting "+inputFileName);
        FileInputFormat.setInputPaths(jobRank, new Path(conf.get("hadoopDir")+"/rankIntermediate"));
        FileOutputFormat.setOutputPath(jobRank, new Path(outputFileName));

        jobRank.setMapOutputKeyClass(IntWritable.class);
        jobRank.setMapOutputValueClass(V);
        jobRank.setOutputKeyClass(LongWritable.class);
        jobRank.setOutputValueClass(V);
        jobRank.setNumReduceTasks(numThreads);
        jobRank.setReducerClass(HadoopFileRankerFinalReducer.class);
        jobRank.setJarByClass(V);
        jobRank.setInputFormatClass(SequenceFileInputFormat.class);
        jobRank.setOutputFormatClass(SequenceFileOutputFormat.class);
        jobRank.setPartitionerClass(HadoopFileRankerPartitioner.class);
        jobRank.waitForCompletion(true);

        try{
            FileSystem fs = FileSystem.get(conf);
            for(int i = 0; i < numThreads-1;++i ) {
                fs.delete(new Path(conf.get("hadoopDir")+"/rank_"+i),true);
            }
            fs.delete(new Path(conf.get("hadoopDir")+"/rankIntermediate"),true);
        } catch(IOException e) {
            System.err.println(e.getMessage());
        }
    }
}
