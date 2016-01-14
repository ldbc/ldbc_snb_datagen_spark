package ldbc.snb.datagen.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

/**
 * Created by aprat on 10/14/14.
 */
public class HadoopFileSorter {
    private Configuration conf;
    private Class<?> K;
    private Class<?> V;

    /**
     *
     * @param conf The configuration object.
     * @param K The Key class of the hadoop sequence file.
     * @param V The Value class of the hadoop sequence file.
     */
    public HadoopFileSorter( Configuration conf, Class<?> K, Class<?> V) {
        this.conf  = new Configuration(conf);
        this.K = K;
        this.V = V;
    }

    /** Sorts a hadoop sequence file
     *
     * @param inputFileName The name of the file to sort.
     * @param outputFileName The name of the sorted file.
     * @throws Exception
     */
    public void run( String inputFileName, String outputFileName ) throws Exception {
        int numThreads = conf.getInt("ldbc.snb.datagen.generator.numThreads",1);
        Job job = Job.getInstance(conf, "Sorting "+inputFileName);

        FileInputFormat.setInputPaths(job, new Path(inputFileName));
        FileOutputFormat.setOutputPath(job, new Path(outputFileName));

        job.setMapOutputKeyClass(K);
        job.setMapOutputValueClass(V);
        job.setOutputKeyClass(K);
        job.setOutputValueClass(V);
        job.setNumReduceTasks(numThreads);

        job.setJarByClass(V);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        InputSampler.Sampler sampler = new InputSampler.RandomSampler(0.1, 1000);
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(),new Path(inputFileName+"_partition.lst"));
        InputSampler.writePartitionFile(job, sampler);
        job.setPartitionerClass(TotalOrderPartitioner.class);
        if(!job.waitForCompletion(true)) {
            throw new Exception();
        }
    }
}
