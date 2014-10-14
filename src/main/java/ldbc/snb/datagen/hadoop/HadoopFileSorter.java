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

import java.io.Serializable;

/**
 * Created by aprat on 10/14/14.
 */
public class HadoopFileSorter{
    Configuration conf;
    Class<?> K;
    Class<?> V;

    public HadoopFileSorter( Configuration conf, Class<?> K, Class<?> V) {
        this.conf  = new Configuration(conf);
        this.K = K;
        this.V = V;
    }

    public void run( String inputFileName, String outputFileName ) throws Exception {
        String hadoopDir = new String( conf.get("outputDir") + "/hadoop" );
        int numThreads = Integer.parseInt(conf.get("numThreads"));
        Job job = new Job(conf, "Sorting "+inputFileName);

        FileInputFormat.setInputPaths(job, new Path(inputFileName));
        FileOutputFormat.setOutputPath(job, new Path(outputFileName));

        job.setMapOutputKeyClass(K);
        job.setMapOutputValueClass(V);
        job.setOutputKeyClass(K);
        job.setOutputValueClass(V);
        job.setNumReduceTasks(numThreads);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        InputSampler.Sampler sampler = new InputSampler.RandomSampler(0.1, 1000);
        InputSampler.writePartitionFile(job, sampler);
        job.setPartitionerClass(TotalOrderPartitioner.class);
        job.waitForCompletion(true);
    }
}
