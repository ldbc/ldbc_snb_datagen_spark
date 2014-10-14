package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.generator.PersonGenerator;
import ldbc.snb.datagen.objects.Person;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by aprat on 8/8/14.
 */
public class HadoopPersonGenerator  {

    public static class HadoopPersonGeneratorMapper  extends Mapper<LongWritable, Text, LongWritable, Person> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            int threadId = Integer.parseInt(value.toString());
            System.out.println("Generating user at mapper " + threadId);
            DatagenParams.readConf(conf);
            DatagenParams.readParameters("/params.ini");
            if (DatagenParams.numPersons % DatagenParams.cellSize != 0) {
                System.err.println("Number of users should be a multiple of the cellsize");
                System.exit(-1);
            }

            // Here we determine the blocks in the "block space" that this mapper is responsible for.
            int numBlocks   = (int) (Math.ceil(DatagenParams.numPersons / (double) DatagenParams.blockSize));
            int initBlock   = (int) (Math.ceil((numBlocks / (double) DatagenParams.numThreads) * threadId));
            int endBlock    = (int) (Math.ceil((numBlocks / (double) DatagenParams.numThreads) * (threadId + 1)));

            PersonGenerator personGenerator = new PersonGenerator();
            for (int i = initBlock; i < endBlock; ++i) {
                Person[] block = personGenerator.generateUserBlock(i,DatagenParams.blockSize);
                int size = block.length;
                for( int j = 0; j < size && DatagenParams.blockSize*i + j < DatagenParams.numPersons ; ++j ) {
                    LongWritable outputKey = new LongWritable(block[j].accountId);
                    try {
                        context.write(outputKey, block[j]);
                    } catch( IOException ioE ) {
                        System.err.println("Input/Output Exception when writing to context.");
                        System.err.println(ioE.getMessage());
                    } catch( InterruptedException iE ) {
                        System.err.println("Interrupted Exception when writing to context.");
                    }
                }
            }
        }
    }


    public static class HadoopPersonGeneratorReducer  extends Reducer<LongWritable, Person, LongWritable, Person> {

        @Override
        public void reduce(LongWritable key, Iterable<Person> valueSet,
                           Context context) throws IOException, InterruptedException {

                for ( Person person : valueSet ) {
                    context.write(key, person);
                }
        }
    }

    private Configuration conf = null;

    public HadoopPersonGenerator( Configuration conf ) {
        this.conf  = new Configuration(conf);
    }

    private static void writeToOutputFile(String filename, int numMaps, Configuration conf) {
        try {
            FileSystem dfs = FileSystem.get(conf);
            OutputStream output = dfs.create(new Path(filename));
            for (int i = 0; i < numMaps; i++)
                output.write((new String(i + "\n").getBytes()));
            output.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /** Generates a Person hadoop sequence file containing key-value paiers
     * where the key is the person id and the value is the person itself.
     *
     * @param outputFileName The name of the file to store the persons.
     * @throws Exception
     */
    public void run( String outputFileName ) throws Exception {

        String hadoopDir = new String( conf.get("outputDir") + "/hadoop" );
        String tempFile = hadoopDir+"/mrInputFile";

        FileSystem dfs = FileSystem.get(conf);
        dfs.delete(new Path(tempFile), true);
        writeToOutputFile(tempFile, Integer.parseInt(conf.get("numThreads")), conf);

        int numThreads = Integer.parseInt(conf.get("numThreads"));
        conf.setInt("mapred.line.input.format.linespermap", 1);
        Job job = new Job(conf, "SIB Generate Users & 1st Dimension");
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Person.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Person.class);
        job.setJarByClass(HadoopPersonGeneratorMapper.class);
        job.setMapperClass(HadoopPersonGeneratorMapper.class);
        job.setReducerClass(HadoopPersonGeneratorReducer.class);
        job.setNumReduceTasks(numThreads);
        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        //job.setPartitionerClass(MapReduceKeyPartitioner.class);
        //job.setSortComparatorClass(MapReduceKeyComparator.class);
        //job.setGroupingComparatorClass(MapReduceKeyGroupKeyComparator.class);
        FileInputFormat.setInputPaths(job, new Path(tempFile));
        FileOutputFormat.setOutputPath(job, new Path(outputFileName));
        job.waitForCompletion(true);
    }
}
