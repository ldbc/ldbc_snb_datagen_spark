package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.objects.Person;
import ldbc.snb.datagen.serializer.InvariantDataExporter;
import ldbc.snb.datagen.serializer.InvariantSerializer;
import ldbc.snb.datagen.serializer.PersonDataExporter;
import ldbc.snb.datagen.serializer.PersonSerializer;
import org.apache.hadoop.conf.Configuration;
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
 * Created by aprat on 12/17/14.
 */
public class HadoopInvariantSerializer {

    private InvariantSerializer invariantSerializer;
    private InvariantDataExporter invariantDataExporter;              /** The data exporter.**/


    private Configuration conf;

    public HadoopInvariantSerializer( Configuration conf ) {
        this.conf = new Configuration(conf);
    }

    public void run() throws Exception {
        try {
            invariantSerializer = (InvariantSerializer) Class.forName(conf.get("invariantSerializer")).newInstance();
            invariantSerializer.initialize(conf,0);
        } catch( Exception e ) {
            System.err.println(e.getMessage());
        }
        invariantDataExporter = new InvariantDataExporter(invariantSerializer);
        invariantDataExporter.exportTags();
        invariantDataExporter.exportOrganizations();
        invariantDataExporter.exportPlaces();
        invariantSerializer.close();
    }
}
