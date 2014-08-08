package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.generator.ScalableGenerator;
import ldbc.snb.datagen.objects.ReducedUserProfile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by aprat on 8/8/14.
 */
public class GenerateUsersMapper extends Mapper<LongWritable, Text, MapReduceKey, ReducedUserProfile> {

    private String outputDir;
    private String homeDir;
    private int numMappers;
    private int fileIdx;

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        fileIdx = Integer.parseInt(value.toString());
        System.out.println("Generating user at mapper " + fileIdx);
        ScalableGenerator generator;
        generator = new ScalableGenerator(fileIdx, conf);
        System.out.println("Successfully init Generator object");
        generator.init();
        int pass = 0;
        generator.mrGenerateUserInfo(pass, context);
    }

}

