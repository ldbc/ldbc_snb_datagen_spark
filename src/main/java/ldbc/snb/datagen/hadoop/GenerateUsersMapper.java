package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.generator.DatagenParams;
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

    private int fileIdx;

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        fileIdx = Integer.parseInt(value.toString());
        System.out.println("Generating user at mapper " + fileIdx);
        ScalableGenerator generator;
        DatagenParams params = new DatagenParams();
        params.readConf(conf);
        params.readParameters("/params.ini");
        generator = new ScalableGenerator(fileIdx, params);
        System.out.println("Successfully init Generator object");
        int pass = 0;
        generator.mrGenerateUserInfo(pass, context);
    }

}

