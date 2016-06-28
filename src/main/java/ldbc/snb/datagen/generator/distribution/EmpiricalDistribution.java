package ldbc.snb.datagen.generator.distribution;

import javafx.util.Pair;
import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.generator.distribution.utils.Bucket;
import ldbc.snb.datagen.generator.distribution.utils.BucketedDistribution;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

/**
 * Created by aprat on 27/06/16.
 */
public class EmpiricalDistribution extends BucketedDistribution{

    String fileName = null;

    @Override
    public ArrayList<Bucket> getBuckets(Configuration conf) {
        fileName = conf.get("ldbc.snb.datagen.generator.distribution.EmpiricalDistribution.fileName");
        ArrayList<Pair<Integer,Integer>> histogram = new ArrayList<Pair<Integer,Integer>>();
        try {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(getClass().getResourceAsStream(fileName), "UTF-8"));
            String line;
            while ((line = reader.readLine()) != null) {
                String data[] = line.split(" ");
                histogram.add(new Pair<Integer,Integer>(Integer.parseInt(data[0]),Integer.parseInt(data[1])));
            }
            reader.close();
            return Bucket.bucketizeHistogram(histogram,1000);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
