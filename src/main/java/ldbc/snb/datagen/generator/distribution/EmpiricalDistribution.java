package ldbc.snb.datagen.generator.distribution;

import ldbc.snb.datagen.generator.tools.Bucket;
import ldbc.snb.datagen.util.GeneratorConfiguration;
import org.apache.commons.math3.util.Pair;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class EmpiricalDistribution extends BucketedDistribution {

    private String fileName = null;

    @Override
    public List<Bucket> getBuckets(GeneratorConfiguration conf) {
        fileName = conf.get("ldbc.snb.datagen.generator.distribution.EmpiricalDistribution.fileName");
        List<Pair<Integer, Integer>> histogram = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(getClass().getResourceAsStream(fileName), StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] data = line.split(" ");
                histogram.add(new Pair<>(Integer.parseInt(data[0]), Integer.parseInt(data[1])));
            }
            reader.close();
            return Bucket.bucketizeHistogram(histogram, 1000);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
