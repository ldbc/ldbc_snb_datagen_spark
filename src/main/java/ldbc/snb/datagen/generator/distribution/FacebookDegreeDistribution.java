
package ldbc.snb.datagen.generator.distribution;

import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.generator.tools.Bucket;
import ldbc.snb.datagen.util.GeneratorConfiguration;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * This class generates Facebook-like social degree distribution
 * <p/>
 * A. Preparation
 * For a specific social network size (total number of persons)
 * 1) compute the mean value of social degree
 * 2) compute the range of each bucket (100 buckets) using the data from facebookBucket100.dat
 * B. Generate social degree for each person
 * 1) Determine the bucket (Take a random number from 0-99)
 * 2) Randomly select a social degree in the range of that bucket
 */

public class FacebookDegreeDistribution extends BucketedDistribution {
    private int mean = 0;
    private static final int FB_MEAN = 190;
    private List<Bucket> buckets;

    @Override
    public List<Bucket> getBuckets(GeneratorConfiguration conf) {
        mean = (int) mean(DatagenParams.numPersons);
        buckets = new ArrayList<>();
        loadFBBuckets();
        rebuildBucketRange();
        return buckets;
    }

    private void loadFBBuckets() {
        try {
            BufferedReader fbDataReader = new BufferedReader(
                    new InputStreamReader(getClass().getResourceAsStream(DatagenParams.fbSocialDegreeFile), StandardCharsets.UTF_8));
            String line;
            while ((line = fbDataReader.readLine()) != null) {
                String[] data = line.split(" ");
                buckets.add(new Bucket(Float.parseFloat(data[0]), Float.parseFloat(data[1])));
            }
            fbDataReader.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void rebuildBucketRange() {
        double newMin;
        double newMax;
        for (Bucket bucket : buckets) {
            newMin = bucket.min() * mean / FB_MEAN;
            newMax = bucket.max() * mean / FB_MEAN;
            if (newMax < newMin) newMax = newMin;
            bucket.min(newMin);
            bucket.max(newMax);
        }
    }

    @Override
    public double mean(long numPersons) {
        return Math.round(Math.pow(numPersons, (0.512 - 0.028 * Math.log10(numPersons))));
    }
}
