package ldbc.snb.datagen.generator.distribution.utils;

import ldbc.snb.datagen.generator.distribution.DegreeDistribution;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.Random;

/**
 * Created by aprat on 3/03/15.
 */
public abstract class BucketedDistribution implements DegreeDistribution {

    private ArrayList<Bucket> buckets_;
    private ArrayList<Random> randomDegree_;
    private Random randomPercentile_;

    public abstract ArrayList<Bucket> getBuckets();

    public void initialize( Configuration conf ) {
        buckets_ = this.getBuckets();
        randomPercentile_ = new Random(0);
        randomDegree_ = new ArrayList<Random>();
        for (int i = 0; i < buckets_.size(); i++) {
            randomDegree_.add(new Random(0));
        }
    };

    public void reset(long seed) {
        Random seedRandom = new Random(53223436L + 1234567 * seed);
        for (int i = 0; i < buckets_.size(); i++) {
            randomDegree_.get(i).setSeed(seedRandom.nextLong());
        }
        randomPercentile_.setSeed(seedRandom.nextLong());
    }

    public long nextDegree() {
        int idx = randomPercentile_.nextInt(buckets_.size());
        int minRange = (int)(buckets_.get(idx).min());
        int maxRange = (int)(buckets_.get(idx).max());
        if( maxRange < minRange ) {
            maxRange = minRange;
        }
        long ret= randomDegree_.get(idx).nextInt( maxRange - minRange  + 1) + minRange;
        return ret;
    }
}
