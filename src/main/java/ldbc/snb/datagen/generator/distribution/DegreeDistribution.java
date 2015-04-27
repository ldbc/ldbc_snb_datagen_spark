/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ldbc.snb.datagen.generator.distribution;

import ldbc.snb.datagen.generator.distribution.utils.Bucket;
import ldbc.snb.datagen.generator.distribution.utils.BucketedDistribution;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.Random;

/**
 *
 * @author aprat
 */
public class DegreeDistribution {

    private ArrayList<Bucket> buckets_;
    private ArrayList<Random> randomDegree_;
    private Random randomPercentile_;


	public void initialize( Configuration conf, BucketedDistribution dist ) {
        dist.initialize(conf);
        buckets_ = dist.getBuckets();
        randomPercentile_ = new Random(0);
        randomDegree_ = new ArrayList<Random>();
        for (int i = 0; i < buckets_.size(); i++) {
            randomDegree_.add(new Random(0));
        }
    }

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
        int maxRange = (int)(Math.floor(buckets_.get(idx).max()));
        if( maxRange < minRange ) maxRange = minRange;
        return randomDegree_.get(idx).nextInt( maxRange - minRange  + 1) + minRange;
    }
}
