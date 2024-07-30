package ldbc.snb.datagen.generator.distribution;

import ldbc.snb.datagen.generator.tools.Bucket;
import ldbc.snb.datagen.util.GeneratorConfiguration;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public abstract class BucketedDistribution extends DegreeDistribution {

    private List<Bucket> buckets;
    private List<Random> randomDegree;
    private Random randomPercentile;

    public abstract List<Bucket> getBuckets(GeneratorConfiguration conf);

    public void initialize(GeneratorConfiguration conf) {
        buckets = this.getBuckets(conf);
        randomPercentile = new Random(0);
        randomDegree = new ArrayList<>();
        for (int i = 0; i < buckets.size(); i++) {
            randomDegree.add(new Random(0));
        }
    }

    public void reset(long seed) {
        Random seedRandom = new Random(53223436L + 1234567 * seed);
        for (int i = 0; i < buckets.size(); i++) {
            randomDegree.get(i).setSeed(seedRandom.nextLong());
        }
        randomPercentile.setSeed(seedRandom.nextLong());
    }

    public long nextDegree() {
        int idx = randomPercentile.nextInt(buckets.size());
        double minRange = (buckets.get(idx).min());
        double maxRange = (buckets.get(idx).max());
        if (maxRange < minRange) {
            maxRange = minRange;
        }
        return randomDegree.get(idx).nextInt((int) maxRange - (int) minRange + 1) + (int) minRange;
    }

    @Override
    public double mean(long numPersons) {
        return -1.0;
    }
}
