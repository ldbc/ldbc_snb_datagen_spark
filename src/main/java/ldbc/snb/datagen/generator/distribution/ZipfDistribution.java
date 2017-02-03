package ldbc.snb.datagen.generator.distribution;

import org.apache.hadoop.conf.Configuration;

import java.util.Random;

/**
 * Created by aprat on 5/03/15.
 */
public class ZipfDistribution extends DegreeDistribution {

    private org.apache.commons.math3.distribution.ZipfDistribution zipf_;
    private double ALPHA_ = 1.7;
    private Random random = new Random();

    public void initialize(Configuration conf) {
        ALPHA_ = conf.getDouble("ldbc.snb.datagen.generator.distribution.ZipfDistribution.alpha",ALPHA_);
        zipf_ = new org.apache.commons.math3.distribution.ZipfDistribution(1000, ALPHA_);
    }

    public void reset (long seed) {
        zipf_.reseedRandomGenerator(seed);
        random.setSeed(seed);
    }

    public long nextDegree(){
        //return zipf_.sample();
        return (long)Math.pow(((Math.pow(1000,ALPHA_+1) - Math.pow(1,ALPHA_+1)) * random.nextDouble() + Math.pow(1,ALPHA_+1)),1/(ALPHA_+1));
    }

    public double mean(long numPersons) {
        return zipf_.getNumericalMean();
    }
}
