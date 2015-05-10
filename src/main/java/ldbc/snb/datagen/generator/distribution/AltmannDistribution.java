package ldbc.snb.datagen.generator.distribution;

import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.generator.distribution.utils.Bucket;
import ldbc.snb.datagen.generator.distribution.utils.BucketedDistribution;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;

/**
 * Created by aprat on 26/02/15.
 */
public class AltmannDistribution extends BucketedDistribution {

    private int NUM_BUCKETS_ = 100;
    private int POPULATION_ = 10000;
    private double normalization_factor_ = 0.0;
    private double GAMMA_ = 0.4577;
    private double DELTA_ = 0.0162;

    private ArrayList<Bucket> buckets_ = null;

    @Override
    public ArrayList<Bucket> getBuckets() {
        return buckets_;
    }

    @Override
    public void initialize(Configuration conf) {
        throw new UnsupportedOperationException("Distribution not implemented");
        /*POPULATION_ = DatagenParams.numPersons;
        for( int i = 1; i <= POPULATION_; ++i ) {
            normalization_factor_+= Math.pow(i,-GAMMA_)*Math.exp(-DELTA_*i);
        }
        ArrayList<Double> histogram = new ArrayList<Double>();
        for( int i = 1; i <= POPULATION_; ++i) {
            histogram.add(POPULATION_*Math.pow(i,-GAMMA_)*Math.exp(-DELTA_*i) / normalization_factor_);
        }

        double scale_factor = DatagenParams.numPersons / POPULATION_;
        buckets_ = Bucket.bucketizeHistogram(histogram, NUM_BUCKETS_);
        for( Bucket e : buckets_) {
            e.max(e.max()*scale_factor);
            e.min(e.min()*scale_factor);
        }
        */
    }
}
