package ldbc.snb.datagen.generator.distribution;

import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.generator.distribution.utils.Bucket;
import ldbc.snb.datagen.generator.distribution.utils.BucketedDistribution;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;

/**
 * Created by aprat on 4/03/15.
 */
public class MOEZipfDistribution implements BucketedDistribution {

    private int NUM_BUCKETS_ = 100;
    private int POPULATION_ = 10000;
    private double normalization_factor_1 = 0.0;
    private double normalization_factor_2 = 0.0;
    private double ALFA_ = 2.2767;
    private double BETA_ = 4.8613;

    private ArrayList<Bucket> buckets_ = null;

    @Override
    public ArrayList<Bucket> getBuckets() {
        return buckets_;
    }

    @Override
    public void initialize(Configuration conf) {
        throw new UnsupportedOperationException("Distribution not implemented");
/*        POPULATION_ = DatagenParams.numPersons;
        for( int i = 1; i <= POPULATION_; ++i ) {
            normalization_factor_1+= Math.pow(i,-ALFA_);
            normalization_factor_2+= Math.pow(i,-(ALFA_+1));
        }
        ArrayList<Double> histogram = new ArrayList<Double>(POPULATION_);
        for( int i = 0; i<  POPULATION_; ++i) {
            histogram.add(0.0);
        }

        Double hurwitz = 0.0;
        for( int i = POPULATION_; i > 0; --i) {
            hurwitz+= Math.pow(i+1,-ALFA_);
            histogram.set(i-1, POPULATION_ * (1.0 - (BETA_ * hurwitz / (normalization_factor_1 - (1.0 - BETA_) * normalization_factor_2))));
//            System.out.println(histogram.get(i-1));
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

