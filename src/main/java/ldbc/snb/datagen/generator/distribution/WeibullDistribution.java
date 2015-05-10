package ldbc.snb.datagen.generator.distribution;

import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.generator.distribution.utils.Bucket;
import ldbc.snb.datagen.generator.distribution.utils.BucketedDistribution;
import org.apache.commons.math3.distribution.*;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;

/**
 * Created by aprat on 5/03/15.
 */
public class WeibullDistribution extends BucketedDistribution {

    private ArrayList<Bucket> buckets_;
    private org.apache.commons.math3.distribution.WeibullDistribution weibull_;
    private double LAMBDA_ = 0.3881;
    private double K_ = 0.2622;

    @Override
    public ArrayList<Bucket> getBuckets() {
        return buckets_;
    }

    @Override
    public void initialize(Configuration conf) {
        weibull_ = new org.apache.commons.math3.distribution.WeibullDistribution(LAMBDA_,K_);

        ArrayList<Double> histogram = new ArrayList<Double>();
        for( int i = 1; i <= DatagenParams.numPersons; ++i ) {
            System.out.println(weibull_.probability(i));
            histogram.add(DatagenParams.numPersons * weibull_.probability(i));
        }

        buckets_ = Bucket.bucketizeHistogram(histogram,100);
    }
}
