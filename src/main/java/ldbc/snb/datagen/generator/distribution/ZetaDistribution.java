package ldbc.snb.datagen.generator.distribution;

import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.generator.distribution.utils.Bucket;
import ldbc.snb.datagen.generator.distribution.utils.BucketedDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;

/**
 * Created by aprat on 5/03/15.
 */
public class ZetaDistribution extends BucketedDistribution {

    private ArrayList<Bucket> buckets_;
    private ZipfDistribution zipf_;
    private double ALPHA_ = 1.7;

    @Override
    public ArrayList<Bucket> getBuckets() {
        return buckets_;
    }

    @Override
    public void initialize(Configuration conf) {
        zipf_ = new ZipfDistribution(DatagenParams.numPersons, ALPHA_);

        ArrayList<Double> histogram = new ArrayList<Double>();
        for( int i = 1; i <= DatagenParams.numPersons; ++i ) {
            histogram.add(DatagenParams.numPersons * zipf_.probability(i));
        }

        buckets_ = Bucket.bucketizeHistogram(histogram,100);
    }
}
