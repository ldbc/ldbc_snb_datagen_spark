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
public class DiscreteWeibullDistribution extends BucketedDistribution {

    private ArrayList<Bucket> buckets_;
    private double BETA_ = 0.7787;
    private double P_ = 0.07;

    @Override
    public ArrayList<Bucket> getBuckets() {

        ArrayList<Double> histogram = new ArrayList<Double>();
        for( int i = 1; i <= DatagenParams.numPersons; ++i ) {
            double prob = Math.pow(1.0-P_,Math.pow(i,BETA_))-Math.pow((1.0-P_),Math.pow(i+1,BETA_));
            histogram.add(DatagenParams.numPersons * prob);
            //System.out.println(DatagenParams.numPersons * prob);
        }

        buckets_ = Bucket.bucketizeHistogram(histogram,10000);

        /*for( Bucket e : buckets_) {
            System.out.println((e.min())+" "+e.max());
        }
        */
        return buckets_;
    }
}
