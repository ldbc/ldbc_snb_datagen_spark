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
public class DiscreteWeibullDistribution extends CumulativeBasedDegreeDistribution {

    //private double BETA_ = 0.7787;
    //private double BETA_ = 0.8211;
    private double BETA_ = 0.8505;
    //private double P_ = 0.062;
    //private double P_ = 0.0448;
    private double P_ = 0.0205;

    public ArrayList<CumulativeEntry> cumulativeProbability( Configuration conf ) {
        BETA_ = conf.getDouble("ldbc.snb.datagen.generator.distribution.DiscreteWeibullDistribution.beta",BETA_);
        P_ = conf.getDouble("ldbc.snb.datagen.generator.distribution.DiscreteWeibullDistribution.p",P_);
        ArrayList<CumulativeEntry> cumulative = new ArrayList<CumulativeEntry>();
        for( int i = 0; i < DatagenParams.numPersons; ++i ) {
            //double prob = Math.pow(1.0-P_,Math.pow(i,BETA_))-Math.pow((1.0-P_),Math.pow(i+1,BETA_));
            double prob = 1.0-Math.pow((1.0-P_),Math.pow(i+1,BETA_));
            CumulativeEntry entry = new CumulativeEntry();
            entry.prob_ = prob;
            entry.value_ = i+1;
            cumulative.add(entry);
        }
        return cumulative;
    }
}
