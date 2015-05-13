package ldbc.snb.datagen.generator.distribution;

import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.generator.distribution.utils.Bucket;
import ldbc.snb.datagen.generator.distribution.utils.BucketedDistribution;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;

/**
 * Created by aprat on 26/02/15.
 */
public class AltmannDistribution extends CumulativeBasedDegreeDistribution {

    private int POPULATION_ = 10000;
    private double normalization_factor_ = 0.0;
    private double ALPHA_ = 0.4577;
    private double BETA_ = 0.0162;


    public ArrayList<CumulativeEntry> cumulativeProbability( Configuration conf ) {
        //throw new UnsupportedOperationException("Distribution not implemented");
        ALPHA_ = conf.getDouble("ldbc.snb.datagen.generator.distribution.AltmannDistribution.alpha",ALPHA_);
        BETA_ = conf.getDouble("ldbc.snb.datagen.generator.distribution.AltmannDistribution.beta",BETA_);

        POPULATION_ = DatagenParams.numPersons;
        for( int i = 1; i <= POPULATION_; ++i ) {
            normalization_factor_+= Math.pow(i,-ALPHA_)*Math.exp(-BETA_*i);
        }
        ArrayList<CumulativeEntry> cumulative = new ArrayList<CumulativeEntry>();
        for( int i = 1; i <= POPULATION_; ++i) {
            double prob = Math.pow(i,-ALPHA_)*Math.exp(-BETA_*i) / normalization_factor_;
            prob += cumulative.size() > 0 ? cumulative.get(i-2).prob_ : 0.0;
            CumulativeEntry entry = new CumulativeEntry();
            entry.prob_ = prob;
            entry.value_ = i;
            cumulative.add(entry);
        }
        return cumulative;
    }
}
