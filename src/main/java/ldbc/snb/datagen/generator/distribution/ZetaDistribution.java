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
public class ZetaDistribution implements DegreeDistribution {

    private ZipfDistribution zipf_;
    private double ALPHA_ = 1.7;

    public void initialize(Configuration conf) {
        zipf_ = new ZipfDistribution(DatagenParams.numPersons, ALPHA_);
    }

    public void reset (long seed) {
        zipf_.reseedRandomGenerator(seed);
    }

    public long nextDegree(){
        return zipf_.sample();
    }
}
