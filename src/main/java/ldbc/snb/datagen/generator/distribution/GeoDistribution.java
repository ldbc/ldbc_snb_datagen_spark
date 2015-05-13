package ldbc.snb.datagen.generator.distribution;

        import ldbc.snb.datagen.generator.DatagenParams;
        import ldbc.snb.datagen.generator.distribution.utils.Bucket;
        import ldbc.snb.datagen.generator.distribution.utils.BucketedDistribution;
        import org.apache.commons.math3.distribution.GeometricDistribution;
        import org.apache.hadoop.conf.Configuration;
        import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf;

        import java.util.ArrayList;

/**
 * Created by aprat on 5/03/15.
 */
public class GeoDistribution implements DegreeDistribution {

    private GeometricDistribution geo_;
    private double ALPHA_ = 0.12;

    public void initialize(Configuration conf) {
        ALPHA_ = conf.getDouble("ldbc.snb.datagen.generator.distribution.GeoDistribution.alpha",ALPHA_);
        geo_ = new GeometricDistribution(ALPHA_);
    }

    public void reset (long seed) {
        geo_.reseedRandomGenerator(seed);
    }

    public long nextDegree(){
        return geo_.sample();
    }
}
