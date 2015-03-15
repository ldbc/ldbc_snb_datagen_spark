package ldbc.snb.datagen.generator.distribution;

        import ldbc.snb.datagen.generator.DatagenParams;
        import ldbc.snb.datagen.generator.distribution.utils.Bucket;
        import ldbc.snb.datagen.generator.distribution.utils.BucketedDistribution;
        import org.apache.commons.math3.distribution.GeometricDistribution;
        import org.apache.hadoop.conf.Configuration;

        import java.util.ArrayList;

/**
 * Created by aprat on 5/03/15.
 */
public class GeoDistribution implements BucketedDistribution {

    private ArrayList<Bucket> buckets_;
    private GeometricDistribution geo_;
    private double ALPHA_ = 0.12;

    @Override
    public ArrayList<Bucket> getBuckets() {
        return buckets_;
    }

    @Override
    public void initialize(Configuration conf) {
        geo_ = new GeometricDistribution(ALPHA_);

        ArrayList<Double> histogram = new ArrayList<Double>();
        for( int i = 1; i <= DatagenParams.numPersons; ++i ) {
            histogram.add(DatagenParams.numPersons * geo_.probability(i));
        }

        buckets_ = Bucket.bucketizeHistogram(histogram,100);
    }
}
