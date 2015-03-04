package ldbc.snb.datagen.generator.distribution.utils;

import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;

/**
 * Created by aprat on 3/03/15.
 */
public interface BucketedDistribution {

    public ArrayList<Bucket> getBuckets();

    public void initialize( Configuration conf );
}
