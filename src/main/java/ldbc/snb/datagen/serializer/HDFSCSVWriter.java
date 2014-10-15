package ldbc.snb.datagen.serializer;

import org.apache.hadoop.conf.Configuration;

/**
 * Created by aprat on 10/15/14.
 */
public class HDFSCSVWriter {

    private String outputDir;
    private String prefix;
    private int numPartitions;
    private boolean compressed;


    public HDFSCSVWriter( Configuration conf, String outputDir, String prefix, int numPartitions, boolean compressed ) {

    }
}
