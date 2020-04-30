package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.util.LdbcConfiguration;
import org.apache.hadoop.conf.Configuration;

public abstract class DatagenHadoopJob {
    protected Configuration hadoopConf;
    protected LdbcConfiguration conf;

    public DatagenHadoopJob(LdbcConfiguration conf, Configuration hadoopConf) {
        this.hadoopConf = new Configuration(hadoopConf);
        this.conf = conf;
    }
}
