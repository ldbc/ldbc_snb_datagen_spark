package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.util.LdbcConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public abstract class DatagenReducer<T, T1, T2, T3> extends Reducer<T, T1, T2, T3> {

    protected Configuration hadoopConf;
    protected LdbcConfiguration conf;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        hadoopConf = context.getConfiguration();
        conf = HadoopConfiguration.extractLdbcConfig(hadoopConf);
    }
}
