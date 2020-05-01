package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.util.LdbcConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public abstract class DatagenReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    protected Configuration hadoopConf;
    protected LdbcConfiguration conf;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        hadoopConf = context.getConfiguration();
        conf = HadoopConfiguration.extractLdbcConfig(hadoopConf);
    }
}
