package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.util.LdbcConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.Map;


public class HadoopConfiguration {

    public static LdbcConfiguration extractLdbcConfig(Configuration hadoop) {
        return new LdbcConfiguration(hadoop.getValByRegex("^(generator|serializer).*$"));
    }

    public static void mergeLdbcIntoHadoop(LdbcConfiguration ldbcConfiguration, Configuration hadoop) {
        for (Map.Entry<String, String> entry : ldbcConfiguration.map.entrySet()) {
            hadoop.set(entry.getKey(), entry.getValue());
        }
    }

    public static Configuration prepare(LdbcConfiguration ldbcConf) throws IOException {
        Configuration hadoopConf = new Configuration();

        ldbcConf.printConfig();
        mergeLdbcIntoHadoop(ldbcConf, hadoopConf);
        FileSystem dfs = FileSystem.get(hadoopConf);

        dfs.delete(new Path(ldbcConf.getBuildDir()), true);
        dfs.delete(new Path(ldbcConf.getSocialNetworkDir()), true);
        return hadoopConf;
    }

    public static int getNumThreads(Configuration hadoopConf) {
        return Integer.parseInt(hadoopConf.get("hadoop.numThreads"));
    }
}
