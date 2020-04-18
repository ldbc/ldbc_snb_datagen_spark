package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.util.Config;
import ldbc.snb.datagen.util.ConfigParser;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.Map;


public class HadoopConfiguration {

    public static Config extractLdbcConfig(Configuration hadoop) {
        return new Config(hadoop.getValByRegex("^(ldbc.snb.datagen).*$"));
    }

    public static void mergeLdbcIntoHadoop(Config config, Configuration hadoop) {
        for (Map.Entry<String, String> entry : config.map.entrySet()) {
            hadoop.set(entry.getKey(), entry.getValue());
        }
    }

    public static Configuration prepare(Map<String, String> conf) throws IOException {
        Configuration hadoopConf = new Configuration();

        if (hadoopConf.get("fs.defaultFS").compareTo("file:///") == 0) {
            System.out.println("Running in standalone mode. Setting numThreads to 1");
            conf.put("ldbc.snb.datagen.generator.numThreads", "1");
        }

        conf.put("ldbc.snb.datagen.serializer.hadoopDir", conf.get("ldbc.snb.datagen.serializer.outputDir") + "/hadoop");
        conf.put("ldbc.snb.datagen.serializer.socialNetworkDir", conf.get("ldbc.snb.datagen.serializer.outputDir") + "/social_network");

        ConfigParser.printConfig(conf);

        mergeLdbcIntoHadoop(new Config(conf), hadoopConf);
        FileSystem dfs = FileSystem.get(hadoopConf);

        dfs.delete(new Path(conf.get("ldbc.snb.datagen.serializer.hadoopDir")), true);
        dfs.delete(new Path(conf.get("ldbc.snb.datagen.serializer.socialNetworkDir")), true);
        FileUtils.deleteDirectory(new File(conf.get("ldbc.snb.datagen.serializer.outputDir") + "/substitution_parameters"));
        return hadoopConf;
    }
}
