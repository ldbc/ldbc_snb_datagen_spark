package ldbc.snb.datagen.util;

import org.apache.hadoop.conf.Configuration;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Properties;

/**
 * Created by aprat on 6/1/14.
 */
public class ConfigParser {

    public static Configuration initialize() {
        Configuration conf = new Configuration();
        conf.set("ldbc.snb.datagen.generator.scaleFactor", Integer.toString(1));
        conf.set("ldbc.snb.datagen.generator.numThreads", Integer.toString(1));
        conf.set("ldbc.snb.datagen.serializer.personSerializer", "ldbc.snb.datagen.serializer.snb.interactive.CSVPersonSerializer");
        conf.set("ldbc.snb.datagen.serializer.invariantSerializer", "ldbc.snb.datagen.serializer.snb.interactive.CSVInvariantSerializer");
        conf.set("ldbc.snb.datagen.serializer.personActivitySerializer", "ldbc.snb.datagen.serializer.snb.interactive.CSVPersonActivitySerializer");
        conf.set("ldbc.snb.datagen.generator.distribution.degreeDistribution", "ldbc.snb.datagen.generator.distribution.FacebookDegreeDistribution");
        conf.set("ldbc.snb.datagen.serializer.compressed", Boolean.toString(false));
        conf.set("ldbc.snb.datagen.serializer.updateStreams", Boolean.toString(false));
        conf.set("ldbc.snb.datagen.serializer.numPartitions", "1");
        conf.set("ldbc.snb.datagen.serializer.outputDir", "./");
        conf.set("ldbc.snb.datagen.serializer.socialNetworkDir", "./social_network");
        conf.set("ldbc.snb.datagen.serializer.socialNetworkDir", "./social_network");
        conf.set("ldbc.snb.datagen.generator.deltaTime", "10000");
        return conf;
    }

    public static Configuration readConfig(Configuration conf, String paramsFile) {
        try {
            Properties properties = new Properties();
            properties.load(new InputStreamReader(new FileInputStream(paramsFile), "UTF-8"));
            for( String s : properties.stringPropertyNames()) {
                conf.set(s,properties.getProperty(s));
            }
            if (conf.get("fs.defaultFS").compareTo("file:///") == 0) {
                System.out.println("Running in standalone mode. Setting numThreads to 1");
                conf.set("ldbc.snb.datagen.generator.numThreads", "1");
            } else {
		    int maxThreads = Math.max(conf.getInt("mapreduce.tasktracker.map.tasks.maximum",1), conf.getInt("mapreduce.tasktracker.reduce.tasks.maximum",1));
		    int numThreads = conf.getInt("ldbc.snb.datagen.generator.numThreads", 1 );
                if ( maxThreads  <  numThreads ) {
			System.out.println("ldbc.snb.datagen.generator.numThreads changed to "+maxThreads);
			System.out.println("Increase the values of mapreduce.tasktracker.map.tasks.maximum and mapreduce.tasktracker.reduce.tasks.maximum "+maxThreads);
			conf.setInt("ldbc.snb.datagen.generator.numThreads", maxThreads);
		} 
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            System.exit(-1);
        }
        return conf;
    }

    private static void checkOption(Configuration conf, String option, Properties properties) {
        String value = properties.getProperty(option);
        if (value != null) {
            conf.set(option, value);
        }
    }

    public static void printConfig(Configuration conf) {
        System.out.println("********* Configuration *********");
        Map<String,String> map = conf.getValByRegex("^(ldbc.snb.datagen).*$");
        for( Map.Entry<String,String> e : map.entrySet() ) {
            System.out.println(e.getKey()+ " "+e.getValue());
        }
        System.out.println("*********************************");
    }
}
