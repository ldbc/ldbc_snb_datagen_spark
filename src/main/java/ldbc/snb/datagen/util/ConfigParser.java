package ldbc.snb.datagen.util;

import org.apache.hadoop.conf.Configuration;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * Created by aprat on 6/1/14.
 */
public class ConfigParser {

    public static Configuration initialize() {
        Configuration conf = new Configuration();
        conf.set("scaleFactor", Integer.toString(1));
        conf.set("numThreads", Integer.toString(1));
        conf.set("serializer", "ldbc.snb.datagen.serializer.snb.interactive.CSVPersonSerializer");
        conf.set("compressed", Boolean.toString(false));
        conf.set("updateStreams", Boolean.toString(false));
        conf.set("outputDir", "./");
        conf.set("deltaTime", "10000");
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
                conf.set("numThreads", "1");
            }
            conf.set("hadoopDir",conf.get("outputDir")+"/hadoop");
            conf.set("socialNetworkDir",conf.get("outputDir")+"/social_network");
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
        if (conf.get("numPersons") != null && conf.get("numYears") != null && conf.get("startYear") != null) {
            System.out.println("numPersons: " + conf.get("numPersons"));
            System.out.println("numYears: " + conf.get("numYears"));
            System.out.println("startYear: " + conf.get("startYear"));
        } else {
            System.out.println("scaleFactor: " + conf.get("scaleFactor"));
        }
        System.out.println("numThreads: " + conf.get("numThreads"));
        System.out.println("serializer: " + conf.get("serializer"));
        System.out.println("compressed: " + conf.get("compressed"));
        System.out.println("updateStreams: " + conf.get("updateStreams"));
        System.out.println("outputDir: " + conf.get("outputDir"));
        System.out.println("*********************************");
    }
}
