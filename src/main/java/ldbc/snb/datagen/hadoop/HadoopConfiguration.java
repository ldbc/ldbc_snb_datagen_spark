package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.hadoop.writer.HdfsCsvWriter;
import ldbc.snb.datagen.serializer.DynamicActivitySerializer;
import ldbc.snb.datagen.serializer.DynamicPersonSerializer;
import ldbc.snb.datagen.serializer.StaticSerializer;
import ldbc.snb.datagen.serializer.snb.csv.dynamicserializer.activity.CsvBasicDynamicActivitySerializer;
import ldbc.snb.datagen.serializer.snb.csv.dynamicserializer.activity.CsvCompositeDynamicActivitySerializer;
import ldbc.snb.datagen.serializer.snb.csv.dynamicserializer.activity.CsvCompositeMergeForeignDynamicActivitySerializer;
import ldbc.snb.datagen.serializer.snb.csv.dynamicserializer.activity.CsvMergeForeignDynamicActivitySerializer;
import ldbc.snb.datagen.serializer.snb.csv.dynamicserializer.person.CsvBasicDynamicPersonSerializer;
import ldbc.snb.datagen.serializer.snb.csv.dynamicserializer.person.CsvCompositeDynamicPersonSerializer;
import ldbc.snb.datagen.serializer.snb.csv.dynamicserializer.person.CsvCompositeMergeForeignDynamicPersonSerializer;
import ldbc.snb.datagen.serializer.snb.csv.dynamicserializer.person.CsvMergeForeignDynamicPersonSerializer;
import ldbc.snb.datagen.serializer.snb.csv.staticserializer.CsvBasicStaticSerializer;
import ldbc.snb.datagen.serializer.snb.csv.staticserializer.CsvCompositeMergeForeignStaticSerializer;
import ldbc.snb.datagen.serializer.snb.csv.staticserializer.CsvCompositeStaticSerializer;
import ldbc.snb.datagen.serializer.snb.csv.staticserializer.CsvMergeForeignStaticSerializer;
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

    public static DynamicPersonSerializer<HdfsCsvWriter> getDynamicPersonSerializer(Configuration hadoopConf) {

        String serializerFormat = hadoopConf.get("serializer.format");

        DynamicPersonSerializer<HdfsCsvWriter> output;
        switch (serializerFormat) {
            case "CsvBasic":
                output = new CsvBasicDynamicPersonSerializer();
                break;
            case "CsvMergeForeign":
                output = new CsvMergeForeignDynamicPersonSerializer();
                break;
            case "CsvComposite":
                output = new CsvCompositeDynamicPersonSerializer();
                break;
            case "CsvCompositeMergeForeign":
                output = new CsvCompositeMergeForeignDynamicPersonSerializer();
                break;
            default:
                throw new IllegalStateException("Unexpected person serializer: " + serializerFormat);
        }

        return output;
    }

    public static DynamicActivitySerializer<HdfsCsvWriter> getDynamicActivitySerializer(Configuration hadoopConf) {

        String serializerFormat = hadoopConf.get("serializer.format");


        DynamicActivitySerializer<HdfsCsvWriter> output;
        switch (serializerFormat) {
            case "CsvBasic":
                output = new CsvBasicDynamicActivitySerializer();
                break;
            case "CsvMergeForeign":
                output = new CsvMergeForeignDynamicActivitySerializer();
                break;
            case "CsvComposite":
                output = new CsvCompositeDynamicActivitySerializer();
                break;
            case "CsvCompositeMergeForeign":
                output = new CsvCompositeMergeForeignDynamicActivitySerializer();
                break;
            default:
                throw new IllegalStateException("Unexpected activity serializer: " + serializerFormat);
        }

        return output;
    }

    public static StaticSerializer<HdfsCsvWriter> getStaticSerializer(Configuration hadoopConf) {

        String serializerFormat = hadoopConf.get("serializer.format");


        StaticSerializer<HdfsCsvWriter> output;
        switch (serializerFormat) {
            case "CsvBasic":
                output = new CsvBasicStaticSerializer();
                break;
            case "CsvComposite":
                output = new CsvCompositeStaticSerializer();
                break;
            case "CsvCompositeMergeForeign":
                output = new CsvCompositeMergeForeignStaticSerializer();
                break;
            case "CsvMergeForeign":
                output = new CsvMergeForeignStaticSerializer();
                break;
            default:
                throw new IllegalStateException("Unexpected static serializer: " + serializerFormat);
        }

        return output;
    }

    public static boolean isCompressed(Configuration hadoopConf) {

        return Boolean.parseBoolean(hadoopConf.get("serializer.compressed"));

    }

}
