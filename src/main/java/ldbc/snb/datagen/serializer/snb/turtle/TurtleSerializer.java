package ldbc.snb.datagen.serializer.snb.turtle;

import ldbc.snb.datagen.hadoop.writer.HDFSCSVWriter;
import ldbc.snb.datagen.hadoop.writer.HDFSWriter;
import ldbc.snb.datagen.serializer.Serializer;
import ldbc.snb.datagen.serializer.snb.csv.FileName;
import org.apache.hadoop.conf.Configuration;
import org.python.compiler.Filename;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface TurtleSerializer extends Serializer<HDFSWriter> {

    default Map<FileName, HDFSWriter> initialize(Configuration conf, int reducerId, boolean dynamic, List<FileName> fileNames) throws IOException {
        Map<FileName, HDFSWriter> writers = new HashMap<>();

        for (FileName f : fileNames) {
            HDFSWriter w = new HDFSWriter(conf.get("ldbc.snb.datagen.serializer.socialNetworkDir"),
                    f.toString() + "_" + reducerId, conf.getInt("ldbc.snb.datagen.numPartitions", 1),
                    conf.getBoolean("ldbc.snb.datagen.serializer.compressed", false), "ttl");
            writers.put(f, w);

            w.writeAllPartitions(Turtle.getNamespaces());
            w.writeAllPartitions(Turtle.getStaticNamespaces());
        }
        return writers;
    }

}
