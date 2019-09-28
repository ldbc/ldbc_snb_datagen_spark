package ldbc.snb.datagen.serializer;

import ldbc.snb.datagen.hadoop.writer.HDFSCSVWriter;
import ldbc.snb.datagen.serializer.snb.csv.FileName;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

abstract public class LDBCSerializer {

    protected Map<FileName, HDFSCSVWriter> writers = new HashMap<>();

    abstract public List<FileName> getFileNames();

    abstract public void writeFileHeaders();

    public void initialize(Configuration conf, int reducerId) throws IOException {
        for (FileName f : getFileNames()) {
            writers.put(f, new HDFSCSVWriter(
                    conf.get("ldbc.snb.datagen.serializer.socialNetworkDir") + "/dynamic/",
                    f.toString() + "_" + reducerId,
                    conf.getInt("ldbc.snb.datagen.numPartitions", 1),
                    conf.getBoolean("ldbc.snb.datagen.serializer.compressed", false), "|",
                    conf.getBoolean("ldbc.snb.datagen.serializer.endlineSeparator", false))
            );
        }
        writeFileHeaders();
    }

    public void close() {
        for (FileName f : getFileNames()) {
            writers.get(f).close();
        }
    }

}
