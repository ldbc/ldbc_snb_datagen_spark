package ldbc.snb.datagen.serializer.yarspg;


import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.hadoop.writer.HdfsYarsPgWriter;
import ldbc.snb.datagen.serializer.FileName;
import ldbc.snb.datagen.serializer.Serializer;
import ldbc.snb.datagen.vocabulary.FOAF;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface YarsPgSerializer extends Serializer<HdfsYarsPgWriter> {

    default Map<FileName, HdfsYarsPgWriter> initialize(FileSystem fs, String outputDir, int reducerId, boolean isCompressed, boolean dynamic,
                                                       List<FileName> fileNames) throws IOException {
        Map<FileName, HdfsYarsPgWriter> writers = new HashMap<>();
        for (FileName f : fileNames) {
            HdfsYarsPgWriter w = new HdfsYarsPgWriter(
                    fs,
                    outputDir + "/yarspg/" + (dynamic ? "/dynamic/" : "/static/") + f.toString() + "/",
                    String.valueOf(reducerId),
                    DatagenParams.numUpdateStreams,
                    isCompressed
            );
            writers.put(f, w);
            String generatedBy = "-" + FOAF.fullprefixed("organization") + ":\"The LDBC Social Network Benchmark\"";
            String generatedDate = "-" + FOAF.fullprefixed("date") + ":\"" + new Date().toString() + "\"";
            w.writeAllPartitions(generatedBy + "\n" + generatedDate + "\n");
        }

        return writers;
    }
}
