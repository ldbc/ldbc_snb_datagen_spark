package ldbc.snb.datagen.serializer.snb.csv;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.hadoop.writer.HdfsCsvWriter;
import ldbc.snb.datagen.serializer.Serializer;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface CsvSerializer extends Serializer<HdfsCsvWriter> {

    default Map<FileName, HdfsCsvWriter> initialize(
            Configuration conf,
            String outputDir,
            int reducerId,
            boolean isCompressed,
            boolean insertTrailingSeparator,
            boolean dynamic,
            List<FileName> fileNames
    ) throws IOException {

        Map<FileName, HdfsCsvWriter> writers = new HashMap<>();
        for (FileName f : fileNames) {
            writers.put(f, new HdfsCsvWriter(
                    conf,
                    outputDir + (dynamic ? "/dynamic/" : "/static/"),
                    f.toString() + "_" + reducerId,
                    DatagenParams.numUpdateStreams,
                    isCompressed,
                    "|",
                    insertTrailingSeparator
                )
            );
        }
        return writers;
    }

}
