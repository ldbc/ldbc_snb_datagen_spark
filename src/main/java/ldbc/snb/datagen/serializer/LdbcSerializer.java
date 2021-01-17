package ldbc.snb.datagen.serializer;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.hadoop.writer.HdfsCsvWriter;
import ldbc.snb.datagen.hadoop.writer.HdfsWriter;
import ldbc.snb.datagen.util.DateUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

abstract public class LdbcSerializer implements Serializer<HdfsCsvWriter> {

    protected Map<FileName, HdfsCsvWriter> writers;

    abstract public List<FileName> getFileNames();

    abstract public void writeFileHeaders();

    public Map<FileName, HdfsCsvWriter> initialize(
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
                            outputDir + (dynamic ? "/dynamic/" : "/static/") + f.toString() + "/",
                            String.valueOf(reducerId),
                            DatagenParams.numUpdateStreams,
                            isCompressed,
                            "|",
                            insertTrailingSeparator
                    )
            );
        }
        return writers;
    }

    public void initialize(Configuration conf, String outputDir, int reducerId, boolean isCompressed, boolean insertTrailingSeparator) throws IOException {
        writers = initialize(conf, outputDir, reducerId, isCompressed, insertTrailingSeparator, isDynamic(), getFileNames());
        writeFileHeaders();
    }

    protected String formatDate(long date) {
        return Long.toString(DateUtils.toEpochMilli(DateUtils.utcDateOfEpochMilli(date)));
    }

    protected String formatDateTime(long date) {
        return Long.toString(date);
    }

    protected abstract boolean isDynamic();

    public void close() {
        for (FileName f : getFileNames()) {
            writers.get(f).close();
        }
    }

}
