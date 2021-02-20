package ldbc.snb.datagen.serializer;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.hadoop.writer.HdfsCsvWriter;
import ldbc.snb.datagen.util.formatter.DateFormatter;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

abstract public class LdbcSerializer implements Serializer<HdfsCsvWriter> {

    private DateFormatter dateFormatter;

    protected Map<FileName, HdfsCsvWriter> writers;

    abstract public List<FileName> getFileNames();

    abstract public void writeFileHeaders();

    public Map<FileName, HdfsCsvWriter> initialize(
            FileSystem fs,
            String outputDir,
            int reducerId,
            boolean isCompressed,
            boolean dynamic,
            List<FileName> fileNames
    ) throws IOException {

        Map<FileName, HdfsCsvWriter> writers = new HashMap<>();
        for (FileName f : fileNames) {
            writers.put(f, new HdfsCsvWriter(
                            fs,
                            outputDir + "/csv/raw/composite_merge_foreign" + (dynamic ? "/dynamic/" : "/static/") + f.toString() + "/",
                            String.valueOf(reducerId),
                            DatagenParams.numUpdateStreams,
                            isCompressed,
                            "|"
                    )
            );
        }
        return writers;
    }

    public void initialize(FileSystem fs, String outputDir, int reducerId, boolean isCompressed) throws IOException {
        writers = initialize(fs, outputDir, reducerId, isCompressed, isDynamic(), getFileNames());
        writeFileHeaders();
        this.dateFormatter = new DateFormatter();
    }

    protected String formatDateTime(long epochMillis) {
        return dateFormatter.formatDateTime(epochMillis);
    }

    protected String formatDate(long epochMillis) {
        return dateFormatter.formatDate(epochMillis);
    }

    protected abstract boolean isDynamic();

    public void close() {
        for (FileName f : getFileNames()) {
            writers.get(f).close();
        }
    }

}
