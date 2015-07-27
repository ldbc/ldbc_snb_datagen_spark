package ldbc.snb.datagen.serializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.zip.GZIPOutputStream;

public class HDFSCSVWriter extends HDFSWriter {

    private String separator = "|";
    private StringBuffer buffer;
    private boolean endLineSeparator = true;

    private OutputStream[] fileOutputStream;

    public HDFSCSVWriter( String outputDir, String prefix, int numPartitions, boolean compressed, String separator, boolean endLineSeparator )  {
       super(outputDir, prefix, numPartitions, compressed, "csv" );
        this.separator = separator;
        this.buffer = new StringBuffer(2048);
        this.endLineSeparator = endLineSeparator;

    }

    public void writeEntry( ArrayList<String> entry ) {
        buffer.setLength(0);
        for( int i = 0; i < entry.size(); ++i)  {
            buffer.append(entry.get(i));
            if((endLineSeparator && i == (entry.size() - 1)) || (i < entry.size() - 1))
                buffer.append(separator);
        }
        buffer.append("\n");
        this.write(buffer.toString());
    }
}
