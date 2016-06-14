package ldbc.snb.datagen.serializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.zip.GZIPOutputStream;

public class HDFSWriter {

    private String outputDir;
    private String prefix;
    private int numPartitions;
    private int currentPartition = 0;
    private boolean compressed;
    private boolean firstEntry = true;
    private StringBuffer buffer;

    private OutputStream[] fileOutputStream;

    public HDFSWriter(String outputDir, String prefix, int numPartitions, boolean compressed, String extension) {
        this.outputDir = outputDir;
        this.prefix = prefix;
        this.numPartitions = numPartitions;
        this.compressed = compressed;
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            fileOutputStream = new OutputStream[numPartitions];
            if (compressed) {
                for (int i = 0; i < numPartitions; i++) {
                    this.fileOutputStream[i] = new GZIPOutputStream(fs.create(new Path(outputDir + "/" + prefix + "_" + i + "."+extension+".gz"),true, 131072));
                }
            } else {
                for (int i = 0; i < numPartitions; i++) {
                    this.fileOutputStream[i] = fs.create(new Path(outputDir + "/" + prefix + "_" + i + "."+extension), true, 131072);
                }
            }
            buffer = new StringBuffer(1024);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(-1);
        }
    }

    public void write( String entry ) {
        buffer.setLength(0);
        buffer.append(entry);
        try {
            fileOutputStream[currentPartition].write(buffer.toString().getBytes("UTF8"));
            currentPartition = ++currentPartition % numPartitions;
        }
        catch (IOException e){
            System.out.println("Cannot write to output file ");
            e.printStackTrace();
        }
    }

    public void writeAllPartitions( String entry ) {
        try {
            for(int i = 0; i < numPartitions;++i ) {
                fileOutputStream[i].write(entry.getBytes("UTF8"));
            }
        }
        catch (IOException e){
            System.out.println("Cannot write to output file ");
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            for (int i = 0; i < numPartitions; ++i) {
                fileOutputStream[i].flush();
                fileOutputStream[i].close();
            }
        } catch (IOException e) {
            System.err.println("Exception when closing a file");
            System.err.println(e.getMessage());
        }
    }
}
