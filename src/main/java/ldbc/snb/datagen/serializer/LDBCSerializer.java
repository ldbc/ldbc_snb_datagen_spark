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

}
