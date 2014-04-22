package ldbc.socialnet.dbgen.serializer.CSVSerializer;

import java.io.OutputStream;
import java.util.ArrayList;

/**
 * Created by aprat on 4/14/14.
 */
public class CSVFileDescriptor {

    public String fileName;
    public ArrayList<String> fields;
    public OutputStream file;
}
