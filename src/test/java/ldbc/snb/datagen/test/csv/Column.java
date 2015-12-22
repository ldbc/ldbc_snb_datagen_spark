package ldbc.snb.datagen.test.csv;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by aprat on 18/12/15.
 */
public class Column {

    public static Set<String> readColumnAsSet(String fileName, int index) {

        Set<String> ret = new HashSet<String>();
        try {
            File file = new File(fileName);
            CsvFileReader csvReader = new CsvFileReader(file);
            while (csvReader.hasNext()) {
                String[] line = csvReader.next();
                ret.add(line[index]);
            }
        }catch(Exception e) {
            System.err.println("Error when reading file "+fileName);
            System.err.println(e.getMessage());
        }
        return ret;
    }


}
