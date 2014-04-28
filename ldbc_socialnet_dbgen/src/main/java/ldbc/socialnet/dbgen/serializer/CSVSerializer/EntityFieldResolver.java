package ldbc.socialnet.dbgen.serializer.CSVSerializer;

import java.util.ArrayList;

/**
 * Created by aprat on 4/14/14.
 */
public interface EntityFieldResolver< T > {

    ArrayList<String> queryField(String string, T object);

}
