package ldbc.socialnet.dbgen.serializer.CSVSerializer;

import ldbc.socialnet.dbgen.objects.Tag;

import java.util.ArrayList;

/**
 * Created by aprat on 4/14/14.
 */
public class TagResolver implements EntityFieldResolver<Tag> {
    @Override
    public ArrayList<String> queryField(String string, Tag object) {
        return null;
    }
}
