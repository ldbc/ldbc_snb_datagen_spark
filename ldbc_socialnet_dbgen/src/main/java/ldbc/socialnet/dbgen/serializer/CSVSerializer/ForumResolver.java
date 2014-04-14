package ldbc.socialnet.dbgen.serializer.CSVSerializer;

import ldbc.socialnet.dbgen.objects.Group;

import java.util.ArrayList;

/**
 * Created by aprat on 4/14/14.
 */
public class ForumResolver implements EntityFieldResolver<Group> {

    @Override
    public ArrayList<String> queryField(String string, Group object) {
        return null;
    }
}
