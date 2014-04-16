package ldbc.socialnet.dbgen.serializer.CSVSerializer;

import ldbc.socialnet.dbgen.objects.Like;

import java.util.ArrayList;

/**
 * Created by aprat on 4/14/14.
 */
public class LikeResolver implements EntityFieldResolver<Like> {
    @Override
    public ArrayList<String> queryField(String string, Like object) {
        return null;
    }
}
