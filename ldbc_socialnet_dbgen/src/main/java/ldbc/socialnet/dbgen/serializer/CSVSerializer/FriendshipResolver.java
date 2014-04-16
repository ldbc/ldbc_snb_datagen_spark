package ldbc.socialnet.dbgen.serializer.CSVSerializer;

import ldbc.socialnet.dbgen.objects.Friend;

import java.util.ArrayList;

/**
 * Created by aprat on 4/14/14.
 */
public class FriendshipResolver implements EntityFieldResolver<Friend> {

    @Override
    public ArrayList<String> queryField(String string, Friend object) {
        return null;
    }
}
