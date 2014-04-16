package ldbc.socialnet.dbgen.serializer.CSVSerializer;

import ldbc.socialnet.dbgen.objects.UserInfo;

import java.util.ArrayList;

/**
 * Created by aprat on 4/14/14.
 */
public class PersonResolver  implements EntityFieldResolver<UserInfo> {

    @Override
    public ArrayList<String> queryField(String string, UserInfo object) {
        return null;
    }
}
