package ldbc.socialnet.dbgen.serializer.CSVSerializer;

import ldbc.socialnet.dbgen.objects.GroupMemberShip;

import java.util.ArrayList;

/**
 * Created by aprat on 4/14/14.
 */
public class MembershipResolver implements EntityFieldResolver<GroupMemberShip> {
    @Override
    public ArrayList<String> queryField(String string, GroupMemberShip object) {
        return null;
    }
}
