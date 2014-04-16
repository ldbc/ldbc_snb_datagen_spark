package ldbc.socialnet.dbgen.serializer.CSVSerializer;

import ldbc.socialnet.dbgen.generator.DateGenerator;
import ldbc.socialnet.dbgen.objects.GroupMemberShip;
import ldbc.socialnet.dbgen.vocabulary.SN;

import java.util.ArrayList;
import java.util.GregorianCalendar;

/**
 * Created by aprat on 4/14/14.
 */
public class MembershipResolver implements EntityFieldResolver<GroupMemberShip> {

    GregorianCalendar date;
    public MembershipResolver() {
        date = new GregorianCalendar();
    }

    @Override
    public ArrayList<String> queryField(String string, GroupMemberShip membership) {
        ArrayList<String> ret = new ArrayList<String>();
        if( string.equals("creationDate") ) {
            date.setTimeInMillis(membership.getJoinDate());
            ret.add(DateGenerator.formatDateDetail(date));
            return ret;
        }

        if( string.equals("forumId") ) {
            ret.add(SN.formId(membership.getGroupId()));
            return ret;
        }

        if( string.equals("userId") ) {
            ret.add(Long.toString(membership.getUserId()));
            return ret;
        }
        return null;
    }
}
