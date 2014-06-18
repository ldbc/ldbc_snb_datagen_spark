package ldbc.socialnet.dbgen.serializer.CSVSerializer;

import ldbc.socialnet.dbgen.generator.DateGenerator;
import ldbc.socialnet.dbgen.objects.Friend;
import ldbc.socialnet.dbgen.vocabulary.SN;

import java.util.ArrayList;
import java.util.GregorianCalendar;

/**
 * Created by aprat on 4/14/14.
 */
public class FriendshipResolver implements EntityFieldResolver<Friend> {

    GregorianCalendar date;
    public FriendshipResolver() {
        date = new GregorianCalendar();
    }

    @Override
    public ArrayList<String> queryField(String string, Friend friend) {

        ArrayList<String> ret = new ArrayList<String>();
        if( string.equals("id1") ) {
            ret.add(Long.toString(friend.getUserAcc()));
            return ret;
        }

        if( string.equals("id2") ) {
            ret.add(Long.toString(friend.getFriendAcc()));
            return ret;
        }

        if( string.equals("creationDate") ) {
            date.setTimeInMillis(friend.getCreatedTime());
            ret.add(DateGenerator.formatDateDetail(date));
            return ret;
        }

        return null;
    }
}
