package ldbc.socialnet.dbgen.serializer.CSVSerializer;

import ldbc.socialnet.dbgen.generator.DateGenerator;
import ldbc.socialnet.dbgen.objects.Like;
import ldbc.socialnet.dbgen.vocabulary.SN;

import java.util.ArrayList;
import java.util.GregorianCalendar;

/**
 * Created by aprat on 4/14/14.
 */
public class LikeResolver implements EntityFieldResolver<Like> {

    GregorianCalendar date;
    public LikeResolver() {
        date = new GregorianCalendar();
    }

    @Override
    public ArrayList<String> queryField(String string, Like like) {

        ArrayList<String> ret = new ArrayList<String>();
        if( string.equals("userId") ) {
            ret.add(Long.toString(like.user));
            return ret;
        }

        if( string.equals("creationDate") ) {
            date.setTimeInMillis(like.date);
            ret.add(DateGenerator.formatDateDetail(date));
            return ret;
        }

        if( string.equals("messageId") ) {
            ret.add(SN.formId(like.messageId));
            return ret;
        }

        return null;
    }
}
