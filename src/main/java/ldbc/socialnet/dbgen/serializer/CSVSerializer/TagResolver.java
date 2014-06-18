package ldbc.socialnet.dbgen.serializer.CSVSerializer;

import ldbc.socialnet.dbgen.objects.Tag;
import ldbc.socialnet.dbgen.vocabulary.DBP;

import java.util.ArrayList;

/**
 * Created by aprat on 4/14/14.
 */
public class TagResolver implements EntityFieldResolver<Tag> {
    @Override
    public ArrayList<String> queryField(String string, Tag tag) {

        ArrayList<String> ret = new ArrayList<String>();
        if( string.equals("id") ) {
            ret.add(Integer.toString(tag.id));
            return ret;
        }

        if( string.equals("name") ) {
            ret.add(tag.name);
            return ret;
        }

        if( string.equals("url") ) {
            ret.add(DBP.getUrl(tag.name));
            return ret;
        }

        if( string.equals("class") ) {
            ret.add(Integer.toString(tag.tagClass));
            return ret;
        }

        return null;
    }
}
