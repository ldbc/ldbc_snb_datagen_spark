package ldbc.socialnet.dbgen.serializer.CSVSerializer;

import ldbc.socialnet.dbgen.objects.TagClass;
import ldbc.socialnet.dbgen.vocabulary.DBPOWL;

import java.util.ArrayList;

/**
 * Created by aprat on 4/14/14.
 */
public class TagClassResolver implements EntityFieldResolver<TagClass> {
    @Override
    public ArrayList<String> queryField(String string, TagClass tagClass) {

        ArrayList<String> ret = new ArrayList<String>();
        if( string.equals("id") ) {
            ret.add(Integer.toString(tagClass.id));
            return ret;
        }

        if( string.equals("name") ) {
            ret.add(tagClass.name);
            return ret;
        }

        if( string.equals("url") ) {
            if( tagClass.name.equals("Thing") ) {
                ret.add("http://www.w3.org/2002/07/owl#Thing");
            } else {
                ret.add(DBPOWL.getUrl(tagClass.name));
            }
            return ret;
        }

        if( string.equals("parent") ) {
            ret.add(Integer.toString(tagClass.parent));
            return ret;
        }
        return null;
    }
}
