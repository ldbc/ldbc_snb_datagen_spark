package ldbc.socialnet.dbgen.serializer.CSVSerializer;

import ldbc.socialnet.dbgen.objects.Organization;
import ldbc.socialnet.dbgen.vocabulary.DBP;

import java.util.ArrayList;

/**
 * Created by aprat on 4/14/14.
 */
public class OrganizationResolver implements EntityFieldResolver<Organization> {

    @Override
    public ArrayList<String> queryField(String string, Organization organization) {

        ArrayList<String> ret = new ArrayList<String>();
        if( string.equals("id") ) {
            ret.add(Long.toString(organization.id));
            return ret;
        }

        if( string.equals("type") ) {
            ret.add(organization.type.toString());
            return ret;
        }

        if( string.equals("name") ) {
            ret.add(organization.name);
            return ret;
        }

        if( string.equals("url") ) {
            ret.add(DBP.getUrl(organization.name));
            return ret;
        }

        if( string.equals("place") ) {
            ret.add(Integer.toString(organization.location));
            return ret;
        }
        return null;
    }
}
