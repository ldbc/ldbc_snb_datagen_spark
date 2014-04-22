package ldbc.socialnet.dbgen.serializer.CSVSerializer;

import ldbc.socialnet.dbgen.dictionary.LocationDictionary;
import ldbc.socialnet.dbgen.objects.Location;
import ldbc.socialnet.dbgen.vocabulary.DBP;
import ldbc.socialnet.dbgen.vocabulary.SN;

import java.util.ArrayList;

/**
 * Created by aprat on 4/14/14.
 */
public class PlaceResolver implements EntityFieldResolver<Location> {

    private LocationDictionary locationDic;

    public PlaceResolver( LocationDictionary locationDic )  {
       this.locationDic = locationDic;
   }

    @Override
    public ArrayList<String> queryField(String string, Location location) {
        ArrayList<String> ret = new ArrayList<String>();
        if( string.equals("id") ) {
            ret.add(Integer.toString(location.getId()));
            return ret;
        }

        if( string.equals("name") ) {
            ret.add(location.getName());
            return ret;
        }

        if( string.equals("url") ) {
            ret.add(DBP.getUrl(location.getName()));
            return ret;
        }

        if( string.equals("type") ) {
            ret.add(location.getType());
            return ret;
        }

        if( string.equals("isPartOf") ) {
            ret.add(Integer.toString(locationDic.belongsTo(location.getId())));
            return ret;
        }
        return null;
    }
}
