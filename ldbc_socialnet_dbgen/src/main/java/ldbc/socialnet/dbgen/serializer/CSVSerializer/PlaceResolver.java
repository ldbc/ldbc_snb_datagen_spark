package ldbc.socialnet.dbgen.serializer.CSVSerializer;

import ldbc.socialnet.dbgen.objects.Location;

import java.util.ArrayList;

/**
 * Created by aprat on 4/14/14.
 */
public class PlaceResolver implements EntityFieldResolver<Location> {
    @Override
    public ArrayList<String> queryField(String string, Location object) {
        return null;
    }
}
