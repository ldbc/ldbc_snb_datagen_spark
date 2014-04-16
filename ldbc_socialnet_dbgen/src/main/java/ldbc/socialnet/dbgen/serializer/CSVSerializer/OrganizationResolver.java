package ldbc.socialnet.dbgen.serializer.CSVSerializer;

import ldbc.socialnet.dbgen.objects.Organization;

import java.util.ArrayList;

/**
 * Created by aprat on 4/14/14.
 */
public class OrganizationResolver implements EntityFieldResolver<Organization> {
    @Override
    public ArrayList<String> queryField(String string, Organization object) {
        return null;
    }
}
