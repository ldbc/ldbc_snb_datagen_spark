package ldbc.socialnet.dbgen.objects;

import ldbc.socialnet.dbgen.generator.ScalableGenerator;

/**
 * Created by aprat on 4/14/14.
 */
public class Organization {


    public long id;
    public String name;
    public ScalableGenerator.OrganisationType type;
    public int location;
}
