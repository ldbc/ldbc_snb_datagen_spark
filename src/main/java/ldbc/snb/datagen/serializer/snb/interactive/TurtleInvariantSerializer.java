package ldbc.snb.datagen.serializer.snb.interactive;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.objects.Organization;
import ldbc.snb.datagen.objects.Place;
import ldbc.snb.datagen.objects.Tag;
import ldbc.snb.datagen.objects.TagClass;

import ldbc.snb.datagen.serializer.HDFSWriter;
import ldbc.snb.datagen.serializer.InvariantSerializer;
import ldbc.snb.datagen.serializer.Turtle;
import ldbc.snb.datagen.vocabulary.*;
import org.apache.hadoop.conf.Configuration;



/**
 * Created by aprat on 12/17/14.
 */
public class TurtleInvariantSerializer extends InvariantSerializer {

    private HDFSWriter[] writers;

    private enum FileNames {
        SOCIAL_NETWORK ("social_network_static");
        private final String name;

        private FileNames( String name ) {
            this.name = name;
        }
        public String toString() {
            return name;
        }
    }

    public void initialize(Configuration conf, int reducerId) {

        int numFiles = FileNames.values().length;
        writers = new HDFSWriter[numFiles];
        for( int i = 0; i < numFiles; ++i) {
            writers[i] = new HDFSWriter(conf.get("ldbc.snb.datagen.serializer.socialNetworkDir"), FileNames.values()[i].toString()+"_"+reducerId,conf.getInt("ldbc.snb.datagen.numPartitions",1),conf.getBoolean("ldbc.snb.datagen.serializer.compressed",false),"ttl");
            writers[i].writeAllPartitions(Turtle.getNamespaces());
            writers[i].writeAllPartitions(Turtle.getStaticNamespaces());
        }
    }

    public void close() {
        int numFiles = FileNames.values().length;
        for(int i = 0; i < numFiles; ++i) {
            writers[i].close();
        }
    }

    protected void serialize(Place place) {
        StringBuffer result = new StringBuffer(350);
        String name = place.getName();
        String type = DBPOWL.City;
        if (place.getType() == Place.COUNTRY) {
            type = DBPOWL.Country;
        } else if (place.getType() == Place.CONTINENT) {
            type = DBPOWL.Continent;
        }

        Turtle.writeDBPData(writers[FileNames.SOCIAL_NETWORK.ordinal()], DBP.fullPrefixed(name), RDF.type, DBPOWL.Place);
        Turtle.writeDBPData(writers[FileNames.SOCIAL_NETWORK.ordinal()], DBP.fullPrefixed(name), RDF.type, type);
        Turtle.writeDBPData(writers[FileNames.SOCIAL_NETWORK.ordinal()], DBP.fullPrefixed(name), FOAF.Name, Turtle.createLiteral(name));
        Turtle.createTripleSPO(result, DBP.fullPrefixed(name), SNVOC.id, Turtle.createLiteral(Long.toString(place.getId())));
        if (place.getType() != Place.CONTINENT) {
            String countryName = Dictionaries.places.getPlaceName(Dictionaries.places.belongsTo(place.getId()));
            Turtle.createTripleSPO(result, DBP.fullPrefixed(name), SNVOC.isPartOf, DBP.fullPrefixed(countryName));
            writers[FileNames.SOCIAL_NETWORK.ordinal()].write(result.toString());
        }
    }

    protected void serialize(Organization organization) {
        StringBuffer result = new StringBuffer(19000);
        if( organization.type == Organization.OrganisationType.company ) {
            Turtle.writeDBPData(writers[FileNames.SOCIAL_NETWORK.ordinal()],DBP.fullPrefixed(organization.name), RDF.type, DBPOWL.Company);
        } else {
            Turtle.writeDBPData(writers[FileNames.SOCIAL_NETWORK.ordinal()], DBP.fullPrefixed(organization.name), RDF.type, DBPOWL.University);
        }
        Turtle.writeDBPData(writers[FileNames.SOCIAL_NETWORK.ordinal()], DBP.fullPrefixed(organization.name), FOAF.Name,
                Turtle.createLiteral(organization.name));
        Turtle.createTripleSPO(result, DBP.fullPrefixed(organization.name),
                SNVOC.locatedIn, DBP.fullPrefixed(Dictionaries.places.getPlaceName(organization.location)));
        Turtle.createTripleSPO(result, DBP.fullPrefixed(organization.name),
                SNVOC.id, Turtle.createLiteral(Long.toString(organization.id)));
        writers[FileNames.SOCIAL_NETWORK.ordinal()].write(result.toString());
    }

    protected void serialize(TagClass tagClass) {

        StringBuffer result = new StringBuffer(350);
        if (tagClass.name.equals("Thing")) {
            Turtle.writeDBPData(writers[FileNames.SOCIAL_NETWORK.ordinal()],"<http://www.w3.org/2002/07/owl#Thing>", RDFS.label, Turtle.createLiteral(Dictionaries.tags.getClassLabel(tagClass.id)));
            Turtle.createTripleSPO(result, "<http://www.w3.org/2002/07/owl#Thing>", RDF.type, SNVOC.TagClass);
            Turtle.createTripleSPO(result, "<http://www.w3.org/2002/07/owl#Thing>", SNVOC.id, Long.toString(tagClass.id));
            writers[FileNames.SOCIAL_NETWORK.ordinal()].write(result.toString());
        } else {
            Turtle.writeDBPData(writers[FileNames.SOCIAL_NETWORK.ordinal()],DBPOWL.prefixed(Dictionaries.tags.getClassName(tagClass.id)), RDFS.label,
                    Turtle.createLiteral(Dictionaries.tags.getClassLabel(tagClass.id)));
            Turtle.createTripleSPO(result, DBP.fullPrefixed(Dictionaries.tags.getClassName(tagClass.id)), RDF.type, SNVOC.TagClass);
            Turtle.createTripleSPO(result, DBP.fullPrefixed(Dictionaries.tags.getClassName(tagClass.id)), SNVOC.id, Long.toString(tagClass.id));
            writers[FileNames.SOCIAL_NETWORK.ordinal()].write(result.toString());
        }
        Integer parent = Dictionaries.tags.getClassParent(tagClass.id);
        if (parent != -1) {
            String parentPrefix;
            if (Dictionaries.tags.getClassName(parent).equals("Thing")) {
                parentPrefix = "<http://www.w3.org/2002/07/owl#Thing>";
            } else {
                parentPrefix = DBPOWL.prefixed(Dictionaries.tags.getClassName(parent));
            }
            Turtle.writeDBPData(writers[FileNames.SOCIAL_NETWORK.ordinal()],DBPOWL.prefixed(Dictionaries.tags.getClassName(tagClass.id)), RDFS.subClassOf, parentPrefix);
        }
    }

    protected void serialize(Tag tag) {
        StringBuffer result = new StringBuffer(350);
        Turtle.writeDBPData(writers[FileNames.SOCIAL_NETWORK.ordinal()],DBP.fullPrefixed(tag.name), FOAF.Name, Turtle.createLiteral(tag.name));
        Integer tagClass = tag.tagClass;
        Turtle.writeDBPData(writers[FileNames.SOCIAL_NETWORK.ordinal()],DBP.fullPrefixed(tag.name), RDF.type, DBPOWL.prefixed(Dictionaries.tags.getClassName(tagClass)));
        Turtle.createTripleSPO(result, DBP.fullPrefixed(tag.name), SNVOC.id, Turtle.createLiteral(Long.toString(tag.id)));
        writers[FileNames.SOCIAL_NETWORK.ordinal()].write(result.toString());
    }
    public void reset() {

    }
}
