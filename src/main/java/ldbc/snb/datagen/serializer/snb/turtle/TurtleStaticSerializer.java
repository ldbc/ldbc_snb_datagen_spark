/* 
 Copyright (c) 2013 LDBC
 Linked Data Benchmark Council (http://www.ldbcouncil.org)
 
 This file is part of ldbc_snb_datagen.
 
 ldbc_snb_datagen is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.
 
 ldbc_snb_datagen is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with ldbc_snb_datagen.  If not, see <http://www.gnu.org/licenses/>.
 
 Copyright (C) 2011 OpenLink Software <bdsmt@openlinksw.com>
 All Rights Reserved.
 
 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation;  only Version 2 of the License dated
 June 1991.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.*/
package ldbc.snb.datagen.serializer.snb.turtle;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.statictype.Organisation;
import ldbc.snb.datagen.entities.statictype.TagClass;
import ldbc.snb.datagen.entities.statictype.place.Place;
import ldbc.snb.datagen.entities.statictype.tag.Tag;
import ldbc.snb.datagen.hadoop.writer.HDFSWriter;
import ldbc.snb.datagen.serializer.StaticSerializer;
import ldbc.snb.datagen.serializer.snb.csv.FileName;
import ldbc.snb.datagen.vocabulary.DBP;
import ldbc.snb.datagen.vocabulary.DBPOWL;
import ldbc.snb.datagen.vocabulary.FOAF;
import ldbc.snb.datagen.vocabulary.RDF;
import ldbc.snb.datagen.vocabulary.RDFS;
import ldbc.snb.datagen.vocabulary.SN;
import ldbc.snb.datagen.vocabulary.SNTAG;
import ldbc.snb.datagen.vocabulary.SNVOC;
import ldbc.snb.datagen.vocabulary.XSD;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.List;


/**
 * Created by aprat on 12/17/14.
 */
public class TurtleStaticSerializer extends StaticSerializer {

    private HDFSWriter[] writers;

    private enum FileNames {
        SOCIAL_NETWORK("social_network_static");
        private final String name;

        private FileNames(String name) {
            this.name = name;
        }

        public String toString() {
            return name;
        }
    }

    @Override
    public List<FileName> getFileNames() {
        return null;
    }

    @Override
    public void writeFileHeaders() {

    }

    @Override
    public void initialize(Configuration conf, int reducerId) throws IOException {

        int numFiles = FileNames.values().length;
        writers = new HDFSWriter[numFiles];
        for (int i = 0; i < numFiles; ++i) {
            writers[i] = new HDFSWriter(conf.get("ldbc.snb.datagen.serializer.socialNetworkDir")+"/static/", FileNames.values()[i]
                    .toString() + "_" + reducerId, conf.getInt("ldbc.snb.datagen.numPartitions", 1), conf
                                                .getBoolean("ldbc.snb.datagen.serializer.compressed", false), "ttl");
            writers[i].writeAllPartitions(Turtle.getNamespaces());
            writers[i].writeAllPartitions(Turtle.getStaticNamespaces());
        }
    }

    @Override
    public void close() {
        int numFiles = FileNames.values().length;
        for (int i = 0; i < numFiles; ++i) {
            writers[i].close();
        }
    }

    @Override
    protected void serialize(final Place place) {
        StringBuffer result = new StringBuffer(350);
        String name = place.getName();
        String type = DBPOWL.City;
        if (place.getType() == Place.COUNTRY) {
            type = DBPOWL.Country;
        } else if (place.getType() == Place.CONTINENT) {
            type = DBPOWL.Continent;
        }

        Turtle.writeDBPData(writers[FileNames.SOCIAL_NETWORK.ordinal()], DBP
                .fullPrefixed(name), RDF.type, DBPOWL.Place);
        Turtle.writeDBPData(writers[FileNames.SOCIAL_NETWORK.ordinal()], DBP.fullPrefixed(name), RDF.type, type);
        Turtle.writeDBPData(writers[FileNames.SOCIAL_NETWORK.ordinal()], DBP.fullPrefixed(name), FOAF.Name, Turtle
                .createLiteral(name));
        Turtle.createTripleSPO(result, DBP.fullPrefixed(name), SNVOC.id,
                               Turtle.createDataTypeLiteral(Long.toString(place.getId()), XSD.Int));
        if (place.getType() != Place.CONTINENT) {
            String countryName = Dictionaries.places.getPlaceName(Dictionaries.places.belongsTo(place.getId()));
            Turtle.createTripleSPO(result, DBP.fullPrefixed(name), SNVOC.isPartOf, DBP.fullPrefixed(countryName));
            writers[FileNames.SOCIAL_NETWORK.ordinal()].write(result.toString());
        }
    }

    @Override
    protected void serialize(final Organisation organisation) {
        StringBuffer result = new StringBuffer(19000);
        if (organisation.type == Organisation.OrganisationType.company) {
            Turtle.writeDBPData(writers[FileNames.SOCIAL_NETWORK.ordinal()], SN
                    .getCompURI(organisation.id), RDF.type, DBPOWL.Company);
            Turtle.writeDBPData(writers[FileNames.SOCIAL_NETWORK.ordinal()], SN
                    .getCompURI(organisation.id), SNVOC.url, DBP.fullPrefixed(organisation.name));
            Turtle.writeDBPData(writers[FileNames.SOCIAL_NETWORK.ordinal()], SN.getCompURI(organisation.id), FOAF.Name,
                                Turtle.createLiteral(organisation.name));
            Turtle.createTripleSPO(result, SN.getCompURI(organisation.id),
                                   SNVOC.locatedIn, DBP
                                           .fullPrefixed(Dictionaries.places.getPlaceName(organisation.location)));
            Turtle.createTripleSPO(result, SN.getCompURI(organisation.id), SNVOC.id,
                                   Turtle.createDataTypeLiteral(Long.toString(organisation.id), XSD.Int));
        } else {
            Turtle.writeDBPData(writers[FileNames.SOCIAL_NETWORK.ordinal()], SN
                    .getUnivURI(organisation.id), RDF.type, DBPOWL.University);
            Turtle.writeDBPData(writers[FileNames.SOCIAL_NETWORK.ordinal()], SN
                    .getUnivURI(organisation.id), SNVOC.url, DBP.fullPrefixed(organisation.name));
            Turtle.writeDBPData(writers[FileNames.SOCIAL_NETWORK.ordinal()], SN.getUnivURI(organisation.id), FOAF.Name,
                                Turtle.createLiteral(organisation.name));
            Turtle.createTripleSPO(result, SN.getUnivURI(organisation.id),
                                   SNVOC.locatedIn, DBP
                                           .fullPrefixed(Dictionaries.places.getPlaceName(organisation.location)));
            Turtle.createTripleSPO(result, SN.getUnivURI(organisation.id), SNVOC.id,
                                   Turtle.createDataTypeLiteral(Long.toString(organisation.id), XSD.Int));
        }

        writers[FileNames.SOCIAL_NETWORK.ordinal()].write(result.toString());
    }

    @Override
    protected void serialize(final TagClass tagClass) {

        StringBuffer result = new StringBuffer(350);
        Turtle.writeDBPData(writers[FileNames.SOCIAL_NETWORK.ordinal()], SN
                .getTagClassURI(tagClass.id), RDFS.label, Turtle
                                    .createLiteral(Dictionaries.tags.getClassName(tagClass.id)));
        Turtle.createTripleSPO(result, SN.getTagClassURI(tagClass.id), RDF.type, SNVOC.TagClass);

        if ("Thing".equals(tagClass.name)) {
            Turtle.createTripleSPO(result, SN
                    .getTagClassURI(tagClass.id), SNVOC.url, "<http://www.w3.org/2002/07/owl#Thing>");
        } else {
            Turtle.createTripleSPO(result, SN.getTagClassURI(tagClass.id), SNVOC.url, DBPOWL
                    .prefixed(Dictionaries.tags.getClassName(tagClass.id)));
        }

        Turtle.createTripleSPO(result, SN.getTagClassURI(tagClass.id), SNVOC.id,
                               Turtle.createDataTypeLiteral(Long.toString(tagClass.id), XSD.Int));
        writers[FileNames.SOCIAL_NETWORK.ordinal()].write(result.toString());

        Integer parent = Dictionaries.tags.getClassParent(tagClass.id);
        if (parent != -1) {
            Turtle.writeDBPData(writers[FileNames.SOCIAL_NETWORK.ordinal()], SN
                    .getTagClassURI(tagClass.id), RDFS.subClassOf, SN.getTagClassURI(parent));
        }
    }

    @Override
    protected void serialize(final Tag tag) {
        StringBuffer result = new StringBuffer(350);
        Turtle.writeDBPData(writers[FileNames.SOCIAL_NETWORK.ordinal()], SNTAG.fullPrefixed(tag.name), FOAF.Name, Turtle
                .createLiteral(tag.name));
        Integer tagClass = tag.tagClass;
        Turtle.writeDBPData(writers[FileNames.SOCIAL_NETWORK.ordinal()], SNTAG.fullPrefixed(tag.name), RDF.type, SN
                .getTagClassURI(tagClass));
        Turtle.createTripleSPO(result, SNTAG.fullPrefixed(tag.name), SNVOC.id,
                               Turtle.createDataTypeLiteral(Long.toString(tag.id), XSD.Int));
        writers[FileNames.SOCIAL_NETWORK.ordinal()].write(result.toString());
    }

}
