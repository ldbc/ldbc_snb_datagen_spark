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


package ldbc.snb.datagen.serializer.snb.interactive;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.objects.Knows;
import ldbc.snb.datagen.objects.Person;
import ldbc.snb.datagen.objects.StudyAt;
import ldbc.snb.datagen.objects.WorkAt;
import ldbc.snb.datagen.serializer.HDFSWriter;
import ldbc.snb.datagen.serializer.PersonSerializer;
import ldbc.snb.datagen.serializer.Turtle;
import ldbc.snb.datagen.vocabulary.*;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.text.SimpleDateFormat;


public class TurtlePersonSerializer extends PersonSerializer {

    private HDFSWriter[] writers;
    private long workAtId = 0;
    private long studyAtId = 0;
    private long knowsId = 0;
    private SimpleDateFormat dateTimeFormat = null;


    private enum FileNames {
        SOCIAL_NETWORK("social_network_person");

        private final String name;

        private FileNames(String name) {
            this.name = name;
        }

        public String toString() {
            return name;
        }
    }

    public void initialize(Configuration conf, int reducerId) throws IOException {
        dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        int numFiles = FileNames.values().length;
        writers = new HDFSWriter[numFiles];
        for (int i = 0; i < numFiles; ++i) {
            writers[i] = new HDFSWriter(conf.get("ldbc.snb.datagen.serializer.socialNetworkDir"), FileNames.values()[i]
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
    protected void serialize(final Person p) {
        StringBuffer result = new StringBuffer(19000);
        String prefix = SN.getPersonURI(p.accountId());
        Turtle.addTriple(result, true, false, prefix, RDF.type, SNVOC.Person);
        Turtle.addTriple(result, false, false, prefix, SNVOC.id,
                         Turtle.createDataTypeLiteral(Long.toString(p.accountId()), XSD.Long));
        Turtle.addTriple(result, false, false, prefix, SNVOC.firstName,
                         Turtle.createLiteral(p.firstName()));
        Turtle.addTriple(result, false, false, prefix, SNVOC.lastName,
                         Turtle.createLiteral(p.lastName()));

        if (p.gender() == 1) {
            Turtle.addTriple(result, false, false, prefix, SNVOC.gender,
                             Turtle.createLiteral("male"));
        } else {
            Turtle.addTriple(result, false, false, prefix, SNVOC.gender,
                             Turtle.createLiteral("female"));
        }
        Turtle.addTriple(result, false, false, prefix, SNVOC.birthday,
                         Turtle.createDataTypeLiteral(Dictionaries.dates.formatDate(p.birthDay()), XSD.Date));
        Turtle.addTriple(result, false, false, prefix, SNVOC.ipaddress,
                         Turtle.createLiteral(p.ipAddress().toString()));
        Turtle.addTriple(result, false, false, prefix, SNVOC.browser,
                         Turtle.createLiteral(Dictionaries.browsers.getName(p.browserId())));
        Turtle.addTriple(result, false, true, prefix, SNVOC.creationDate,
                         Turtle.createDataTypeLiteral(dateTimeFormat.format(p.creationDate()), XSD.DateTime));

        Turtle.createTripleSPO(result, prefix, SNVOC.locatedIn, DBP
                .fullPrefixed(Dictionaries.places.getPlaceName(p.cityId())));

        for (Integer i : p.languages()) {
            Turtle.createTripleSPO(result, prefix, SNVOC.speaks,
                                   Turtle.createLiteral(Dictionaries.languages.getLanguageName(i)));
        }

        for (String email : p.emails()) {
            Turtle.createTripleSPO(result, prefix, SNVOC.email, Turtle.createLiteral(email));
        }

        for (Integer tag : p.interests()) {
            String interest = Dictionaries.tags.getName(tag);
            Turtle.createTripleSPO(result, prefix, SNVOC.hasInterest, SNTAG.fullPrefixed(interest));
        }
        writers[FileNames.SOCIAL_NETWORK.ordinal()].write(result.toString());
    }

    @Override
    protected void serialize(final StudyAt studyAt) {
        String prefix = SN.getPersonURI(studyAt.user);
        StringBuffer result = new StringBuffer(19000);
        long id = SN.formId(studyAtId);
        Turtle.createTripleSPO(result, prefix, SNVOC.studyAt, SN.getStudyAtURI(id));
        Turtle.createTripleSPO(result, SN.getStudyAtURI(id), SNVOC.hasOrganisation,
                               SN.getUnivURI(studyAt.university));
        String yearString = Dictionaries.dates.formatYear(studyAt.year);
        Turtle.createTripleSPO(result, SN.getStudyAtURI(id), SNVOC.classYear,
                               Turtle.createDataTypeLiteral(yearString, XSD.Integer));
        studyAtId++;
        writers[FileNames.SOCIAL_NETWORK.ordinal()].write(result.toString());
    }

    @Override
    protected void serialize(final WorkAt workAt) {
        String prefix = SN.getPersonURI(workAt.user);
        StringBuffer result = new StringBuffer(19000);
        long id = SN.formId(workAtId);
        Turtle.createTripleSPO(result, prefix, SNVOC.workAt, SN.getWorkAtURI(id));
        Turtle.createTripleSPO(result, SN.getWorkAtURI(id), SNVOC.hasOrganisation,
                               SN.getCompURI(workAt.company));
        String yearString = Dictionaries.dates.formatYear(workAt.year);
        Turtle.createTripleSPO(result, SN.getWorkAtURI(id), SNVOC.workFrom,
                               Turtle.createDataTypeLiteral(yearString, XSD.Integer));
        workAtId++;
        writers[FileNames.SOCIAL_NETWORK.ordinal()].write(result.toString());
    }

    @Override
    protected void serialize(final Person p, Knows knows) {
        String prefix = SN.getPersonURI(p.accountId());
        StringBuffer result = new StringBuffer(19000);
        long id = SN.formId(knowsId);
        Turtle.createTripleSPO(result, prefix, SNVOC.knows, SN.getKnowsURI(id));
        Turtle.createTripleSPO(result, SN.getKnowsURI(id), SNVOC.hasPerson,
                               SN.getPersonURI(knows.to().accountId()));

        Turtle.createTripleSPO(result, SN.getKnowsURI(id), SNVOC.creationDate,
                               Turtle.createDataTypeLiteral(dateTimeFormat.format(knows.creationDate()), XSD.DateTime));
        writers[FileNames.SOCIAL_NETWORK.ordinal()].write(result.toString());
        knowsId++;
    }


    public void reset() {
        workAtId = 0;
        studyAtId = 0;
        knowsId = 0;
    }

}
