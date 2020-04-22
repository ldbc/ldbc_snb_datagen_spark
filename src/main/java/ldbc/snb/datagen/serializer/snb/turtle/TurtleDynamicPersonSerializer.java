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

import com.google.common.collect.ImmutableList;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.entities.dynamic.relations.Knows;
import ldbc.snb.datagen.entities.dynamic.relations.StudyAt;
import ldbc.snb.datagen.entities.dynamic.relations.WorkAt;
import ldbc.snb.datagen.hadoop.writer.HdfsWriter;
import ldbc.snb.datagen.serializer.DynamicPersonSerializer;
import ldbc.snb.datagen.serializer.snb.csv.FileName;
import ldbc.snb.datagen.vocabulary.*;

import java.util.List;

import static ldbc.snb.datagen.serializer.snb.csv.FileName.SOCIAL_NETWORK_PERSON;

public class TurtleDynamicPersonSerializer extends DynamicPersonSerializer<HdfsWriter> implements TurtleSerializer {

    private long workAtId = 0;
    private long studyAtId = 0;
    private long knowsId = 0;

    @Override
    public List<FileName> getFileNames() {
        return ImmutableList.of(SOCIAL_NETWORK_PERSON);
    }

    @Override
    public void writeFileHeaders() { }

    @Override
    protected void serialize(final Person p) {
        StringBuffer result = new StringBuffer(19000);
        String prefix = SN.getPersonURI(p.getAccountId());
        Turtle.addTriple(result, true, false, prefix, RDF.type, SNVOC.Person);
        Turtle.addTriple(result, false, false, prefix, SNVOC.id,
                         Turtle.createDataTypeLiteral(Long.toString(p.getAccountId()), XSD.Long));
        Turtle.addTriple(result, false, false, prefix, SNVOC.firstName,
                         Turtle.createLiteral(p.getFirstName()));
        Turtle.addTriple(result, false, false, prefix, SNVOC.lastName,
                         Turtle.createLiteral(p.getLastName()));

        if (p.getGender() == 1) {
            Turtle.addTriple(result, false, false, prefix, SNVOC.gender,
                             Turtle.createLiteral("male"));
        } else {
            Turtle.addTriple(result, false, false, prefix, SNVOC.gender,
                             Turtle.createLiteral("female"));
        }
        Turtle.addTriple(result, false, false, prefix, SNVOC.birthday,
                         Turtle.createDataTypeLiteral(Dictionaries.dates.formatDate(p.getBirthday()), XSD.Date));
        Turtle.addTriple(result, false, false, prefix, SNVOC.ipaddress,
                         Turtle.createLiteral(p.getIpAddress().toString()));
        Turtle.addTriple(result, false, false, prefix, SNVOC.browser,
                         Turtle.createLiteral(Dictionaries.browsers.getName(p.getBrowserId())));
        Turtle.addTriple(result, false, true, prefix, SNVOC.creationDate,
                         Turtle.createDataTypeLiteral(TurtleDateTimeFormat.get().format(p.getCreationDate()), XSD.DateTime));

        Turtle.createTripleSPO(result, prefix, SNVOC.locatedIn, DBP
                .fullPrefixed(Dictionaries.places.getPlaceName(p.getCityId())));

        for (Integer i : p.getLanguages()) {
            Turtle.createTripleSPO(result, prefix, SNVOC.speaks,
                                   Turtle.createLiteral(Dictionaries.languages.getLanguageName(i)));
        }

        for (String email : p.getEmails()) {
            Turtle.createTripleSPO(result, prefix, SNVOC.email, Turtle.createLiteral(email));
        }

        for (Integer tag : p.getInterests()) {
            String interest = Dictionaries.tags.getName(tag);
            Turtle.createTripleSPO(result, prefix, SNVOC.hasInterest, SNTAG.fullPrefixed(interest));
        }
        writers.get(SOCIAL_NETWORK_PERSON).write(result.toString());
    }

    @Override
    protected void serialize(final StudyAt studyAt,final Person person) {
        String prefix = SN.getPersonURI(studyAt.person);
        StringBuffer result = new StringBuffer(19000);
        long id = SN.formId(studyAtId);
        Turtle.createTripleSPO(result, prefix, SNVOC.studyAt, SN.getStudyAtURI(id));
        Turtle.createTripleSPO(result, SN.getStudyAtURI(id), SNVOC.hasOrganisation,
                               SN.getUnivURI(studyAt.university));
        String yearString = Dictionaries.dates.formatYear(studyAt.year);
        Turtle.createTripleSPO(result, SN.getStudyAtURI(id), SNVOC.classYear,
                               Turtle.createDataTypeLiteral(yearString, XSD.Integer));
        studyAtId++;
        writers.get(SOCIAL_NETWORK_PERSON).write(result.toString());
    }

    @Override
    protected void serialize(final WorkAt workAt,final Person person) {
        String prefix = SN.getPersonURI(workAt.person);
        StringBuffer result = new StringBuffer(19000);
        long id = SN.formId(workAtId);
        Turtle.createTripleSPO(result, prefix, SNVOC.workAt, SN.getWorkAtURI(id));
        Turtle.createTripleSPO(result, SN.getWorkAtURI(id), SNVOC.hasOrganisation,
                               SN.getCompURI(workAt.company));
        String yearString = Dictionaries.dates.formatYear(workAt.year);
        Turtle.createTripleSPO(result, SN.getWorkAtURI(id), SNVOC.workFrom,
                               Turtle.createDataTypeLiteral(yearString, XSD.Integer));
        workAtId++;
        writers.get(SOCIAL_NETWORK_PERSON).write(result.toString());
    }

    @Override
    protected void serialize(final Person p, Knows knows) {
        String prefix = SN.getPersonURI(p.getAccountId());
        StringBuffer result = new StringBuffer(19000);
        long id = SN.formId(knowsId);
        Turtle.createTripleSPO(result, prefix, SNVOC.knows, SN.getKnowsURI(id));
        Turtle.createTripleSPO(result, SN.getKnowsURI(id), SNVOC.hasPerson,
                               SN.getPersonURI(knows.to().getAccountId()));

        Turtle.createTripleSPO(result, SN.getKnowsURI(id), SNVOC.creationDate,
                               Turtle.createDataTypeLiteral(TurtleDateTimeFormat.get().format(knows.getCreationDate()), XSD.DateTime));
        writers.get(SOCIAL_NETWORK_PERSON).write(result.toString());
        knowsId++;
    }

}
