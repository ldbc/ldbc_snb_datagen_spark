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

package ldbc.snb.datagen.serializer.snb.csv.dynamicserializer.person;

import com.google.common.collect.ImmutableList;
import ldbc.snb.datagen.DatagenMode;
import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.entities.dynamic.relations.Knows;
import ldbc.snb.datagen.entities.dynamic.relations.StudyAt;
import ldbc.snb.datagen.entities.dynamic.relations.WorkAt;
import ldbc.snb.datagen.util.DateUtils;
import ldbc.snb.datagen.hadoop.writer.HdfsCsvWriter;
import ldbc.snb.datagen.serializer.DynamicPersonSerializer;
import ldbc.snb.datagen.serializer.snb.csv.CsvSerializer;
import ldbc.snb.datagen.serializer.snb.csv.FileName;

import java.util.List;

import static ldbc.snb.datagen.serializer.snb.csv.FileName.*;

public class CsvBasicDynamicPersonSerializer extends DynamicPersonSerializer<HdfsCsvWriter> implements CsvSerializer {

    @Override
    public List<FileName> getFileNames() {
        return ImmutableList.of(PERSON, PERSON_SPEAKS_LANGUAGE, PERSON_EMAIL_EMAILADDRESS, PERSON_ISLOCATEDIN_PLACE, PERSON_HASINTEREST_TAG, PERSON_WORKAT_ORGANISATION, PERSON_STUDYAT_ORGANISATION, PERSON_KNOWS_PERSON);
    }

    @Override
    public void writeFileHeaders() {
        if (DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) {
            writers.get(PERSON).writeHeader(ImmutableList.of("creationDate", "id", "firstName", "lastName", "gender", "birthday", "locationIP", "browserUsed","deletionDate"));
            writers.get(PERSON_SPEAKS_LANGUAGE).writeHeader(ImmutableList.of("creationDate", "Person.id", "language","deletionDate"));
            writers.get(PERSON_EMAIL_EMAILADDRESS).writeHeader(ImmutableList.of("creationDate", "Person.id", "email","deletionDate"));
            writers.get(PERSON_ISLOCATEDIN_PLACE).writeHeader(ImmutableList.of("creationDate", "Person.id", "Place.id","deletionDate"));
            writers.get(PERSON_HASINTEREST_TAG).writeHeader(ImmutableList.of("creationDate", "Person.id", "Tag.id","deletionDate"));
            writers.get(PERSON_STUDYAT_ORGANISATION).writeHeader(ImmutableList.of("creationDate", "Person.id", "Organisation.id", "classYear","deletionDate"));
            writers.get(PERSON_WORKAT_ORGANISATION).writeHeader(ImmutableList.of("creationDate", "Person.id", "Organisation.id", "workFrom","deletionDate"));
            writers.get(PERSON_KNOWS_PERSON).writeHeader(ImmutableList.of("creationDate", "Person.id", "Person.id"));
        } else {
            writers.get(PERSON).writeHeader(ImmutableList.of("creationDate", "id", "firstName", "lastName", "gender", "birthday", "locationIP", "browserUsed"));
            writers.get(PERSON_SPEAKS_LANGUAGE).writeHeader(ImmutableList.of("creationDate", "Person.id", "language"));
            writers.get(PERSON_EMAIL_EMAILADDRESS).writeHeader(ImmutableList.of("creationDate", "Person.id", "email"));
            writers.get(PERSON_ISLOCATEDIN_PLACE).writeHeader(ImmutableList.of("creationDate", "Person.id", "Place.id"));
            writers.get(PERSON_HASINTEREST_TAG).writeHeader(ImmutableList.of("creationDate", "Person.id", "Tag.id"));
            writers.get(PERSON_STUDYAT_ORGANISATION).writeHeader(ImmutableList.of("creationDate", "Person.id", "Organisation.id", "classYear"));
            writers.get(PERSON_WORKAT_ORGANISATION).writeHeader(ImmutableList.of("creationDate", "Person.id", "Organisation.id", "workFrom"));
            writers.get(PERSON_KNOWS_PERSON).writeHeader(ImmutableList.of("creationDate", "Person.id", "Person.id"));
        }
    }

    @Override
    protected void serialize(final Person person) {
        if (DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) {
            //"creationDate",  "id", "firstName", "lastName", "gender", "birthday", "locationIP", "browserUsed", "deletionDate"
            writers.get(PERSON).writeEntry(ImmutableList.of(
                    Dictionaries.dates.formatDateTime(person.getCreationDate()),
                    Long.toString(person.getAccountId()),
                    person.getFirstName(),
                    person.getLastName(),
                    getGender(person.getGender()),
                    Dictionaries.dates.formatDate(person.getBirthday()),
                    person.getIpAddress().toString(),
                    Dictionaries.browsers.getName(person.getBrowserId()),
                    Dictionaries.dates.formatDateTime(person.getDeletionDate())
            ));

            for (Integer i : person.getLanguages()) {
                //"creationDate",  "Person.id", "language", "deletionDate"
                writers.get(PERSON_SPEAKS_LANGUAGE).writeEntry(ImmutableList.of(
                        Dictionaries.dates.formatDateTime(person.getCreationDate()),
                        Long.toString(person.getAccountId()),
                        Dictionaries.languages.getLanguageName(i),
                        Dictionaries.dates.formatDateTime(person.getDeletionDate())));
            }
            for (String s : person.getEmails()) {
                //"creationDate",  "Person.id", "email", "deletionDate"
                writers.get(PERSON_EMAIL_EMAILADDRESS).writeEntry(ImmutableList.of(
                        Dictionaries.dates.formatDateTime(person.getCreationDate()),
                        Long.toString(person.getAccountId()),
                        s,
                        Dictionaries.dates.formatDateTime(person.getDeletionDate())
                ));
            }
            //"creationDate",  "Person.id", "Place.id", "deletionDate"
            writers.get(PERSON_ISLOCATEDIN_PLACE).writeEntry(ImmutableList.of(
                    Dictionaries.dates.formatDateTime(person.getCreationDate()),
                    Long.toString(person.getAccountId()),
                    Integer.toString(person.getCityId()),
                    Dictionaries.dates.formatDateTime(person.getDeletionDate())
            ));

            for (Integer integer : person.getInterests()) {
                //"creationDate",  "Person.id", "Tag.id", "deletionDate"
                writers.get(PERSON_HASINTEREST_TAG).writeEntry(ImmutableList.of(
                        Dictionaries.dates.formatDateTime(person.getCreationDate()),
                        Long.toString(person.getAccountId()),
                        Integer.toString(integer),
                        Dictionaries.dates.formatDateTime(person.getDeletionDate())
                ));
            }
        } else {
            //"creationDate",  "id", "firstName", "lastName", "gender", "birthday", "locationIP", "browserUsed"
            writers.get(PERSON).writeEntry(ImmutableList.of(
                    Dictionaries.dates.formatDateTime(person.getCreationDate()),
                    Long.toString(person.getAccountId()),
                    person.getFirstName(),
                    person.getLastName(),
                    getGender(person.getGender()),
                    Dictionaries.dates.formatDate(person.getBirthday()),
                    person.getIpAddress().toString(),
                    Dictionaries.browsers.getName(person.getBrowserId())
            ));

            for (Integer i : person.getLanguages()) {
                //"creationDate",  "Person.id", "language"
                writers.get(PERSON_SPEAKS_LANGUAGE).writeEntry(ImmutableList.of(
                        Dictionaries.dates.formatDateTime(person.getCreationDate()),
                        Long.toString(person.getAccountId()),
                        Dictionaries.languages.getLanguageName(i)
                ));
            }
            for (String s : person.getEmails()) {
                //"creationDate",  "Person.id", "email"
                writers.get(PERSON_EMAIL_EMAILADDRESS).writeEntry(ImmutableList.of(
                        Dictionaries.dates.formatDateTime(person.getCreationDate()),
                        Long.toString(person.getAccountId()),
                        s
                ));
            }
            //"creationDate",  "Person.id", "Place.id"
            writers.get(PERSON_ISLOCATEDIN_PLACE).writeEntry(ImmutableList.of(
                    Dictionaries.dates.formatDateTime(person.getCreationDate()),
                    Long.toString(person.getAccountId()),
                    Integer.toString(person.getCityId())
            ));

            for (Integer integer : person.getInterests()) {
                //"creationDate",  "Person.id", "Tag.id"
                writers.get(PERSON_HASINTEREST_TAG).writeEntry(ImmutableList.of(
                        Dictionaries.dates.formatDateTime(person.getCreationDate()),
                        Long.toString(person.getAccountId()),
                        Integer.toString(integer)
                ));
            }
        }
    }

    @Override
    protected void serialize(final StudyAt studyAt, final Person person) {
        if (DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) {
            //"creationDate",  "Person.id", "Organisation.id", "classYear","deletionDate"
            writers.get(PERSON_STUDYAT_ORGANISATION).writeEntry(ImmutableList.of(
                    Dictionaries.dates.formatDateTime(person.getCreationDate()),
                    Long.toString(studyAt.person),
                    Long.toString(studyAt.university),
                    DateUtils.formatYear(studyAt.year),
                    Dictionaries.dates.formatDateTime(person.getDeletionDate())
                    ));
        } else {
            //"creationDate",  "Person.id", "Organisation.id", "classYear"
            writers.get(PERSON_STUDYAT_ORGANISATION).writeEntry(ImmutableList.of(
                    Dictionaries.dates.formatDateTime(person.getCreationDate()),
                    Long.toString(studyAt.person),
                    Long.toString(studyAt.university),
                    DateUtils.formatYear(studyAt.year)
            ));
        }
    }

    @Override
    protected void serialize(final WorkAt workAt, final Person person) {
        if (DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) {
            //"creationDate",  "Person.id", "Organisation.id", "workFrom","deletionDate"
            writers.get(PERSON_WORKAT_ORGANISATION).writeEntry(ImmutableList.of(
                    Dictionaries.dates.formatDateTime(person.getCreationDate()),
                    Long.toString(workAt.person),
                    Long.toString(workAt.company),
                    DateUtils.formatYear(workAt.year),
                    Dictionaries.dates.formatDateTime(person.getDeletionDate())
                    ));
        } else {
            //"creationDate",  "Person.id", "Organisation.id", "workFrom"
            writers.get(PERSON_WORKAT_ORGANISATION).writeEntry(ImmutableList.of(
                    Dictionaries.dates.formatDateTime(person.getCreationDate()),
                    Long.toString(workAt.person),
                    Long.toString(workAt.company),
                    DateUtils.formatYear(workAt.year)
            ));
        }
    }

    @Override
    protected void serialize(final Person person, Knows knows) {
        if (DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) {
            //"creationDate",  "Person.id", "Person.id","deletionDate"
            writers.get(PERSON_KNOWS_PERSON).writeEntry(ImmutableList.of(
                    Dictionaries.dates.formatDateTime(knows.getCreationDate()),
                    Long.toString(person.getAccountId()),
                    Long.toString(knows.to().getAccountId()),
                    Dictionaries.dates.formatDateTime(knows.getDeletionDate())
            ));
        } else {
            //"creationDate",  "Person.id", "Person.id"
            writers.get(PERSON_KNOWS_PERSON).writeEntry(ImmutableList.of(
                    Dictionaries.dates.formatDateTime(knows.getCreationDate()),
                    Long.toString(person.getAccountId()),
                    Long.toString(knows.to().getAccountId())
            ));
        }
    }

}
