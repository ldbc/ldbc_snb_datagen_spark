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
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.entities.dynamic.relations.Knows;
import ldbc.snb.datagen.entities.dynamic.relations.StudyAt;
import ldbc.snb.datagen.entities.dynamic.relations.WorkAt;
import ldbc.snb.datagen.hadoop.writer.HdfsCsvWriter;
import ldbc.snb.datagen.serializer.DynamicPersonSerializer;
import ldbc.snb.datagen.serializer.snb.csv.CsvSerializer;
import ldbc.snb.datagen.serializer.snb.csv.FileName;

import java.util.Iterator;
import java.util.List;

import static ldbc.snb.datagen.serializer.snb.csv.FileName.*;

public class CsvBasicDynamicPersonSerializer extends DynamicPersonSerializer<HdfsCsvWriter> implements CsvSerializer {

    @Override
    public List<FileName> getFileNames() {
        return ImmutableList.of(PERSON,PERSON_SPEAKS_LANGUAGE,PERSON_HAS_EMAIL,PERSON_LOCATED_IN_PLACE,PERSON_HAS_INTEREST_TAG,PERSON_WORK_AT,PERSON_STUDY_AT,PERSON_KNOWS_PERSON);
    }

    @Override
    public void writeFileHeaders() {
        writers.get(PERSON)                 .writeHeader(ImmutableList.of("creationDate","deletionDate","id","firstName","lastName","gender","birthday","locationIP","browserUsed"));
        writers.get(PERSON_SPEAKS_LANGUAGE) .writeHeader(ImmutableList.of("creationDate","deletionDate","Person.id","language"));
        writers.get(PERSON_HAS_EMAIL)       .writeHeader(ImmutableList.of("creationDate","deletionDate","Person.id","email"));
        writers.get(PERSON_LOCATED_IN_PLACE).writeHeader(ImmutableList.of("creationDate","deletionDate","Person.id","Place.id"));
        writers.get(PERSON_HAS_INTEREST_TAG).writeHeader(ImmutableList.of("creationDate","deletionDate","Person.id","Tag.id"));
        writers.get(PERSON_STUDY_AT)        .writeHeader(ImmutableList.of("creationDate","deletionDate","Person.id","Organisation.id","classYear"));
        writers.get(PERSON_WORK_AT)         .writeHeader(ImmutableList.of("creationDate","deletionDate","Person.id","Organisation.id","workFrom"));
        writers.get(PERSON_KNOWS_PERSON)    .writeHeader(ImmutableList.of("creationDate","deletionDate","Person.id","Person.id"));
    }

    @Override
    protected void serialize(final Person person) {
        //"creationDate","deletionDate","id","firstName","lastName","gender","birthday","locationIP","browserUsed"
        writers.get(PERSON).writeEntry(ImmutableList.of(
                Dictionaries.dates.formatDateTime(person.getCreationDate()),
                Dictionaries.dates.formatDateTime(person.getDeletionDate()),
                Long.toString(person.getAccountId()),
                person.getFirstName(),
                person.getLastName(),
                getGender(person.getGender()),
                Dictionaries.dates.formatDate(person.getBirthday()),
                person.getIpAddress().toString(),
                Dictionaries.browsers.getName(person.getBrowserId())
            ));

        for (Integer i: person.getLanguages()) {
            //"creationDate","deletionDate","Person.id","language"
            writers.get(PERSON_SPEAKS_LANGUAGE).writeEntry(ImmutableList.of(
                    Dictionaries.dates.formatDateTime(person.getCreationDate()),
                    Dictionaries.dates.formatDateTime(person.getDeletionDate()),
                    Long.toString(person.getAccountId()),
                    Dictionaries.languages.getLanguageName(i)
            ));
        }
        for (String s : person.getEmails()) {
            //"creationDate","deletionDate","Person.id","email"
            writers.get(PERSON_HAS_EMAIL).writeEntry(ImmutableList.of(
                    Dictionaries.dates.formatDateTime(person.getCreationDate()),
                    Dictionaries.dates.formatDateTime(person.getDeletionDate()),
                    Long.toString(person.getAccountId()),
                    s
            ));
        }
        //"creationDate","deletionDate","Person.id","Place.id"
        writers.get(PERSON_LOCATED_IN_PLACE).writeEntry(ImmutableList.of(
                Dictionaries.dates.formatDateTime(person.getCreationDate()),
                Dictionaries.dates.formatDateTime(person.getDeletionDate()),
                Long.toString(person.getAccountId()),
                Integer.toString(person.getCityId())
        ));

        for (Integer integer : person.getInterests()) {
            //"creationDate","deletionDate","Person.id","Tag.id"
            writers.get(PERSON_HAS_INTEREST_TAG).writeEntry(ImmutableList.of(
                    Dictionaries.dates.formatDateTime(person.getCreationDate()),
                    Dictionaries.dates.formatDateTime(person.getDeletionDate()),
                    Long.toString(person.getAccountId()),
                    Integer.toString(integer)
            ));
        }
    }

    @Override
    protected void serialize(final StudyAt studyAt,final Person person) {
        //"creationDate","deletionDate","Person.id","Organisation.id","classYear"
        writers.get(PERSON_STUDY_AT).writeEntry(ImmutableList.of(
                Dictionaries.dates.formatDateTime(person.getCreationDate()),
                Dictionaries.dates.formatDateTime(person.getDeletionDate()),
                Long.toString(studyAt.user),
                Long.toString(studyAt.university),
                Dictionaries.dates.formatYear(studyAt.year)
        ));
    }

    @Override
    protected void serialize(final WorkAt workAt,final Person person) {
        //"creationDate","deletionDate","Person.id","Organisation.id","workFrom"
        writers.get(PERSON_WORK_AT).writeEntry(ImmutableList.of(
                Dictionaries.dates.formatDateTime(person.getCreationDate()),
                Dictionaries.dates.formatDateTime(person.getDeletionDate()),
                Long.toString(workAt.user),
                Long.toString(workAt.company),
                Dictionaries.dates.formatYear(workAt.year)
        ));
    }

    @Override
    protected void serialize(final Person person, Knows knows) {
        //"creationDate","deletionDate","Person.id","Person.id"
        writers.get(PERSON_KNOWS_PERSON).writeEntry(ImmutableList.of(
                Dictionaries.dates.formatDateTime(knows.getCreationDate()),
                Dictionaries.dates.formatDateTime(knows.getDeletionDate()),
                Long.toString(person.getAccountId()),
                Long.toString(knows.to().getAccountId())
        ));
    }

}
