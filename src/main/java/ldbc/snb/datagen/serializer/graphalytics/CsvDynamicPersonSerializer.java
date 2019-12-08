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


package ldbc.snb.datagen.serializer.graphalytics;

import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.entities.dynamic.relations.Knows;
import ldbc.snb.datagen.entities.dynamic.relations.StudyAt;
import ldbc.snb.datagen.entities.dynamic.relations.WorkAt;
import ldbc.snb.datagen.hadoop.writer.HdfsCsvWriter;
import ldbc.snb.datagen.serializer.DynamicPersonSerializer;
import ldbc.snb.datagen.serializer.snb.csv.CsvSerializer;
import ldbc.snb.datagen.serializer.snb.csv.FileName;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CsvDynamicPersonSerializer extends DynamicPersonSerializer<HdfsCsvWriter> implements CsvSerializer {

    private HdfsCsvWriter[] writers;

    private enum FileNames {
        PERSON_KNOWS_PERSON("person_knows_person");

        private final String name;

        FileNames(String name) {
            this.name = name;
        }

        public String toString() {
            return name;
        }
    }

    @Override
    public List<FileName> getFileNames() {
        return Collections.emptyList();
    }

    @Override
    public void writeFileHeaders() {

    }

    @Override
    public void initialize(Configuration conf, int reducerId) throws IOException {
        int numFiles = FileNames.values().length;
        writers = new HdfsCsvWriter[numFiles];
        for (int i = 0; i < numFiles; ++i) {
            writers[i] = new HdfsCsvWriter(conf.get("ldbc.snb.datagen.serializer.socialNetworkDir"), FileNames
                    .values()[i].toString() + "_" + reducerId, conf.getInt("ldbc.snb.datagen.numPartitions", 1), conf
                                                   .getBoolean("ldbc.snb.datagen.serializer.compressed", false), "|", conf
                                                   .getBoolean("ldbc.snb.datagen.serializer.endlineSeparator", false));
        }

        List<String> arguments = new ArrayList<>();
        arguments.clear();
        arguments.add("Person.id");
        arguments.add("Person.id");
        writers[FileNames.PERSON_KNOWS_PERSON.ordinal()].writeHeader(arguments);

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
        //Intentionally left empty
    }

    @Override
    protected void serialize(final StudyAt studyAt) {
        //Intentionally left empty
    }

    @Override
    protected void serialize(final WorkAt workAt) {
        //Intentionally left empty
    }

    @Override
    protected void serialize(final Person p, Knows knows) {
        List<String> arguments = new ArrayList<>();
        arguments.add(Long.toString(p.accountId()));
        arguments.add(Long.toString(knows.to().accountId()));
        writers[FileNames.PERSON_KNOWS_PERSON.ordinal()].writeEntry(arguments);
    }

}
