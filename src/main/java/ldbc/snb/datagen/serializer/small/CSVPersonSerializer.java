/*
 * Copyright (c) 2013 LDBC
 * Linked Data Benchmark Council (http://ldbc.eu)
 *
 * This file is part of ldbc_socialnet_dbgen.
 *
 * ldbc_socialnet_dbgen is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ldbc_socialnet_dbgen is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with ldbc_socialnet_dbgen.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2011 OpenLink Software <bdsmt@openlinksw.com>
 * All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation;  only Version 2 of the License dated
 * June 1991.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */


package ldbc.snb.datagen.serializer.small;

        import ldbc.snb.datagen.dictionary.Dictionaries;
        import ldbc.snb.datagen.objects.Knows;
        import ldbc.snb.datagen.objects.Person;
        import ldbc.snb.datagen.objects.StudyAt;
        import ldbc.snb.datagen.objects.WorkAt;
        import ldbc.snb.datagen.serializer.HDFSCSVWriter;
        import ldbc.snb.datagen.serializer.PersonSerializer;
        import org.apache.hadoop.conf.Configuration;

        import java.util.ArrayList;
        import java.util.Iterator;

public class CSVPersonSerializer extends PersonSerializer {

    private HDFSCSVWriter [] writers;

    private enum FileNames {
        PERSON ("user"),
        PERSON_KNOWS_PERSON("user_knows_user");

        private final String name;

        private FileNames( String name ) {
            this.name = name;
        }
        public String toString() {
            return name;
        }
    }

    public CSVPersonSerializer() {
    }

    public void initialize(Configuration conf, int reducerId) {
        int numFiles = FileNames.values().length;
        writers = new HDFSCSVWriter[numFiles];
        for( int i = 0; i < numFiles; ++i) {
            writers[i] = new HDFSCSVWriter(conf.get("ldbc.snb.datagen.serializer.socialNetworkDir"),FileNames.values()[i].toString()+"_"+reducerId,conf.getInt("ldbc.snb.datagen.numPartitions",1),conf.getBoolean("ldbc.snb.datagen.serializer.compressed",false),"|", conf.getBoolean("ldbc.snb.datagen.serializer.endlineSeparator",false));
        }

        ArrayList<String> arguments = new ArrayList<String>();
        arguments.add("id");
        arguments.add("nickname");
        writers[FileNames.PERSON.ordinal()].writeEntry(arguments);
        arguments.clear();


        arguments.add("User.id");
        arguments.add("User.id");
        writers[FileNames.PERSON_KNOWS_PERSON.ordinal()].writeEntry(arguments);
        arguments.clear();

    }

    @Override
    public void close() {
        int numFiles = FileNames.values().length;
        for(int i = 0; i < numFiles; ++i) {
            writers[i].close();
        }
    }

    @Override
    protected void serialize(final Person p) {

        ArrayList<String> arguments = new ArrayList<String>();

        arguments.add(Long.toString(p.accountId()));
        arguments.add(p.firstName()+" "+p.lastName());
        writers[FileNames.PERSON.ordinal()].writeEntry(arguments);
    }

    @Override
    protected void serialize(StudyAt studyAt) {
    }

    @Override
    protected void serialize(WorkAt workAt) {
    }

    @Override
    protected void serialize(final Person p, Knows knows) {
        ArrayList<String> arguments = new ArrayList<String>();

        arguments.add(Long.toString(p.accountId()));
        arguments.add(Long.toString(knows.to().accountId()));
        writers[FileNames.PERSON_KNOWS_PERSON.ordinal()].writeEntry(arguments);
    }
    public void reset() {

    }
}
