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
package ldbc.snb.datagen.hadoop.serializer;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.statictype.Organisation;
import ldbc.snb.datagen.entities.statictype.TagClass;
import ldbc.snb.datagen.entities.statictype.place.Place;
import ldbc.snb.datagen.entities.statictype.tag.Tag;
import ldbc.snb.datagen.hadoop.LdbcDatagen;
import ldbc.snb.datagen.hadoop.writer.HdfsCsvWriter;
import ldbc.snb.datagen.hadoop.writer.HdfsWriter;
import ldbc.snb.datagen.serializer.StaticSerializer;
import ldbc.snb.datagen.util.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

public class HadoopStaticSerializer {

    private StaticSerializer[] staticSerializer;
    private TreeSet<Integer> exportedClasses;
    private int currentFile = 0;

    private Configuration conf;

    public HadoopStaticSerializer(Configuration conf) {
        this.conf = new Configuration(conf);
        exportedClasses = new TreeSet<>();
        LdbcDatagen.initializeContext(this.conf);
    }

    public void run() {

        try {
            staticSerializer = new StaticSerializer[DatagenParams.numThreads];
            for (int i = 0; i < DatagenParams.numThreads; ++i) {
                staticSerializer[i] = DatagenParams.getStaticSerializer();
                staticSerializer[i].initialize(conf, i);
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            throw new RuntimeException(e);
        }

        exportPlaces();
        exportTags();
        exportOrganisations();

        for (int i = 0; i < DatagenParams.numThreads; ++i) {
            staticSerializer[i].close();
        }
    }

    private int nextFile() {
        int ret = currentFile;
        currentFile = (++currentFile) % DatagenParams.numThreads;
        return ret;
    }

    private void exportTagHierarchy(Tag tag) {
        int classId = tag.tagClass;
        while (classId != -1 && !exportedClasses.contains(classId)) {
            exportedClasses.add(classId);
            TagClass tagClass = new TagClass();
            tagClass.id = classId;
            tagClass.name = StringUtils.clampString(Dictionaries.tags.getClassName(classId), 256);
            tagClass.parent = Dictionaries.tags.getClassParent(tagClass.id);
            staticSerializer[nextFile()].export(tagClass);
            classId = tagClass.parent;
        }
    }

    public void exportPlaces() {
        Set<Integer> locations = Dictionaries.places.getPlaces();
        for (Integer location : locations) {
            Place place = Dictionaries.places.getLocation(location);
            place.setName(StringUtils.clampString(place.getName(), 256));
            staticSerializer[nextFile()].export(place);
        }
    }

    public void exportOrganisations() {
        Set<Long> companies = Dictionaries.companies.getCompanies();
        Iterator<Long> it = companies.iterator();
        while (it.hasNext()) {
            Organisation company = new Organisation();
            company.id = it.next();
            company.type = Organisation.OrganisationType.company;
            company.name = StringUtils.clampString(Dictionaries.companies.getCompanyName(company.id), 256);
            company.location = Dictionaries.companies.getCountry(company.id);
            staticSerializer[nextFile()].export(company);
        }

        Set<Long> universities = Dictionaries.universities.getUniversities();
        it = universities.iterator();
        while (it.hasNext()) {
            Organisation university = new Organisation();
            university.id = it.next();
            university.type = Organisation.OrganisationType.university;
            university.name = StringUtils.clampString(Dictionaries.universities.getUniversityName(university.id), 256);
            university.location = Dictionaries.universities.getUniversityCity(university.id);
            staticSerializer[nextFile()].export(university);
        }
    }

    public void exportTags() {
        Set<Integer> tags = Dictionaries.tags.getTags();
        for (Integer integer : tags) {
            Tag tag = new Tag();
            tag.id = integer;
            tag.name = StringUtils.clampString(Dictionaries.tags.getName(tag.id), 256);
            tag.name.replace("\"", "\\\"");
            tag.tagClass = Dictionaries.tags.getTagClass(tag.id);
            staticSerializer[nextFile()].export(tag);
            exportTagHierarchy(tag);
        }
    }

}
