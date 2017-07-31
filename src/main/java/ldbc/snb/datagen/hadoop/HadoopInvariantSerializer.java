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
package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.generator.LDBCDatagen;
import ldbc.snb.datagen.objects.Organization;
import ldbc.snb.datagen.objects.Place;
import ldbc.snb.datagen.objects.Tag;
import ldbc.snb.datagen.objects.TagClass;
import ldbc.snb.datagen.serializer.InvariantSerializer;
import ldbc.snb.datagen.util.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by aprat on 12/17/14.
 */
public class HadoopInvariantSerializer {

    private InvariantSerializer[] invariantSerializer_;
    private TreeSet<Integer> exportedClasses_;
    private int currentFile_ = 0;

    private Configuration conf_;

    public HadoopInvariantSerializer(Configuration conf) {
        conf_ = new Configuration(conf);
        exportedClasses_ = new TreeSet<Integer>();
        LDBCDatagen.initializeContext(conf_);
    }

    public void run() throws Exception {

        try {
            invariantSerializer_ = new InvariantSerializer[DatagenParams.numThreads];
            for (int i = 0; i < DatagenParams.numThreads; ++i) {
                invariantSerializer_[i] = (InvariantSerializer) Class
                        .forName(conf_.get("ldbc.snb.datagen.serializer.invariantSerializer")).newInstance();
                invariantSerializer_[i].initialize(conf_, i);
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
        }

        exportPlaces();
        exportTags();
        exportOrganizations();

        for (int i = 0; i < DatagenParams.numThreads; ++i) {
            invariantSerializer_[i].close();
        }
    }

    private int nextFile() {
        int ret = currentFile_;
        currentFile_ = (++currentFile_) % DatagenParams.numThreads;
        return ret;
    }

    private void exportTagHierarchy(Tag tag) {
        int classId = tag.tagClass;
        while (classId != -1 && !exportedClasses_.contains(classId)) {
            exportedClasses_.add(classId);
            TagClass tagClass = new TagClass();
            tagClass.id = classId;
            tagClass.name = StringUtils.clampString(Dictionaries.tags.getClassName(classId), 256);
            tagClass.parent = Dictionaries.tags.getClassParent(tagClass.id);
            invariantSerializer_[nextFile()].export(tagClass);
            classId = tagClass.parent;
        }
    }

    public void exportPlaces() {
        Set<Integer> locations = Dictionaries.places.getPlaces();
        Iterator<Integer> it = locations.iterator();
        while (it.hasNext()) {
            Place place = Dictionaries.places.getLocation(it.next());
            place.setName(StringUtils.clampString(place.getName(), 256));
            invariantSerializer_[nextFile()].export(place);
        }
    }

    public void exportOrganizations() {
        Set<Long> companies = Dictionaries.companies.getCompanies();
        Iterator<Long> it = companies.iterator();
        while (it.hasNext()) {
            Organization company = new Organization();
            company.id = it.next();
            company.type = Organization.OrganisationType.company;
            company.name = StringUtils.clampString(Dictionaries.companies.getCompanyName(company.id), 256);
            company.location = Dictionaries.companies.getCountry(company.id);
            invariantSerializer_[nextFile()].export(company);
        }

        Set<Long> universities = Dictionaries.universities.getUniversities();
        it = universities.iterator();
        while (it.hasNext()) {
            Organization university = new Organization();
            university.id = it.next();
            university.type = Organization.OrganisationType.university;
            university.name = StringUtils.clampString(Dictionaries.universities.getUniversityName(university.id), 256);
            university.location = Dictionaries.universities.getUniversityCity(university.id);
            invariantSerializer_[nextFile()].export(university);
        }
    }

    public void exportTags() {
        Set<Integer> tags = Dictionaries.tags.getTags();
        Iterator<Integer> it = tags.iterator();
        while (it.hasNext()) {
            Tag tag = new Tag();
            tag.id = it.next();
            tag.name = StringUtils.clampString(Dictionaries.tags.getName(tag.id), 256);
            tag.name.replace("\"", "\\\"");
            tag.tagClass = Dictionaries.tags.getTagClass(tag.id);
            invariantSerializer_[nextFile()].export(tag);
            exportTagHierarchy(tag);
        }
    }

}
