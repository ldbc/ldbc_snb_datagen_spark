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

package ldbc.socialnet.dbgen.serializer;

import ldbc.socialnet.dbgen.dictionary.*;
import ldbc.socialnet.dbgen.generator.ScalableGenerator;
import ldbc.socialnet.dbgen.objects.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by aprat on 4/14/14.
 */
public class DataExporter {

    enum DataFormat {
        CSV,
        CSV_MERGE_FOREIGN,
        TURTLE,
        N3
    }

    private Serializer staticSerializer         = null;
    private Serializer updateStreamSerializer   = null;
    private long dateThreshold                  = 0;

    private LocationDictionary locationDic = null;
    private CompanyDictionary companyDic = null;
    private UniversityDictionary universityDic = null;
    private TagDictionary tagDic = null;
    private int reducerId = 0;

    public DataExporter( DataFormat format,
                         String directory,
                         int reducerId,
                         long dateThreshold,
                         boolean exportText,
                         boolean compressed,
                         TagDictionary tagDic,
                         BrowserDictionary browsers,
                         CompanyDictionary companyDic,
                         UniversityDictionary universityDic,
                         IPAddressDictionary ipDic,
                         LocationDictionary locationDic,
                         LanguageDictionary languageDic ) {
        this.locationDic = locationDic;
        this.companyDic = companyDic;
        this.universityDic = universityDic;
        this.tagDic = tagDic;
        this.reducerId = reducerId;
    }

    public void close() {
        staticSerializer.close();
        updateStreamSerializer.close();
    }

    public Long unitsGenerated() {
       return staticSerializer.unitsGenerated() + staticSerializer.unitsGenerated();
    }

    public void exportCommonEntities() {
        if( reducerId == 0 ) {
            // Locations
            exportLocations();
            // Organisations
            exportOrganisations();
            // Tags
            exportTags();
        }
    }

    public void printLocationHierarchy(int baseId) {
        ArrayList<Integer> areas = new ArrayList<Integer>();
        do {
            areas.add(baseId);
            baseId = locationDic.belongsTo(baseId);
        } while (baseId != -1);

        TreeSet<Integer> exportedPlaces = new TreeSet<Integer>();
        for (int i = areas.size() - 1; i >= 0; i--) {
            if (!exportedPlaces.contains(areas.get(i))) {
                exportedPlaces.add(areas.get(i));
                Location location = locationDic.getLocation(areas.get(i));
                staticSerializer.serialize(location);
            }
        }
    }

    public void exportTagHierarchy(Tag tag) {
        int classId = tag.tagClass;
        TreeSet<Integer> exportedClasses = new TreeSet<Integer>();
        while (classId != -1 && !exportedClasses.contains(classId)) {
            exportedClasses.add(classId);
            TagClass tagClass = new TagClass();
            tagClass.id = classId;
            tagClass.name = tagDic.getClassName(classId);
            tagClass.parent = tagDic.getClassParent(tagClass.id);
            staticSerializer.serialize(tagClass);
            classId = tagClass.parent;
        }
    }

    private void exportLocations() {
        Set<Integer> locations = locationDic.getLocations();
        Iterator<Integer> it = locations.iterator();
        while(it.hasNext()) {
            printLocationHierarchy(it.next());
        }
    }

    private void exportOrganisations() {
        Set<Long> companies = companyDic.getCompanies();
        Iterator<Long> it = companies.iterator();
        while(it.hasNext()) {
            Organization company = new Organization();
            company.id = it.next();
            company.type = ScalableGenerator.OrganisationType.company;
            company.name = companyDic.getCompanyName(company.id);
            company.location = companyDic.getCountry(company.id);
            staticSerializer.serialize(company);
        }

        Set<Long> universities = universityDic.getUniversities();
        it = universities.iterator();
        while(it.hasNext()) {
            Organization university = new Organization();
            university.id = it.next();
            university.type = ScalableGenerator.OrganisationType.university;
            university.name = universityDic.getUniversityName(university.id);
            university.location = universityDic.getUniversityLocation(university.id);
            staticSerializer.serialize(university);
        }
    }

    public void exportTags() {
        Set<Integer>  tags = tagDic.getTags();
        Iterator<Integer> it = tags.iterator();
        while(it.hasNext()) {
            Tag tag = new Tag();
            tag.id = it.next();
            tag.name = tagDic.getName(tag.id);
            tag.name.replace("\"", "\\\"");
            tag.tagClass = tagDic.getTagClass(tag.id);
            staticSerializer.serialize(tag);
            exportTagHierarchy(tag);
        }
    }


    public void export(ReducedUserProfile user, UserExtraInfo extraInfo) {
       long date =  user.getCreationDate();
       if( date <= dateThreshold ) {
            staticSerializer.serialize(user, extraInfo);
       } else {
            updateStreamSerializer.serialize(user, extraInfo);
       }

       Friend friends[] = user.getFriendList();
       short numFriends = user.getNumFriends();
        for( int i = 0; i < numFriends; ++i ) {
            if( friends[i].getCreatedTime() <= dateThreshold ) {
                staticSerializer.serialize(user, friends[i]);
            } else {
                updateStreamSerializer.serialize(user, friends[i]);
            }
        }
    }

    public void export(Post post) {
        long date =  post.getCreationDate();
        if( date <= dateThreshold ) {
            staticSerializer.serialize(post);
        } else {
            updateStreamSerializer.serialize(post);
        }
        long likeUsers[] = post.getInterestedUserAccs();
        long likeTimes[] = post.getInterestedUserAccsTimestamp();
        int numLikes = likeUsers.length;
        for( int i = 0; i < numLikes; ++i ) {
            if( likeTimes[i] <= dateThreshold ) {
                staticSerializer.serialize(post,likeUsers[i], likeTimes[i]);
            } else {
                updateStreamSerializer.serialize(post, likeUsers[i], likeTimes[i]);
            }
        }
    }

    public void export(Photo photo){
        long date =  photo.getTakenTime();
        if( date <= dateThreshold ) {
            staticSerializer.serialize(photo);
        } else {
            updateStreamSerializer.serialize(photo);
        }

        long likeUsers[] = photo.getInterestedUserAccs();
        long likeTimes[] = photo.getInterestedUserAccsTimestamp();
        int numLikes = likeUsers.length;
        for( int i = 0; i < numLikes; ++i ) {
            if( likeTimes[i] <= dateThreshold ) {
                staticSerializer.serialize(photo,likeUsers[i], likeTimes[i]);
            } else {
                updateStreamSerializer.serialize(photo, likeUsers[i], likeTimes[i]);
            }
        }
    }

    public void export(Comment comment) {
        long date =  comment.getCreationDate();
        if( date <= dateThreshold ) {
            staticSerializer.serialize(comment);
        } else {
            updateStreamSerializer.serialize(comment);
        }

        long likeUsers[] = comment.getInterestedUserAccs();
        long likeTimes[] = comment.getInterestedUserAccsTimestamp();
        int numLikes = likeUsers.length;
        for( int i = 0; i < numLikes; ++i ) {
            if( likeTimes[i] <= dateThreshold ) {
                staticSerializer.serialize(comment, likeUsers[i], likeTimes[i]);
            } else {
                updateStreamSerializer.serialize(comment, likeUsers[i], likeTimes[i]);
            }
        }
    }

    public void export(Group group) {
        long date =  group.getCreatedDate();
        if( date <= dateThreshold ) {
            staticSerializer.serialize(group);
        } else {
            updateStreamSerializer.serialize(group);
        }

        GroupMemberShip memberships[] = group.getMemberShips();
        int numMembers = group.getNumMemberAdded();
        for( int i = 0; i < numMembers; ++i ) {
            if( memberships[i].getJoinDate() <= dateThreshold ) {
                staticSerializer.serialize(group, memberships[i]);
            } else {
                updateStreamSerializer.serialize(group, memberships[i]);
            }
        }
    }

    public void resetState(long block) {

    }
}
