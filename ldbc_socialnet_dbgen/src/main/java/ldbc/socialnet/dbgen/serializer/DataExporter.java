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
import ldbc.socialnet.dbgen.serializer.CSVSerializer.CSVSerializer;

import java.util.*;

/**
 * Created by aprat on 4/14/14.
 */
public class DataExporter {

    public enum DataFormat {
        CSV,
        CSV_MERGE_FOREIGN,
        TURTLE,
        N3,
        NONE
    }

    private Serializer staticSerializer         = null;
    private UpdateEventSerializer updateStreamSerializer   = null;
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
                         LanguageDictionary languageDic,
                         String configFile,
                         Statistics statistics) {
        this.locationDic = locationDic;
        this.companyDic = companyDic;
        this.universityDic = universityDic;
        this.tagDic = tagDic;
        this.reducerId = reducerId;
        this.dateThreshold = dateThreshold;
        if( format == DataFormat.CSV ) {
            staticSerializer = new CSVOriginal(directory,reducerId,tagDic,browsers,companyDic,universityDic,ipDic,locationDic,languageDic,0,dateThreshold,exportText,compressed);
        } else if( format == DataFormat.CSV_MERGE_FOREIGN ) {
            staticSerializer = new CSVMergeForeign(directory,reducerId,tagDic,browsers,companyDic,universityDic,ipDic,locationDic,languageDic,0,dateThreshold,exportText,compressed);
        } else if( format == DataFormat.TURTLE ) {
            staticSerializer = new Turtle(directory,reducerId,1,true,tagDic,browsers,companyDic,universityDic,ipDic,locationDic,languageDic,exportText,compressed);
        } else if( format == DataFormat.N3 ) {
            staticSerializer = new Turtle(directory,reducerId,1,false,tagDic,browsers,companyDic,universityDic,ipDic,locationDic,languageDic,exportText,compressed);
        } else if( format == DataFormat.NONE) {
            staticSerializer = new EmptySerializer();
        }
        updateStreamSerializer = new UpdateEventSerializer(directory,"updateStreams_"+reducerId+".csv",exportText, compressed,tagDic,browsers,languageDic,ipDic, statistics);
        exportCommonEntities();
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
//            printLocationHierarchy(it.next());
            Location location = locationDic.getLocation(it.next());
            staticSerializer.serialize(location);
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


    public void export( UserInfo userInfo ) {
       long creationDate =  userInfo.user.getCreationDate();
       if( creationDate <= dateThreshold ) {
            staticSerializer.serialize(userInfo);
       } else {
           updateStreamSerializer.serialize(userInfo);
       }


        long universityId = userInfo.extraInfo.getUniversity();
        if ( universityId != -1){
            if (userInfo.extraInfo.getClassYear() != -1 ) {
                StudyAt studyAt = new StudyAt();
                studyAt.year = userInfo.extraInfo.getClassYear();
                studyAt.user = userInfo.user.getAccountId();
                studyAt.university = universityId;
                if( creationDate <= dateThreshold ) {
                    staticSerializer.serialize(studyAt);
                } else {
                    updateStreamSerializer.serialize(studyAt);
                }
            }
        }
        Iterator<Long> it = userInfo.extraInfo.getCompanies().iterator();
        while (it.hasNext()) {
            long companyId = it.next();
            WorkAt workAt = new WorkAt();
            workAt.company = companyId;
            workAt.user = userInfo.user.getAccountId();
            workAt.year = userInfo.extraInfo.getWorkFrom(companyId);
            if( creationDate <= dateThreshold ) {
                staticSerializer.serialize(workAt);
            } else {
                updateStreamSerializer.serialize(workAt);
            }
        }

        Friend friends[] = userInfo.user.getFriendList();
        int numFriends = friends.length;
        for( int i = 0; i < numFriends; ++i ) {
            if (friends[i] != null && friends[i].getCreatedTime() != -1) {
                if( friends[i].getCreatedTime() <= dateThreshold ) {
                    staticSerializer.serialize(friends[i]);
                } else {
                    updateStreamSerializer.serialize(friends[i]);
                }
            }
        }

        Group group = new Group();
        //The forums of the user
        group.setCreatedDate(userInfo.user.getCreationDate());
        group.setGroupName("Wall of " + userInfo.extraInfo.getFirstName() + " " + userInfo.extraInfo.getLastName());
        group.setGroupId(userInfo.user.getForumWallId());
        group.setModeratorId(userInfo.user.getAccountId());

        Iterator<Integer> itTags = userInfo.user.getSetOfTags().iterator();
        Integer tags[] = new Integer[userInfo.user.getSetOfTags().size()];
        int index = 0;
        while (itTags.hasNext()){
            tags[index] = itTags.next();
            index++;
        }
        group.setTags(tags);

        if( group.getCreatedDate() <= dateThreshold ) {
            staticSerializer.serialize(group);
        } else {
            updateStreamSerializer.serialize(group);
        }

        for (int i = 0; i < friends.length; i ++){
            if (friends[i] != null && friends[i].getCreatedTime() != -1){
                GroupMemberShip membership = new GroupMemberShip();
                membership.setGroupId(group.getGroupId());
                membership.setJoinDate(friends[i].getCreatedTime());
                membership.setUserId(friends[i].getFriendAcc());
                if( membership.getJoinDate() <= dateThreshold ) {
                    staticSerializer.serialize(membership);
                } else {
                    updateStreamSerializer.serialize(membership);
                }
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
        exportLikes(post,0);
    }

    public void export(Photo photo){
        long date =  photo.getCreationDate();
        if( date <= dateThreshold ) {
            staticSerializer.serialize(photo);
        } else {
            updateStreamSerializer.serialize(photo);
        }
        exportLikes(photo,2);
    }

    private void exportLikes ( Message message, int type ) {
        long likeUsers[] = message.getInterestedUserAccs();
        long likeTimes[] = message.getInterestedUserAccsTimestamp();
        int numLikes = likeUsers.length;
        for( int i = 0; i < numLikes; ++i ) {
            Like like = new Like();
            like.date = likeTimes[i];
            like.user = likeUsers[i];
            like.messageId = message.getMessageId();
            like.type = type;
            if( like.date <= dateThreshold ) {
                staticSerializer.serialize(like);
            } else {
                updateStreamSerializer.serialize(like);
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
        exportLikes(comment,1);
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
                staticSerializer.serialize(memberships[i]);
            } else {
                updateStreamSerializer.serialize(memberships[i]);
            }
        }
    }

    public void resetState(long block) {

    }
}
