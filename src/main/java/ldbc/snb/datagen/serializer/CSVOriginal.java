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
package ldbc.snb.datagen.serializer;

import ldbc.snb.datagen.dictionary.*;
import ldbc.snb.datagen.generator.DateGenerator;
import ldbc.snb.datagen.objects.*;
import ldbc.snb.datagen.vocabulary.DBP;
import ldbc.snb.datagen.vocabulary.DBPOWL;
import ldbc.snb.datagen.vocabulary.SN;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.zip.GZIPOutputStream;

/**
 * CSV serializer.
 */
public class CSVOriginal implements Serializer {

    //    private FileWriter[][] dataFileWriter;
    OutputStream[] fileOutputStream;

    private long csvRows;
    private GregorianCalendar date;

    /**
     * Generator input classes.
     */
    private CompanyDictionary companyDic;
    private UniversityDictionary universityDic;
    private HashMap<String, Integer> universityToCountry;
    private BrowserDictionary browserDic;
    private PlaceDictionary locationDic;
    private LanguageDictionary languageDic;
    private TagDictionary tagDic;
    private IPAddressDictionary ipDic;
    private boolean exportText;

    /**
     * Used to avoid serialize more than once the same data.
     */
    HashMap<Integer, Integer> printedTagClasses;
    Vector<Integer> locations;
    Vector<Integer> serializedLanguages;
    HashMap<String, Integer> universities;
    HashMap<String, Integer> companies;

    private final String NEWLINE = "\n";
    private final String SEPARATOR = "|";

    /**
     * The fileName vector and the enum Files serves the purpose of facilitate the serialization method
     * in the gatherData methods. Both of them must be coherent or it won't work.
     */
    private final String[] fileNames = {
            "tag",
            "post",
            "forum",
            "person",
            "comment",
            "place",
            "tagclass",
            "organisation",
            "person_likes_post",
            "person_likes_comment",
            "person_hasInterest_tag",
            "person_knows_person",
            "person_speaks_language",
            "person_workAt_organisation",
            "person_studyAt_organisation",
            "person_isLocatedIn_place",
            "person_email_emailaddress",
            "post_isLocatedIn_place",
            "post_hasTag_tag",
            "post_hasCreator_person",
            "comment_isLocatedIn_place",
            "comment_replyOf_post",
            "comment_replyOf_comment",
            "comment_hasCreator_person",
            "comment_hasTag_tag",
            "tag_hasType_tagclass",
            "tagclass_isSubclassOf_tagclass",
            "place_isPartOf_place",
            "organisation_isLocatedIn_place",
            "forum_hasModerator_person",
            "forum_containerOf_post",
            "forum_hasTag_tag",
            "forum_hasMember_person"
    };

    enum Files {
        TAG,
        POST,
        FORUM,
        PERSON,
        COMMENT,
        PLACE,
        TAGCLASS,
        ORGANISATION,
        PERSON_LIKE_POST,
        PERSON_LIKE_COMMENT,
        PERSON_INTEREST_TAG,
        PERSON_KNOWS_PERSON,
        PERSON_SPEAKS_LANGUAGE,
        PERSON_WORK_AT_ORGANISATION,
        PERSON_STUDY_AT_ORGANISATION,
        PERSON_LOCATED_IN_PLACE,
        PERSON_HAS_EMAIL_EMAIL,
        POST_LOCATED_PLACE,
        POST_HAS_TAG_TAG,
        POST_HAS_CREATOR_PERSON,
        COMMENT_LOCATED_PLACE,
        COMMENT_REPLY_OF_POST,
        COMMENT_REPLY_OF_COMMENT,
        COMMENT_HAS_CREATOR_PERSON,
        COMMENT_HAS_TAG_TAG,
        TAG_HAS_TYPE_TAGCLASS,
        TAGCLASS_IS_SUBCLASS_OF_TAGCLASS,
        PLACE_PART_OF_PLACE,
        ORGANISATION_BASED_NEAR_PLACE,
        FORUM_HAS_MODERATOR_PERSON,
        FORUM_CONTAINER_OF_POST,
        FORUM_HASTAG_TAG,
        FORUM_HASMEMBER_PERSON,
        NUM_FILES
    }

    /**
     * The field names of the CSV files. They are the first thing written in their respective file.
     */
    private final String[][] fieldNames = {
            {"id", "name", "url"},
            {"id", "imageFile", "creationDate", "locationIP", "browserUsed", "language", "content", "length"},
            {"id", "title", "creationDate"},
            {"id", "firstName", "lastName", "gender", "birthday", "creationDate", "locationIP", "browserUsed"},
            {"id", "creationDate", "locationIP", "browserUsed", "content", "length"},
            {"id", "name", "url", "type"},
            {"id", "name", "url"},
            {"id", "type", "name", "url"},
            {"Person.id", "Post.id", "creationDate"},
            {"Person.id", "Comment.id", "creationDate"},
            {"Person.id", "Tag.id"},
            {"Person.id", "Person.id", "creationDate"},
            {"Person.id", "language"},
            {"Person.id", "Organisation.id", "workFrom"},
            {"Person.id", "Organisation.id", "classYear"},
            {"Person.id", "Place.id"},
            {"Person.id", "email"},
            {"Post.id", "Place.id"},
            {"Post.id", "Tag.id"},
            {"Post.id", "Person.id"},
            {"Comment.id", "Place.id"},
            {"Comment.id", "Post.id"},
            {"Comment.id", "Comment.id"},
            {"Comment.id", "Person.id"},
            {"Comment.id", "Tag.id"},
            {"Tag.id", "TagClass.id"},
            {"TagClass.id", "TagClass.id"},
            {"Place.id", "Place.id"},
            {"Organisation.id", "Place.id"},
            {"Forum.id", "Person.id"},
            {"Forum.id", "Post.id"},
            {"Forum.id", "Tag.id"},
            {"Forum.id", "Person.id", "joinDate"}
    };

    /**
     * Constructor.
     *
     * @param file:        The basic file name.
     * @param tagDic:      The tag dictionary used in the generation.
     * @param browsers:    The browser dictionary used in the generation.
     * @param ipDic:       The IP dictionary used in the generation.
     * @param locationDic: The location dictionary used in the generation.
     * @param languageDic: The language dictionary used in the generation.
     */
    public CSVOriginal(String file, int reducerID,
                       TagDictionary tagDic, BrowserDictionary browsers,
                       CompanyDictionary companyDic, UniversityDictionary universityDictionary,
                       IPAddressDictionary ipDic, PlaceDictionary locationDic, LanguageDictionary languageDic, boolean exportText, boolean compressed) {

        this.tagDic = tagDic;
        this.browserDic = browsers;
        this.locationDic = locationDic;
        this.languageDic = languageDic;
        this.companyDic = companyDic;
        this.universityDic = universityDictionary;
        this.ipDic = ipDic;
        this.exportText = exportText;
        csvRows = 0l;
        date = new GregorianCalendar();
        locations = new Vector<Integer>();
        companies = new HashMap<String, Integer>();
        universities = new HashMap<String, Integer>();
        serializedLanguages = new Vector<Integer>();
        printedTagClasses = new HashMap<Integer, Integer>();


        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            //fileOutputStream = new OutputStream[Files.NUM_FILES.ordinal()];
            fileOutputStream = new OutputStream[Files.NUM_FILES.ordinal()];
            if (compressed) {
                for (int i = 0; i < Files.NUM_FILES.ordinal(); i++) {
                    //this.fileOutputStream[i] = new GZIPOutputStream(new FileOutputStream(file +"/"+fileNames[i] +"_"+reducerID+".csv.gz"));
                    this.fileOutputStream[i] = new GZIPOutputStream(fs.create(new Path(file + "/" + fileNames[i] + "_" + reducerID + ".csv.gz")));
                }
            } else {
                for (int i = 0; i < Files.NUM_FILES.ordinal(); i++) {
                    //this.fileOutputStream[i] = new FileOutputStream(file +"/"+fileNames[i] +"_"+reducerID+".csv");
                    this.fileOutputStream[i] = fs.create(new Path(file + "/" + fileNames[i] + "_" + reducerID + ".csv"));
                }
            }

            for (int j = 0; j < Files.NUM_FILES.ordinal(); j++) {
                Vector<String> arguments = new Vector<String>();
                for (int k = 0; k < fieldNames[j].length; k++) {
                    arguments.add(fieldNames[j][k]);
                }
                ToCSV(arguments, j);
            }
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(-1);
        }
    }

    /**
     * Returns the number of CSV rows written.
     */
    public Long unitsGenerated() {
        return csvRows;
    }

    @Override
    public void serialize(UserInfo info) {

        Vector<String> arguments = new Vector<String>();

        if (info.extraInfo == null) {
            System.err.println("LDBC socialnet must serialize the extraInfo");
            System.exit(-1);
        }

        arguments.add(Long.toString(info.user.getAccountId()));
        arguments.add(info.extraInfo.getFirstName());
        arguments.add(info.extraInfo.getLastName());
        arguments.add(info.extraInfo.getGender());
        if (info.user.getBirthDay() != -1) {
            date.setTimeInMillis(info.user.getBirthDay());
            String dateString = DateGenerator.formatDate(date);
            arguments.add(dateString);
        } else {
            String empty = "";
            arguments.add(empty);
        }
        date.setTimeInMillis(info.user.getCreationDate());
        String dateString = DateGenerator.formatDateDetail(date);
        arguments.add(dateString);
        if (info.user.getIpAddress() != null) {
            arguments.add(info.user.getIpAddress().toString());
        } else {
            String empty = "";
            arguments.add(empty);
        }
        if (info.user.getBrowserId() >= 0) {
            arguments.add(browserDic.getName(info.user.getBrowserId()));
        } else {
            String empty = "";
            arguments.add(empty);
        }
        ToCSV(arguments, Files.PERSON.ordinal());

        ArrayList<Integer> languages = info.extraInfo.getLanguages();
        for (int i = 0; i < languages.size(); i++) {
            arguments.add(Long.toString(info.user.getAccountId()));
            arguments.add(languageDic.getLanguageName(languages.get(i)));
            ToCSV(arguments, Files.PERSON_SPEAKS_LANGUAGE.ordinal());
        }

        Iterator<String> itString = info.extraInfo.getEmail().iterator();
        while (itString.hasNext()) {
            String email = itString.next();
            arguments.add(Long.toString(info.user.getAccountId()));
            arguments.add(email);
            ToCSV(arguments, Files.PERSON_HAS_EMAIL_EMAIL.ordinal());
        }

        arguments.add(Long.toString(info.user.getAccountId()));
        arguments.add(Integer.toString(info.extraInfo.getLocationId()));
        ToCSV(arguments, Files.PERSON_LOCATED_IN_PLACE.ordinal());

        Iterator<Integer> itInteger = info.user.getInterests().iterator();
        while (itInteger.hasNext()) {
            Integer interestIdx = itInteger.next();
            arguments.add(Long.toString(info.user.getAccountId()));
            arguments.add(Integer.toString(interestIdx));
            ToCSV(arguments, Files.PERSON_INTEREST_TAG.ordinal());
        }
    }


    @Override
    public void serialize(Friend friend) {
        Vector<String> arguments = new Vector<String>();
        if (friend != null && friend.getCreatedTime() != -1) {
            arguments.add(Long.toString(friend.getUserAcc()));
            arguments.add(Long.toString(friend.getFriendAcc()));
            date.setTimeInMillis(friend.getCreatedTime());
            arguments.add(DateGenerator.formatDateDetail(date));
            ToCSV(arguments, Files.PERSON_KNOWS_PERSON.ordinal());
        }
    }

    @Override
    public void serialize(Post post) {

        Vector<String> arguments = new Vector<String>();
        String empty = "";

        arguments.add(SN.formId(post.getMessageId()));
        arguments.add(empty);
        date.setTimeInMillis(post.getCreationDate());
        String dateString = DateGenerator.formatDateDetail(date);
        arguments.add(dateString);
        if (post.getIpAddress() != null) {
            arguments.add(post.getIpAddress().toString());
        } else {
            arguments.add(empty);
        }
        if (post.getBrowserIdx() != -1) {
            arguments.add(browserDic.getName(post.getBrowserIdx()));
        } else {
            arguments.add(empty);
        }
        if (post.getLanguage() != -1) {
            arguments.add(languageDic.getLanguageName(post.getLanguage()));
        } else {
            arguments.add(empty);
        }
        if (exportText) {
            arguments.add(post.getContent());
        } else {
            arguments.add(empty);
        }
        arguments.add(Integer.toString(post.getTextSize()));
        ToCSV(arguments, Files.POST.ordinal());

        if (post.getIpAddress() != null) {
            arguments.add(SN.formId(post.getMessageId()));
            arguments.add(Integer.toString(ipDic.getLocation(post.getIpAddress())));
            ToCSV(arguments, Files.POST_LOCATED_PLACE.ordinal());
        }
        arguments.add(SN.formId(post.getGroupId()));
        arguments.add(SN.formId(post.getMessageId()));
        ToCSV(arguments, Files.FORUM_CONTAINER_OF_POST.ordinal());

        arguments.add(SN.formId(post.getMessageId()));
        arguments.add(Long.toString(post.getAuthorId()));
        ToCSV(arguments, Files.POST_HAS_CREATOR_PERSON.ordinal());

        Iterator<Integer> it = post.getTags().iterator();
        while (it.hasNext()) {
            Integer tagId = it.next();
            arguments.add(SN.formId(post.getMessageId()));
            arguments.add(Integer.toString(tagId));
            ToCSV(arguments, Files.POST_HAS_TAG_TAG.ordinal());
        }
    }

    @Override
    public void serialize(Like like) {
        Vector<String> arguments = new Vector<String>();
        date.setTimeInMillis(like.date);
        String dateString = DateGenerator.formatDateDetail(date);
        arguments.add(Long.toString(like.user));
        arguments.add(SN.formId(like.messageId));
        arguments.add(dateString);
        if (like.type == 0 || like.type == 2) {
            ToCSV(arguments, Files.PERSON_LIKE_POST.ordinal());
        } else {
            ToCSV(arguments, Files.PERSON_LIKE_COMMENT.ordinal());
        }

    }

    @Override
    public void serialize(Photo photo) {

        Vector<String> arguments = new Vector<String>();

        String empty = "";
        arguments.add(SN.formId(photo.getMessageId()));
        arguments.add(photo.getContent());
        date.setTimeInMillis(photo.getCreationDate());
        String dateString = DateGenerator.formatDateDetail(date);
        arguments.add(dateString);
        if (photo.getIpAddress() != null) {
            arguments.add(photo.getIpAddress().toString());
        } else {
            arguments.add(empty);
        }
        if (photo.getBrowserIdx() != -1) {
            arguments.add(browserDic.getName(photo.getBrowserIdx()));
        } else {
            arguments.add(empty);
        }
        arguments.add(empty);
        arguments.add(empty);
        arguments.add(Integer.toString(0));
        ToCSV(arguments, Files.POST.ordinal());

        if (photo.getIpAddress() != null) {
            arguments.add(SN.formId(photo.getMessageId()));
            arguments.add(Integer.toString(ipDic.getLocation(photo.getIpAddress())));
            ToCSV(arguments, Files.POST_LOCATED_PLACE.ordinal());
        }

        arguments.add(SN.formId(photo.getMessageId()));
        arguments.add(Long.toString(photo.getAuthorId()));
        ToCSV(arguments, Files.POST_HAS_CREATOR_PERSON.ordinal());

        arguments.add(SN.formId(photo.getGroupId()));
        arguments.add(SN.formId(photo.getMessageId()));
        ToCSV(arguments, Files.FORUM_CONTAINER_OF_POST.ordinal());

        Iterator<Integer> it = photo.getTags().iterator();
        while (it.hasNext()) {
            Integer tagId = it.next();
            String tag = tagDic.getName(tagId);
            arguments.add(SN.formId(photo.getMessageId()));
            arguments.add(Integer.toString(tagId));
            ToCSV(arguments, Files.POST_HAS_TAG_TAG.ordinal());
        }
    }


    @Override
    public void serialize(Comment comment) {

        Vector<String> arguments = new Vector<String>();

        date.setTimeInMillis(comment.getCreationDate());
        String dateString = DateGenerator.formatDateDetail(date);
        arguments.add(SN.formId(comment.getMessageId()));
        arguments.add(dateString);
        if (comment.getIpAddress() != null) {
            arguments.add(comment.getIpAddress().toString());
        } else {
            String empty = "";
            arguments.add(empty);
        }
        if (comment.getBrowserIdx() != -1) {
            arguments.add(browserDic.getName(comment.getBrowserIdx()));
        } else {
            String empty = "";
            arguments.add(empty);
        }
        if (exportText) {
            arguments.add(comment.getContent());
        } else {
            arguments.add(new String(""));
        }
        arguments.add(Integer.toString(comment.getTextSize()));
        ToCSV(arguments, Files.COMMENT.ordinal());

        if (comment.getReplyOf() == comment.getPostId()) {
            arguments.add(SN.formId(comment.getMessageId()));
            arguments.add(SN.formId(comment.getPostId()));
            ToCSV(arguments, Files.COMMENT_REPLY_OF_POST.ordinal());
        } else {
            arguments.add(SN.formId(comment.getMessageId()));
            arguments.add(SN.formId(comment.getReplyOf()));
            ToCSV(arguments, Files.COMMENT_REPLY_OF_COMMENT.ordinal());
        }
        if (comment.getIpAddress() != null) {
            arguments.add(SN.formId(comment.getMessageId()));
            arguments.add(Integer.toString(ipDic.getLocation(comment.getIpAddress())));
            ToCSV(arguments, Files.COMMENT_LOCATED_PLACE.ordinal());
        }

        arguments.add(SN.formId(comment.getMessageId()));
        arguments.add(Long.toString(comment.getAuthorId()));
        ToCSV(arguments, Files.COMMENT_HAS_CREATOR_PERSON.ordinal());

        Iterator<Integer> it = comment.getTags().iterator();
        while (it.hasNext()) {
            Integer tagId = it.next();
            arguments.add(SN.formId(comment.getMessageId()));
            arguments.add(Integer.toString(tagId));
            ToCSV(arguments, Files.COMMENT_HAS_TAG_TAG.ordinal());
        }
    }

    @Override
    public void serialize(Forum forum) {

        Vector<String> arguments = new Vector<String>();

        date.setTimeInMillis(forum.getCreatedDate());
        String dateString = DateGenerator.formatDateDetail(date);

        arguments.add(SN.formId(forum.getForumId()));
        arguments.add(forum.getForumName());
        arguments.add(dateString);
        ToCSV(arguments, Files.FORUM.ordinal());

        arguments.add(SN.formId(forum.getForumId()));
        arguments.add(Long.toString(forum.getModeratorId()));
        ToCSV(arguments, Files.FORUM_HAS_MODERATOR_PERSON.ordinal());

        Integer groupTags[] = forum.getTags();
        for (int i = 0; i < groupTags.length; i++) {
            arguments.add(SN.formId(forum.getForumId()));
            arguments.add(Integer.toString(groupTags[i]));
            ToCSV(arguments, Files.FORUM_HASTAG_TAG.ordinal());
        }

    }

    @Override
    public void serialize(ForumMembership membership) {
        Vector<String> arguments = new Vector<String>();
        date.setTimeInMillis(membership.getJoinDate());
        String dateString = DateGenerator.formatDateDetail(date);

        arguments.add(SN.formId(membership.getForumId()));
        arguments.add(Long.toString(membership.getUserId()));
        arguments.add(dateString);
        ToCSV(arguments, Files.FORUM_HASMEMBER_PERSON.ordinal());
    }

    @Override
    public void serialize(WorkAt workAt) {
        Vector<String> arguments = new Vector<String>();
        date.setTimeInMillis(workAt.year);
        String dateString = DateGenerator.formatYear(date);
        arguments.add(Long.toString(workAt.user));
        arguments.add(Long.toString(workAt.company));
        arguments.add(dateString);
        ToCSV(arguments, Files.PERSON_WORK_AT_ORGANISATION.ordinal());
    }

    @Override
    public void serialize(StudyAt studyAt) {
        Vector<String> arguments = new Vector<String>();
        date.setTimeInMillis(studyAt.year);
        String dateString = DateGenerator.formatYear(date);
        arguments.add(Long.toString(studyAt.user));
        arguments.add(Long.toString(studyAt.university));
        arguments.add(dateString);
        ToCSV(arguments, Files.PERSON_STUDY_AT_ORGANISATION.ordinal());
    }

    @Override
    public void serialize(Organization organization) {
        Vector<String> arguments = new Vector<String>();
        arguments.add(Long.toString(organization.id));
        arguments.add(organization.type.toString());
        arguments.add(organization.name);
        arguments.add(DBP.getUrl(organization.name));
        ToCSV(arguments, Files.ORGANISATION.ordinal());

        arguments.add(Long.toString(organization.id));
        arguments.add(Integer.toString(organization.location));
        ToCSV(arguments, Files.ORGANISATION_BASED_NEAR_PLACE.ordinal());
    }

    @Override
    public void serialize(Tag tag) {
        Vector<String> arguments = new Vector<String>();
        arguments.add(Integer.toString(tag.id));
        arguments.add(tag.name);
        arguments.add(DBP.getUrl(tag.name));
        ToCSV(arguments, Files.TAG.ordinal());

        arguments.add(Integer.toString(tag.id));
        arguments.add(Integer.toString(tag.tagClass));
        ToCSV(arguments, Files.TAG_HAS_TYPE_TAGCLASS.ordinal());
    }

    @Override
    public void serialize(Place place) {
        Vector<String> arguments = new Vector<String>();
        arguments.add(Integer.toString(place.getId()));
        arguments.add(place.getName());
        arguments.add(DBP.getUrl(place.getName()));
        arguments.add(place.getType());
        ToCSV(arguments, Files.PLACE.ordinal());

        if (place.getType() == Place.CITY ||
                place.getType() == Place.COUNTRY) {
            arguments.add(Integer.toString(place.getId()));
            arguments.add(Integer.toString(locationDic.belongsTo(place.getId())));
            ToCSV(arguments, Files.PLACE_PART_OF_PLACE.ordinal());
        }
    }

    @Override
    public void serialize(TagClass tagClass) {
        Vector<String> arguments = new Vector<String>();
        arguments.add(Integer.toString(tagClass.id));
        arguments.add(tagClass.name);
        if (tagClass.name.equals("Thing")) {
            arguments.add("http://www.w3.org/2002/07/owl#Thing");
        } else {
            arguments.add(DBPOWL.getUrl(tagClass.name));
        }
        ToCSV(arguments, Files.TAGCLASS.ordinal());

        if (tagClass.parent != -1) {
            arguments.add(Integer.toString(tagClass.id));
            arguments.add(Integer.toString(tagClass.parent));
            ToCSV(arguments, Files.TAGCLASS_IS_SUBCLASS_OF_TAGCLASS.ordinal());
        }
    }

    public void ToCSV(Vector<String> columns, int index) {
        StringBuffer result = new StringBuffer();
        result.append(columns.get(0));
        for (int i = 1; i < columns.size(); i++) {
            result.append(SEPARATOR);
            result.append(columns.get(i));
        }
        result.append(SEPARATOR);
        result.append(NEWLINE);
        WriteTo(result.toString(), index);
        columns.clear();
    }

    /**
     * Writes the data into the appropriate file.
     *
     * @param data:  The string data.
     * @param index: The file index.
     */
    public void WriteTo(String data, int index) {
        try {
            byte[] dataArray = data.getBytes("UTF8");
            fileOutputStream[index].write(dataArray);
            csvRows++;
        } catch (IOException e) {
            System.out.println("Cannot write to output file ");
            e.printStackTrace();
        }
    }


    /**
     * Ends the serialization.
     */
    public void close() {
        try {
            for (int j = 0; j < Files.NUM_FILES.ordinal(); j++) {
                fileOutputStream[j].flush();
                fileOutputStream[j].close();
            }
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(-1);
        }
    }


    public void resetState(long seed) {

    }
}
