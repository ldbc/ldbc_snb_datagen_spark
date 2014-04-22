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

import java.io.IOException;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.util.*;
import java.util.zip.GZIPOutputStream;

import ldbc.socialnet.dbgen.dictionary.*;
import ldbc.socialnet.dbgen.generator.DateGenerator;
import ldbc.socialnet.dbgen.generator.ScalableGenerator;
import ldbc.socialnet.dbgen.objects.*;
import ldbc.socialnet.dbgen.vocabulary.DBP;
import ldbc.socialnet.dbgen.vocabulary.DBPOWL;
import ldbc.socialnet.dbgen.vocabulary.SN;

/**
 * CSV Merge Foreign serializer.
 */
public class CSVMergeForeign implements Serializer {
	
    private OutputStream[] fileOutputStream;

    private long csvRows;
    private GregorianCalendar date;
    
    /**
     * Generator input classes.
     */
    private CompanyDictionary companyDic;
    private UniversityDictionary universityDic;
    private HashMap<String, Integer> universityToCountry;
    private BrowserDictionary  browserDic;
    private LocationDictionary locationDic;
    private LanguageDictionary languageDic;
    private TagDictionary tagDic;
    private IPAddressDictionary ipDic;
    private boolean exportText;


    /**
     * Used to avoid serialize more than once the same data.
     */
    HashMap<Integer, Integer> printedTagClasses;
    Vector<Integer> locations;
    TreeMap<String, Integer> companies;
    TreeMap<String, Integer> universities;

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
            "person_email_emailaddress",
            "post_hasTag_tag",
            "comment_hasTag_tag",
	        "tag_hasType_tagclass",
	        "tagclass_isSubclassOf_tagclass",
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
        PERSON_HAS_EMAIL_EMAIL,
        POST_HAS_TAG_TAG,
        COMMENT_HAS_TAG_TAG,
    	TAG_HAS_TYPE_TAGCLASS,
    	TAGCLASS_IS_SUBCLASS_OF_TAGCLASS,
    	FORUM_HASTAG_TAG,
        FORUM_HASMEMBER_PERSON,
    	NUM_FILES
    }

    /**
     * The field names of the CSV files. They are the first thing written in their respective file.
     */
    private final String[][] fieldNames = {
            {"id", "name", "url"},
            {"id", "imageFile", "creationDate", "locationIP", "browserUsed", "language", "content", "length", "creator","Forum.id","place"},
            {"id", "title", "creationDate","moderator"},
            {"id", "firstName", "lastName", "gender", "birthday", "creationDate", "locationIP", "browserUsed","place"},
            {"id", "creationDate", "locationIP", "browserUsed", "content", "length", "creator", "place","replyOfPost","replyOfComment"},
            {"id", "name", "url", "type","isPartOf"},
            {"id", "name", "url"},
            {"id", "type", "name", "url","place"},
            {"Person.id", "Post.id", "creationDate"},
            {"Person.id", "Comment.id", "creationDate"},
            {"Person.id", "Tag.id"},
            {"Person.id", "Person.id","creationDate"},
            {"Person.id", "language"},
            {"Person.id", "Organisation.id", "workFrom"},
            {"Person.id", "Organisation.id", "classYear"},
            {"Person.id", "email"},
            {"Post.id", "Tag.id"},
            {"Comment.id", "Tag.id"},
            {"Tag.id", "TagClass.id"},
            {"TagClass.id", "TagClass.id"},
            {"Forum.id", "Tag.id"},
            {"Forum.id", "Person.id", "joinDate"}
    };

    /**
     * Constructor.
     * 
     * @param file: The basic file name.
     * @param tagDic: The tag dictionary used in the generation.
     * @param browsers: The browser dictionary used in the generation.
     * @param ipDic: The IP dictionary used in the generation.
     * @param locationDic: The location dictionary used in the generation.
     * @param languageDic: The language dictionary used in the generation.
     */
	public CSVMergeForeign(String file, int reducerID,
            TagDictionary tagDic, BrowserDictionary browsers, 
            CompanyDictionary companyDic, UniversityDictionary universityDictionary,
            IPAddressDictionary ipDic, LocationDictionary locationDic, LanguageDictionary languageDic, boolean exportText, boolean compressed) {

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
		companies = new TreeMap<String,Integer>();
        universities = new TreeMap<String,Integer>();
		printedTagClasses = new HashMap<Integer, Integer>();
		

        try{
            fileOutputStream = new OutputStream[Files.NUM_FILES.ordinal()];
            if( compressed ) {
                for (int i = 0; i < Files.NUM_FILES.ordinal(); i++) {
                    this.fileOutputStream[i] = new GZIPOutputStream(new FileOutputStream(file +"/"+fileNames[i] +"_"+reducerID+".csv.gz"));
                }
            } else {
                for (int i = 0; i < Files.NUM_FILES.ordinal(); i++) {
                    this.fileOutputStream[i] = new FileOutputStream(file +"/"+fileNames[i] +"_"+reducerID+".csv");
                }
            }

            for (int j = 0; j < Files.NUM_FILES.ordinal(); j++) {
                Vector<String> arguments = new Vector<String>();
                for (int k = 0; k < fieldNames[j].length; k++) {
                    arguments.add(fieldNames[j][k]);
                }
                ToCSV(arguments, j);
            }

		} catch(IOException e){
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
        if (info.user.getBirthDay() != -1 ) {
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
        if (info.user.getBrowserIdx() >= 0) {
            arguments.add(browserDic.getName(info.user.getBrowserIdx()));
        } else {
            String empty = "";
            arguments.add(empty);
        }
        arguments.add(Integer.toString(info.extraInfo.getLocationId()));
        ToCSV( arguments, Files.PERSON.ordinal());

        Vector<Integer> languages = info.extraInfo.getLanguages();
        for (int i = 0; i < languages.size(); i++) {
            arguments.add(Long.toString(info.user.getAccountId()));
            arguments.add(languageDic.getLanguagesName(languages.get(i)));
            ToCSV(arguments, Files.PERSON_SPEAKS_LANGUAGE.ordinal());
        }

        Iterator<String> itString = info.extraInfo.getEmail().iterator();
        while (itString.hasNext()){
            String email = itString.next();
            arguments.add(Long.toString(info.user.getAccountId()));
            arguments.add(email);
            ToCSV( arguments, Files.PERSON_HAS_EMAIL_EMAIL.ordinal());
        }


        Iterator<Integer> itInteger = info.user.getSetOfTags().iterator();
        while (itInteger.hasNext()){
            Integer interestIdx = itInteger.next();
            arguments.add(Long.toString(info.user.getAccountId()));
            arguments.add(Integer.toString(interestIdx));
            ToCSV(arguments,Files.PERSON_INTEREST_TAG.ordinal());
        }
    }

    @Override
    public void serialize(Friend friend) {
        Vector<String> arguments = new Vector<String>();
        if (friend != null && friend.getCreatedTime() != -1){
            arguments.add(Long.toString(friend.getUserAcc()));
            arguments.add(Long.toString(friend.getFriendAcc()));
            date.setTimeInMillis(friend.getCreatedTime());
            arguments.add(DateGenerator.formatDateDetail(date));
            ToCSV(arguments,Files.PERSON_KNOWS_PERSON.ordinal());
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
        if (post.getBrowserIdx() != -1){
            arguments.add(browserDic.getName(post.getBrowserIdx()));
        } else {
            arguments.add(empty);
        }
        if (post.getLanguage() != -1) {
            arguments.add(languageDic.getLanguagesName(post.getLanguage()));
        } else {
            arguments.add(empty);
        }
        if( exportText ) {
            arguments.add(post.getContent());
        } else {
            arguments.add(empty);
        }
        arguments.add(Integer.toString(post.getTextSize()));
        arguments.add(Long.toString(post.getAuthorId()));
        arguments.add(SN.formId(post.getGroupId()));
        arguments.add(Integer.toString(ipDic.getLocation(post.getIpAddress())));
        ToCSV( arguments, Files.POST.ordinal());

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
        if( like.type == 0 || like.type == 2 ) {
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
        if (photo.getBrowserIdx() != -1){
            arguments.add(browserDic.getName(photo.getBrowserIdx()));
        } else {
            arguments.add(empty);
        }
        arguments.add(empty);
        arguments.add(empty);
        arguments.add(Integer.toString(0));
        arguments.add(Long.toString(photo.getAuthorId()));
        arguments.add(SN.formId(photo.getGroupId()));
        arguments.add(Integer.toString(ipDic.getLocation(photo.getIpAddress())));
        ToCSV(arguments, Files.POST.ordinal());

        Iterator<Integer> it = photo.getTags().iterator();
        while (it.hasNext()) {
            Integer tagId = it.next();
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
        if (comment.getBrowserIdx() != -1){
            arguments.add(browserDic.getName(comment.getBrowserIdx()));
        } else {
            String empty = "";
            arguments.add(empty);
        }
        if( exportText ) {
            arguments.add(comment.getContent());
        }
        else {
            arguments.add("");
        }
        arguments.add(Integer.toString(comment.getTextSize()));
        arguments.add(Long.toString(comment.getAuthorId()));
        arguments.add(Integer.toString(ipDic.getLocation(comment.getIpAddress())));
        if (comment.getReplyOf() == comment.getPostId()) {
            arguments.add(SN.formId(comment.getPostId()));
            String empty = "";
            arguments.add(empty);
        } else {
            String empty = "";
            arguments.add(empty);
            arguments.add(SN.formId(comment.getReplyOf()));
        }
        ToCSV( arguments, Files.COMMENT.ordinal());

        Iterator<Integer> it = comment.getTags().iterator();
        while (it.hasNext()) {
            Integer tagId = it.next();
            arguments.add(SN.formId(comment.getMessageId()));
            arguments.add(Integer.toString(tagId));
            ToCSV(arguments, Files.COMMENT_HAS_TAG_TAG.ordinal());
        }
    }

    @Override
    public void serialize(Group group) {
        Vector<String> arguments = new Vector<String>();

        date.setTimeInMillis(group.getCreatedDate());
        String dateString = DateGenerator.formatDateDetail(date);

        arguments.add(SN.formId(group.getGroupId()));
        arguments.add(group.getGroupName());
        arguments.add(dateString);
        arguments.add(Long.toString(group.getModeratorId()));
        ToCSV(arguments,Files.FORUM.ordinal());

        Integer groupTags[] = group.getTags();
        for (int i = 0; i < groupTags.length; i ++) {
            arguments.add(SN.formId(group.getGroupId()));
            arguments.add(Integer.toString(groupTags[i]));
            ToCSV(arguments,Files.FORUM_HASTAG_TAG.ordinal());
        }
    }

    @Override
    public void serialize(GroupMemberShip membership) {
        Vector<String> arguments = new Vector<String>();
        date.setTimeInMillis(membership.getJoinDate());
        String dateString = DateGenerator.formatDateDetail(date);

        arguments.add(SN.formId(membership.getGroupId()));
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
        arguments.add(ScalableGenerator.OrganisationType.company.toString());
        arguments.add(organization.name);
        arguments.add(DBP.getUrl(organization.name));
        arguments.add(Integer.toString(organization.location));
        ToCSV(arguments, Files.ORGANISATION.ordinal());
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
    public void serialize(Location location) {
        Vector<String> arguments = new Vector<String>();
        arguments.add(Integer.toString(location.getId()));
        arguments.add(location.getName());
        arguments.add(DBP.getUrl(location.getName()));
        arguments.add(location.getType());

        if (location.getType() == Location.CITY ||
                location.getType() == Location.COUNTRY) {
            arguments.add(Integer.toString(locationDic.belongsTo(location.getId())));
        } else {
            String empty = "";
            arguments.add(empty);
        }
        ToCSV(arguments, Files.PLACE.ordinal());
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
            arguments.add(tagClass.toString());
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
     * @param data: The string data.
     * @param index: The file index.
     */
    public void WriteTo(String data, int index) {
        try {
            byte [] dataArray = data.getBytes("UTF8");
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
        } catch(IOException e) {
            System.err.println(e.getMessage());
            System.exit(-1);
        }
    }

    public void resetState(long seed) {

    }
}
