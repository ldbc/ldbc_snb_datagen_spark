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

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import ldbc.socialnet.dbgen.dictionary.BrowserDictionary;
import ldbc.socialnet.dbgen.dictionary.CompanyDictionary;
import ldbc.socialnet.dbgen.dictionary.IPAddressDictionary;
import ldbc.socialnet.dbgen.dictionary.LanguageDictionary;
import ldbc.socialnet.dbgen.dictionary.LocationDictionary;
import ldbc.socialnet.dbgen.dictionary.TagDictionary;
import ldbc.socialnet.dbgen.generator.DateGenerator;
import ldbc.socialnet.dbgen.generator.ScalableGenerator;
import ldbc.socialnet.dbgen.objects.Comment;
import ldbc.socialnet.dbgen.objects.Friend;
import ldbc.socialnet.dbgen.objects.Group;
import ldbc.socialnet.dbgen.objects.GroupMemberShip;
import ldbc.socialnet.dbgen.objects.Location;
import ldbc.socialnet.dbgen.objects.Photo;
import ldbc.socialnet.dbgen.objects.Post;
import ldbc.socialnet.dbgen.objects.ReducedUserProfile;
import ldbc.socialnet.dbgen.objects.UserExtraInfo;
import ldbc.socialnet.dbgen.vocabulary.DBP;
import ldbc.socialnet.dbgen.vocabulary.DBPOWL;
import ldbc.socialnet.dbgen.vocabulary.SN;

/**
 * CSV serializer.
 */
public class CSV implements Serializer {
	
    private FileWriter[][] dataFileWriter;
    private int[] currentWriter;
    
    private long csvRows;
    private GregorianCalendar date;
    
    /**
     * Generator input classes.
     */
    private CompanyDictionary companyDic;
    private HashMap<String, Integer> universityToCountry;
    private BrowserDictionary  browserDic;
    private LocationDictionary locationDic;
    private LanguageDictionary languageDic;
    private TagDictionary tagDic;
    private IPAddressDictionary ipDic;
    private boolean exportText;

    /**
     * Used to create an unique id to each file. It is used only in case of an unnumbered entity or in the relations.
     */
    private long[] idList;
    
    /**
     * Used to avoid serialize more than once the same data.
     */
    HashMap<Integer, Integer> printedTagClasses;
    Vector<Integer> locations;
    Vector<Integer> serializedLanguages;
    Vector<String> organisations;
    Vector<String> interests;
    Vector<String> tagList;
    Vector<String> ipList;
    
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
            {"id", "imageFile", "creationDate", "locationIP", "browserUsed", "language", "content","length"},
            {"id", "title", "creationDate"},
            {"id", "firstName", "lastName", "gender", "birthday", "creationDate", "locationIP", "browserUsed"},
            {"id", "creationDate", "locationIP", "browserUsed", "content","length"},
            {"id", "name", "url", "type"},
            {"id", "name", "url"},
            {"id", "type", "name", "url"},
            {"Person.id", "Post.id", "creationDate"},
            {"Person.id", "Tag.id"},
            {"Person.id", "Person.id"},
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
     * @param file: The basic file name.
     * @param nrOfOutputFiles: How many files will be created.
     * @param tagDic: The tag dictionary used in the generation.
     * @param browsers: The browser dictionary used in the generation.
     * @param companyToCountry: HashMap of company names to country IDs.
     * @param univesityToCountry: HashMap of universities names to country IDs.
     * @param ipDic: The IP dictionary used in the generation.
     * @param locationDic: The location dictionary used in the generation.
     * @param languageDic: The language dictionary used in the generation.
     */
	public CSV(String file, int reducerID, int nrOfOutputFiles,
            TagDictionary tagDic, BrowserDictionary browsers, 
            CompanyDictionary companies, HashMap<String, Integer> univesityToCountry,
            IPAddressDictionary ipDic, LocationDictionary locationDic, LanguageDictionary languageDic, boolean exportText) {
        
        this.tagDic = tagDic;  
        this.browserDic = browsers;
        this.locationDic = locationDic;
        this.languageDic = languageDic;
        this.companyDic = companies;
        this.universityToCountry = univesityToCountry;
        this.ipDic = ipDic;
        this.exportText = exportText;
        
        csvRows = 0l;
        date = new GregorianCalendar();
		locations = new Vector<Integer>();
		organisations = new Vector<String>();
		interests = new Vector<String>();
		tagList = new Vector<String>();
		ipList = new Vector<String>();
		serializedLanguages = new Vector<Integer>();
		printedTagClasses = new HashMap<Integer, Integer>();
		
		idList = new long[Files.NUM_FILES.ordinal()];
		currentWriter = new int[Files.NUM_FILES.ordinal()];
		for (int i = 0; i < Files.NUM_FILES.ordinal(); i++) {
		    idList[i]  = 0;
			currentWriter[i] = 0;
		}

		int nrOfDigits = ((int)Math.log10(nrOfOutputFiles)) + 1;
		String formatString = "%0" + nrOfDigits + "d";
		try{
			dataFileWriter = new FileWriter[nrOfOutputFiles][Files.NUM_FILES.ordinal()];
			if(nrOfOutputFiles==1) {
				for (int i = 0; i < Files.NUM_FILES.ordinal(); i++) {
					this.dataFileWriter[0][i] = new FileWriter(file +"/"+fileNames[i] +"_"+reducerID+".csv");
				}
			} else {
				for(int i=0;i<nrOfOutputFiles;i++) {
					for (int j = 0; j < Files.NUM_FILES.ordinal(); j++) {
						dataFileWriter[i][j] = new FileWriter(file +"/"+fileNames[j] + String.format(formatString, i+1)+"_"+reducerID+ ".csv");
					}
				}
			}

			for(int i=0; i<nrOfOutputFiles; i++) {
			    for (int j = 0; j < Files.NUM_FILES.ordinal(); j++) {
			        Vector<String> arguments = new Vector<String>();
			        for (int k = 0; k < fieldNames[j].length; k++) {
			            arguments.add(fieldNames[j][k]);
			        }
			        ToCSV(arguments, j);
			    }
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
	
	/**
	 * Writes the data into the appropriate file.
	 * 
	 * @param column: The column data.
	 * @param index: The file index.
	 */
	public void ToCSV(Vector<String> columns, int index) {
        StringBuffer result = new StringBuffer();
        result.append(columns.get(0));
        for (int i = 1; i < columns.size(); i++) {
            result.append(SEPARATOR);
            result.append(columns.get(i));
        }
        result.append(NEWLINE);
        WriteTo(result.toString(), index);
        columns.clear();
        idList[index]++;
    }

	/**
     * Writes the data into the appropriate file.
     * 
     * @param data: The string data.
     * @param index: The file index.
     */
    public void WriteTo(String data, int index) {
        try {
            dataFileWriter[currentWriter[index]][index].append(data);
            currentWriter[index] = (currentWriter[index] + 1) % dataFileWriter.length;
            csvRows++;
        } catch (IOException e) {
            System.out.println("Cannot write to output file ");
            e.printStackTrace();
        }
    }
	
	/**
     * Serializes the tag data and its class hierarchy.
     * 
     * @param tagId: The tag id.
     */
	public void printTagHierarchy(Integer tagId) {
	    Vector<String> arguments = new Vector<String>();
	    Integer tagClass = tagDic.getTagClass(tagId);
	    
	    arguments.add(tagId.toString());
        arguments.add(tagClass.toString());
        ToCSV(arguments, Files.TAG_HAS_TYPE_TAGCLASS.ordinal());
	    
        while (tagClass != -1 && !printedTagClasses.containsKey(tagClass)) {
            printedTagClasses.put(tagClass, tagClass);
            arguments.add(tagClass.toString());
            arguments.add(tagDic.getClassName(tagClass));
            if (tagDic.getClassName(tagClass).equals("Thing")) {
                arguments.add("http://www.w3.org/2002/07/owl#Thing");
            } else {
                arguments.add(DBPOWL.getUrl(tagDic.getClassName(tagClass)));
            }
            ToCSV(arguments, Files.TAGCLASS.ordinal());
            
            Integer parent = tagDic.getClassParent(tagClass);
            if (parent != -1) {
                arguments.add(tagClass.toString());
                arguments.add(parent.toString());   
                ToCSV(arguments, Files.TAGCLASS_IS_SUBCLASS_OF_TAGCLASS.ordinal());
            }
            tagClass = parent;
        }
	}
	
	/**
     * Writes the base location and its hierarchy.
     * 
     * @param baseId: The base location id.
     */
	public void printLocationHierarchy(int baseId) {
	    Vector<String> arguments = new Vector<String>();
	    
        ArrayList<Integer> areas = new ArrayList<Integer>();
        do {
            areas.add(baseId);
            baseId = locationDic.belongsTo(baseId);
        } while (baseId != -1);
        
        for (int i = areas.size() - 1; i >= 0; i--) {
            if (locations.indexOf(areas.get(i)) == -1) {
                locations.add(areas.get(i));
                arguments.add(Integer.toString(areas.get(i)));
                arguments.add(locationDic.getLocationName(areas.get(i)));
                arguments.add(DBP.getUrl(locationDic.getLocationName(areas.get(i))));
                arguments.add(locationDic.getType(areas.get(i)));
                ToCSV(arguments, Files.PLACE.ordinal());
                if (locationDic.getType(areas.get(i)) == Location.CITY ||
                        locationDic.getType(areas.get(i)) == Location.COUNTRY) {
                    arguments.add(Integer.toString(areas.get(i)));
                    arguments.add(Integer.toString(areas.get(i+1)));
                    ToCSV(arguments, Files.PLACE_PART_OF_PLACE.ordinal());
                }
            }
        }
    }
	
	/**
     * @See {@link ldbc.socialnet.dbgen.serializer.Serializer#gatherData(ReducedUserProfile, UserExtraInfo)}
     */
	public void gatherData(ReducedUserProfile profile, UserExtraInfo extraInfo){
		Vector<String> arguments = new Vector<String>();
		
		if (extraInfo == null) {
            System.err.println("LDBC socialnet must serialize the extraInfo");
            System.exit(-1);
        }
		
		printLocationHierarchy(extraInfo.getLocationId());
		Iterator<String> itString = extraInfo.getCompanies().iterator();
		while (itString.hasNext()) {
		    String company = itString.next();
		    int parentId = companyDic.getCountry(company);
		    printLocationHierarchy(parentId);
		}
		printLocationHierarchy(universityToCountry.get(extraInfo.getUniversity()));
        printLocationHierarchy(ipDic.getLocation(profile.getIpAddress()));
        

        arguments.add(Long.toString(profile.getAccountId()));
        arguments.add(extraInfo.getFirstName());
        arguments.add(extraInfo.getLastName());
        arguments.add(extraInfo.getGender());
        if (profile.getBirthDay() != -1 ) {
            date.setTimeInMillis(profile.getBirthDay());
            String dateString = DateGenerator.formatDate(date);
            arguments.add(dateString);
        } else {
            String empty = "";
            arguments.add(empty);
        }
		date.setTimeInMillis(profile.getCreationDate());
		String dateString = DateGenerator.formatDateDetail(date);
		arguments.add(dateString);
        if (profile.getIpAddress() != null) {
            arguments.add(profile.getIpAddress().toString());
        } else {
            String empty = "";
            arguments.add(empty);
        }
        if (profile.getBrowserIdx() >= 0) {
            arguments.add(browserDic.getName(profile.getBrowserIdx()));
        } else {
            String empty = "";
            arguments.add(empty);
        }
		ToCSV(arguments, Files.PERSON.ordinal());

		Vector<Integer> languages = extraInfo.getLanguages();
		for (int i = 0; i < languages.size(); i++) {
		    arguments.add(Long.toString(profile.getAccountId()));
		    arguments.add(languageDic.getLanguagesName(languages.get(i)));
		    ToCSV(arguments, Files.PERSON_SPEAKS_LANGUAGE.ordinal());
		}

		itString = extraInfo.getEmail().iterator();
		while (itString.hasNext()){
		    String email = itString.next();
		    arguments.add(Long.toString(profile.getAccountId()));
		    arguments.add(email);
		    ToCSV(arguments, Files.PERSON_HAS_EMAIL_EMAIL.ordinal());
		}

		arguments.add(Long.toString(profile.getAccountId()));
		arguments.add(Integer.toString(extraInfo.getLocationId()));
		ToCSV(arguments, Files.PERSON_LOCATED_IN_PLACE.ordinal());

		int organisationId = -1;
		if (!extraInfo.getUniversity().equals("")){
		    organisationId = organisations.indexOf(extraInfo.getUniversity());
		    if(organisationId == -1) {
		        organisationId = organisations.size();
		        organisations.add(extraInfo.getUniversity());

		        arguments.add(SN.formId(organisationId));
		        arguments.add(ScalableGenerator.OrganisationType.university.toString());
		        arguments.add(extraInfo.getUniversity());
		        arguments.add(DBP.getUrl(extraInfo.getUniversity()));
		        ToCSV(arguments, Files.ORGANISATION.ordinal());

		        arguments.add(SN.formId(organisationId));
		        arguments.add(Integer.toString(universityToCountry.get(extraInfo.getUniversity())));
		        ToCSV(arguments, Files.ORGANISATION_BASED_NEAR_PLACE.ordinal());
		    }
		    
		    if (extraInfo.getClassYear() != -1 ) {
	            date.setTimeInMillis(extraInfo.getClassYear());
	            dateString = DateGenerator.formatYear(date);

	            arguments.add(Long.toString(profile.getAccountId()));
	            arguments.add(SN.formId(organisationId));
	            arguments.add(dateString);
	            ToCSV(arguments, Files.PERSON_STUDY_AT_ORGANISATION.ordinal());
	        }
		}

		itString = extraInfo.getCompanies().iterator();
		while (itString.hasNext()) {
		    String company = itString.next();
		    organisationId = organisations.indexOf(company);
		    if(organisationId == -1) {
		        organisationId = organisations.size();
		        organisations.add(company);

		        arguments.add(SN.formId(organisationId));
		        arguments.add(ScalableGenerator.OrganisationType.company.toString());
		        arguments.add(company);
		        arguments.add(DBP.getUrl(company));
		        ToCSV(arguments, Files.ORGANISATION.ordinal());

		        arguments.add(SN.formId(organisationId));
		        arguments.add(Integer.toString(companyDic.getCountry(company)));
		        ToCSV(arguments, Files.ORGANISATION_BASED_NEAR_PLACE.ordinal());
		    }
		    date.setTimeInMillis(extraInfo.getWorkFrom(company));
		    dateString = DateGenerator.formatYear(date);

		    arguments.add(Long.toString(profile.getAccountId()));
		    arguments.add(SN.formId(organisationId));
		    arguments.add(dateString);
		    ToCSV(arguments, Files.PERSON_WORK_AT_ORGANISATION.ordinal());
		}
		
        Iterator<Integer> itInteger = profile.getSetOfTags().iterator();
        while (itInteger.hasNext()){
            Integer interestIdx = itInteger.next();
            String interest = tagDic.getName(interestIdx);
            
            if (interests.indexOf(interest) == -1) {
                interests.add(interest);
                
                arguments.add(Integer.toString(interestIdx));
                arguments.add(interest.replace("\"", "\\\""));
                arguments.add(DBP.getUrl(interest));
                ToCSV(arguments, Files.TAG.ordinal());
                
                printTagHierarchy(interestIdx);
            }
            
            arguments.add(Long.toString(profile.getAccountId()));
            arguments.add(Integer.toString(interestIdx));
            ToCSV(arguments, Files.PERSON_INTEREST_TAG.ordinal());
        }   
        
        Friend friends[] = profile.getFriendList();
        for (int i = 0; i < friends.length; i++) {
            if (friends[i] != null && friends[i].getCreatedTime() != -1){

                arguments.add(Long.toString(profile.getAccountId()));
                arguments.add(Long.toString(friends[i].getFriendAcc()));
                ToCSV(arguments,Files.PERSON_KNOWS_PERSON.ordinal());
            }
        }
		
        //The forums of the user
		date.setTimeInMillis(profile.getCreationDate());
        dateString = DateGenerator.formatDateDetail(date);

        String title = "Wall of " + extraInfo.getFirstName() + " " + extraInfo.getLastName();
        arguments.add(SN.formId(profile.getForumWallId()));
        arguments.add(title);
        arguments.add(dateString);
        ToCSV(arguments,Files.FORUM.ordinal());
        
        arguments.add(SN.formId(profile.getForumWallId()));
        arguments.add(Long.toString(profile.getAccountId()));
        ToCSV(arguments,Files.FORUM_HAS_MODERATOR_PERSON.ordinal());
        
        itInteger = profile.getSetOfTags().iterator();
        while (itInteger.hasNext()){
            Integer interestIdx = itInteger.next();
            arguments.add(SN.formId(profile.getForumWallId()));
            arguments.add(Integer.toString(interestIdx));
            ToCSV(arguments, Files.FORUM_HASTAG_TAG.ordinal());
        }   
             
        for (int i = 0; i < friends.length; i ++){
            if (friends[i] != null && friends[i].getCreatedTime() != -1){
                date.setTimeInMillis(friends[i].getCreatedTime());
                dateString = DateGenerator.formatDateDetail(date);
                
                arguments.add(SN.formId(profile.getForumWallId()));
                arguments.add(Long.toString(friends[i].getFriendAcc()));
                arguments.add(dateString);
                ToCSV(arguments,Files.FORUM_HASMEMBER_PERSON.ordinal());
            }
        }
	}

	/**
     * @See {@link ldbc.socialnet.dbgen.serializer.Serializer#gatherData(Post)}
     */
	public void gatherData(Post post){
	    Vector<String> arguments = new Vector<String>();
	    String empty = "";
	    
	    if (post.getIpAddress() != null) {
            printLocationHierarchy(ipDic.getLocation(post.getIpAddress()));
        }
	    
	    arguments.add(SN.formId(post.getPostId()));
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
        if(exportText ) {
	        arguments.add(post.getContent());
        } else {
            arguments.add(empty);
        }
        arguments.add(Integer.toString(post.getTextSize()));
	    ToCSV(arguments, Files.POST.ordinal());

	    if (post.getIpAddress() != null) {
	        arguments.add(SN.formId(post.getPostId()));
	        arguments.add(Integer.toString(ipDic.getLocation(post.getIpAddress())));
	        ToCSV(arguments, Files.POST_LOCATED_PLACE.ordinal());
	    }
	    arguments.add(SN.formId(post.getGroupId()));
	    arguments.add(SN.formId(post.getPostId()));
	    ToCSV(arguments, Files.FORUM_CONTAINER_OF_POST.ordinal());

	    arguments.add(SN.formId(post.getPostId()));
	    arguments.add(Long.toString(post.getAuthorId()));
	    ToCSV(arguments, Files.POST_HAS_CREATOR_PERSON.ordinal());

	    Iterator<Integer> it = post.getTags().iterator();
	    while (it.hasNext()) {
	        Integer tagId = it.next();
	        String tag = tagDic.getName(tagId);
	        if (interests.indexOf(tag) == -1) {
	            interests.add(tag);
	            arguments.add(Integer.toString(tagId));
	            arguments.add(tag.replace("\"", "\\\""));
	            arguments.add(DBP.getUrl(tag));
	            ToCSV(arguments, Files.TAG.ordinal());

	            printTagHierarchy(tagId);
	        }

	        arguments.add(SN.formId(post.getPostId()));
	        arguments.add(Integer.toString(tagId));
	        ToCSV(arguments, Files.POST_HAS_TAG_TAG.ordinal());
	    }

	    long userLikes[] = post.getInterestedUserAccs();
	    long likeTimestamps[] = post.getInterestedUserAccsTimestamp();
	    for (int i = 0; i < userLikes.length; i ++) {
	        date.setTimeInMillis(likeTimestamps[i]);
	        dateString = DateGenerator.formatDateDetail(date);
	        arguments.add(Long.toString(userLikes[i]));
	        arguments.add(SN.formId(post.getPostId()));
	        arguments.add(dateString);
	        ToCSV(arguments, Files.PERSON_LIKE_POST.ordinal());
	    }
	}

	/**
     * @See {@link ldbc.socialnet.dbgen.serializer.Serializer#gatherData(Comment)}
     */
	public void gatherData(Comment comment){
	    Vector<String> arguments = new Vector<String>();
	    
	    if (comment.getIpAddress() != null) {
            printLocationHierarchy(ipDic.getLocation(comment.getIpAddress()));
        }
	    
	    date.setTimeInMillis(comment.getCreationDate());
        String dateString = DateGenerator.formatDateDetail(date); 
	    arguments.add(SN.formId(comment.getCommentId()));
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
        } else {
            arguments.add(new String(""));
        }
        arguments.add(Integer.toString(comment.getTextSize()));
	    ToCSV(arguments, Files.COMMENT.ordinal());
	    
	    if (comment.getReplyOf() == -1) {
            arguments.add(SN.formId(comment.getCommentId()));
            arguments.add(SN.formId(comment.getPostId()));
            ToCSV(arguments, Files.COMMENT_REPLY_OF_POST.ordinal());
        } else {
            arguments.add(SN.formId(comment.getCommentId()));
            arguments.add(SN.formId(comment.getReplyOf()));
            ToCSV(arguments, Files.COMMENT_REPLY_OF_COMMENT.ordinal());
        }
	    if (comment.getIpAddress() != null) {
            arguments.add(SN.formId(comment.getCommentId()));
            arguments.add(Integer.toString(ipDic.getLocation(comment.getIpAddress())));
            ToCSV(arguments, Files.COMMENT_LOCATED_PLACE.ordinal());
        }
	    
	    arguments.add(SN.formId(comment.getCommentId()));
	    arguments.add(Long.toString(comment.getAuthorId()));
	    ToCSV(arguments, Files.COMMENT_HAS_CREATOR_PERSON.ordinal());
	}

	/**
     * @See {@link ldbc.socialnet.dbgen.serializer.Serializer#gatherData(Photo)}
     */
	public void gatherData(Photo photo){
	    Vector<String> arguments = new Vector<String>();
	    
	    if (photo.getIpAddress() != null) {
            printLocationHierarchy(ipDic.getLocation(photo.getIpAddress()));
        }

	    String empty = "";
	    arguments.add(SN.formId(photo.getPhotoId()));
	    arguments.add(photo.getImage());
	    date.setTimeInMillis(photo.getTakenTime());
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
	    ToCSV(arguments, Files.POST.ordinal());

	    if (photo.getIpAddress() != null) {
	        arguments.add(SN.formId(photo.getPhotoId()));
	        arguments.add(Integer.toString(ipDic.getLocation(photo.getIpAddress())));
	        ToCSV(arguments, Files.POST_LOCATED_PLACE.ordinal());
	    }

	    arguments.add(SN.formId(photo.getPhotoId()));
	    arguments.add(Long.toString(photo.getCreatorId()));
	    ToCSV(arguments, Files.POST_HAS_CREATOR_PERSON.ordinal());

	    arguments.add(SN.formId(photo.getAlbumId()));
	    arguments.add(SN.formId(photo.getPhotoId()));
	    ToCSV(arguments, Files.FORUM_CONTAINER_OF_POST.ordinal());

	    Iterator<Integer> it = photo.getTags().iterator();
	    while (it.hasNext()) {
	        Integer tagId = it.next();
	        String tag = tagDic.getName(tagId);
	        if (interests.indexOf(tag) == -1) {
	            interests.add(tag);
	            arguments.add(Integer.toString(tagId));
	            arguments.add(tag.replace("\"", "\\\""));
	            arguments.add(DBP.getUrl(tag));
	            ToCSV(arguments, Files.TAG.ordinal());

	            printTagHierarchy(tagId);
	        }

	        arguments.add(SN.formId(photo.getPhotoId()));
	        arguments.add(Integer.toString(tagId));
	        ToCSV(arguments, Files.POST_HAS_TAG_TAG.ordinal());
	    }

	    long userLikes[] = photo.getInterestedUserAccs();
	    long likeTimestamps[] = photo.getInterestedUserAccsTimestamp();
	    for (int i = 0; i < userLikes.length; i ++) {
	        date.setTimeInMillis(likeTimestamps[i]);
	        dateString = DateGenerator.formatDateDetail(date);
	        arguments.add(Long.toString(userLikes[i]));
	        arguments.add(SN.formId(photo.getPhotoId()));
	        arguments.add(dateString);
	        ToCSV(arguments, Files.PERSON_LIKE_POST.ordinal());
	    }
	}
	
	/**
     * @See {@link ldbc.socialnet.dbgen.serializer.Serializer#gatherData(Group)}
     */
	public void gatherData(Group group) {
	    Vector<String> arguments = new Vector<String>();
	    
	    date.setTimeInMillis(group.getCreatedDate());
        String dateString = DateGenerator.formatDateDetail(date);  
        
	    arguments.add(SN.formId(group.getGroupId()));
	    arguments.add(group.getGroupName());
	    arguments.add(dateString);
	    ToCSV(arguments,Files.FORUM.ordinal());
	    
	    arguments.add(SN.formId(group.getGroupId()));
	    arguments.add(Long.toString(group.getModeratorId()));
	    ToCSV(arguments,Files.FORUM_HAS_MODERATOR_PERSON.ordinal());
	    
	    Integer groupTags[] = group.getTags();
        for (int i = 0; i < groupTags.length; i ++) {
            String interest = tagDic.getName(groupTags[i]);
            
            if (interests.indexOf(interest) == -1) {
                interests.add(interest);
                
                arguments.add(Integer.toString(groupTags[i]));
                arguments.add(interest.replace("\"", "\\\""));
                arguments.add(DBP.getUrl(interest));
                ToCSV(arguments, Files.TAG.ordinal());
                
                printTagHierarchy(groupTags[i]);
            }
            
            arguments.add(SN.formId(group.getGroupId()));
            arguments.add(Integer.toString(groupTags[i]));
            ToCSV(arguments,Files.FORUM_HASTAG_TAG.ordinal());
        }
	    
	    GroupMemberShip memberShips[] = group.getMemberShips();
        int numMemberAdded = group.getNumMemberAdded();
        for (int i = 0; i < numMemberAdded; i ++) {
            date.setTimeInMillis(memberShips[i].getJoinDate());
            dateString = DateGenerator.formatDateDetail(date);
            
            arguments.add(SN.formId(group.getGroupId()));
            arguments.add(Long.toString(memberShips[i].getUserId()));
            arguments.add(dateString);
            ToCSV(arguments,Files.FORUM_HASMEMBER_PERSON.ordinal());
        }
	}

	/**
     * Ends the serialization.
     */
	public void close() {
		try {
			for (int i = 0; i < dataFileWriter.length; i++) {
				for (int j = 0; j < Files.NUM_FILES.ordinal(); j++) {
					dataFileWriter[i][j].flush();
					dataFileWriter[i][j].close();
				}
			}
		} catch(IOException e) {
			System.err.println(e.getMessage());
			System.exit(-1);
		}
	}
}
