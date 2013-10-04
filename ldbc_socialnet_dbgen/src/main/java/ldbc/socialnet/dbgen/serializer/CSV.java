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


public class CSV implements Serializer {
	
	final String NEWLINE = "\n";
	final String SEPARATOR = "|";

	final String[] fileNames = {
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

    final String[][] fieldNames = {
            {"id", "name", "url"},
            {"id", "imageFile", "creationDate", "locationIP", "browserUsed", "language", "content"},
            {"id", "title", "creationDate"},
            {"id", "firstName", "lastName", "gender", "birthday", "creationDate", "locationIP", "browserUsed"},
            {"id", "creationDate", "locationIP", "browserUsed", "content"},
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

	private long nrTriples;
	private FileWriter[][] dataFileWriter;
	int[] currentWriter;
	long[] idList;
	static long membershipId = 0;
	static long friendshipId = 0; 
	static long gpsId = 0; 
	static long emailId = 0;
	static long ipId = 0;
	
	HashMap<Integer, Integer> printedTagClasses;
	
	HashMap<String, Integer> companyToCountry;
    HashMap<String, Integer> universityToCountry;
	Vector<String>	vBrowserNames;
	Vector<Integer> locations;
	Vector<Integer> serializedLanguages;
	Vector<String> organisations;
	Vector<String> interests;
	Vector<String> tagList;
	Vector<String> ipList;
	
	GregorianCalendar date;
	LocationDictionary locationDic;
	LanguageDictionary languageDic;
	TagDictionary tagDic;
	IPAddressDictionary ipDic;
	
	public CSV(String file, boolean forwardChaining) {
		this(file, forwardChaining, 1);
	}
	
	public CSV(String file, boolean forwardChaining, int nrOfOutputFiles) {
		vBrowserNames = new Vector<String>();
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
		date = new GregorianCalendar();
		int nrOfDigits = ((int)Math.log10(nrOfOutputFiles)) + 1;
		String formatString = "%0" + nrOfDigits + "d";
		try{
			dataFileWriter = new FileWriter[nrOfOutputFiles][Files.NUM_FILES.ordinal()];
			if(nrOfOutputFiles==1) {
				for (int i = 0; i < Files.NUM_FILES.ordinal(); i++) {
					this.dataFileWriter[0][i] = new FileWriter(file + fileNames[i] + ".csv");
				}
			} else {
				for(int i=0;i<nrOfOutputFiles;i++) {
					for (int j = 0; j < Files.NUM_FILES.ordinal(); j++) {
						dataFileWriter[i][j] = new FileWriter(file + fileNames[j] + String.format(formatString, i+1) + ".csv");
					}
				}
			}

			for(int i=0;i<nrOfOutputFiles;i++) {
			    for (int j = 0; j < Files.NUM_FILES.ordinal(); j++) {
			        Vector<String> arguments = new Vector<String>();
			        for (int k = 0; k < fieldNames[j].length; k++) {
			            arguments.add(fieldNames[j][k]);
			        }
			        ToCSV(arguments, j);
			    }
			}
				
		} catch(IOException e){
			System.err.println("Could not open File for writing.");
			System.err.println(e.getMessage());
			System.exit(-1);
		}
		nrTriples = 0l;
	}
	
	public CSV(String file, boolean forwardChaining, int nrOfOutputFiles, 
            TagDictionary tagDic, Vector<String> _vBrowsers, 
            HashMap<String, Integer> companyToCountry, HashMap<String, Integer> univesityToCountry,
            IPAddressDictionary ipDic, LocationDictionary locationDic, LanguageDictionary languageDic) {
	    
	    this(file, forwardChaining, nrOfOutputFiles);
        this.tagDic = tagDic;  
        this.vBrowserNames = _vBrowsers;
        this.locationDic = locationDic;
        this.languageDic = languageDic;
        this.companyToCountry = companyToCountry;
        this.universityToCountry = univesityToCountry;
        this.ipDic = ipDic;
    }
	
	public Long triplesGenerated() {
		return nrTriples;
	}
	
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
	
	public void ToCSV(Vector<String> arguments, int index) {
		StringBuffer result = new StringBuffer();
		result.append(arguments.get(0));
		for (int i = 1; i < arguments.size(); i++) {
			result.append(SEPARATOR);
			result.append(arguments.get(i));
		}
		result.append(NEWLINE);
		WriteTo(result.toString(), index);
		arguments.clear();
		idList[index]++;
	}

	public void WriteTo(String data, int index) {
		try {
			dataFileWriter[currentWriter[index]][index].append(data);
			currentWriter[index] = (currentWriter[index] + 1) % dataFileWriter.length;
		} catch (IOException e) {
			System.out.println("Cannot write to output file ");
			e.printStackTrace();
		}
	}
	
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
                //print location
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
		    int parentId = companyToCountry.get(company);
		    printLocationHierarchy(parentId);
		}
		printLocationHierarchy(universityToCountry.get(extraInfo.getOrganization()));
        printLocationHierarchy(ipDic.getLocation(profile.getIpAddress()));
        

        arguments.add(Integer.toString(profile.getAccountId()));
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
		date.setTimeInMillis(profile.getCreatedDate());
		String dateString = DateGenerator.formatDateDetail(date);
		arguments.add(dateString);
        if (profile.getIpAddress() != null) {
            arguments.add(profile.getIpAddress().toString());
        } else {
            String empty = "";
            arguments.add(empty);
        }
        if (profile.getBrowserIdx() >= 0) {
            arguments.add(vBrowserNames.get(profile.getBrowserIdx()));
        } else {
            String empty = "";
            arguments.add(empty);
        }
		ToCSV(arguments, Files.PERSON.ordinal());

		Vector<Integer> languages = extraInfo.getLanguages();
		for (int i = 0; i < languages.size(); i++) {
		    arguments.add(Integer.toString(profile.getAccountId()));
		    arguments.add(languageDic.getLanguagesName(languages.get(i)));
		    ToCSV(arguments, Files.PERSON_SPEAKS_LANGUAGE.ordinal());
		}

		itString = extraInfo.getEmail().iterator();
		while (itString.hasNext()){
		    String email = itString.next();
		    arguments.add(Integer.toString(profile.getAccountId()));
		    arguments.add(email);
		    ToCSV(arguments, Files.PERSON_HAS_EMAIL_EMAIL.ordinal());
		}

		arguments.add(Integer.toString(profile.getAccountId()));
		arguments.add(Integer.toString(extraInfo.getLocationId()));
		ToCSV(arguments, Files.PERSON_LOCATED_IN_PLACE.ordinal());

		int organisationId = -1;
		if (!extraInfo.getOrganization().equals("")){
		    organisationId = organisations.indexOf(extraInfo.getOrganization());
		    if(organisationId == -1) {
		        organisationId = organisations.size();
		        organisations.add(extraInfo.getOrganization());

		        arguments.add(SN.formId(organisationId));
		        arguments.add(ScalableGenerator.OrganisationType.university.toString());
		        arguments.add(extraInfo.getOrganization());
		        arguments.add(DBP.getUrl(extraInfo.getOrganization()));
		        ToCSV(arguments, Files.ORGANISATION.ordinal());

		        arguments.add(SN.formId(organisationId));
		        arguments.add(Integer.toString(universityToCountry.get(extraInfo.getOrganization())));
		        ToCSV(arguments, Files.ORGANISATION_BASED_NEAR_PLACE.ordinal());
		    }
		}

		if (extraInfo.getClassYear() != -1 ) {
		    date.setTimeInMillis(extraInfo.getClassYear());
		    dateString = DateGenerator.formatYear(date);

		    arguments.add(Integer.toString(profile.getAccountId()));
		    arguments.add(SN.formId(organisationId));
		    arguments.add(dateString);
		    ToCSV(arguments, Files.PERSON_STUDY_AT_ORGANISATION.ordinal());
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
		        arguments.add(Integer.toString(companyToCountry.get(company)));
		        ToCSV(arguments, Files.ORGANISATION_BASED_NEAR_PLACE.ordinal());
		    }
		    date.setTimeInMillis(extraInfo.getWorkFrom(company));
		    dateString = DateGenerator.formatYear(date);

		    arguments.add(Integer.toString(profile.getAccountId()));
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
            
            arguments.add(Integer.toString(profile.getAccountId()));
            arguments.add(Integer.toString(interestIdx));
            ToCSV(arguments, Files.PERSON_INTEREST_TAG.ordinal());
        }   
        
        Friend friends[] = profile.getFriendList();         
        for (int i = 0; i < friends.length; i ++) {
            if (friends[i] != null && friends[i].getCreatedTime() != -1){

                arguments.add(Integer.toString(profile.getAccountId()));
                arguments.add(Integer.toString(friends[i].getFriendAcc()));
                ToCSV(arguments,Files.PERSON_KNOWS_PERSON.ordinal());
            }
        }
		
        //The forums of the user
		date.setTimeInMillis(profile.getCreatedDate());
        dateString = DateGenerator.formatDateDetail(date);

        String title = "Wall of " + extraInfo.getFirstName() + " " + extraInfo.getLastName();
        arguments.add(SN.formId(profile.getForumWallId()));
        arguments.add(title);
        arguments.add(dateString);
        ToCSV(arguments,Files.FORUM.ordinal());
        
        arguments.add(SN.formId(profile.getForumWallId()));
        arguments.add(Integer.toString(profile.getAccountId()));
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
                arguments.add(Integer.toString(friends[i].getFriendAcc()));
                arguments.add(dateString);
                ToCSV(arguments,Files.FORUM_HASMEMBER_PERSON.ordinal());
            }
        }
	}

	public void gatherData(Post post){
	    Vector<String> arguments = new Vector<String>();
	    String empty = "";
	    
	    arguments.add(SN.formId(post.getPostId()));
	    arguments.add(empty);
	    date.setTimeInMillis(post.getCreatedDate());
	    String dateString = DateGenerator.formatDateDetail(date);
	    arguments.add(dateString);
	    if (post.getIpAddress() != null) {
	        arguments.add(post.getIpAddress().toString());
	    } else {
	        arguments.add(empty);
	    }
	    if (post.getBrowserIdx() != -1){
	        arguments.add(vBrowserNames.get(post.getBrowserIdx()));
	    } else {
	        arguments.add(empty);
	    }
	    if (post.getLanguage() != -1) {
            arguments.add(languageDic.getLanguagesName(post.getLanguage()));
        } else {
            arguments.add(empty);
        }
	    arguments.add(post.getContent());
	    ToCSV(arguments, Files.POST.ordinal());

	    if (post.getIpAddress() != null) {
	        arguments.add(SN.formId(post.getPostId()));
	        arguments.add(Integer.toString(ipDic.getLocation(post.getIpAddress())));
	        ToCSV(arguments, Files.POST_LOCATED_PLACE.ordinal());
	    }
	    arguments.add(SN.formId(post.getForumId()));
	    arguments.add(SN.formId(post.getPostId()));
	    ToCSV(arguments, Files.FORUM_CONTAINER_OF_POST.ordinal());

	    arguments.add(SN.formId(post.getPostId()));
	    arguments.add(Integer.toString(post.getAuthorId()));
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

	    int userLikes[] = post.getInterestedUserAccs();
	    long likeTimestamps[] = post.getInterestedUserAccsTimestamp();
	    for (int i = 0; i < userLikes.length; i ++) {
	        date.setTimeInMillis(likeTimestamps[i]);
	        dateString = DateGenerator.formatDateDetail(date);
	        arguments.add(Integer.toString(userLikes[i]));
	        arguments.add(SN.formId(post.getPostId()));
	        arguments.add(dateString);
	        ToCSV(arguments, Files.PERSON_LIKE_POST.ordinal());
	    }
	}

	public void gatherData(Comment comment){
	    Vector<String> arguments = new Vector<String>();
	    
	    date.setTimeInMillis(comment.getCreateDate());
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
            arguments.add(vBrowserNames.get(comment.getBrowserIdx()));
        } else {
            String empty = "";
            arguments.add(empty);
        }
	    arguments.add(comment.getContent());
	    ToCSV(arguments, Files.COMMENT.ordinal());
	    
	    if (comment.getReply_of() == -1) {
            arguments.add(SN.formId(comment.getCommentId()));
            arguments.add(SN.formId(comment.getPostId()));
            ToCSV(arguments, Files.COMMENT_REPLY_OF_POST.ordinal());
        } else {
            arguments.add(SN.formId(comment.getCommentId()));
            arguments.add(SN.formId(comment.getReply_of()));
            ToCSV(arguments, Files.COMMENT_REPLY_OF_COMMENT.ordinal());
        }
	    if (comment.getIpAddress() != null) {
            arguments.add(SN.formId(comment.getCommentId()));
            arguments.add(Integer.toString(ipDic.getLocation(comment.getIpAddress())));
            ToCSV(arguments, Files.COMMENT_LOCATED_PLACE.ordinal());
        }
	    
	    arguments.add(SN.formId(comment.getCommentId()));
	    arguments.add(Integer.toString(comment.getAuthorId()));
	    ToCSV(arguments, Files.COMMENT_HAS_CREATOR_PERSON.ordinal());
	}

	public void gatherData(Photo photo){
	    Vector<String> arguments = new Vector<String>();

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
	        arguments.add(vBrowserNames.get(photo.getBrowserIdx()));
	    } else {
	        arguments.add(empty);
	    }
	    arguments.add(empty);
	    arguments.add(empty);
	    ToCSV(arguments, Files.POST.ordinal());

	    if (photo.getIpAddress() != null) {
	        arguments.add(SN.formId(photo.getPhotoId()));
	        arguments.add(Integer.toString(ipDic.getLocation(photo.getIpAddress())));
	        ToCSV(arguments, Files.POST_LOCATED_PLACE.ordinal());
	    }

	    arguments.add(SN.formId(photo.getPhotoId()));
	    arguments.add(Integer.toString(photo.getCreatorId()));
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

	    int userLikes[] = photo.getInterestedUserAccs();
	    long likeTimestamps[] = photo.getInterestedUserAccsTimestamp();
	    for (int i = 0; i < userLikes.length; i ++) {
	        date.setTimeInMillis(likeTimestamps[i]);
	        dateString = DateGenerator.formatDateDetail(date);
	        arguments.add(Integer.toString(userLikes[i]));
	        arguments.add(SN.formId(photo.getPhotoId()));
	        arguments.add(dateString);
	        ToCSV(arguments, Files.PERSON_LIKE_POST.ordinal());
	    }
	}
	
	public void gatherData(Group group) {
	    Vector<String> arguments = new Vector<String>();
	    
	    date.setTimeInMillis(group.getCreatedDate());
        String dateString = DateGenerator.formatDateDetail(date);  
        
	    arguments.add(SN.formId(group.getForumWallId()));
	    arguments.add(group.getGroupName());
	    arguments.add(dateString);
	    ToCSV(arguments,Files.FORUM.ordinal());
	    
	    arguments.add(SN.formId(group.getForumWallId()));
	    arguments.add(Integer.toString(group.getModeratorId()));
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
            
            arguments.add(SN.formId(group.getForumWallId()));
            arguments.add(Integer.toString(groupTags[i]));
            ToCSV(arguments,Files.FORUM_HASTAG_TAG.ordinal());
        }
	    
	    GroupMemberShip memberShips[] = group.getMemberShips();
        int numMemberAdded = group.getNumMemberAdded();
        for (int i = 0; i < numMemberAdded; i ++) {
            date.setTimeInMillis(memberShips[i].getJoinDate());
            dateString = DateGenerator.formatDateDetail(date);
            
            arguments.add(SN.formId(group.getForumWallId()));
            arguments.add(Integer.toString(memberShips[i].getUserId()));
            arguments.add(dateString);
            ToCSV(arguments,Files.FORUM_HASMEMBER_PERSON.ordinal());
        }
	}

	public void serialize() {
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
