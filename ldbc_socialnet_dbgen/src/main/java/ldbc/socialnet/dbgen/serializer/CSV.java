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
import ldbc.socialnet.dbgen.generator.DateGenerator;
import ldbc.socialnet.dbgen.objects.Comment;
import ldbc.socialnet.dbgen.objects.Friend;
import ldbc.socialnet.dbgen.objects.FriendShip;
import ldbc.socialnet.dbgen.objects.GPS;
import ldbc.socialnet.dbgen.objects.Group;
import ldbc.socialnet.dbgen.objects.GroupMemberShip;
import ldbc.socialnet.dbgen.objects.Location;
import ldbc.socialnet.dbgen.objects.Photo;
import ldbc.socialnet.dbgen.objects.PhotoAlbum;
import ldbc.socialnet.dbgen.objects.Post;
import ldbc.socialnet.dbgen.objects.ReducedUserProfile;
import ldbc.socialnet.dbgen.objects.SocialObject;
import ldbc.socialnet.dbgen.objects.UserExtraInfo;
import ldbc.socialnet.dbgen.objects.UserProfile;
import ldbc.socialnet.dbgen.vocabulary.RDF;
import ldbc.socialnet.dbgen.vocabulary.SN;
import ldbc.socialnet.dbgen.vocabulary.SNVOC;


public class CSV implements Serializer {
	
	final String NEWLINE = "\n";
	final String SEPARATOR = "|";
    final String TYPE_PERSON = "Person";
    final String TYPE_ORGANIZATION = "Organization";
    final String TYPE_LOCATION = "Location";
    final String TYPE_FORUM = "Forum";
    final String TYPE_POST = "Post";
    final String TYPE_TAG = "Tag";
    final String TYPE_COMMENT = "Comment";
    final String TYPE_GROUP = "Group";
    final String TYPE_EMAIL = "EmailAddress";
    final String TYPE_IPADDRESS = "IPAddress";
    
    final String[] fileNames = {
    		                "tag",
    		                "post",
    		                "group",
    		                "forum",
    		                "person",
    		                "comment",
    		                "location",
    		                "emailaddress",
                            "ipaddress",
    		                "organization",
    		                "language",
    		                "person_like_post",
    		                "person_interest_tag",
    		                "person_knows_person",
    		                "person_speaks_language",
    		                "post_connection_ipaddress",
    		                "comment_connection_ipaddress",
    		                "group_has_tag_tag",
    		                "post_has_tag_tag",
    		                "post_annotated_language",
    		                "person_work_at_organization",
    		                "person_study_at_organization",
    		                "location_part_of_location",
    		                "comment_reply_of_post",
    		                "comment_reply_of_comment",
    		                "person_based_near_location",
    		                "organization_based_near_location",
    		                "ipaddress_located_in_location",
    		                "person_has_email_emailaddress",
    		                "person_connection_ipaddress",
    		                "person_membership_group",
    		                "person_creator_of_group",
    		                "person_creator_of_post",
    		                "person_creator_of_comment",
    		                "person_moderator_of_forum",
    		                "forum_container_of_post",
    		                };
    
    enum Files
    {
    	TAG,
    	POST,
    	GROUP,
    	FORUM,
    	PERSON,
    	COMMENT,
    	LOCATION,
    	EMAIL,
    	IPADDRESS,
    	ORGANIZATION,
    	LANGUAGE,
    	PERSON_LIKE_POST,
    	PERSON_INTEREST_TAG,
    	PERSON_KNOWS_PERSON,
    	PERSON_SPEAKS_LANGUAGE,
    	POST_CONNECTION_IPADDRESS,
    	COMMENT_CONNECTION_IPADDRESS,
    	GROUP_HAS_TAG_TAG,
    	POST_HAS_TAG_TAG,
    	POST_ANNOTATED_LANGUAGE,
    	PERSON_WORK_AT_ORGANIZATION,
    	PERSON_STUDY_AT_ORGANIZATION,
    	LOCATION_PART_OF_LOCATION,
    	COMMENT_REPLY_OF_POST,
    	COMMENT_REPLY_OF_COMMENT,
    	PERSON_BASED_NEAR_LOCATION,
    	ORGANIZATION_BASED_NEAR_LOCATION,
    	IPADDRESS_LOCATED_IN_LOCATION,
    	PERSON_HAS_EMAIL_EMAIL,
    	PERSON_CONNECTION_IPADDRESS,
    	PERSON_MEMBERSHIP_GROUP,
    	PERSON_CREATOR_OF_GROUP,
    	PERSON_CREATOR_OF_POST,
    	PERSON_CREATOR_OF_COMMENT,
    	PERSON_MODERATOR_OF_FORUM,
    	FORUM_CONTAINER_OF_POST,
    	NUM_FILES
    }
	
	private long nrTriples;
	private FileWriter[][] dataFileWriter;
	private boolean forwardChaining;
	private boolean haveToGeneratePrefixes = true;
	int[] currentWriter;
	static long membershipId = 0;
	static long friendshipId = 0; 
	static long gpsId = 0; 
	static long emailId = 0;
	static long ipId = 0;
	
	HashMap<Integer, String> interestIdsNames;
	HashMap<String, Integer> companyToCountry;
	Vector<String>	vBrowserNames;
	Vector<Integer> locations;
	Vector<Integer> serializedLanguages;
	Vector<String> organizations;
	Vector<String> interests;
	Vector<String> tagList;
	Vector<String> ipList;
	
	GregorianCalendar date;
	LocationDictionary locationDic;
	LanguageDictionary languageDic;
	IPAddressDictionary ipDic;
	
	public CSV(String file, boolean forwardChaining)
	{
		this(file, forwardChaining, 1);
	}
	
	public CSV(String file, boolean forwardChaining, int nrOfOutputFiles)
	{
		vBrowserNames = new Vector<String>();
		locations = new Vector<Integer>();
		organizations = new Vector<String>();
		interests = new Vector<String>();
		tagList = new Vector<String>();
		ipList = new Vector<String>();
		serializedLanguages = new Vector<Integer>();
		
		currentWriter = new int[Files.NUM_FILES.ordinal()];
		for (int i = 0; i < Files.NUM_FILES.ordinal(); i++)
		{
			currentWriter[i] = 0;
		}
		date = new GregorianCalendar();
		int nrOfDigits = ((int)Math.log10(nrOfOutputFiles)) + 1;
		String formatString = "%0" + nrOfDigits + "d";
		try{
			dataFileWriter = new FileWriter[nrOfOutputFiles][Files.NUM_FILES.ordinal()];
			if(nrOfOutputFiles==1)
				for (int i = 0; i < Files.NUM_FILES.ordinal(); i++)
				{
					this.dataFileWriter[0][i] = new FileWriter(file + fileNames[i] + ".csv");
				}
			else
				for(int i=0;i<nrOfOutputFiles;i++)
				{
					for (int j = 0; j < Files.NUM_FILES.ordinal(); j++)
					{
						dataFileWriter[i][j] = new FileWriter(file + fileNames[j] + String.format(formatString, i+1) + ".csv");
					}
				}
				
		} catch(IOException e){
			System.err.println("Could not open File for writing.");
			System.err.println(e.getMessage());
			System.exit(-1);
		}
		
		this.forwardChaining = forwardChaining;
		nrTriples = 0l;
		
		CSVShutdown sd = new CSVShutdown(this);
		Runtime.getRuntime().addShutdownHook(sd);
	}
	
	public CSV(String file, boolean forwardChaining, int nrOfOutputFiles, 
            HashMap<Integer, String> _interestIdsNames, Vector<String> _vBrowsers, 
            HashMap<String, Integer> companyToCountry, IPAddressDictionary ipDic,
            LocationDictionary locationDic, LanguageDictionary languageDic)
    {
	    this(file, forwardChaining, nrOfOutputFiles);
        this.interestIdsNames = _interestIdsNames;  
        this.vBrowserNames = _vBrowsers;
        this.locationDic = locationDic;
        this.languageDic = languageDic;
        this.companyToCountry = companyToCountry;
        this.ipDic = ipDic;
    }
	
	@Override
	public Long triplesGenerated() {
		return nrTriples;
	}

	@Override
	public void gatherData(SocialObject socialObject){
		if(haveToGeneratePrefixes) {
			haveToGeneratePrefixes = false;
		}

		if(socialObject instanceof UserProfile){
			UserContainer container = new UserContainer((UserProfile)socialObject);
			convertUserProfile(container, null);
		}
		else if(socialObject instanceof FriendShip){
			convertFriendShip((FriendShip)socialObject);
		}
		else if(socialObject instanceof Post){
			convertPost((Post)socialObject, true, true);
		}
		else if(socialObject instanceof Comment){
			convertComment((Comment)socialObject);
		}
		else if (socialObject instanceof PhotoAlbum){
			convertPhotoAlbum((PhotoAlbum)socialObject);
		}
		else if (socialObject instanceof Photo){
			convertPhoto((Photo)socialObject, true, true);
		}
		else if (socialObject instanceof Group){
			convertGroup((Group)socialObject);
		}
		else if (socialObject instanceof GPS){
			convertGPS((GPS)socialObject);
		}
	} 

	@Override
	public void gatherData(ReducedUserProfile userProfile, UserExtraInfo extraInfo){
		UserContainer container = new UserContainer(userProfile);
		convertUserProfile(container, extraInfo);
	}
	public void gatherData(Post post, boolean isLikeStream){
		convertPost(post, !isLikeStream, isLikeStream);
	}

	public void gatherData(Photo photo, boolean isLikeStream){
		convertPhoto(photo, !isLikeStream, isLikeStream);
	}	
	
	public void ToCSV(Vector<String> arguments, int index)
	{
		StringBuffer result = new StringBuffer();
		result.append(arguments.get(0));
		for (int i = 1; i < arguments.size(); i++)
		{
			result.append(SEPARATOR);
			result.append(arguments.get(i));
		}
		result.append(NEWLINE);
		WriteTo(result.toString(), index);
		arguments.clear();
	}

	public void WriteTo(String data, int index)
	{
		try
		{
			dataFileWriter[currentWriter[index]][index].append(data);
			currentWriter[index] = (currentWriter[index] + 1) % dataFileWriter.length;
		}
		catch (IOException e)
		{
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
                arguments.add(locationDic.getType(areas.get(i)));
                ToCSV(arguments, Files.LOCATION.ordinal());
                if (locationDic.getType(areas.get(i)) == Location.CITY ||
                        locationDic.getType(areas.get(i)) == Location.COUNTRY) {
                    arguments.add(Integer.toString(areas.get(i)));
                    arguments.add(Integer.toString(areas.get(i+1)));
                    ToCSV(arguments, Files.LOCATION_PART_OF_LOCATION.ordinal());
                }
            }
        }
    }
	
	//public String convertUserProfile(ReducedUserProfile profile, UserExtraInfo extraInfo){
	public void convertUserProfile(UserContainer profile, UserExtraInfo extraInfo){
		Vector<String> arguments = new Vector<String>();
		
		if (extraInfo != null) {
            
            if (locations.indexOf(extraInfo.getLocationId()) == -1) {
                int parentId = extraInfo.getLocationId();
                printLocationHierarchy(parentId);
            }
            
            Iterator<String> it = extraInfo.getCompanies().iterator();
            while (it.hasNext()) {
                String company = it.next();
                int parentId = companyToCountry.get(company);
                printLocationHierarchy(parentId);
            }
        }
        printLocationHierarchy(ipDic.getLocation(profile.getIpAddress()));
        
		if (extraInfo != null)
		{
			// a person is created
			arguments.add(Integer.toString(profile.getAccountId()));
			arguments.add(extraInfo.getFirstName());
			arguments.add(extraInfo.getLastName());
			arguments.add(extraInfo.getGender());
			if (profile.getBirthDay() != -1 )
			{
				date.setTimeInMillis(profile.getBirthDay());
				String dateString = DateGenerator.formatDate(date);
				arguments.add(dateString);
			}
			else
			{
				String empty = "";
				arguments.add(empty);
			}
		}
		date.setTimeInMillis(profile.getCreatedDate());
		String dateString = DateGenerator.formatDateDetail(date);
		arguments.add(dateString);
		ToCSV(arguments, Files.PERSON.ordinal());
		
		if (extraInfo != null)
        {
		    Vector<Integer> languages = extraInfo.getLanguages();
		    for (int i = 0; i < languages.size(); i++) {
		        if (serializedLanguages.indexOf(languages.get(i)) == -1) {
		            serializedLanguages.add(languages.get(i));
		            arguments.add(Integer.toString(languages.get(i)));
		            arguments.add(languageDic.getLanguagesName(languages.get(i)));
		            ToCSV(arguments, Files.LANGUAGE.ordinal());
		        }
		        
		        arguments.add(Integer.toString(profile.getAccountId()));
		        arguments.add(Integer.toString(languages.get(i)));
                ToCSV(arguments, Files.PERSON_SPEAKS_LANGUAGE.ordinal());
		    }
		    
		    Iterator<String> it = extraInfo.getEmail().iterator();
		    while (it.hasNext()){
		        String email = it.next();
		        arguments.add(Long.toString(emailId));
		        arguments.add(email);
		        ToCSV(arguments, Files.EMAIL.ordinal());
	            arguments.add(Integer.toString(profile.getAccountId()));
	            arguments.add(Long.toString(emailId));
	            ToCSV(arguments, Files.PERSON_HAS_EMAIL_EMAIL.ordinal());
	            emailId++;
		    }
		    
		    int ipId = ipList.indexOf(profile.getIpAddress().toString());
		    if (ipId == -1) {
		        ipId = ipList.size();
		        ipList.add(profile.getIpAddress().toString());
		        arguments.add(Integer.toString(ipId));
		        arguments.add(profile.getIpAddress().toString());
		        ToCSV(arguments, Files.IPADDRESS.ordinal());
		    }
		    
            arguments.add(Integer.toString(profile.getAccountId()));
            arguments.add(Integer.toString(ipId));
            arguments.add(vBrowserNames.get(profile.getBrowserIdx()));
            ToCSV(arguments, Files.PERSON_CONNECTION_IPADDRESS.ordinal());
            
            arguments.add(Integer.toString(ipId));
            arguments.add(Integer.toString(ipDic.getLocation(profile.getIpAddress())));
            ToCSV(arguments, Files.IPADDRESS_LOCATED_IN_LOCATION.ordinal());

			//based_near relationship
			arguments.add(Integer.toString(profile.getAccountId()));
			arguments.add(Integer.toString(extraInfo.getLocationId()));
			ToCSV(arguments, Files.PERSON_BASED_NEAR_LOCATION.ordinal());
			
			//dc:organization
			int organizationId = -1;
			if (!extraInfo.getOrganization().equals("")){
				organizationId = organizations.indexOf(extraInfo.getOrganization());
				if(organizationId == -1)
				{
					organizationId = organizations.size();
					organizations.add(extraInfo.getOrganization());
					
					arguments.add(Integer.toString(organizationId));
					arguments.add(extraInfo.getOrganization());
					ToCSV(arguments, Files.ORGANIZATION.ordinal());
				}
			}

			//sib:class_year
			if (extraInfo.getClassYear() != -1 ){
				date.setTimeInMillis(extraInfo.getClassYear());
				dateString = DateGenerator.formatYear(date);
				
				arguments.add(Integer.toString(profile.getAccountId()));
				arguments.add(Integer.toString(organizationId));
				arguments.add(dateString);
				ToCSV(arguments, Files.PERSON_STUDY_AT_ORGANIZATION.ordinal());
			}

			//sib:workAt
			it = extraInfo.getCompanies().iterator();
			while (it.hasNext()) {
				String company = it.next();
				organizationId = organizations.indexOf(company);
				if(organizationId == -1)
				{
					organizationId = organizations.size();
					organizations.add(company);
					
					arguments.add(Integer.toString(organizationId));
					arguments.add(company);
					ToCSV(arguments, Files.ORGANIZATION.ordinal());
				}
				date.setTimeInMillis(extraInfo.getWorkFrom(company));
				dateString = DateGenerator.formatYear(date);
				
                arguments.add(Integer.toString(organizationId));
                arguments.add(Integer.toString(companyToCountry.get(company)));
                ToCSV(arguments, Files.ORGANIZATION_BASED_NEAR_LOCATION.ordinal());
				
				arguments.add(Integer.toString(profile.getAccountId()));
				arguments.add(Integer.toString(organizationId));
				arguments.add(dateString);
				ToCSV(arguments, Files.PERSON_WORK_AT_ORGANIZATION.ordinal());
			}
		}
		
        //The forums of the user
		date.setTimeInMillis(profile.getCreatedDate());
        dateString = DateGenerator.formatDateDetail(date);
        arguments.add(Integer.toString(profile.getForumWallId()));
        if (extraInfo != null) {
            arguments.add(extraInfo.getFirstName() + " " + extraInfo.getLastName());
        } else {
            arguments.add("" + profile.getAccountId());
        }
        arguments.add(dateString);
        ToCSV(arguments,Files.FORUM.ordinal());
        
        arguments.add(Integer.toString(profile.getAccountId()));
        arguments.add(Integer.toString(profile.getForumWallId()));
        ToCSV(arguments,Files.PERSON_MODERATOR_OF_FORUM.ordinal());
		
		// For the interests
		Iterator<Integer> it = profile.getSetOfInterests().iterator();
		while (it.hasNext()){
			Integer interestIdx = it.next();
			String interest = interestIdsNames.get(interestIdx);
			
			if (interests.indexOf(interest) == -1)
			{
				interests.add(interest);
				
				arguments.add(Integer.toString(interestIdx));
				arguments.add(interest);
				ToCSV(arguments, Files.TAG.ordinal());
			}
			
			arguments.add(Integer.toString(profile.getAccountId()));
			arguments.add(Integer.toString(interestIdx));
			ToCSV(arguments, Files.PERSON_INTEREST_TAG.ordinal());
		}	
		
		//For the friendships
		Friend friends[] = profile.getFriendList();			
		for (int i = 0; i < friends.length; i ++){
			if (friends[i] != null){

				//foaf:knows
				if (extraInfo == null || friends[i].getCreatedTime() != -1){
					
					arguments.add(Integer.toString(profile.getAccountId()));
					arguments.add(Integer.toString(friends[i].getFriendAcc()));
					ToCSV(arguments,Files.PERSON_KNOWS_PERSON.ordinal());
				}
			}
		}
	}

	public void convertFriendShip(FriendShip friendShip){
		Vector<String> arguments = new Vector<String>();
		
		arguments.add(Integer.toString(friendShip.getUserAcc01()));
		arguments.add(Integer.toString(friendShip.getUserAcc02()));
		ToCSV(arguments,Files.PERSON_KNOWS_PERSON.ordinal());
		
		arguments.add(Integer.toString(friendShip.getUserAcc02()));
		arguments.add(Integer.toString(friendShip.getUserAcc01()));
		ToCSV(arguments,Files.PERSON_KNOWS_PERSON.ordinal());
	}

	public void convertPost(Post post, boolean body, boolean isLiked){
		Vector<String> arguments = new Vector<String>();
		if (body) {
		    arguments.add(Long.toString(post.getPostId()));
            if (post.getTitle() != null) {
                arguments.add(post.getTitle());
            } else {
                String empty = "";
                arguments.add(empty);
            }
            date.setTimeInMillis(post.getCreatedDate());
            String dateString = DateGenerator.formatDateDetail(date);
            arguments.add(dateString);
            arguments.add(post.getContent());
            ToCSV(arguments, Files.POST.ordinal());
            
            if (serializedLanguages.indexOf(post.getLanguage()) == -1) {
                serializedLanguages.add(post.getLanguage());
                arguments.add(Integer.toString(post.getLanguage()));
                arguments.add(languageDic.getLanguagesName(post.getLanguage()));
                ToCSV(arguments, Files.LANGUAGE.ordinal());
            }
            
            arguments.add(Long.toString(post.getPostId()));
            arguments.add(Integer.toString(post.getLanguage()));
            ToCSV(arguments, Files.POST_ANNOTATED_LANGUAGE.ordinal());
            
            //sioc:ip_address
            if (post.getIpAddress() != null) {
                int ipId = ipList.indexOf(post.getIpAddress().toString());
                if (ipId == -1) {
                    ipId = ipList.size();
                    ipList.add(post.getIpAddress().toString());
                    arguments.add(Integer.toString(ipId));
                    arguments.add(post.getIpAddress().toString());
                    ToCSV(arguments, Files.IPADDRESS.ordinal());
                    
                    arguments.add(Integer.toString(ipId));
                    arguments.add(Integer.toString(ipDic.getLocation(post.getIpAddress())));
                    ToCSV(arguments, Files.IPADDRESS_LOCATED_IN_LOCATION.ordinal());
                }
                arguments.add(Long.toString(post.getPostId()));
                arguments.add(Integer.toString(ipId));
                if (post.getBrowserIdx() != -1){
                    arguments.add(vBrowserNames.get(post.getBrowserIdx()));
                } else {
                    String empty = "";
                    arguments.add(empty);
                }
                ToCSV(arguments, Files.POST_CONNECTION_IPADDRESS.ordinal());
            }

            arguments.add(Integer.toString(post.getForumId()));
            arguments.add(Long.toString(post.getPostId()));
            ToCSV(arguments, Files.FORUM_CONTAINER_OF_POST.ordinal());
            
            arguments.add(Integer.toString(post.getAuthorId()));
            arguments.add(Long.toString(post.getPostId()));
            ToCSV(arguments, Files.PERSON_CREATOR_OF_POST.ordinal());

            Iterator<Integer> it = post.getTags().iterator();
            while (it.hasNext()) {
                Integer tagId = it.next();
                String tag = interestIdsNames.get(tagId);
                if (interests.indexOf(tag) == -1)
                {
                    interests.add(tag);
                    
                    arguments.add(Integer.toString(tagId));
                    arguments.add(tag);
                    ToCSV(arguments, Files.TAG.ordinal());
                }
                
                arguments.add(Long.toString(post.getPostId()));
                arguments.add(Integer.toString(tagId));
                ToCSV(arguments, Files.POST_HAS_TAG_TAG.ordinal());
            }
        }

        if (isLiked) {
            int userLikes[] = post.getInterestedUserAccs();
            long likeTimestamps[] = post.getInterestedUserAccsTimestamp();
            for (int i = 0; i < userLikes.length; i ++) {
                date.setTimeInMillis(likeTimestamps[i]);
                String dateString = DateGenerator.formatDateDetail(date);
                arguments.add(Integer.toString(userLikes[i]));
                arguments.add(Long.toString(post.getPostId()));
                arguments.add(dateString);
                ToCSV(arguments, Files.PERSON_LIKE_POST.ordinal());
            }
        }
	}

	public void convertComment(Comment comment){
	    Vector<String> arguments = new Vector<String>();
	    
	    date.setTimeInMillis(comment.getCreateDate());
        String dateString = DateGenerator.formatDateDetail(date); 
	    arguments.add(Long.toString(comment.getCommentId()));
	    arguments.add(dateString);
	    arguments.add(comment.getContent());
	    ToCSV(arguments, Files.COMMENT.ordinal());
	    
	    if (comment.getReply_of() == -1){
            arguments.add(Long.toString(comment.getCommentId()));
            arguments.add(Long.toString(comment.getPostId()));
            ToCSV(arguments, Files.COMMENT_REPLY_OF_POST.ordinal());
        }
        else {
            arguments.add(Long.toString(comment.getCommentId()));
            arguments.add(Long.toString(comment.getReply_of()));
            ToCSV(arguments, Files.COMMENT_REPLY_OF_COMMENT.ordinal());
        }
	    
	    if (comment.getIpAddress() != null) {
	        int ipId = ipList.indexOf(comment.getIpAddress().toString());
            if (ipId == -1) {
                ipId = ipList.size();
                ipList.add(comment.getIpAddress().toString());
                arguments.add(Integer.toString(ipId));
                arguments.add(comment.getIpAddress().toString());
                ToCSV(arguments, Files.IPADDRESS.ordinal());
            }
            
            arguments.add(Long.toString(comment.getCommentId()));
            arguments.add(Integer.toString(ipId));
            if (comment.getBrowserIdx() != -1){
                arguments.add(vBrowserNames.get(comment.getBrowserIdx()));
            } else {
                String empty = "";
                arguments.add(empty);
            }
            ToCSV(arguments, Files.COMMENT_CONNECTION_IPADDRESS.ordinal());
	    }
	    
	    arguments.add(Integer.toString(comment.getAuthorId()));
	    arguments.add(Long.toString(comment.getCommentId()));
	    ToCSV(arguments, Files.PERSON_CREATOR_OF_COMMENT.ordinal());
	}

	public void convertPhotoAlbum(PhotoAlbum album){
		Vector<String> arguments = new Vector<String>();
		
		
		date.setTimeInMillis(album.getCreatedDate());
        String dateString = DateGenerator.formatDateDetail(date);
		arguments.add(Long.toString(album.getAlbumId()));
		arguments.add(album.getTitle());
		arguments.add(dateString);
		ToCSV(arguments, Files.FORUM.ordinal());
		
		arguments.add(Integer.toString(album.getCreatorId()));
		arguments.add(Long.toString(album.getAlbumId()));
		ToCSV(arguments, Files.PERSON_MODERATOR_OF_FORUM.ordinal());
	}

	public void convertPhoto(Photo photo, boolean body, boolean isLiked){
		Vector<String> arguments = new Vector<String>();
        if (body) {
            String empty = "";
            arguments.add(Long.toString(photo.getPhotoId()));
            arguments.add(photo.getImage());
            arguments.add(empty);
            date.setTimeInMillis(photo.getTakenTime());
            String dateString = DateGenerator.formatDateDetail(date);
            arguments.add(dateString);
            arguments.add(empty);
            ToCSV(arguments, Files.POST.ordinal());
            
            //sioc:ip_address
            if (photo.getIpAddress() != null) {
                int ipId = ipList.indexOf(photo.getIpAddress().toString());
                if (ipId == -1) {
                    ipId = ipList.size();
                    ipList.add(photo.getIpAddress().toString());
                    arguments.add(Integer.toString(ipId));
                    arguments.add(photo.getIpAddress().toString());
                    ToCSV(arguments, Files.IPADDRESS.ordinal());
                    
                    arguments.add(Integer.toString(ipId));
                    arguments.add(Integer.toString(ipDic.getLocation(photo.getIpAddress())));
                    ToCSV(arguments, Files.IPADDRESS_LOCATED_IN_LOCATION.ordinal());
                }
                arguments.add(Long.toString(photo.getPhotoId()));
                arguments.add(Integer.toString(ipId));
                if (photo.getBrowserIdx() != -1){
                    arguments.add(vBrowserNames.get(photo.getBrowserIdx()));
                } else {
                    arguments.add(empty);
                }
                ToCSV(arguments, Files.POST_CONNECTION_IPADDRESS.ordinal());
            }
            
            arguments.add(Integer.toString(photo.getCreatorId()));
            arguments.add(Long.toString(photo.getPhotoId()));
            ToCSV(arguments, Files.PERSON_CREATOR_OF_POST.ordinal());
            
            arguments.add(Long.toString(photo.getAlbumId()));
            arguments.add(Long.toString(photo.getPhotoId()));
            ToCSV(arguments, Files.FORUM_CONTAINER_OF_POST.ordinal());

            Iterator<Integer> it = photo.getTags().iterator();
            while (it.hasNext()) {
                Integer tagId = it.next();
                String tag = interestIdsNames.get(tagId);
                if (interests.indexOf(tag) == -1)
                {
                    interests.add(tag);
                    
                    arguments.add(Integer.toString(tagId));
                    arguments.add(tag);
                    ToCSV(arguments, Files.TAG.ordinal());
                }
                
                arguments.add(Long.toString(photo.getPhotoId()));
                arguments.add(Integer.toString(tagId));
                ToCSV(arguments, Files.POST_HAS_TAG_TAG.ordinal());
            }
        }

        if (isLiked) {
            int userLikes[] = photo.getInterestedUserAccs();
            long likeTimestamps[] = photo.getInterestedUserAccsTimestamp();
            for (int i = 0; i < userLikes.length; i ++) {
                date.setTimeInMillis(likeTimestamps[i]);
                String dateString = DateGenerator.formatDateDetail(date);
                arguments.add(Integer.toString(userLikes[i]));
                arguments.add(Long.toString(photo.getPhotoId()));
                arguments.add(dateString);
                ToCSV(arguments, Files.PERSON_LIKE_POST.ordinal());
            }
        }
	}	

	public void convertGPS(GPS gps){
		Vector<String> arguments = new Vector<String>();
	}

	public void convertGroup(Group group){
	    Vector<String> arguments = new Vector<String>();
	    
	    date.setTimeInMillis(group.getCreatedDate());
        String dateString = DateGenerator.formatDateDetail(date);  
        
	    arguments.add(Integer.toString(group.getGroupId()));
	    arguments.add(group.getGroupName());
	    arguments.add(dateString);
	    ToCSV(arguments,Files.GROUP.ordinal());
	    
        arguments.add(Integer.toString(group.getModeratorId()));
	    arguments.add(Integer.toString(group.getGroupId()));
	    ToCSV(arguments,Files.PERSON_CREATOR_OF_GROUP.ordinal());
	    
	    Integer groupTags[] = group.getTags();
        for (int i = 0; i < groupTags.length; i ++){
            String interest = interestIdsNames.get(groupTags[i]);
            
            if (interests.indexOf(interest) == -1)
            {
                interests.add(interest);
                
                arguments.add(Integer.toString(groupTags[i]));
                arguments.add(interest);
                ToCSV(arguments, Files.TAG.ordinal());
            }
            
            arguments.add(Integer.toString(group.getGroupId()));
            arguments.add(Integer.toString(groupTags[i]));
            ToCSV(arguments,Files.GROUP_HAS_TAG_TAG.ordinal());
        }
	    
	    GroupMemberShip memberShips[] = group.getMemberShips();
        int numMemberAdded = group.getNumMemberAdded();
        for (int i = 0; i < numMemberAdded; i ++){
            date.setTimeInMillis(memberShips[i].getJoinDate());
            dateString = DateGenerator.formatDateDetail(date);
            
            arguments.add(Integer.toString(memberShips[i].getUserId()));
            arguments.add(Integer.toString(group.getGroupId()));
            arguments.add(dateString);
            ToCSV(arguments,Files.PERSON_MEMBERSHIP_GROUP.ordinal());
        }
        
        //The forums of the group
        arguments.add(Integer.toString(group.getForumWallId()));
        arguments.add(group.getGroupName());
        arguments.add(dateString);
        ToCSV(arguments,Files.FORUM.ordinal());

        arguments.add(Integer.toString(group.getModeratorId()));
        arguments.add(Integer.toString(group.getForumWallId()));
        ToCSV(arguments,Files.PERSON_MODERATOR_OF_FORUM.ordinal());
	}

	@Override
	public void serialize() {
		//Close files
		try {
			for (int i = 0; i < dataFileWriter.length; i++)
			{
				for (int j = 0; j < Files.NUM_FILES.ordinal(); j++)
				{
					dataFileWriter[i][j].flush();
					dataFileWriter[i][j].close();
				}
			}
		} catch(IOException e) {
			System.err.println(e.getMessage());
			System.exit(-1);
		}
	}


	class CSVShutdown extends Thread {
		CSV serializer;
		CSVShutdown(CSV t) {
			serializer = t;
		}

		@Override
		public void run() {

			for (int i = 0; i < dataFileWriter.length; i++)
			{
				for (int j = 0; j < Files.NUM_FILES.ordinal(); j++)
				{
					try {
						serializer.dataFileWriter[i][j].flush();
						serializer.dataFileWriter[i][j].close();
					} catch(IOException e) {
						// Do nothing
					}
				}
			}
		}
	}	
}
