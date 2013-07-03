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
import java.util.HashSet;
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
import ldbc.socialnet.dbgen.vocabulary.DBP;
import ldbc.socialnet.dbgen.vocabulary.DBPOWL;
import ldbc.socialnet.dbgen.vocabulary.DBPPROP;
import ldbc.socialnet.dbgen.vocabulary.FOAF;
import ldbc.socialnet.dbgen.vocabulary.RDF;
import ldbc.socialnet.dbgen.vocabulary.RDFS;
import ldbc.socialnet.dbgen.vocabulary.SN;
import ldbc.socialnet.dbgen.vocabulary.SNVOC;
import ldbc.socialnet.dbgen.vocabulary.XSD;


public class Turtle implements Serializer {
	
    private static final String STATIC_DBP_DATA_FILE = "static_dbp";
    
    private boolean isTurtle;
	private long nrTriples;
	private FileWriter[] dataFileWriter;
	private FileWriter[] staticdbpFileWriter;
	private boolean forwardChaining;
	private boolean haveToGeneratePrefixes = true;
	int currentWriter = 0;
	static long membershipId = 0;
	static long friendshipId = 0; 
	static long gpsId = 0; 
	static long likeId = 0;
	static long workatId = 0;
	static long studyAt = 0;
	static long locationPartOfId = 0;
	static long speakId = 0;
	
	HashMap<Integer, String> interestIdsNames;
	HashMap<String, Integer> companyToCountry;
	HashMap<String, Integer> universityToCountry;
	HashMap<String, Integer> printedIpaddresses;
	HashMap<String, Integer> printedOrganizations;
	HashMap<Integer, Integer> printedLanguages;
	HashMap<Integer, Integer> printedTags;
	Vector<String>	vBrowserNames;
	Vector<Integer> printedLocations;
	
	GregorianCalendar date;
	LocationDictionary locationDic;
	LanguageDictionary languageDic;
	IPAddressDictionary ipDic;
	
	public Turtle(String file, boolean forwardChaining, int nrOfOutputFiles, boolean isTurtle)
	{
	    this.isTurtle = isTurtle;
		date = new GregorianCalendar();
		printedIpaddresses = new HashMap<String, Integer>();
		printedOrganizations = new HashMap<String, Integer>();
		printedLanguages = new HashMap<Integer, Integer>();
		printedTags = new HashMap<Integer, Integer>();
		int nrOfDigits = ((int)Math.log10(nrOfOutputFiles)) + 1;
		String formatString = "%0" + nrOfDigits + "d";
		try{
		    String extension = (isTurtle) ? ".ttl": ".n3";
			dataFileWriter = new FileWriter[nrOfOutputFiles];
			staticdbpFileWriter = new FileWriter[nrOfOutputFiles];
			if(nrOfOutputFiles==1) {
				this.dataFileWriter[0] = new FileWriter(file + extension);
				this.staticdbpFileWriter[0] = new FileWriter(file+STATIC_DBP_DATA_FILE + extension);
			} else {
				for(int i=1;i<=nrOfOutputFiles;i++) {
					dataFileWriter[i-1] = new FileWriter(file + String.format(formatString, i) + extension);
					this.staticdbpFileWriter[i-1] = new FileWriter(file+STATIC_DBP_DATA_FILE + String.format(formatString, i) + extension);
				}
			}
				
		} catch(IOException e){
			System.err.println("Could not open File for writing.");
			System.err.println(e.getMessage());
			System.exit(-1);
		}
		
		try {
			for(int i=0;i<nrOfOutputFiles;i++) {
				dataFileWriter[i].append(getNamespaces());
			}
			for(int i=0;i<nrOfOutputFiles;i++) {
                staticdbpFileWriter[i].append(getStaticNamespaces());
            }
		} catch(IOException e) {
			System.err.println(e.getMessage());
		}
		
		this.forwardChaining = forwardChaining;
		nrTriples = 0l;
		
		TurtleShutdown sd = new TurtleShutdown(this);
		Runtime.getRuntime().addShutdownHook(sd);
	}

	public Turtle(String file, boolean forwardChaining, int nrOfOutputFiles, boolean isTurtle,
	        HashMap<Integer, String> _interestIdsNames, Vector<String> _vBrowsers, 
	        HashMap<String, Integer> companyToCountry, HashMap<String, Integer> univesityToCountry,
	        IPAddressDictionary ipDic,  LocationDictionary locationDic, 
	        LanguageDictionary languageDic) {
	    this(file, forwardChaining, nrOfOutputFiles, isTurtle);
	    this.interestIdsNames = _interestIdsNames;  
	    this.vBrowserNames = _vBrowsers;
	    this.locationDic = locationDic;
	    this.companyToCountry = companyToCountry;
	    this.universityToCountry = univesityToCountry;
	    this.ipDic = ipDic;
	    this.languageDic = languageDic;
	    printedLocations = new Vector<Integer>();
	}

	@Override
	public Long triplesGenerated() {
		return nrTriples;
	}

	@Override
	public void gatherData(SocialObject socialObject){
	    if(haveToGeneratePrefixes) {
	        generatePrefixes();
	        haveToGeneratePrefixes = false;
	    }

	    try {
	        if(socialObject instanceof UserProfile){
	            UserContainer container = new UserContainer((UserProfile)socialObject);
	            dataFileWriter[currentWriter].append(convertUserProfile(container, null));
	        }
	        else if(socialObject instanceof FriendShip){
	            dataFileWriter[currentWriter].append(convertFriendShip((FriendShip)socialObject));
	        }
	        else if(socialObject instanceof Post){
	            dataFileWriter[currentWriter].append(convertPost((Post)socialObject, true, true));
	        }
	        else if(socialObject instanceof Comment){
	            dataFileWriter[currentWriter].append(convertComment((Comment)socialObject));
	        }
	        else if (socialObject instanceof PhotoAlbum){
	            dataFileWriter[currentWriter].append(convertPhotoAlbum((PhotoAlbum)socialObject));
	        }
	        else if (socialObject instanceof Photo){
	            dataFileWriter[currentWriter].append(convertPhoto((Photo)socialObject, true, true));
	        }
	        else if (socialObject instanceof Group){
	            dataFileWriter[currentWriter].append(convertGroup((Group)socialObject));
	        }
	        else if (socialObject instanceof GPS){
	            dataFileWriter[currentWriter].append(convertGPS((GPS)socialObject));
	        }				
	        currentWriter = (currentWriter + 1) % dataFileWriter.length;
	    }
	    catch(IOException e){
	        System.out.println("Cannot write to output file ");
	        e.printStackTrace();
	        System.exit(-1);
	    }
	} 
	
	@Override
	public void gatherData(ReducedUserProfile userProfile, UserExtraInfo extraInfo){
		
		try {
		    UserContainer container = new UserContainer(userProfile);
			dataFileWriter[currentWriter].append(convertUserProfile(container, extraInfo));
		} catch (IOException e) {
			System.out.println("Cannot write to output file ");
			e.printStackTrace();
		}
	}
	public void gatherData(Post post, boolean isLikeStream){
		
		try {
			dataFileWriter[currentWriter].append(convertPost(post, !isLikeStream, isLikeStream));
		} catch (IOException e) {
			System.out.println("Cannot write to output file ");
			e.printStackTrace();
		}
	}
	
	public void gatherData(Photo photo, boolean isLikeStream){
		
		try {
			dataFileWriter[currentWriter].append(convertPhoto(photo, !isLikeStream, isLikeStream));
		} catch (IOException e) {
			System.out.println("Cannot write to output file ");
			e.printStackTrace();
		}
	}	
	private String getNamespaces() {
	    StringBuffer result = new StringBuffer();
		createPrefixLine(result, RDF.PREFIX, RDF.NS);
		createPrefixLine(result, RDFS.PREFIX, RDFS.NS);
		createPrefixLine(result, XSD.PREFIX, XSD.NS);
		createPrefixLine(result, DBP.PREFIX, DBP.NS);
		createPrefixLine(result, SNVOC.PREFIX, SNVOC.NS);
		createPrefixLine(result, SN.PREFIX, SN.NS);
		return result.toString();
	}
	
	private String getStaticNamespaces() {
        StringBuffer result = new StringBuffer();
        createPrefixLine(result, RDF.PREFIX, RDF.NS);
        createPrefixLine(result, FOAF.PREFIX, FOAF.NS);
        createPrefixLine(result, DBP.PREFIX, DBP.NS);
        createPrefixLine(result, DBPOWL.PREFIX, DBPOWL.NS);
        createPrefixLine(result, DBPPROP.PREFIX, DBPPROP.NS);
        createPrefixLine(result, SNVOC.PREFIX, SNVOC.NS);
        return result.toString();
    }
	
	private void createPrefixLine(StringBuffer result, String prefix, String namespace) {
		result.append("@prefix ");
		result.append(prefix);
		result.append(" ");
		result.append(createURIref(namespace));
		result.append(" .\n");
	}

	//Create URIREF from URI
	private String createURIref(String uri)
	{
		StringBuffer result = new StringBuffer();
		result.append("<");
		result.append(uri);
		result.append(">");
		return result.toString();
	}
	
	private void generatePrefixes() {
	    
	}
	
	private void writeDBPData(String left, String middle, String right) {
	    try {
	        StringBuffer result = new StringBuffer();
	        createTripleSPO(result, left, middle, right);
	        staticdbpFileWriter[currentWriter].append(result);
	    } catch (IOException e) {
            System.out.println("Cannot write to output file ");
            e.printStackTrace();
        }
	}
	
	private void AddTriple(StringBuffer result, boolean beginning, 
            boolean end, String left, String middle, String right, String extra, boolean ignoreExtra) {
	    if (isTurtle && beginning) {
            result.append(left + "\n");
	    }
        if (isTurtle && end){
            if (extra.isEmpty()) {
                result.append(createTriplePOEnd(middle, right));
            } else {
                result.append(createTriplePOOEnd(middle, right, extra));
            }
        } else if (isTurtle) {
            if (extra.isEmpty()) {
                result.append(createTriplePO(middle, right));
            } else {
                result.append(createTriplePOO(middle, right, extra));
            }
        } else {
            createTripleSPO(result, left, middle, right);
            if (!extra.isEmpty() && !ignoreExtra) {
                createTripleSPO(result, left, middle, extra);
            }
        }
	}
	
	private void AddTriple(StringBuffer result, boolean beginning, 
            boolean end, String left, String middle, String right, String extra) {
	    AddTriple(result, beginning, end, left, middle, right, extra, false);
	}
	
	private void AddTriple(StringBuffer result, boolean beginning, 
	        boolean end, String left, String middle, String right) {
	    AddTriple(result, beginning, end, left, middle, right, "", false);
	}
	
	public void printLocationHierarchy(int baseId) {
	    ArrayList<Integer> areas = new ArrayList<Integer>();
        do {
            areas.add(baseId);
            baseId = locationDic.belongsTo(baseId);
        } while (baseId != -1);
        
        for (int i = areas.size() - 1; i >= 0; i--) {
            if (printedLocations.indexOf(areas.get(i)) == -1) {
                printedLocations.add(areas.get(i));
                
                if (locationDic.getType(areas.get(i)) != Location.CONTINENT) {
                    String type = (locationDic.getType(areas.get(i)) == Location.COUNTRY) ? DBPOWL.Country : DBPOWL.City;
                    String name = locationDic.getLocationName(areas.get(i));
                    writeDBPData(DBP.fullPrefixed(name), RDF.type, type);
                    writeDBPData(DBP.fullPrefixed(name), FOAF.Name, createLiteral(name));
                    if (locationDic.getType(areas.get(i)) == Location.CITY) {
                        String countryName = locationDic.getLocationName(areas.get(i+1));
                        writeDBPData(DBP.fullPrefixed(name), DBPOWL.AttrCountry, DBP.fullPrefixed(countryName));
                    }
                }
            }
        }
	}
	
	public String convertUserProfile(UserContainer profile, UserExtraInfo extraInfo){
		StringBuffer result = new StringBuffer();
		//First the uriref for the subject
		
		if (extraInfo != null) {
		    
		    if (printedLocations.indexOf(extraInfo.getLocationId()) == -1) {
		        int parentId = extraInfo.getLocationId();
		        printLocationHierarchy(parentId);
		    }
		    
		    Iterator<String> it = extraInfo.getCompanies().iterator();
            while (it.hasNext()) {
                String company = it.next();
                int parentId = companyToCountry.get(company);
                printLocationHierarchy(parentId);
            }
            printLocationHierarchy(universityToCountry.get(extraInfo.getOrganization()));
		}
		printLocationHierarchy(ipDic.getLocation(profile.getIpAddress()));
		
		
		//result.append(UserProfile.getPrefixed(profile.getAccountId()));
		String prefix = SN.getPersonURI(profile.getAccountId());
		AddTriple(result, true, false, prefix, RDF.type, SNVOC.Person);
		
		if (extraInfo != null) {
		    //foaf:firstName
		    AddTriple(result, false, false, prefix, SNVOC.FirstName, 
		            createLiteral(extraInfo.getFirstName()));

		    //foaf:lastName
		    AddTriple(result, false, false, prefix, SNVOC.LastName, 
                    createLiteral(extraInfo.getLastName()));
		    // For user's extra info
		    if (!extraInfo.getGender().equals("")) {
		        AddTriple(result, false, false, prefix, SNVOC.Gender,
		                createLiteral(extraInfo.getGender()));
		    }
		}

		if (profile.getBirthDay() != -1 ){
		    date.setTimeInMillis(profile.getBirthDay());
		    String dateString = DateGenerator.formatDate(date);
		    AddTriple(result, false, false, prefix, SNVOC.Birthday,
		            createDataTypeLiteral(dateString, XSD.Date));
		}
		
		if (profile.getIpAddress() != null) {
            AddTriple(result, false, false, prefix, SNVOC.Ip_address, 
                    createLiteral(profile.getIpAddress().toString()));
            if (profile.getBrowserIdx() >= 0) {
                AddTriple(result, false, false, prefix, SNVOC.Browser,
                        createLiteral(vBrowserNames.get(profile.getBrowserIdx())));
            }
        }
		//dc:created
        date.setTimeInMillis(profile.getCreatedDate());
        String dateString = DateGenerator.formatDateDetail(date);
        AddTriple(result, false, true, prefix, SNVOC.Created,
                createDataTypeLiteral(dateString, XSD.DateTime));
		
		if (extraInfo != null) {
		    createTripleSPO(result, prefix, SNVOC.Based_near, DBP.fullPrefixed(locationDic.getLocationName(extraInfo.getLocationId())));
		    
		    if (!extraInfo.getOrganization().equals("")) {
		        if (!printedOrganizations.containsKey(extraInfo.getOrganization())) {
		            int organizationId = printedOrganizations.size();
                    printedOrganizations.put(extraInfo.getOrganization(), organizationId);
		            writeDBPData(DBP.fullPrefixed(extraInfo.getOrganization()), RDF.type, DBPOWL.Organisation);
		            writeDBPData(DBP.fullPrefixed(extraInfo.getOrganization()), FOAF.Name, createLiteral(extraInfo.getOrganization()));
		            int locationId = universityToCountry.get(extraInfo.getOrganization());
		            writeDBPData(DBP.fullPrefixed(extraInfo.getOrganization()), DBPOWL.AttrCountry, 
		                    DBP.fullPrefixed(locationDic.getLocationName(locationId)));
		        }
		        createTripleSPO(result, prefix, SNVOC.Study_at, SN.getStudyAtURI(studyAt));
		        createTripleSPO(result, SN.getStudyAtURI(studyAt), SNVOC.LinkOrganization, 
		                DBP.fullPrefixed(extraInfo.getOrganization()));
		        if (extraInfo.getClassYear() != -1 ){
		            date.setTimeInMillis(extraInfo.getClassYear());
		            String yearString = DateGenerator.formatYear(date);
		            createTripleSPO(result, SN.getStudyAtURI(studyAt), SNVOC.ClassYear,
		                    createDataTypeLiteral(yearString, XSD.Year));
		        }

		        studyAt++;
		    }
		}
        
        if (extraInfo != null) {
            Vector<Integer> languages = extraInfo.getLanguages();
            for (int i = 0; i < languages.size(); i++) {
                createTripleSPO(result, prefix, SNVOC.Speaks, 
                        createLiteral(languageDic.getLanguagesName(languages.get(i))));
                speakId++;
            }
        }
		
		if (extraInfo != null) {
		    //sib:workAt
            Iterator<String> it = extraInfo.getCompanies().iterator();
            while (it.hasNext()) {
                String company = it.next();
                if (!printedOrganizations.containsKey(company)) {
                    int organizationId = printedOrganizations.size();
                    printedOrganizations.put(company, organizationId);
                    writeDBPData(DBP.fullPrefixed(company), RDF.type, DBPOWL.Organisation);
                    writeDBPData(DBP.fullPrefixed(company), FOAF.Name, createLiteral(company));
                    int locationId = companyToCountry.get(company);
                    writeDBPData(DBP.fullPrefixed(company), DBPOWL.AttrCountry, 
                            DBP.fullPrefixed(locationDic.getLocationName(locationId)));
                }
                
                createTripleSPO(result, prefix, SNVOC.Work_at, SN.getWorkAtURI(workatId));
                createTripleSPO(result, SN.getWorkAtURI(workatId), SNVOC.LinkOrganization, 
                        DBP.fullPrefixed(company));
                date.setTimeInMillis(extraInfo.getWorkFrom(company));
                String yearString = DateGenerator.formatYear(date);
                createTripleSPO(result, SN.getWorkAtURI(workatId), SNVOC.WorkFrom,
                        createDataTypeLiteral(yearString, XSD.Year));

                workatId++;
            }
		}
		
		//The forums of the user
        String forumPrefix = SN.getForumURI(profile.getForumWallId());
        AddTriple(result, true, false, forumPrefix, RDF.type, SNVOC.Forum);
        String title = "Wall of ";
        if (extraInfo != null) {
            title += extraInfo.getFirstName() + " " + extraInfo.getLastName();
        } else {
            title += "" + profile.getAccountId();
        }
        AddTriple(result, false, false, forumPrefix, SNVOC.Title, createLiteral(title));
        AddTriple(result, false, true, forumPrefix, SNVOC.Created, 
                createDataTypeLiteral(dateString, XSD.DateTime));
        
        createTripleSPO(result, forumPrefix,  SNVOC.Moderator, prefix);
		
		// has_mail
		if (extraInfo != null) {
		    Iterator<String> it = extraInfo.getEmail().iterator();
		    while (it.hasNext()){
		        String email = it.next();
		        createTripleSPO(result, prefix, SNVOC.Has_mail, createLiteral(email));
		    }
		}
		
		// For the interests
		Iterator<Integer> it = profile.getSetOfInterests().iterator();
		while (it.hasNext()) {
			Integer interestIdx = it.next();
			String interest = interestIdsNames.get(interestIdx);
			createTripleSPO(result, prefix, SNVOC.Interest, DBP.fullPrefixed(interest));  
			if (!printedTags.containsKey(interestIdx)) {
			    printedTags.put(interestIdx, interestIdx);
			    writeDBPData(DBP.fullPrefixed(interest), FOAF.Name, createLiteral(interest.replace("\"", "\\\"")));
			}
		}

		//For the friendships
		Friend friends[] = profile.getFriendList();
		for (int i = 0; i < friends.length; i ++){
			if (friends[i] != null){				
				//foaf:knows
				if (extraInfo == null || friends[i].getCreatedTime() != -1){
				    createTripleSPO(result, prefix, SNVOC.Knows,
				            SN.getPersonURI(friends[i].getFriendAcc()));
				}
			}
		}
		
		
		return result.toString();	
	}
	
	public String convertFriendShip(FriendShip friendShip){
		StringBuffer result = new StringBuffer();
		
		String person1 = SN.getPersonURI(friendShip.getUserAcc01());
		String person2 = SN.getPersonURI(friendShip.getUserAcc02());
		createTripleSPO(result, person1, SNVOC.Knows, person2);
		createTripleSPO(result, person2, SNVOC.Knows, person1);
		
		return result.toString();
	}

	public String convertPost(Post post, boolean body, boolean isLiked){
	    StringBuffer result = new StringBuffer();

	    if (body) {
	        
	        if (post.getIpAddress() != null) {
	            printLocationHierarchy(ipDic.getLocation(post.getIpAddress()));
	        }
	        
	        String prefix = SN.getPostURI(post.getPostId());
	        AddTriple(result, true, false, prefix, RDF.type, SNVOC.Post);

	        if (post.getTitle() != null) {
	            AddTriple(result, false, false, prefix, SNVOC.Title, createLiteral(post.getTitle()));
	        }
	        date.setTimeInMillis(post.getCreatedDate());
	        String dateString = DateGenerator.formatDateDetail(date);
	        AddTriple(result, false, false, prefix, SNVOC.Created, 
	                createDataTypeLiteral(dateString, XSD.DateTime));
	        if (post.getIpAddress() != null) {
	            AddTriple(result, false, false, prefix, SNVOC.Ip_address, 
	                    createLiteral(post.getIpAddress().toString()));
	            if (post.getBrowserIdx() >= 0) {
	                AddTriple(result, false, false, prefix, SNVOC.Browser,
	                        createLiteral(vBrowserNames.get(post.getBrowserIdx())));
	            }
	        }
	        AddTriple(result, false, true, prefix, SNVOC.Content,
                    createLiteral(post.getContent()));
	        
	        if (post.getIpAddress() != null) {
	            createTripleSPO(result, prefix, SNVOC.Annotated,
	                    createLiteral(languageDic.getLanguagesName(post.getLanguage())));
	            createTripleSPO(result, prefix, SNVOC.Located,
	                    DBP.fullPrefixed(locationDic.getLocationName((ipDic.getLocation(post.getIpAddress())))));
	        }

	        createTripleSPO(result, SN.getForumURI(post.getForumId()),
	                SNVOC.Container_of, prefix);

	        createTripleSPO(result, prefix,
                    SNVOC.Creator, SN.getPersonURI(post.getAuthorId()));

	        Iterator<Integer> it = post.getTags().iterator();
	        while (it.hasNext()) {
	            Integer tagId = it.next();
                String tag = interestIdsNames.get(tagId);
	            createTripleSPO(result, prefix, SNVOC.Has_tag, DBP.fullPrefixed(tag));
	            if (!printedTags.containsKey(tagId)) {
	                printedTags.put(tagId, tagId);
	                writeDBPData(DBP.fullPrefixed(tag), FOAF.Name, createLiteral(tag.replace("\"", "\\\"")));
	            }
	        }
	    }

	    if (isLiked) {
	        String prefix = SN.getPostURI(post.getPostId());
	        int userLikes[] = post.getInterestedUserAccs();
	        long likeTimestamps[] = post.getInterestedUserAccsTimestamp();
	        for (int i = 0; i < userLikes.length; i ++) {
	            String likePrefix = SN.getLikeURI(likeId);
	            createTripleSPO(result, SN.getPersonURI(userLikes[i]), 
	                    SNVOC.Like, likePrefix);
	            
	            AddTriple(result, true, false, likePrefix, SNVOC.LinkPost, prefix);
	            date.setTimeInMillis(likeTimestamps[i]);
	            String dateString = DateGenerator.formatDateDetail(date);
	            AddTriple(result, false, true, likePrefix, SNVOC.Created,
	                    createDataTypeLiteral(dateString, XSD.DateTime));
	            likeId++;
	        }
	    }

		return result.toString();
	}

	public String convertComment(Comment comment){
		StringBuffer result = new StringBuffer();
		String prefix = SN.getCommentURI(comment.getCommentId());
        AddTriple(result, true, false, prefix, RDF.type, SNVOC.Comment);
        date.setTimeInMillis(comment.getCreateDate());
        String dateString = DateGenerator.formatDateDetail(date); 
        AddTriple(result, false, false, prefix, SNVOC.Created, createDataTypeLiteral(dateString, XSD.DateTime));
        if (comment.getIpAddress() != null) {
            AddTriple(result, false, false, prefix, SNVOC.Ip_address, 
                    createLiteral(comment.getIpAddress().toString()));
            if (comment.getBrowserIdx() >= 0) {
                AddTriple(result, false, false, prefix, SNVOC.Browser,
                        createLiteral(vBrowserNames.get(comment.getBrowserIdx())));
            }
        }
        AddTriple(result, false, true, prefix, SNVOC.Content, createLiteral(comment.getContent()));
        
        String replied = (comment.getReply_of() == -1) ? SN.getPostURI(comment.getPostId()) : 
            SN.getCommentURI(comment.getReply_of());
        createTripleSPO(result, prefix, SNVOC.Reply_of, replied);
        if (comment.getIpAddress() != null) {
            createTripleSPO(result, prefix, SNVOC.Located,
                    DBP.fullPrefixed(locationDic.getLocationName((ipDic.getLocation(comment.getIpAddress())))));
        }
        
        //user sioc:creator_of
        createTripleSPO(result, prefix, SNVOC.Creator,
                SN.getPersonURI(comment.getAuthorId()));

		return result.toString();
	}
	
	public String convertPhotoAlbum(PhotoAlbum album){
		StringBuffer result = new StringBuffer();
		
		String prefix = SN.getForumURI(album.getAlbumId());
        AddTriple(result, true, false, prefix, RDF.type, SNVOC.Forum);
        AddTriple(result, false, false, prefix, SNVOC.Title, createLiteral(album.getTitle()));
        date.setTimeInMillis(album.getCreatedDate());
        String dateString = DateGenerator.formatDateDetail(date);     
        AddTriple(result, false, true, prefix, SNVOC.Created, createDataTypeLiteral(dateString, XSD.DateTime));

        createTripleSPO(result, prefix, SNVOC.Moderator, SN.getPersonURI(album.getCreatorId()));
		
		return result.toString(); 
	}	

	public String convertPhoto(Photo photo, boolean body, boolean isLiked){
		StringBuffer result = new StringBuffer();
		if (body)  {
		    String prefix = SN.getPostURI(photo.getPhotoId());
	        AddTriple(result, true, false, prefix, RDF.type, SNVOC.Post);
	        AddTriple(result, false, false, prefix, SNVOC.Image, createLiteral(photo.getImage()));
	        date.setTimeInMillis(photo.getTakenTime());
            String dateString = DateGenerator.formatDateDetail(date);
            if (photo.getIpAddress() != null) {
                AddTriple(result, false, false, prefix, SNVOC.Ip_address, 
                        createLiteral(photo.getIpAddress().toString()));
                if (photo.getBrowserIdx() >= 0) {
                    AddTriple(result, false, false, prefix, SNVOC.Browser,
                            createLiteral(vBrowserNames.get(photo.getBrowserIdx())));
                }
            }
            AddTriple(result, false, true, prefix, SNVOC.Created, 
                    createDataTypeLiteral(dateString, XSD.DateTime));
            
            createTripleSPO(result, prefix, SNVOC.Creator, SN.getPersonURI(photo.getCreatorId()));
            createTripleSPO(result, SN.getForumURI(photo.getAlbumId()), SNVOC.Container_of, prefix);
            createTripleSPO(result, prefix, SNVOC.Located,
                    DBP.fullPrefixed(locationDic.getLocationName((ipDic.getLocation(photo.getIpAddress())))));
            
            Iterator<Integer> it = photo.getTags().iterator();
            while (it.hasNext()) {
                Integer tagId = it.next();
                String tag = interestIdsNames.get(tagId);
                createTripleSPO(result, prefix, SNVOC.Has_tag, DBP.fullPrefixed(tag));
                if (!printedTags.containsKey(tagId)) {
                    printedTags.put(tagId, tagId);
                    writeDBPData(DBP.fullPrefixed(tag), FOAF.Name, createLiteral(tag.replace("\"", "\\\"")));
                }
            }
		}
		if (isLiked) {
		    String prefix = SN.getPostURI(photo.getPhotoId());
            int userLikes[] = photo.getInterestedUserAccs();
            long likeTimestamps[] = photo.getInterestedUserAccsTimestamp();
            for (int i = 0; i < userLikes.length; i ++) {
                String likePrefix = SN.getLikeURI(likeId);
                createTripleSPO(result, SN.getPersonURI(userLikes[i]), 
                        SNVOC.Like, likePrefix);
                
                AddTriple(result, true, false, likePrefix, SNVOC.LinkPost, prefix);
                date.setTimeInMillis(likeTimestamps[i]);
                String dateString = DateGenerator.formatDateDetail(date);
                AddTriple(result, false, true, likePrefix, SNVOC.Created,
                        createDataTypeLiteral(dateString, XSD.DateTime));
                likeId++;
            }
		}
		return result.toString(); 
	}	

	public String convertGPS(GPS gps){
		StringBuffer result = new StringBuffer();
		return result.toString(); 
	}

	public String convertGroup(Group group){
	    StringBuffer result = new StringBuffer();

	    String prefix = SN.getGroupURI(group.getGroupId());
	    AddTriple(result, true, false, prefix, RDF.type, SNVOC.Group);

	    AddTriple(result, false, false, prefix, SNVOC.Name, createLiteral(group.getGroupName()));

	    date.setTimeInMillis(group.getCreatedDate());
	    String dateString = DateGenerator.formatDateDetail(date);  
	    AddTriple(result, false, true, prefix, SNVOC.Created, 
	            createDataTypeLiteral(dateString, XSD.DateTime));
	    
	    Integer groupTags[] = group.getTags();
        for (int i = 0; i < groupTags.length; i ++){
            String tag = interestIdsNames.get(groupTags[i]);
            createTripleSPO(result, prefix, SNVOC.Has_tag, DBP.fullPrefixed(tag));
            if (!printedTags.containsKey(groupTags[i])) {
                printedTags.put(groupTags[i], groupTags[i]);
                writeDBPData(DBP.fullPrefixed(tag), FOAF.Name, createLiteral(tag.replace("\"", "\\\"")));
            }
        }

	    createTripleSPO(result, prefix, SNVOC.Creator, SN.getPersonURI(group.getModeratorId()));

	    GroupMemberShip memberShips[] = group.getMemberShips();
	    int numMemberAdded = group.getNumMemberAdded();
	    for (int i = 0; i < numMemberAdded; i ++){

	        String memberhipPrefix = SN.getMembershipURI(membershipId);
	        createTripleSPO(result, prefix, SNVOC.Member, memberhipPrefix);
	        
	        AddTriple(result, true, false, memberhipPrefix, SNVOC.LinkPerson, SN.getPersonURI(group.getModeratorId()));
	        date.setTimeInMillis(memberShips[i].getJoinDate());
	        dateString = DateGenerator.formatDateDetail(date);
	        AddTriple(result, false, true, memberhipPrefix, SNVOC.Joined,
	                createDataTypeLiteral(dateString, XSD.DateTime));

	        membershipId++;
	    }
	    
	    // Forums of the group
	    String forumPrefix = SN.getForumURI(group.getForumWallId());
        AddTriple(result, true, false, forumPrefix, RDF.type, SNVOC.Forum);
        AddTriple(result, false, false, forumPrefix, SNVOC.Title, createLiteral(group.getGroupName()));
        AddTriple(result, false, true, forumPrefix, SNVOC.Created, 
                createDataTypeLiteral(dateString, XSD.DateTime));

        createTripleSPO(result, forumPrefix,
                SNVOC.Moderator, SN.getPersonURI(group.getModeratorId()));
        
	    return result.toString();
	}
	
	//Create Literal
	private String createLiteral(String value)
	{
		StringBuffer result = new StringBuffer();
		result.append("\"");
		result.append(value);
		result.append("\"");
		return result.toString();
	}
	
	//Create typed literal
	private String createDataTypeLiteral(String value, String datatypeURI)
	{
		StringBuffer result = new StringBuffer();
		result.append("\"");
		result.append(value);
		result.append("\"^^");
		result.append(datatypeURI);
		return result.toString();
	}

	/*
	 * Create a triple consisting of subject predicate and object, end with "."
	 */
	private void createTripleSPO(StringBuffer result, String subject, String predicate, String object)
	{
		result.append(subject);		
		result.append(" ");
		result.append(predicate);
		result.append(" ");
		result.append(object);
		result.append(" .\n");
		
		nrTriples++;
	}
	/*
	 * Create an abbreviated triple consisting of predicate and object; end with ";"
	 */
	private String createTriplePO(String predicate, String object)
	{
		StringBuffer result = new StringBuffer();
		result.append("    ");
		result.append(predicate);
		result.append(" ");
		result.append(object);
		result.append(" ;\n");
		
		nrTriples++;
		
		return result.toString();
	}

	/*
	 * Create an abbreviated triple consisting of predicate and object; end with "."
	 */
	private String createTriplePOEnd(String predicate, String object)
	{
		StringBuffer result = new StringBuffer();
		result.append("    ");
		result.append(predicate);
		result.append(" ");
		result.append(object);
		result.append(" .\n");
		
		nrTriples++;
		
		return result.toString();
	}
	
	// Create a triple with two objects separated by an ",". the triple is ended with ";" 
	private String createTriplePOO(String predicate, String object1, String object2)
	{
		StringBuffer result = new StringBuffer();
		result.append("    ");
		result.append(predicate);
		result.append(" ");
		result.append(object1);
		result.append(" , ");
		result.append(object2);		
		result.append(" ;\n");
		
		nrTriples = nrTriples + 2;
		
		return result.toString();
	}
	
	// Create a triple with two objects separated by an ",". the triple is ended with "." 
	private String createTriplePOOEnd(String predicate, String object1, String object2)
	{
		StringBuffer result = new StringBuffer();
		result.append("    ");
		result.append(predicate);
		result.append(" ");
		result.append(object1);
		result.append(" , ");
		result.append(object2);		
		result.append(" .\n");
		
		nrTriples = nrTriples + 2;
		
		return result.toString();
	}
	
	@Override
	public void serialize() {
		//Close files
		try {
			for(int i=0;i<dataFileWriter.length;i++) {
				dataFileWriter[i].flush();
				dataFileWriter[i].close();
				staticdbpFileWriter[i].flush();
                staticdbpFileWriter[i].close();
			}
		} catch(IOException e) {
			System.err.println(e.getMessage());
			System.exit(-1);
		}
	}
	

	class TurtleShutdown extends Thread {
		Turtle serializer;
		TurtleShutdown(Turtle t) {
			serializer = t;
		}
		
		@Override
        public void run() {
			
			for(int i=0;i<dataFileWriter.length;i++) {
				try {
					serializer.dataFileWriter[i].flush();
					serializer.dataFileWriter[i].close();
					staticdbpFileWriter[i].flush();
					staticdbpFileWriter[i].close();
				} catch(IOException e) {
					// Do nothing
				}
			}
		}
	}	
}
