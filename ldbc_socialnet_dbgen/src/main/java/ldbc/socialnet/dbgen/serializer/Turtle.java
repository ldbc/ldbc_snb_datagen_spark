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
import ldbc.socialnet.dbgen.vocabulary.FOAF;
import ldbc.socialnet.dbgen.vocabulary.RDF;
import ldbc.socialnet.dbgen.vocabulary.RDFS;
import ldbc.socialnet.dbgen.vocabulary.SN;
import ldbc.socialnet.dbgen.vocabulary.SNVOC;
import ldbc.socialnet.dbgen.vocabulary.XSD;

/**
 * Turtle serializer.
 */
public class Turtle implements Serializer {
	
    private static final String STATIC_DBP_DATA_FILE = "static_dbp";
    
	private FileWriter[] dataFileWriter;
	private FileWriter[] staticdbpFileWriter;
	private int currentWriter = 0;
	
	private long nrTriples;
	private GregorianCalendar date;
	
	/**
	 * Generator input classes.
	 */
	private boolean isTurtle;
	private CompanyDictionary companyDic;
	private HashMap<String, Integer> universityToCountry;
	private BrowserDictionary  browserDic;
	private LocationDictionary locationDic;
	private LanguageDictionary languageDic;
	private TagDictionary tagDic;
	private IPAddressDictionary ipDic;
    private boolean exportText;
    
	/**
	 * Used to give an unique ID to blank nodes.
	 */
	private long membershipId = 0;
	private long likeId       = 0;
	private long workAtId     = 0;
	private long studyAtId    = 0;
	
	/**
	 * Used to avoid serialize more than once the same data.
	 */
	private HashMap<String, Integer>  printedOrganizations;
	private HashMap<Integer, Integer> printedTags;
	private HashMap<Integer, Integer> printedTagClasses;
	private HashMap<Integer, Integer> printedLocations;
	
	/**
	 * Constructor.
	 * 
	 * @param file: The basic file name.
	 * @param nrOfOutputFiles: How many files will be created.
	 * @param isTurtle: If the RDF admits turtle abbreviation syntax.
	 * @param tagDic: The tag dictionary used in the generation.
	 * @param browsers: The browser dictionary used in the generation.
	 * @param companyToCountry: The company dictionaty used in the generation.
	 * @param univesityToCountry: HashMap of universities names to country IDs.
	 * @param ipDic: The IP dictionary used in the generation.
	 * @param locationDic: The location dictionary used in the generation.
	 * @param languageDic: The language dictionary used in the generation.
	 */
	public Turtle(String file, int nrOfOutputFiles, boolean isTurtle,
            TagDictionary tagDic, BrowserDictionary browsers, 
            CompanyDictionary companies, HashMap<String, Integer> univesityToCountry,
            IPAddressDictionary ipDic,  LocationDictionary locationDic, 
            LanguageDictionary languageDic, boolean exportText) {
	    
	    this.isTurtle = isTurtle;
	    this.tagDic = tagDic;  
        this.browserDic = browsers;
        this.locationDic = locationDic;
        this.companyDic = companies;
        this.universityToCountry = univesityToCountry;
        this.ipDic = ipDic;
        this.languageDic = languageDic;
        this.exportText = exportText;
        
        nrTriples = 0l;
		date = new GregorianCalendar();
		printedOrganizations = new HashMap<String, Integer>();
		printedTags = new HashMap<Integer, Integer>();
		printedTagClasses = new HashMap<Integer, Integer>();
		printedLocations = new HashMap<Integer, Integer>();
		
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
			
			for(int i=0;i<nrOfOutputFiles;i++) {
                dataFileWriter[i].append(getNamespaces());
                staticdbpFileWriter[i].append(getStaticNamespaces());
            }
			
		} catch(IOException e) {
			System.err.println(e.getMessage());
			System.exit(-1);
		}
	}

	/**
	 * Returns how many triples have been generated.
	 */
	public Long unitsGenerated() {
		return nrTriples;
	}

	/**
	 * Writes the collected data to the appropriate file.
	 * 
	 * @param data: The string to write.
	 */
	public void toWriter(String data){
	    try {
	        dataFileWriter[currentWriter].append(data);
	        currentWriter = (currentWriter + 1) % dataFileWriter.length;
	    } catch(IOException e){
	        System.out.println("Cannot write to output file ");
	        e.printStackTrace();
	        System.exit(-1);
	    }
	}
	
	/**
     * Gets the namespace for the generator file.
     */
	private String getNamespaces() {
	    StringBuffer result = new StringBuffer(350);
		createPrefixLine(result, RDF.PREFIX, RDF.NAMESPACE);
		createPrefixLine(result, RDFS.PREFIX, RDFS.NAMESPACE);
		createPrefixLine(result, XSD.PREFIX, XSD.NAMESPACE);
		createPrefixLine(result, SNVOC.PREFIX, SNVOC.NAMESPACE);
		createPrefixLine(result, SN.PREFIX, SN.NAMESPACE);
		createPrefixLine(result, DBP.PREFIX, DBP.NAMESPACE);
		return result.toString();
	}
	
	/**
     * Gets the namespace for the static dbpedia file.
     */
	private String getStaticNamespaces() {
        StringBuffer result = new StringBuffer(400);
        createPrefixLine(result, RDF.PREFIX, RDF.NAMESPACE);
        createPrefixLine(result, RDFS.PREFIX, RDFS.NAMESPACE);
        createPrefixLine(result, FOAF.PREFIX, FOAF.NAMESPACE);
        createPrefixLine(result, DBP.PREFIX, DBP.NAMESPACE);
        createPrefixLine(result, DBPOWL.PREFIX, DBPOWL.NAMESPACE);
        createPrefixLine(result, SNVOC.PREFIX, SNVOC.NAMESPACE);
        return result.toString();
    }
	
	/**
	 * 
	 * @param result: The StringBuffer to append to.
	 * @param prefix: The RDF namespace prefix abbreviation.
	 * @param namespace: The RDF namespace.
	 */
	private void createPrefixLine(StringBuffer result, String prefix, String namespace) {
		result.append("@prefix ");
		result.append(prefix);
		result.append(" ");
		result.append("<");
		result.append(namespace);
		result.append(">");
		result.append(" .\n");
	}
	
	/**
	 * Writes a RDF triple in the dbpedia static data file.
	 * 
	 * @param subject: The RDF subject.
	 * @param predicate: The RDF predicate.
	 * @param object: The RDF object.
	 */
	private void writeDBPData(String subject, String predicate, String object) {
	    try {
	        StringBuffer result = new StringBuffer(150);
	        createTripleSPO(result, subject, predicate, object);
	        staticdbpFileWriter[currentWriter].append(result);
	    } catch (IOException e) {
            System.out.println("Cannot write to output file ");
            e.printStackTrace();
        }
	}
	
	/**
	 * Serializes the tag data and its class hierarchy.
	 * 
	 * @param result: The StringBuffer to append to.
	 * @param tagId: The tag id.
	 * @param tag: The tag name.
	 */
	private void writeTagData(StringBuffer result, Integer tagId, String tag) {
	    writeDBPData(DBP.fullPrefixed(tag), FOAF.Name, createLiteral(tag.replace("\"", "\\\"")));
	    Integer tagClass = tagDic.getTagClass(tagId);
	    writeDBPData(DBP.fullPrefixed(tag), RDF.type, DBPOWL.prefixed(tagDic.getClassName(tagClass)));
	    while (tagClass != -1 && !printedTagClasses.containsKey(tagClass)) {
	        printedTagClasses.put(tagClass, tagClass);
	        if (tagDic.getClassName(tagClass).equals("Thing")) {
	            writeDBPData("<http://www.w3.org/2002/07/owl#Thing>", RDFS.label, createLiteral(tagDic.getClassLabel(tagClass)));
	            createTripleSPO(result, "<http://www.w3.org/2002/07/owl#Thing>", RDF.type, SNVOC.TagClass);
	        } else {
	            writeDBPData(DBPOWL.prefixed(tagDic.getClassName(tagClass)), RDFS.label, createLiteral(tagDic.getClassLabel(tagClass)));
	            createTripleSPO(result, DBP.fullPrefixed(tagDic.getClassName(tagClass)), RDF.type, SNVOC.TagClass);
	        }
	        Integer parent = tagDic.getClassParent(tagClass);
	        if (parent != -1) {
	            String parentPrefix;
	            if (tagDic.getClassName(parent).equals("Thing")) {
	                parentPrefix = "<http://www.w3.org/2002/07/owl#Thing>";
	            } else {
	                parentPrefix = DBPOWL.prefixed(tagDic.getClassName(parent));
	            }
	            writeDBPData(DBPOWL.prefixed(tagDic.getClassName(tagClass)), RDFS.subClassOf, parentPrefix);
	        }
	        tagClass = parent;
	    }
	}
	
	/**
     * Adds the appropriate triple kind into the input StringBuffer.
     * 
     * @param result: The StringBuffer to append to.
     * @param beginning: The beggining of a subject abbreviation block.
     * @param end: The end of a subject abbreviation block.
     * @param subject: The RDF subject.
     * @param predicate: The RDF predicate.
     * @param object: The RDF first object.
     * @param object: The RDF second object.
     */
	private void AddTriple(StringBuffer result, boolean beginning, 
            boolean end, String subject, String predicate, String object1, String object2) {

	    if (isTurtle) {
	        
	        if (beginning) {
	            result.append(subject + "\n");
	        }
	        
	        if (object2.isEmpty()) {
	            createTriplePO(result, predicate, object1, end);
	        } else {
                createTriplePOO(result, predicate, object1, object2, end);
            }
	    } else {
            createTripleSPO(result, subject, predicate, object1);
            if (!object2.isEmpty()) {
                createTripleSPO(result, subject, predicate, object2);
            }
        }
	}
	
	/**
	 * Adds the appropriate triple kind into the input StringBuffer.
	 * 
	 * @param result: The StringBuffer to append to.
	 * @param beginning: The beggining of a subject abbreviation block.
	 * @param end: The end of a subject abbreviation block.
	 * @param subject: The RDF subject.
	 * @param predicate: The RDF predicate.
	 * @param object: The RDF object.
	 */
	private void AddTriple(StringBuffer result, boolean beginning, 
	        boolean end, String subject, String predicate, String object) {
	    AddTriple(result, beginning, end, subject, predicate, object, "");
	}
	
	/**
	 * Writes the base location and its hierarchy.
	 * 
	 * @param result: The StringBuffer to append to.
	 * @param baseId: The base location id.
	 */
	public void printLocationHierarchy(StringBuffer result, int baseId) {
	    ArrayList<Integer> areas = new ArrayList<Integer>();
        do {
            areas.add(baseId);
            baseId = locationDic.belongsTo(baseId);
        } while (baseId != -1);
        
        for (int i = areas.size() - 1; i >= 0; i--) {
            if (!printedLocations.containsKey(areas.get(i))) {
                String name = locationDic.getLocationName(areas.get(i));
                printedLocations.put(areas.get(i), areas.get(i));
                String type = DBPOWL.City;
                if (locationDic.getType(areas.get(i)) == Location.COUNTRY) {
                    type = DBPOWL.Country;
                } else if (locationDic.getType(areas.get(i)) == Location.CONTINENT) {
                    type = DBPOWL.Continent;
                }
                
                writeDBPData(DBP.fullPrefixed(name), RDF.type, DBPOWL.Place);
                writeDBPData(DBP.fullPrefixed(name), RDF.type, type);
                writeDBPData(DBP.fullPrefixed(name), FOAF.Name, createLiteral(name));
                if (locationDic.getType(areas.get(i)) != Location.CONTINENT) {
                    String countryName = locationDic.getLocationName(areas.get(i+1));
                    createTripleSPO(result, DBP.fullPrefixed(name), SNVOC.isPartOf, DBP.fullPrefixed(countryName));
                }
            }
        }
	}
	
	/**
     * @See {@link ldbc.socialnet.dbgen.serializer.Serializer#gatherData(ReducedUserProfile, UserExtraInfo)}
     */
	public void gatherData(ReducedUserProfile profile, UserExtraInfo extraInfo){
		StringBuffer result = new StringBuffer(19000);
		
		if (extraInfo == null) {
		    System.err.println("LDBC socialnet must serialize the extraInfo");
		    System.exit(-1);
		}

		printLocationHierarchy(result, extraInfo.getLocationId());
		Iterator<String> itString = extraInfo.getCompanies().iterator();
		while (itString.hasNext()) {
		    String company = itString.next();
		    int parentId = companyDic.getCountry(company);
		    printLocationHierarchy(result, parentId);
		}
		printLocationHierarchy(result, universityToCountry.get(extraInfo.getOrganization()));
		printLocationHierarchy(result, ipDic.getLocation(profile.getIpAddress()));
		
		
		String prefix = SN.getPersonURI(profile.getAccountId());
		AddTriple(result, true, false, prefix, RDF.type, SNVOC.Person);

		AddTriple(result, false, false, prefix, SNVOC.firstName, 
		        createLiteral(extraInfo.getFirstName()));

		AddTriple(result, false, false, prefix, SNVOC.lastName, 
		        createLiteral(extraInfo.getLastName()));

		if (!extraInfo.getGender().equals("")) {
		    AddTriple(result, false, false, prefix, SNVOC.gender,
		            createLiteral(extraInfo.getGender()));
		}

		if (profile.getBirthDay() != -1 ){
		    date.setTimeInMillis(profile.getBirthDay());
		    String dateString = DateGenerator.formatDate(date);
		    AddTriple(result, false, false, prefix, SNVOC.birthday,
		            createDataTypeLiteral(dateString, XSD.Date));
		}
		
		if (profile.getIpAddress() != null) {
            AddTriple(result, false, false, prefix, SNVOC.ipaddress, 
                    createLiteral(profile.getIpAddress().toString()));
            if (profile.getBrowserIdx() >= 0) {
                AddTriple(result, false, false, prefix, SNVOC.browser,
                        createLiteral(browserDic.getName(profile.getBrowserIdx())));
            }
        }

        date.setTimeInMillis(profile.getCreatedDate());
        String dateString = DateGenerator.formatDateDetail(date);
        AddTriple(result, false, true, prefix, SNVOC.creationDate,
                createDataTypeLiteral(dateString, XSD.DateTime));

        createTripleSPO(result, prefix, SNVOC.locatedIn, DBP.fullPrefixed(locationDic.getLocationName(extraInfo.getLocationId())));

        if (!extraInfo.getOrganization().equals("")) {
            if (!printedOrganizations.containsKey(extraInfo.getOrganization())) {
                int organizationId = printedOrganizations.size();
                printedOrganizations.put(extraInfo.getOrganization(), organizationId);
                
                writeDBPData(DBP.fullPrefixed(extraInfo.getOrganization()), RDF.type, DBPOWL.Organisation);
                writeDBPData(DBP.fullPrefixed(extraInfo.getOrganization()), FOAF.Name, createLiteral(extraInfo.getOrganization()));
                int locationId = universityToCountry.get(extraInfo.getOrganization());
                createTripleSPO(result, DBP.fullPrefixed(extraInfo.getOrganization()), 
                        SNVOC.locatedIn, DBP.fullPrefixed(locationDic.getLocationName(locationId)));
            }
            
            if (extraInfo.getClassYear() != -1 ){
            createTripleSPO(result, prefix, SNVOC.studyAt, SN.getStudyAtURI(studyAtId));
            createTripleSPO(result, SN.getStudyAtURI(studyAtId), SNVOC.hasOrganisation, 
                    DBP.fullPrefixed(extraInfo.getOrganization()));
                date.setTimeInMillis(extraInfo.getClassYear());
                String yearString = DateGenerator.formatYear(date);
                createTripleSPO(result, SN.getStudyAtURI(studyAtId), SNVOC.classYear,
                        createDataTypeLiteral(yearString, XSD.Integer));
            }

            studyAtId++;
        }

        Vector<Integer> languages = extraInfo.getLanguages();
        for (int i = 0; i < languages.size(); i++) {
            createTripleSPO(result, prefix, SNVOC.speaks, 
                    createLiteral(languageDic.getLanguagesName(languages.get(i))));
        }

        itString = extraInfo.getCompanies().iterator();
        while (itString.hasNext()) {
            String company = itString.next();
            if (!printedOrganizations.containsKey(company)) {
                int organizationId = printedOrganizations.size();
                printedOrganizations.put(company, organizationId);
                
                writeDBPData(DBP.fullPrefixed(company), RDF.type, DBPOWL.Organisation);
                writeDBPData(DBP.fullPrefixed(company), FOAF.Name, createLiteral(company));
                int locationId = companyDic.getCountry(company);
                createTripleSPO(result, DBP.fullPrefixed(company), 
                        SNVOC.locatedIn, DBP.fullPrefixed(locationDic.getLocationName(locationId)));
            }

            createTripleSPO(result, prefix, SNVOC.workAt, SN.getWorkAtURI(workAtId));
            createTripleSPO(result, SN.getWorkAtURI(workAtId), SNVOC.hasOrganisation, 
                    DBP.fullPrefixed(company));
            date.setTimeInMillis(extraInfo.getWorkFrom(company));
            String yearString = DateGenerator.formatYear(date);
            createTripleSPO(result, SN.getWorkAtURI(workAtId), SNVOC.workFrom,
                    createDataTypeLiteral(yearString, XSD.Integer));

            workAtId++;
        }

        itString = extraInfo.getEmail().iterator();
        while (itString.hasNext()){
            String email = itString.next();
            createTripleSPO(result, prefix, SNVOC.email, createLiteral(email));
        }
		
        Iterator<Integer> itInteger = profile.getSetOfTags().iterator();
		while (itInteger.hasNext()) {
			Integer interestIdx = itInteger.next();
			String interest = tagDic.getName(interestIdx);
			createTripleSPO(result, prefix, SNVOC.hasInterest, DBP.fullPrefixed(interest));  
			if (!printedTags.containsKey(interestIdx)) {
			    printedTags.put(interestIdx, interestIdx);
			    createTripleSPO(result, DBP.fullPrefixed(interest), RDF.type, SNVOC.Tag);
			    //writeTagData(result, interestIdx, interest.replace("\"", "\\\""));
			    writeTagData(result, interestIdx, interest);
			}
		}
		
		Friend friends[] = profile.getFriendList();
		for (int i = 0; i < friends.length; i++){
			if (friends[i] != null && friends[i].getCreatedTime() != -1){
			    createTripleSPO(result, prefix, SNVOC.knows,
			            SN.getPersonURI(friends[i].getFriendAcc()));
			}
		}
		
		//User wall
        String title = "Wall of " + extraInfo.getFirstName() + " " + extraInfo.getLastName();
        String forumPrefix = SN.getForumURI(profile.getForumWallId());
        AddTriple(result, true, false, forumPrefix, RDF.type, SNVOC.Forum);
        AddTriple(result, false, false, forumPrefix, SNVOC.title, createLiteral(title));
        AddTriple(result, false, false, forumPrefix, SNVOC.creationDate, 
                createDataTypeLiteral(dateString, XSD.DateTime));
        AddTriple(result, false, true, forumPrefix, SNVOC.hasModerator, prefix);
        
        itInteger = profile.getSetOfTags().iterator();
        while (itInteger.hasNext()) {
            Integer interestIdx = itInteger.next();
            String interest = tagDic.getName(interestIdx);
            createTripleSPO(result, forumPrefix, SNVOC.hasTag, DBP.fullPrefixed(interest));
        }
        
        
        for (int i = 0; i < friends.length; i ++){
            if (friends[i] != null){
                if (friends[i].getCreatedTime() != -1){

                    String memberhipPrefix = SN.getMembershipURI(membershipId);
                    createTripleSPO(result, forumPrefix, SNVOC.hasMember, memberhipPrefix);

                    AddTriple(result, true, false, memberhipPrefix, SNVOC.hasPerson, SN.getPersonURI(friends[i].getFriendAcc()));
                    date.setTimeInMillis(friends[i].getCreatedTime());
                    dateString = DateGenerator.formatDateDetail(date);
                    AddTriple(result, false, true, memberhipPrefix, SNVOC.joinDate,
                            createDataTypeLiteral(dateString, XSD.DateTime));

                    membershipId++;
                }
            }
        }
		
		toWriter(result.toString());
	}

	/**
     * @See {@link ldbc.socialnet.dbgen.serializer.Serializer#gatherData(Post)}
     */
	public void gatherData(Post post){
	    StringBuffer result = new StringBuffer(2500);

	    if (post.getIpAddress() != null) {
	        printLocationHierarchy(result, ipDic.getLocation(post.getIpAddress()));
	    }
	    date.setTimeInMillis(post.getCreationDate());
	    String dateString = DateGenerator.formatDateDetail(date);
	    String prefix = SN.getPostURI(post.getPostId());

	    AddTriple(result, true, false, prefix, RDF.type, SNVOC.Post);
	    AddTriple(result, false, false, prefix, SNVOC.creationDate, 
	            createDataTypeLiteral(dateString, XSD.DateTime));
	    if (post.getIpAddress() != null) {
	        AddTriple(result, false, false, prefix, SNVOC.ipaddress, 
	                createLiteral(post.getIpAddress().toString()));
	        if (post.getBrowserIdx() >= 0) {
	            AddTriple(result, false, false, prefix, SNVOC.browser,
	                    createLiteral(browserDic.getName(post.getBrowserIdx())));
	        }
	    }
        if( exportText ) {
	    AddTriple(result, false, false, prefix, SNVOC.content,
	            createLiteral(post.getContent()));
        }
        AddTriple(result, false, true, prefix, SNVOC.length,
                createLiteral(Integer.toString(post.getContent().length())));

	    if (post.getLanguage() != -1) {
	        createTripleSPO(result, prefix, SNVOC.language,
	                createLiteral(languageDic.getLanguagesName(post.getLanguage())));
	    }

	    if (post.getIpAddress() != null) {;
	    createTripleSPO(result, prefix, SNVOC.locatedIn,
	            DBP.fullPrefixed(locationDic.getLocationName((ipDic.getLocation(post.getIpAddress())))));
	    }

	    createTripleSPO(result, SN.getForumURI(post.getGroupId()),SNVOC.containerOf, prefix);

	    createTripleSPO(result, prefix, SNVOC.hasCreator, SN.getPersonURI(post.getAuthorId()));

	    Iterator<Integer> it = post.getTags().iterator();
	    while (it.hasNext()) {
	        Integer tagId = it.next();
	        String tag = tagDic.getName(tagId);
	        createTripleSPO(result, prefix, SNVOC.hasTag, DBP.fullPrefixed(tag));
	        if (!printedTags.containsKey(tagId)) {
	            printedTags.put(tagId, tagId);
	            createTripleSPO(result, DBP.fullPrefixed(tag), RDF.type, SNVOC.Tag);
	            writeTagData(result, tagId, tag);
	        }
	    }

	    int userLikes[] = post.getInterestedUserAccs();
	    long likeTimestamps[] = post.getInterestedUserAccsTimestamp();

	    for (int i = 0; i < userLikes.length; i ++) {
	        String likePrefix = SN.getLikeURI(likeId);
	        createTripleSPO(result, SN.getPersonURI(userLikes[i]), 
	                SNVOC.like, likePrefix);

	        AddTriple(result, true, false, likePrefix, SNVOC.hasPost, prefix);
	        date.setTimeInMillis(likeTimestamps[i]);
	        dateString = DateGenerator.formatDateDetail(date);
	        AddTriple(result, false, true, likePrefix, SNVOC.creationDate,
	                createDataTypeLiteral(dateString, XSD.DateTime));
	        likeId++;
	    }

	    toWriter(result.toString());
	}

	/**
     * @See {@link ldbc.socialnet.dbgen.serializer.Serializer#gatherData(Comment)}
     */
	public void gatherData(Comment comment){
		StringBuffer result = new StringBuffer(2000);
		
		if (comment.getIpAddress() != null) {
            printLocationHierarchy(result, ipDic.getLocation(comment.getIpAddress()));
        }
		
		String prefix = SN.getCommentURI(comment.getCommentId());
		date.setTimeInMillis(comment.getCreationDate());
		String dateString = DateGenerator.formatDateDetail(date); 
		
		AddTriple(result, true, false, prefix, RDF.type, SNVOC.Comment);
		AddTriple(result, false, false, prefix, SNVOC.creationDate, createDataTypeLiteral(dateString, XSD.DateTime));
		if (comment.getIpAddress() != null) {
		    AddTriple(result, false, false, prefix, SNVOC.ipaddress, 
		            createLiteral(comment.getIpAddress().toString()));
		    if (comment.getBrowserIdx() >= 0) {
		        AddTriple(result, false, false, prefix, SNVOC.browser,
		                createLiteral(browserDic.getName(comment.getBrowserIdx())));
		    }
		}
        if( exportText ) {
            AddTriple(result, false, false, prefix, SNVOC.content, createLiteral(comment.getContent()));
        }
        AddTriple(result, false, true, prefix, SNVOC.length,createLiteral(Integer.toString(comment.getContent().length())));

		String replied = (comment.getReplyOf() == -1) ? SN.getPostURI(comment.getPostId()) : 
		    SN.getCommentURI(comment.getReplyOf());
		createTripleSPO(result, prefix, SNVOC.replyOf, replied);
		if (comment.getIpAddress() != null) {
		    createTripleSPO(result, prefix, SNVOC.locatedIn,
		            DBP.fullPrefixed(locationDic.getLocationName((ipDic.getLocation(comment.getIpAddress())))));
		}

		createTripleSPO(result, prefix, SNVOC.hasCreator,
		        SN.getPersonURI(comment.getAuthorId()));

		toWriter(result.toString());
	}
	
	/**
     * @See {@link ldbc.socialnet.dbgen.serializer.Serializer#gatherData(Photo)}
     */
	public void gatherData(Photo photo){
	    StringBuffer result = new StringBuffer(2500);
	    
	    if (photo.getIpAddress() != null) {
            printLocationHierarchy(result, ipDic.getLocation(photo.getIpAddress()));
        }

	    String prefix = SN.getPostURI(photo.getPhotoId());
	    AddTriple(result, true, false, prefix, RDF.type, SNVOC.Post);
	    AddTriple(result, false, false, prefix, SNVOC.hasImage, createLiteral(photo.getImage()));
	    date.setTimeInMillis(photo.getTakenTime());
	    String dateString = DateGenerator.formatDateDetail(date);
	    if (photo.getIpAddress() != null) {
	        AddTriple(result, false, false, prefix, SNVOC.ipaddress, 
	                createLiteral(photo.getIpAddress().toString()));
	        if (photo.getBrowserIdx() >= 0) {
	            AddTriple(result, false, false, prefix, SNVOC.browser,
	                    createLiteral(browserDic.getName(photo.getBrowserIdx())));
	        }
	    }
	    AddTriple(result, false, true, prefix, SNVOC.creationDate, 
	            createDataTypeLiteral(dateString, XSD.DateTime));

	    createTripleSPO(result, prefix, SNVOC.hasCreator, SN.getPersonURI(photo.getCreatorId()));
	    createTripleSPO(result, SN.getForumURI(photo.getAlbumId()), SNVOC.containerOf, prefix);
	    createTripleSPO(result, prefix, SNVOC.locatedIn,
	            DBP.fullPrefixed(locationDic.getLocationName((ipDic.getLocation(photo.getIpAddress())))));

	    Iterator<Integer> it = photo.getTags().iterator();
	    while (it.hasNext()) {
	        Integer tagId = it.next();
	        String tag = tagDic.getName(tagId);
	        createTripleSPO(result, prefix, SNVOC.hasTag, DBP.fullPrefixed(tag));
	        if (!printedTags.containsKey(tagId)) {
	            printedTags.put(tagId, tagId);
	            createTripleSPO(result, DBP.fullPrefixed(tag), RDF.type, SNVOC.Tag);
	            writeTagData(result, tagId, tag);
	        }
	    }

	    int userLikes[] = photo.getInterestedUserAccs();
	    long likeTimestamps[] = photo.getInterestedUserAccsTimestamp();
	    for (int i = 0; i < userLikes.length; i ++) {
	        String likePrefix = SN.getLikeURI(likeId);
	        createTripleSPO(result, SN.getPersonURI(userLikes[i]), 
	                SNVOC.like, likePrefix);

	        AddTriple(result, true, false, likePrefix, SNVOC.hasPost, prefix);
	        date.setTimeInMillis(likeTimestamps[i]);
	        dateString = DateGenerator.formatDateDetail(date);
	        AddTriple(result, false, true, likePrefix, SNVOC.creationDate,
	                createDataTypeLiteral(dateString, XSD.DateTime));
	        likeId++;
	    }

	    toWriter(result.toString());
	}

	/**
	 * @See {@link ldbc.socialnet.dbgen.serializer.Serializer#gatherData(Group)}
	 */
	public void gatherData(Group group){
	    StringBuffer result = new StringBuffer(12000);
	    date.setTimeInMillis(group.getCreatedDate());
	    String dateString = DateGenerator.formatDateDetail(date);  

	    String forumPrefix = SN.getForumURI(group.getForumWallId());
	    AddTriple(result, true, false, forumPrefix, RDF.type, SNVOC.Forum);
	    AddTriple(result, false, false, forumPrefix, SNVOC.title, createLiteral(group.getGroupName()));
	    AddTriple(result, false, true, forumPrefix, SNVOC.creationDate, 
	            createDataTypeLiteral(dateString, XSD.DateTime));

	    createTripleSPO(result, forumPrefix,
	            SNVOC.hasModerator, SN.getPersonURI(group.getModeratorId()));

	    Integer groupTags[] = group.getTags();
	    for (int i = 0; i < groupTags.length; i ++){
	        String tag = tagDic.getName(groupTags[i]);
	        createTripleSPO(result, forumPrefix, SNVOC.hasTag, DBP.fullPrefixed(tag));
	        if (!printedTags.containsKey(groupTags[i])) {
	            printedTags.put(groupTags[i], groupTags[i]);
	            createTripleSPO(result, DBP.fullPrefixed(tag), RDF.type, SNVOC.Tag);
	            writeTagData(result, groupTags[i], tag);
	        }
	    }

	    GroupMemberShip memberShips[] = group.getMemberShips();
	    int numMemberAdded = group.getNumMemberAdded();
	    for (int i = 0; i < numMemberAdded; i ++){

	        String memberhipPrefix = SN.getMembershipURI(membershipId);
	        createTripleSPO(result, forumPrefix, SNVOC.hasMember, memberhipPrefix);

	        AddTriple(result, true, false, memberhipPrefix, SNVOC.hasPerson, SN.getPersonURI(memberShips[i].getUserId()));
	        date.setTimeInMillis(memberShips[i].getJoinDate());
	        dateString = DateGenerator.formatDateDetail(date);
	        AddTriple(result, false, true, memberhipPrefix, SNVOC.joinDate,
	                createDataTypeLiteral(dateString, XSD.DateTime));

	        membershipId++;
	    }
        
	    toWriter(result.toString());
	}
	
	/**
	 * Builds a plain RDF literal.
	 * 
	 * See<a href="http://www.w3.org/TR/rdf-concepts/#section-Literals">RDF literals.</a>
	 * 
	 * @param value: The value.
	 * @return The RDF literal string representation.
	 */
	private String createLiteral(String value) {
		return "\"" + value + "\"";
	}
	
	/**
     * Builds a typed RDF literal.
     * 
     * See<a href="http://www.w3.org/TR/rdf-concepts/#section-Literals">RDF literals.</a>
     * 
     * @param value: The literal value.
     * @param datatypeURI: The data type.
     * @return The RDF typed literal string representation.
     */
	private String createDataTypeLiteral(String value, String datatypeURI) {
		return "\"" + value + "\"^^" + datatypeURI;
	}

	/**
	 * Builds a simple turtle triple: subject predicate object .
	 * 
	 * See <a href="http://www.w3.org/TeamSubmission/turtle/">Turtle</a>
	 * 
	 * @param result: The stringBuffer where the triple representation will be appended to.
	 * @param subject: The RDF subject.
	 * @param predicate: The RDF predicate.
	 * @param object: The RDF object.
	 */
	private void createTripleSPO(StringBuffer result, String subject, String predicate, String object) {
		result.append(subject);		
		result.append(" ");
		result.append(predicate);
		result.append(" ");
		result.append(object);
		result.append(" .\n");
		
		nrTriples++;
	}
	
	/**
     * Builds a subject abbreviated turtle triple.
     * 
     * See <a href="http://www.w3.org/TeamSubmission/turtle/">Turtle</a>
     * 
     * @param result: The stringBuffer where the triple representation will be appended to.
     * @param predicate: The RDF predicate.
     * @param object: The RDF object.
     * @param endSubjectRepeat: The marker to end the subject repetition symbol.
     */
	private void createTriplePO(StringBuffer result, String predicate, String object, boolean endSubjectRepeat) {
		result.append("    ");
		result.append(predicate);
		result.append(" ");
		result.append(object);
		if (endSubjectRepeat) {
            result.append(" .\n");
        } else {
            result.append(" ;\n");
        }
		
		nrTriples++;
	}
	
	/**
     * Builds a subject abbreviated turtle triple with two objects.
     * 
     * See <a href="http://www.w3.org/TeamSubmission/turtle/">Turtle</a>
     * 
     * @param result: The stringBuffer where the triple representation will be appended to.
     * @param predicate: The RDF predicate.
     * @param object1: The first RDF object.
     * @param object2: The second RDF object.
     * @param endSubjectRepeat: The marker to end the subject repetition symbol.
     */
	private void createTriplePOO(StringBuffer result, String predicate, String object1, String object2, boolean endSubjectRepeat) {
		result.append("    ");
		result.append(predicate);
		result.append(" ");
		result.append(object1);
		result.append(" , ");
		result.append(object2);
		if (endSubjectRepeat) {
		    result.append(" .\n");
		} else {
		    result.append(" ;\n");
		}
		
		nrTriples = nrTriples + 2;
	}
	
	/**
	 * Ends the serialization.
	 */
	public void close() {
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
}
