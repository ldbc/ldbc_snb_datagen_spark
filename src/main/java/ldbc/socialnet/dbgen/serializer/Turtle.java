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

import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;
import java.util.zip.GZIPOutputStream;

import ldbc.socialnet.dbgen.dictionary.*;
import ldbc.socialnet.dbgen.generator.DateGenerator;
import ldbc.socialnet.dbgen.objects.*;
import ldbc.socialnet.dbgen.vocabulary.DBP;
import ldbc.socialnet.dbgen.vocabulary.DBPOWL;
import ldbc.socialnet.dbgen.vocabulary.FOAF;
import ldbc.socialnet.dbgen.vocabulary.RDF;
import ldbc.socialnet.dbgen.vocabulary.RDFS;
import ldbc.socialnet.dbgen.vocabulary.SN;
import ldbc.socialnet.dbgen.vocabulary.SNVOC;
import ldbc.socialnet.dbgen.vocabulary.XSD;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Turtle serializer.
 */
public class Turtle implements Serializer {
	
    private static final String STATIC_DBP_DATA_FILE = "static_dbp";
    
	private OutputStream[] dataFileWriter;
	private OutputStream[] staticdbpFileWriter;

	private int currentWriter = 0;
	
	private long nrTriples;
	private GregorianCalendar date;
	
	/**
	 * Generator input classes.
	 */
	private boolean isTurtle;
	private CompanyDictionary companyDic;
    private UniversityDictionary universityDic;
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
    private long knowsId      = 0;
	
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
	 * @param ipDic: The IP dictionary used in the generation.
	 * @param locationDic: The location dictionary used in the generation.
	 * @param languageDic: The language dictionary used in the generation.
	 */
	public Turtle(String file, int reducerId, int nrOfOutputFiles, boolean isTurtle,
            TagDictionary tagDic, BrowserDictionary browsers, 
            CompanyDictionary companies, UniversityDictionary universityDictionary,
            IPAddressDictionary ipDic,  LocationDictionary locationDic, 
            LanguageDictionary languageDic, boolean exportText, boolean compressed) {
	    
	    this.isTurtle = isTurtle;
	    this.tagDic = tagDic;  
        this.browserDic = browsers;
        this.locationDic = locationDic;
        this.companyDic = companies;
        this.universityDic = universityDictionary;
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
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
		    String extension = (isTurtle) ? ".ttl": ".n3";
            if( compressed ) {
                dataFileWriter = new OutputStream[nrOfOutputFiles];
                staticdbpFileWriter = new OutputStream[nrOfOutputFiles];
                if(nrOfOutputFiles==1) {
                    this.dataFileWriter[0] = new GZIPOutputStream( fs.create( new Path(file + "/"+reducerId+"_ldbc_socialnet"+extension+".gz")));
                    this.staticdbpFileWriter[0] = new GZIPOutputStream( fs.create( new Path(file+"/"+reducerId+"_ldbc_socialnet_"+STATIC_DBP_DATA_FILE + extension+".gz")));
                } else {
                    for(int i=1;i<=nrOfOutputFiles;i++) {
                        this.dataFileWriter[i-1] = new GZIPOutputStream( fs.create( new Path(file + "/"+reducerId+"_ldbc_socialnet"+String.format(formatString, i) + extension+".gz")));
                        this.staticdbpFileWriter[i-1] = new GZIPOutputStream( fs.create( new Path(file+"/"+reducerId+"_ldbc_socialnet_"+STATIC_DBP_DATA_FILE + String.format(formatString, i) + extension+".gz")));
                    }
                }
            } else {
                dataFileWriter = new OutputStream[nrOfOutputFiles];
                staticdbpFileWriter = new OutputStream[nrOfOutputFiles];

                if(nrOfOutputFiles==1) {
                    this.dataFileWriter[0] = fs.create( new Path(file +"/"+reducerId+"_ldbc_socialnet"+extension));
                    this.staticdbpFileWriter[0] = fs.create( new Path(file+"/"+reducerId+"_ldbc_socialnet_"+STATIC_DBP_DATA_FILE + extension));
                } else {
                    for(int i=1;i<=nrOfOutputFiles;i++) {
                        this.dataFileWriter[i-1] = fs.create( new Path(file +"/"+reducerId+"_ldbc_socialnet"+ String.format(formatString, i) + extension));
                        this.staticdbpFileWriter[i-1] = fs.create( new Path(file+"/"+reducerId+"_ldbc_socialnet_"+STATIC_DBP_DATA_FILE + String.format(formatString, i) + extension));
                    }
                }

            }
			
			for(int i=0;i<nrOfOutputFiles;i++) {
                toWriter(i,getNamespaces());
                writeDBPData(i,getStaticNamespaces());
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

    @Override
    public void serialize(UserInfo info) {

        StringBuffer result = new StringBuffer(19000);

        if (info.extraInfo == null) {
            System.err.println("LDBC socialnet must serialize the extraInfo");
            System.exit(-1);
        }

        String prefix = SN.getPersonURI(info.user.getAccountId());
        AddTriple(result, true, false, prefix, RDF.type, SNVOC.Person);

        AddTriple(result, false, false, prefix, SNVOC.id,
                createLiteral(Long.toString(info.user.getAccountId())));

        AddTriple(result, false, false, prefix, SNVOC.firstName,
                createLiteral(info.extraInfo.getFirstName()));

        AddTriple(result, false, false, prefix, SNVOC.lastName,
                createLiteral(info.extraInfo.getLastName()));

        if (!info.extraInfo.getGender().equals("")) {
            AddTriple(result, false, false, prefix, SNVOC.gender,
                    createLiteral(info.extraInfo.getGender()));
        }

        if (info.user.getBirthDay() != -1 ){
            date.setTimeInMillis(info.user.getBirthDay());
            String dateString = DateGenerator.formatDate(date);
            AddTriple(result, false, false, prefix, SNVOC.birthday,
                    createDataTypeLiteral(dateString, XSD.Date));
        }

        if (info.user.getIpAddress() != null) {
            AddTriple(result, false, false, prefix, SNVOC.ipaddress,
                    createLiteral(info.user.getIpAddress().toString()));
            if (info.user.getBrowserId() >= 0) {
                AddTriple(result, false, false, prefix, SNVOC.browser,
                        createLiteral(browserDic.getName(info.user.getBrowserId())));
            }
        }

        date.setTimeInMillis(info.user.getCreationDate());
        String dateString = DateGenerator.formatDateDetail(date);
        AddTriple(result, false, true, prefix, SNVOC.creationDate,
                createDataTypeLiteral(dateString, XSD.DateTime));

        createTripleSPO(result, prefix, SNVOC.locatedIn, DBP.fullPrefixed(locationDic.getLocationName(info.extraInfo.getLocationId())));


        Vector<Integer> languages = info.extraInfo.getLanguages();
        for (int i = 0; i < languages.size(); i++) {
            createTripleSPO(result, prefix, SNVOC.speaks,
                    createLiteral(languageDic.getLanguagesName(languages.get(i))));
        }


        Iterator<String> itString = info.extraInfo.getEmail().iterator();
        while (itString.hasNext()){
            String email = itString.next();
            createTripleSPO(result, prefix, SNVOC.email, createLiteral(email));
        }

        Iterator<Integer> itInteger = info.user.getInterests().iterator();
        while (itInteger.hasNext()) {
            Integer interestIdx = itInteger.next();
            String interest = tagDic.getName(interestIdx);
            createTripleSPO(result, prefix, SNVOC.hasInterest, DBP.fullPrefixed(interest));
        }
        toWriter(result.toString());
    }

    @Override
    public void serialize(Friend friend) {
        String prefix = SN.getPersonURI(friend.getUserAcc());
        StringBuffer result = new StringBuffer(19000);
        createTripleSPO(result, prefix, SNVOC.knows, SN.getKnowsURI(knowsId));
        createTripleSPO(result, SN.getKnowsURI(knowsId), SNVOC.hasPerson,
                SN.getPersonURI(friend.getFriendAcc()));
        date.setTimeInMillis(friend.getCreatedTime());
        createTripleSPO(result, SN.getKnowsURI(knowsId), SNVOC.creationDate,
                createDataTypeLiteral(DateGenerator.formatDateDetail(date), XSD.DateTime));
        toWriter(result.toString());
        knowsId++;
    }

    @Override
    public void serialize(Post post) {

        StringBuffer result = new StringBuffer(2500);

        date.setTimeInMillis(post.getCreationDate());
        String dateString = DateGenerator.formatDateDetail(date);
        String prefix = SN.getPostURI(post.getMessageId());

        AddTriple(result, true, false, prefix, RDF.type, SNVOC.Post);

        AddTriple(result, false, false, prefix, SNVOC.id,
                createLiteral(SN.formId(post.getMessageId())));

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
                createLiteral(Integer.toString(post.getTextSize())));

        if (post.getLanguage() != -1) {
            createTripleSPO(result, prefix, SNVOC.language,
                    createLiteral(languageDic.getLanguagesName(post.getLanguage())));
        }

        if (post.getIpAddress() != null) {
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
        }

        toWriter(result.toString());
    }

    @Override
    public void serialize(Like like) {

        StringBuffer result = new StringBuffer(2500);
        String likePrefix = SN.getLikeURI(likeId);
        createTripleSPO(result, SN.getPersonURI(like.user),
                SNVOC.like, likePrefix);

        if( like.type == 0 || like.type == 2 ) {
            String prefix = SN.getPostURI(like.messageId);
            AddTriple(result, true, false, likePrefix, SNVOC.hasPost, prefix);
        } else {
            String prefix = SN.getCommentURI(like.messageId);
            AddTriple(result, true, false, likePrefix, SNVOC.hasComment, prefix);
        }
        date.setTimeInMillis(like.date);
        String dateString = DateGenerator.formatDateDetail(date);
        AddTriple(result, false, true, likePrefix, SNVOC.creationDate,
                createDataTypeLiteral(dateString, XSD.DateTime));
        toWriter(result.toString());
        likeId++;
    }

    @Override
    public void serialize(Photo photo) {

        StringBuffer result = new StringBuffer(2500);


        String prefix = SN.getPostURI(photo.getMessageId());
        AddTriple(result, true, false, prefix, RDF.type, SNVOC.Post);

        AddTriple(result, false, false, prefix, SNVOC.id,
                createLiteral(SN.formId(photo.getMessageId())));

        AddTriple(result, false, false, prefix, SNVOC.hasImage, createLiteral(photo.getContent()));
        date.setTimeInMillis(photo.getCreationDate());
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

        createTripleSPO(result, prefix, SNVOC.hasCreator, SN.getPersonURI(photo.getAuthorId()));
        createTripleSPO(result, SN.getForumURI(photo.getGroupId()), SNVOC.containerOf, prefix);
        createTripleSPO(result, prefix, SNVOC.locatedIn,
                DBP.fullPrefixed(locationDic.getLocationName((ipDic.getLocation(photo.getIpAddress())))));

        Iterator<Integer> it = photo.getTags().iterator();
        while (it.hasNext()) {
            Integer tagId = it.next();
            String tag = tagDic.getName(tagId);
            createTripleSPO(result, prefix, SNVOC.hasTag, DBP.fullPrefixed(tag));
        }
        toWriter(result.toString());
    }

    @Override
    public void serialize(Comment comment) {

        StringBuffer result = new StringBuffer(2000);

        String prefix = SN.getCommentURI(comment.getMessageId());
        date.setTimeInMillis(comment.getCreationDate());
        String dateString = DateGenerator.formatDateDetail(date);

        AddTriple(result, true, false, prefix, RDF.type, SNVOC.Comment);

        AddTriple(result, false, false, prefix, SNVOC.id,
                createLiteral(SN.formId(comment.getMessageId())));

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
        AddTriple(result, false, true, prefix, SNVOC.length,createLiteral(Integer.toString(comment.getTextSize())));

        String replied = (comment.getReplyOf() == comment.getPostId()) ? SN.getPostURI(comment.getPostId()) :
                SN.getCommentURI(comment.getReplyOf());
        createTripleSPO(result, prefix, SNVOC.replyOf, replied);
        if (comment.getIpAddress() != null) {
            createTripleSPO(result, prefix, SNVOC.locatedIn,
                    DBP.fullPrefixed(locationDic.getLocationName((ipDic.getLocation(comment.getIpAddress())))));
        }

        createTripleSPO(result, prefix, SNVOC.hasCreator,
                SN.getPersonURI(comment.getAuthorId()));

        Iterator<Integer> it = comment.getTags().iterator();
        while (it.hasNext()) {
            Integer tagId = it.next();
            String tag = tagDic.getName(tagId);
            createTripleSPO(result, prefix, SNVOC.hasTag, DBP.fullPrefixed(tag));
        }

        toWriter(result.toString());
    }

    @Override
    public void serialize(Group group) {

        StringBuffer result = new StringBuffer(12000);
        date.setTimeInMillis(group.getCreatedDate());
        String dateString = DateGenerator.formatDateDetail(date);

        String forumPrefix = SN.getForumURI(group.getGroupId());
        AddTriple(result, true, false, forumPrefix, RDF.type, SNVOC.Forum);

        AddTriple(result, false, false, forumPrefix, SNVOC.id,
                createLiteral(SN.formId(group.getGroupId())));

        AddTriple(result, false, false, forumPrefix, SNVOC.title, createLiteral(group.getGroupName()));
        AddTriple(result, false, true, forumPrefix, SNVOC.creationDate,
                createDataTypeLiteral(dateString, XSD.DateTime));

        createTripleSPO(result, forumPrefix,
                SNVOC.hasModerator, SN.getPersonURI(group.getModeratorId()));

        Integer groupTags[] = group.getTags();
        for (int i = 0; i < groupTags.length; i ++){
            String tag = tagDic.getName(groupTags[i]);
            createTripleSPO(result, forumPrefix, SNVOC.hasTag, DBP.fullPrefixed(tag));
        }
        toWriter(result.toString());
    }

    @Override
    public void serialize(GroupMemberShip membership) {

        String memberhipPrefix = SN.getMembershipURI(membershipId);
        String forumPrefix = SN.getForumURI(membership.getGroupId());
        StringBuffer result = new StringBuffer(19000);
        createTripleSPO(result, forumPrefix, SNVOC.hasMember, memberhipPrefix);

        AddTriple(result, true, false, memberhipPrefix, SNVOC.hasPerson, SN.getPersonURI(membership.getUserId()));
        date.setTimeInMillis(membership.getJoinDate());
        String dateString = DateGenerator.formatDateDetail(date);
        AddTriple(result, false, true, memberhipPrefix, SNVOC.joinDate,
                createDataTypeLiteral(dateString, XSD.DateTime));
        membershipId++;
        toWriter(result.toString());
    }

    @Override
    public void serialize(WorkAt workAt) {

        String prefix = SN.getPersonURI(workAt.user);
        StringBuffer result = new StringBuffer(19000);
        createTripleSPO(result, prefix, SNVOC.workAt, SN.getWorkAtURI(workAtId));
        createTripleSPO(result, SN.getWorkAtURI(workAtId), SNVOC.hasOrganisation,
                DBP.fullPrefixed(companyDic.getCompanyName(workAt.company)));
        date.setTimeInMillis(workAt.year);
        String yearString = DateGenerator.formatYear(date);
        createTripleSPO(result, SN.getWorkAtURI(workAtId), SNVOC.workFrom,
                createDataTypeLiteral(yearString, XSD.Integer));
        workAtId++;
        toWriter(result.toString());
    }

    @Override
    public void serialize(StudyAt studyAt) {
        String prefix = SN.getPersonURI(studyAt.user);
        StringBuffer result = new StringBuffer(19000);
        createTripleSPO(result, prefix, SNVOC.studyAt, SN.getStudyAtURI(studyAtId));
        createTripleSPO(result, SN.getStudyAtURI(studyAtId), SNVOC.hasOrganisation,
                DBP.fullPrefixed(universityDic.getUniversityName(studyAt.university)));
        date.setTimeInMillis(studyAt.year);
        String yearString = DateGenerator.formatYear(date);
        createTripleSPO(result, SN.getStudyAtURI(studyAtId), SNVOC.classYear,
                createDataTypeLiteral(yearString, XSD.Integer));
            studyAtId++;
        toWriter(result.toString());
    }

    @Override
    public void serialize(Organization organization) {

        StringBuffer result = new StringBuffer(19000);
        writeDBPData(DBP.fullPrefixed(organization.name), RDF.type, DBPOWL.Organisation);
        writeDBPData(DBP.fullPrefixed(organization.name), FOAF.Name, createLiteral(organization.name));
        createTripleSPO(result, DBP.fullPrefixed(organization.name),
                SNVOC.locatedIn, DBP.fullPrefixed(locationDic.getLocationName(organization.location)));
        createTripleSPO(result, DBP.fullPrefixed(organization.name),
                SNVOC.id, createLiteral(Long.toString(organization.id)));
        toWriter(result.toString());
    }

    @Override
    public void serialize(Tag tag) {
        StringBuffer result = new StringBuffer(350);
        writeDBPData(DBP.fullPrefixed(tag.name), FOAF.Name, createLiteral(tag.name));
        Integer tagClass = tag.tagClass;
        writeDBPData(DBP.fullPrefixed(tag.name), RDF.type, DBPOWL.prefixed(tagDic.getClassName(tagClass)));
        createTripleSPO(result, DBP.fullPrefixed(tag.name), SNVOC.id, createLiteral(Long.toString(tag.id)));
        toWriter(result.toString());
    }

    @Override
    public void serialize(Location location) {

        StringBuffer result = new StringBuffer(350);
        String name = location.getName();
        String type = DBPOWL.City;
        if (location.getType() == Location.COUNTRY) {
            type = DBPOWL.Country;
        } else if (location.getType() == Location.CONTINENT) {
            type = DBPOWL.Continent;
        }

        writeDBPData(DBP.fullPrefixed(name), RDF.type, DBPOWL.Place);
        writeDBPData(DBP.fullPrefixed(name), RDF.type, type);
        writeDBPData(DBP.fullPrefixed(name), FOAF.Name, createLiteral(name));
        createTripleSPO(result, DBP.fullPrefixed(name), SNVOC.id, createLiteral(Long.toString(location.getId())));
        if (location.getType() != Location.CONTINENT) {
            String countryName = locationDic.getLocationName(locationDic.belongsTo(location.getId()));
            createTripleSPO(result, DBP.fullPrefixed(name), SNVOC.isPartOf, DBP.fullPrefixed(countryName));
            toWriter(result.toString());
        }

    }

    @Override
    public void serialize(TagClass tagClass) {
        StringBuffer result = new StringBuffer(350);
        if (tagClass.name.equals("Thing")) {
            writeDBPData("<http://www.w3.org/2002/07/owl#Thing>", RDFS.label, createLiteral(tagDic.getClassLabel(tagClass.id)));
            createTripleSPO(result, "<http://www.w3.org/2002/07/owl#Thing>", RDF.type, SNVOC.TagClass);
            createTripleSPO(result, "<http://www.w3.org/2002/07/owl#Thing>", SNVOC.id, Long.toString(tagClass.id));
            toWriter(result.toString());
        } else {
            writeDBPData(DBPOWL.prefixed(tagDic.getClassName(tagClass.id)), RDFS.label, createLiteral(tagDic.getClassLabel(tagClass.id)));
            createTripleSPO(result, DBP.fullPrefixed(tagDic.getClassName(tagClass.id)), RDF.type, SNVOC.TagClass);
            createTripleSPO(result, DBP.fullPrefixed(tagDic.getClassName(tagClass.id)), SNVOC.id, Long.toString(tagClass.id));
            toWriter(result.toString());
        }
        Integer parent = tagDic.getClassParent(tagClass.id);
        if (parent != -1) {
            String parentPrefix;
            if (tagDic.getClassName(parent).equals("Thing")) {
                parentPrefix = "<http://www.w3.org/2002/07/owl#Thing>";
            } else {
                parentPrefix = DBPOWL.prefixed(tagDic.getClassName(parent));
            }
            writeDBPData(DBPOWL.prefixed(tagDic.getClassName(tagClass.id)), RDFS.subClassOf, parentPrefix);
        }
    }

    /**
	 * Writes the collected data to the appropriate file.
	 * 
	 * @param data: The string to write.
	 */
	public void toWriter(int index, String data){
	    try {
            byte[] dataArray =data.getBytes("UTF8");
	        dataFileWriter[index].write(dataArray);
	    } catch(IOException e){
	        System.out.println("Cannot write to output file ");
	        e.printStackTrace();
	        System.exit(-1);
	    }
	}

    public void toWriter(String data){
        toWriter(currentWriter,data);
        currentWriter = (currentWriter + 1) % dataFileWriter.length;
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
//        createPrefixLine(result, SNVOC.PREFIX, SNVOC.NAMESPACE);
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
	private void writeDBPData(int index, String subject, String predicate, String object) {
	    try {
	        StringBuffer result = new StringBuffer(150);
	        createTripleSPO(result, subject, predicate, object);
	        staticdbpFileWriter[index].write(result.toString().getBytes("UTF8"));
	    } catch (IOException e) {
            System.out.println("Cannot write to output file ");
            e.printStackTrace();
        }
	}

    private void writeDBPData(String subject, String predicate, String object) {
        writeDBPData(currentWriter,subject,predicate,object);
    }

    private void writeDBPData(int index, String data) {
        try {
            staticdbpFileWriter[index].write(data.getBytes("UTF8"));
        } catch (IOException e) {
            System.out.println("Cannot write to output file ");
            e.printStackTrace();
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

    public void resetState(long seed) {
        membershipId = 0;
        likeId       = 0;
        workAtId     = 0;
        studyAtId    = 0;
        knowsId      = 0;
    }
}
