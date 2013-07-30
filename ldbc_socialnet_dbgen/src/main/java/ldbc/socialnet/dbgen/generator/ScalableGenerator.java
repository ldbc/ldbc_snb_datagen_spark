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
package ldbc.socialnet.dbgen.generator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Writer;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.Vector;

import ldbc.socialnet.dbgen.dictionary.BrowserDictionary;
import ldbc.socialnet.dbgen.dictionary.CompanyDictionary;
import ldbc.socialnet.dbgen.dictionary.EmailDictionary;
import ldbc.socialnet.dbgen.dictionary.IPAddressDictionary;
import ldbc.socialnet.dbgen.dictionary.LanguageDictionary;
import ldbc.socialnet.dbgen.dictionary.LocationDictionary;
import ldbc.socialnet.dbgen.dictionary.NamesDictionary;
import ldbc.socialnet.dbgen.dictionary.OrganizationsDictionary;
import ldbc.socialnet.dbgen.dictionary.PopularPlacesDictionary;
import ldbc.socialnet.dbgen.dictionary.TagDictionary;
import ldbc.socialnet.dbgen.dictionary.TagMatrix;
import ldbc.socialnet.dbgen.dictionary.TagTextDictionary;
import ldbc.socialnet.dbgen.dictionary.UserAgentDictionary;
import ldbc.socialnet.dbgen.objects.Comment;
import ldbc.socialnet.dbgen.objects.Friend;
import ldbc.socialnet.dbgen.objects.Group;
import ldbc.socialnet.dbgen.objects.GroupMemberShip;
import ldbc.socialnet.dbgen.objects.Photo;
import ldbc.socialnet.dbgen.objects.Post;
import ldbc.socialnet.dbgen.objects.ReducedUserProfile;
import ldbc.socialnet.dbgen.objects.RelationshipStatus;
import ldbc.socialnet.dbgen.objects.UserExtraInfo;
import ldbc.socialnet.dbgen.objects.UserProfile;
import ldbc.socialnet.dbgen.serializer.CSV;
import ldbc.socialnet.dbgen.serializer.Serializer;
import ldbc.socialnet.dbgen.serializer.Turtle;
import ldbc.socialnet.dbgen.storage.MFStoreManager;
import ldbc.socialnet.dbgen.storage.StorageManager;
import ldbc.socialnet.dbgen.vocabulary.SN;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

public class ScalableGenerator{

    public static int  numMaps = -1;
    public static long postId  = -1;
    
    /**
     * How many hadoop jobs are used to generate the friendships
     */
    private static final int NUM_FRIENDSHIP_HADOOP_JOBS = 3;
    /**
     * How many friends (percentagewise) will be generated in each hadoop job.
     */
    private static final double  friendRatioPerJob[] = { 0.45, 0.45, 0.1 };
    
    private static final int startMonth = 1; 
    private static final int startDate  = 1;
    private static final int endMonth   = 1;
    private static final int endDate    = 1;
    
    /**
     * Ceil limit to the random number used to sort the users in the third hadoop job.
     * Creates ids within the interval [0, USER_RANDOM_ID_LIMIT)
     */
    private static final int USER_RANDOM_ID_LIMIT = 100;
    
    //schema behaviour paramaters which are not in the private parameter file.
    //If any of these are upgraded to the parameter file copy the variable elsewhere below,
    //so this list is updated with only the variables not appearing in the parameter file.
    /**
     * Power Law distribution alpha value
     */
    private static final double alpha = 0.4;
    
    private static final int maxNumLikes = 10;
    /**
     * Cumulative probability to join a group for the user direct friends, friends of friends and
     * friends of the friends of the user friend.
     */
    private static final double levelProbs[] = { 0.5, 0.8, 1.0 };
    /**
     * Probability to join a group for the user direct friends, friends of friends and
     * friends of the friends of the user friend.
     */
    private static final double joinProbs[] = { 0.7, 0.4, 0.1 };
    
    
    //Files and folders
    private static final String  DICTIONARY_DIRECTORY = "/dictionaries/";
    private static final String  IPZONE_DIRECTORY     = "/ipaddrByCountries";
    private static final String  PARAMETERS_FILE      = "params.ini";
    private              String  RDF_OUTPUT_FILE      = "ldbc_socialnet_dbg"; //Not static or final: Will be modified to add the machine prefix
    
    // Dictionaries dataset files
    private static final String   browserDicFile         = DICTIONARY_DIRECTORY + "browsersDic.txt";
    private static final String   companiesDicFile       = DICTIONARY_DIRECTORY + "companiesByCountry.txt";
    private static final String   countryAbbrMappingFile = DICTIONARY_DIRECTORY + "countryAbbrMapping.txt";
    private static final String   mainTagDicFile         = DICTIONARY_DIRECTORY + "dicCelebritiesByCountry.txt";
    private static final String   countryDicFile         = DICTIONARY_DIRECTORY + "dicLocation.txt";
    private static final String   tagNamesFile           = DICTIONARY_DIRECTORY + "dicTopic.txt";
    private static final String   emailDicFile           = DICTIONARY_DIRECTORY + "email.txt";
    private static final String   givennamesDicFile      = DICTIONARY_DIRECTORY + "givennameByCountryBirthPlace.txt.freq.full";
    private static final String   organizationsDicFile   = DICTIONARY_DIRECTORY + "institutesCityByCountry.txt";
    private static final String   cityDicFile            = DICTIONARY_DIRECTORY + "institutesCityByCountry.txt";
    private static final String   languageDicFile        = DICTIONARY_DIRECTORY + "languagesByCountry.txt";
    private static final String   popularPlacesDicFile   = DICTIONARY_DIRECTORY + "popularPlacesByCountry.txt";
    private static final String   agentFile              = DICTIONARY_DIRECTORY + "smartPhonesProviders.txt";
    private static final String   surnamesDicFile        = DICTIONARY_DIRECTORY + "surnameByCountryBirthPlace.txt.freq.sort";
    private static final String   tagClassFile           = DICTIONARY_DIRECTORY + "tagClasses.txt";
    private static final String   tagHierarchyFile       = DICTIONARY_DIRECTORY + "tagHierarchy.txt";
    private static final String   tagTextFile            = DICTIONARY_DIRECTORY + "tagText.txt";
    private static final String   topicTagDicFile        = DICTIONARY_DIRECTORY + "topicMatrixId.txt";
    
    //private parameters
    private final String CELL_SIZE                     = "cellSize";
    private final String NUM_CELL_WINDOW               = "numberOfCellPerWindow";
    private final String MIN_FRIENDS                   = "minNumFriends";
    private final String MAX_FRIENDS                   = "maxNumFriends";
    private final String FRIEND_REJECT                 = "friendRejectRatio";
    private final String FRIEND_REACCEPT               = "friendReApproveRatio";
    private final String USER_MIN_TAGS                 = "minNumTagsPerUser";
    private final String USER_MAX_TAGS                 = "maxNumTagsPerUser";
    private final String USER_MAX_POST_MONTH           = "maxNumPostPerMonth";
    private final String MAX_COMMENT_POST              = "maxNumComments";
    private final String LIMIT_CORRELATED              = "limitProCorrelated";
    private final String BASE_CORRELATED               = "baseProbCorrelated";
    private final String MAX_EMAIL                     = "maxEmails";
    private final String MAX_COMPANIES                 = "maxCompanies";
    private final String ENGLISH_RATIO                 = "probEnglish";
    private final String SECOND_LANGUAGE_RATIO         = "probSecondLang";
    private final String OTHER_BROWSER_RATIO           = "probAnotherBrowser";
    private final String MIN_TEXT_SIZE                 = "minTextSize";
    private final String MAX_TEXT_SIZE                 = "maxTextSize";
    private final String MIN_COMMENT_SIZE              = "minCommentSize";
    private final String MAX_COMMENT_SIZE              = "maxCommentSize";
    private final String REDUCE_TEXT_RATIO             = "ratioReduceText";
    private final String MAX_PHOTOALBUM                = "maxNumPhotoAlbumsPerMonth";
    private final String MAX_PHOTO_PER_ALBUM           = "maxNumPhotoPerAlbums";
    private final String USER_MAX_GROUP                = "maxNumGroupCreatedPerUser";
    private final String MAX_GROUP_MEMBERS             = "maxNumMemberGroup";
    private final String GROUP_MODERATOR_RATIO         = "groupModeratorProb";
    private final String GROUP_MAX_POST_MONTH          = "maxNumGroupPostPerMonth";
    private final String MISSING_RATIO                 = "missingRatio";
    private final String STATUS_MISSING_RATIO          = "missingStatusRatio";
    private final String STATUS_SINGLE_RATIO           = "probSingleStatus";
    private final String SMARTHPHONE_RATIO             = "probHavingSmartPhone";
    private final String AGENT_SENT_RATIO              = "probSentFromAgent";
    private final String DIFFERENT_IP_IN_TRAVEL_RATIO  = "probDiffIPinTravelSeason";
    private final String DIFFERENT_IP_NOT_TRAVEL_RATIO = "probDiffIPnotTravelSeason";
    private final String DIFFERENT_IP_TRAVELLER_RATIO  = "probDiffIPforTraveller";
    private final String COMPANY_UNCORRELATED_RATIO    = "probUnCorrelatedCompany";
    private final String UNIVERSITY_UNCORRELATED_RATIO = "probUnCorrelatedOrganization";
    private final String BEST_UNIVERSTY_RATIO          = "probTopUniv";
    private final String MAX_POPULAR_PLACES            = "maxNumPopularPlaces";
    private final String POPULAR_PLACE_RATIO           = "probPopularPlaces";
    private final String TAG_UNCORRELATED_COUNTRY      = "tagCountryCorrProb";
    
    /**
     * This array provides a quick way to check if any of the required parameters is missing and throw the appropriate
     * exception in the method loadParamsFromFile()
     */
    private final String[] checkParameters = {CELL_SIZE, NUM_CELL_WINDOW, MIN_FRIENDS, MAX_FRIENDS, FRIEND_REJECT,
            FRIEND_REACCEPT, USER_MIN_TAGS, USER_MAX_TAGS, USER_MAX_POST_MONTH, MAX_COMMENT_POST, LIMIT_CORRELATED,
            BASE_CORRELATED, MAX_EMAIL, MAX_COMPANIES, ENGLISH_RATIO, SECOND_LANGUAGE_RATIO, OTHER_BROWSER_RATIO,
            MIN_TEXT_SIZE, MAX_TEXT_SIZE, MIN_COMMENT_SIZE, MAX_COMMENT_SIZE, REDUCE_TEXT_RATIO, MAX_PHOTOALBUM,
            MAX_PHOTO_PER_ALBUM, USER_MAX_GROUP, MAX_GROUP_MEMBERS, GROUP_MODERATOR_RATIO, GROUP_MAX_POST_MONTH, 
            MISSING_RATIO, STATUS_MISSING_RATIO, STATUS_SINGLE_RATIO, SMARTHPHONE_RATIO, AGENT_SENT_RATIO, 
            DIFFERENT_IP_IN_TRAVEL_RATIO, DIFFERENT_IP_NOT_TRAVEL_RATIO, DIFFERENT_IP_TRAVELLER_RATIO, 
            COMPANY_UNCORRELATED_RATIO, UNIVERSITY_UNCORRELATED_RATIO, BEST_UNIVERSTY_RATIO, MAX_POPULAR_PLACES, 
            POPULAR_PLACE_RATIO, TAG_UNCORRELATED_COUNTRY};
    
    
    //final user parameters
    private final String NUM_USERS        = "numtotalUser";
    private final String START_YEAR       = "startYear";
    private final String NUM_YEARS        = "numYears";
    private final String SERIALIZER_TYPE  = "serializerType";
    
    /**
     * This array provides a quick way to check if any of the required parameters is missing and throw the appropriate
     * exception in the method loadParamsFromFile()
     */
    private final String[] publicCheckParameters = {NUM_USERS, START_YEAR, NUM_YEARS, SERIALIZER_TYPE};
    
    
    // Gender string representation, both representations vector/standalone so the string is coherent.
    private final String MALE   = "male";
    private final String FEMALE = "female";
    private final String gender[] = { MALE, FEMALE};
    
	// For sliding window
	int 					cellSize; // Number of user in one cell
	int 					numberOfCellPerWindow;
	int	 					numtotalUser;
	int 					windowSize;
	int 					lastCellPos;
	int 					lastCell;
	int 					lastMapCellPos;
	int 					lastMapCell;
	int 					startMapUserIdx;
	int						machineId;
	
	ReducedUserProfile  	reducedUserProfiles[];
	ReducedUserProfile 		reducedUserProfilesCell[];
	
	// For (de-)serialization
	FileOutputStream 		ofUserProfiles;
	ObjectOutputStream 		oosUserProfile;
	FileInputStream 		ifUserProfile;
	ObjectInputStream 		oisUserProfile;

	// For multiple files
	boolean 				isMultipleFile = true;
	int 					numFiles       = 10;
	int                     numCellRead    = 0;
	int 					numCellPerfile;
	int 					numCellInLastFile;
	HashSet<Integer> 		selectedFileIdx;
	
	// For friendship generation
	int 				friendshipNo;
	int 				minNoFriends;
	int 				maxNoFriends;
	double 				friendRejectRatio;
	double 				friendReApproveRatio;
	
	StorageManager	 	storeManager[];
	StorageManager		groupStoreManager; 		
	
	MRWriter			mrWriter; 
	
	Random 				randFriendReject;
	Random 				randFriendReapprov;
	Random 				randInitiator;
	double 				baseProbCorrelated;
	double	 			limitProCorrelated; 
	
	// For each user
	int                 minNoTagsPerUser; 
	int					maxNoTagsPerUser; 
	int 				maxNumPostPerMonth;
	int 				maxNumComments;
	

	// Random values generators
	PowerDistGenerator 	randPowerlaw;
	PowerDistGenerator  randTagPowerlaw;
	Long[] 				seeds;
	Random 				randUniform;
	Random 				randNumInterest;
	Random 				randNumTags;
	Random 				randomFriendIdx;
	Random 				randNumberPost;
	Random 				randNumberComments;
	Random 				randNumberPhotoAlbum;
	Random 				randNumberPhotos;
	Random 				randNumberGroup;
	Random 				randNumberUserPerGroup;
	Random 				randGender;
	Random				randUserRandomIdx;	   // For generating the random dimension
	Random              randNumPopularPlaces; 
    Random              randFriendLevelSelect; // For select an level of moderator's friendship
    Random              randMembership;        // For deciding whether or not a user is joined
    Random              randMemberIdxSelector;
    Random              randGroupMemStep;
    Random              randGroupModerator;    // Decide whether a user can be moderator of groups or not
    Random              randNumberGroupPost;
    Random              randomExtraInfo;
    Random              randomExactLongLat;
    Random              randomHaveStatus;
    Random              randomStatusSingle;
    Random              randomStatus;
    Random              randUserAgent;
    Random              randFileSelect;
    Random              randIdsInWindow;
    
	DateGenerator 		dateTimeGenerator;
	int					startYear;
	int                 endYear;
	
	// Dictionary classes
	LocationDictionary 		locationDic;
	LanguageDictionary      languageDic;
	TagDictionary 			mainTagDic;
	TagTextDictionary       tagTextDic;
	TagMatrix	 			topicTagDic;
	NamesDictionary 		namesDictionary;
	OrganizationsDictionary organizationsDictionary;
	CompanyDictionary       companiesDictionary;
	UserAgentDictionary     userAgentDic;
    EmailDictionary         emailDic;
    BrowserDictionary       browserDic;
    PopularPlacesDictionary popularDictionary;
    IPAddressDictionary     ipAddDictionary;
	
    int                     maxNumPopularPlaces;
	double                  tagCountryCorrProb; 
	double 					probUnCorrelatedOrganization;
	double 					probTopUniv; // 90% users go to top university
	double 					probUnCorrelatedCompany;
	double 					probPopularPlaces;		//probability of taking a photo at popular place

	// For generating texts of posts and comments
	int                     maxEmails;
	int                     maxCompanies;
	double                  probEnglish;
    double                  probSecondLang;

	int 					minTextSize;
	int 					maxTextSize;
	int 					minCommentSize;
	int 					maxCommentSize;
	double 					ratioReduceText; // 80% text has size less than 1/2 max size

	// For photo generator
	PhotoGenerator 			photoGenerator;
	int 					maxNumPhotoAlbumsPerMonth;
	int 					maxNumPhotoPerAlbums;

	// For generating groups
	GroupGenerator 			groupGenerator;
	int 					maxNumGroupCreatedPerUser;
	int 					maxNumMemberGroup;
	double 					groupModeratorProb;

	// For group posts
	int 					maxNumGroupPostPerMonth;


	// For serialize to RDF format
	Serializer 		serializer;
	String 			serializerType;
	String 			outUserProfileName = "userProf.ser";
	String 			outUserProfile;
	int 			numRdfOutputFile = 1;
	boolean 		forwardChaining = false;
	int				mapreduceFileIdx; 
	String 			sibOutputDir;
	String 			sibHomeDir;
	
	// For user's extra info
	double 					missingRatio;
	double 					missingStatusRatio;
	double 					probSingleStatus; // Status "Single" has more probability than others'
	
	
	double 		probAnotherBrowser;
	double 		probHavingSmartPhone;
	double 		probSentFromAgent;

	// The probability that normal user posts from different location
	double 		probDiffIPinTravelSeason; // in travel season
	double 		probDiffIPnotTravelSeason; // not in travel season

	// The probability that travellers post from different location
	double 		probDiffIPforTraveller;
	
	// Writing data for test driver
	OutputDataWriter 	outputDataWriter;
	int 				thresholdPopularUser = 40;
	int 				numPopularUser = 0;
	
	// Data accessed from the hadoop jobs
	public ReducedUserProfile[] cellReducedUserProfiles;
	public int     numUserProfilesRead = 0;
    public int     numUserForNewCell   = 0;
    public int     mrCurCellPost       = 0;
    public int     exactOutput         = 0; 
	
    
	/**
	 * Gets a string representation for a period of time.
	 * 
	 * @param startTime Start time in milliseconds.
	 * @param endTime End time in milliseconds.
	 * @return The time representation in minutes and seconds.
	 */
	public static String getDurationInMinutes(long startTime, long endTime){
       return (endTime - startTime) / 60000 + " minutes and " + 
                ((endTime - startTime) % 60000) / 1000 + " seconds";
    }
	
	/**
	 * Creates the ScalableGenerator
	 * 
	 * @param mapreduceFileId The file id used to pass data for successive hadoop jobs.
	 * @param sibOutputDir The output directory
	 * @param sibHomeDir The ldbc_socialnet_dbgen base directory.
	 */
	public ScalableGenerator(int mapreduceFileId, String sibOutputDir, String sibHomeDir){
		this.mapreduceFileIdx = mapreduceFileId;
		this.sibOutputDir = sibOutputDir;
		this.sibHomeDir = sibHomeDir; 
		
		System.out.println("Map Reduce File Idx is: " + mapreduceFileIdx);
		if (mapreduceFileIdx != -1){
			outUserProfile = "mr" + mapreduceFileIdx + "_" + outUserProfileName;
		}
		
		System.out.println("Current directory in ScaleGenerator is " + sibHomeDir);
	}
	
	public int getCellSize() {
        return cellSize;
    }
	
	/**
	 * Initializes the generator reading the private parameter file, the user parameter file
	 * and initialize all the internal variables.
	 * 
	 * @param numMaps: How many hadoop reduces are performing the job.
	 * @param mapId: The hadoop reduce id.
	 */
	public void init(int numMaps, int mapId){
		
		this.machineId = mapId;
		numFiles = numMaps;
        RDF_OUTPUT_FILE = "mr" + mapreduceFileIdx + "_" + RDF_OUTPUT_FILE;
		
		loadParamsFromFile();		
		_init(mapId, true);
		
		System.out.println("Number of files " + numFiles);
		System.out.println("Number of cells per file " + numCellPerfile);
		System.out.println("Number of cells in last file " + numCellInLastFile);
		
		mrWriter = new MRWriter(cellSize, windowSize, sibOutputDir); 

	}
	
	/**
	 * Reads and loads the private parameter file and the user parameter file.
	 */
	private void loadParamsFromFile() {
		try {
		    //First read the internal params.ini
			Properties properties = new Properties();
			properties.load(new InputStreamReader(getClass( ).getResourceAsStream("/"+PARAMETERS_FILE), "UTF-8"));
			for (int i = 0; i < checkParameters.length; i++) {
			    if (properties.getProperty(checkParameters[i]) == null) {
			        throw new IllegalStateException("Missing " + checkParameters[i] + " parameter");
			    }
			}
			
			cellSize = Short.parseShort(properties.getProperty(CELL_SIZE));
			numberOfCellPerWindow = Integer.parseInt(properties.getProperty(NUM_CELL_WINDOW));
			minNoFriends = Integer.parseInt(properties.getProperty(MIN_FRIENDS));
			maxNoFriends = Integer.parseInt(properties.getProperty(MAX_FRIENDS));
			thresholdPopularUser = (int) (maxNoFriends * 0.9);
			friendRejectRatio = Double.parseDouble(properties.getProperty(FRIEND_REJECT));
			friendReApproveRatio = Double.parseDouble(properties.getProperty(FRIEND_REACCEPT));
			minNoTagsPerUser= Integer.parseInt(properties.getProperty(USER_MIN_TAGS));
			maxNoTagsPerUser= Integer.parseInt(properties.getProperty(USER_MAX_TAGS));
			maxNumPostPerMonth = Integer.parseInt(properties.getProperty(USER_MAX_POST_MONTH));
			maxNumComments = Integer.parseInt(properties.getProperty(MAX_COMMENT_POST));
			limitProCorrelated = Double.parseDouble(properties.getProperty(LIMIT_CORRELATED));
			baseProbCorrelated = Double.parseDouble(properties.getProperty(BASE_CORRELATED));
			maxEmails = Integer.parseInt(properties.getProperty(MAX_EMAIL));
			maxCompanies = Integer.parseInt(properties.getProperty(MAX_EMAIL));
			probEnglish = Double.parseDouble(properties.getProperty(MAX_EMAIL));
			probSecondLang = Double.parseDouble(properties.getProperty(MAX_EMAIL));
			probAnotherBrowser = Double.parseDouble(properties.getProperty(OTHER_BROWSER_RATIO));
			minTextSize = Integer.parseInt(properties.getProperty(MIN_TEXT_SIZE));
			maxTextSize = Integer.parseInt(properties.getProperty(MAX_TEXT_SIZE));
			minCommentSize = Integer.parseInt(properties.getProperty(MIN_COMMENT_SIZE));
			maxCommentSize = Integer.parseInt(properties.getProperty(MAX_COMMENT_SIZE));
			ratioReduceText = Double.parseDouble(properties.getProperty(REDUCE_TEXT_RATIO));
			maxNumPhotoAlbumsPerMonth = Integer.parseInt(properties.getProperty(MAX_PHOTOALBUM));
			maxNumPhotoPerAlbums = Integer.parseInt(properties.getProperty(MAX_PHOTO_PER_ALBUM));
			maxNumGroupCreatedPerUser = Integer.parseInt(properties.getProperty(USER_MAX_GROUP));
			maxNumMemberGroup = Integer.parseInt(properties.getProperty(MAX_GROUP_MEMBERS));
			groupModeratorProb = Double.parseDouble(properties.getProperty(GROUP_MODERATOR_RATIO));
			maxNumGroupPostPerMonth = Integer.parseInt(properties.getProperty(GROUP_MAX_POST_MONTH));
			missingRatio = Double.parseDouble(properties.getProperty(MISSING_RATIO));
			missingStatusRatio = Double.parseDouble(properties.getProperty(STATUS_MISSING_RATIO));
			probSingleStatus = Double.parseDouble(properties.getProperty(STATUS_SINGLE_RATIO));
			probHavingSmartPhone = Double.parseDouble(properties.getProperty(SMARTHPHONE_RATIO));
			probSentFromAgent = Double.parseDouble(properties.getProperty(AGENT_SENT_RATIO));
			probDiffIPinTravelSeason = Double.parseDouble(properties.getProperty(DIFFERENT_IP_IN_TRAVEL_RATIO));
			probDiffIPnotTravelSeason = Double.parseDouble(properties.getProperty(DIFFERENT_IP_NOT_TRAVEL_RATIO));
			probDiffIPforTraveller = Double.parseDouble(properties.getProperty(DIFFERENT_IP_TRAVELLER_RATIO));
			probUnCorrelatedCompany = Double.parseDouble(properties.getProperty(COMPANY_UNCORRELATED_RATIO));
			probUnCorrelatedOrganization = Double.parseDouble(properties.getProperty(UNIVERSITY_UNCORRELATED_RATIO));
			probTopUniv = Double.parseDouble(properties.getProperty(BEST_UNIVERSTY_RATIO));
			maxNumPopularPlaces = Integer.parseInt(properties.getProperty(MAX_POPULAR_PLACES));
			probPopularPlaces = Double.parseDouble(properties.getProperty(POPULAR_PLACE_RATIO));
			tagCountryCorrProb = Double.parseDouble(properties.getProperty(TAG_UNCORRELATED_COUNTRY));
			
		} catch (Exception e) {
		    System.err.println(e.getMessage());
            e.printStackTrace();
            System.exit(-1);
        }
			
		try {
			//read the user param file
            Properties properties = new Properties();
            properties.load(new InputStreamReader(new FileInputStream(sibHomeDir + PARAMETERS_FILE), "UTF-8"));
            for (int i = 0; i < publicCheckParameters.length; i++) {
                if (properties.getProperty(publicCheckParameters[i]) == null) {
                    throw new IllegalStateException("Missing " + publicCheckParameters[i] + " parameter");
                }
            }
            
            int numYears;
			numtotalUser = Integer.parseInt(properties.getProperty(NUM_USERS));
			startYear = Integer.parseInt(properties.getProperty(START_YEAR));
			numYears = Integer.parseInt(properties.getProperty(NUM_YEARS));
			endYear = startYear + numYears;
			serializerType = properties.getProperty(SERIALIZER_TYPE);
			if (!serializerType.equals("ttl") && !serializerType.equals("n3") && 
			        !serializerType.equals("csv")) {
                throw new IllegalStateException("serializerType must be ttl, n3 or csv");
            }
		} catch (Exception e) {
		    System.err.println(e.getMessage());
		    System.exit(-1);
		}
	}

	/**
     * Initializes the internal variables.
     * @param mapId: The hadoop reduce id.
     * @param isFullLoad: if has to initialize all the data.
     */
	private void _init(int mapId, boolean isFullLoad) {
	    
	    if (numtotalUser % cellSize != 0) {
	        System.err.println("Number of users should be a multiple of the cellsize");
	        System.exit(-1);
	    }
	    
	    windowSize = cellSize * numberOfCellPerWindow;
	    
	    lastCellPos     = (numtotalUser - windowSize) / cellSize;
        lastCell        = (numtotalUser / cellSize) - 1; // The last cell of the sliding process
        lastMapCellPos  = (numtotalUser / numMaps - windowSize) / cellSize;
        lastMapCell     = (numtotalUser / (numMaps * cellSize)) - 1;
        startMapUserIdx = (numtotalUser / numMaps) * mapId; 
        
        // For multiple output files
        numCellPerfile    = (lastCell + 1) / numFiles;
        numCellInLastFile = (lastCell + 1) - numCellPerfile * (numFiles - 1);

        if (numCellPerfile < numberOfCellPerWindow) {
            System.err.println("The number of Cell per file should be greater than that of a window ");
            System.exit(-1);
        }

		seedGenerate(mapId);
		randPowerlaw    = new PowerDistGenerator(minNoFriends,     maxNoFriends + 1,     alpha, seeds[2]);
		randTagPowerlaw = new PowerDistGenerator(minNoTagsPerUser, maxNoTagsPerUser + 1, alpha, seeds[2]);
		randUniform = new Random(seeds[3]);
		randGender = new Random(seeds[3]);
		randNumInterest = new Random(seeds[4]);
		randNumTags = new Random(seeds[4]);
		randomFriendIdx = new Random(seeds[6]);
		randFileSelect = new Random(seeds[7]);
		randIdsInWindow = new Random(seeds[8]);
		randNumberPost = new Random(seeds[9]);
		randNumberComments = new Random(seeds[10]);
		randNumberPhotoAlbum = new Random(seeds[11]);
		randNumberPhotos = new Random(seeds[12]);
		randNumberGroup = new Random(seeds[13]);
		randNumberUserPerGroup = new Random(seeds[14]);
		randMemberIdxSelector = new Random(seeds[18]);
		randGroupMemStep = new Random(seeds[19]);
		randFriendLevelSelect = new Random(seeds[20]);
		randMembership = new Random(seeds[21]);
		randGroupModerator = new Random(seeds[22]);
		randomExtraInfo = new Random(seeds[27]);
		randomExactLongLat = new Random(seeds[27]);
		randUserAgent = new Random(seeds[29]);
		randNumberGroupPost = new Random(seeds[36]);
		randFriendReject = new Random(seeds[37]);
		randFriendReapprov = new Random(seeds[38]);
		randInitiator = new Random(seeds[39]);
		randomHaveStatus = new Random(seeds[41]);
		randomStatusSingle = new Random(seeds[42]);
		randomStatus = new Random(seeds[43]);
		randNumPopularPlaces = new Random(seeds[47]);
		randUserRandomIdx = new Random(seeds[48]);

		reducedUserProfiles = new ReducedUserProfile[windowSize];	// Collect of reduced user profile
		cellReducedUserProfiles = new ReducedUserProfile[cellSize];
		
		dateTimeGenerator = new DateGenerator(new GregorianCalendar(startYear, startMonth, startDate), 
		        new GregorianCalendar(endYear, endMonth, endDate), seeds[0], seeds[1], alpha);

		
		if (isFullLoad) {
	
			System.out.println("Building location dictionary ");
			locationDic = new LocationDictionary(numtotalUser, seeds[7] , countryDicFile, cityDicFile);
			locationDic.init();
			
			System.out.println("Building language dictionary ");
			languageDic = new LanguageDictionary(languageDicFile, locationDic.getLocationNameMapping(), 
			        probEnglish, probSecondLang, seeds[11]);
			languageDic.init();
			
			System.out.println("Building Tag dictionary ");
			mainTagDic = new TagDictionary(tagNamesFile, mainTagDicFile, tagClassFile, tagHierarchyFile,
					locationDic.getLocationNameMapping().size(), seeds[5], tagCountryCorrProb);
			mainTagDic.extractTags();
			
			System.out.println("Building Tag-text dictionary ");
			tagTextDic = new TagTextDictionary(tagTextFile, dateTimeGenerator, mainTagDic.getTagsNamesMapping(),
			        minTextSize, maxTextSize, minCommentSize, maxCommentSize, ratioReduceText, seeds[15], seeds[16]);
			tagTextDic.initialize();

			System.out.println("Building Tag Matrix dictionary ");
			topicTagDic = new TagMatrix(topicTagDicFile, mainTagDic.getNumCelebrity() , seeds[5]);
			topicTagDic.initMatrix();
	
			System.out.println("Building IP addresses dictionary ");
			ipAddDictionary = new IPAddressDictionary(countryAbbrMappingFile,
			        IPZONE_DIRECTORY, locationDic.getVecLocations(), seeds[33],
					probDiffIPinTravelSeason, probDiffIPnotTravelSeason,
					probDiffIPforTraveller);
			ipAddDictionary.init();
			
			System.out.println("Building Names dictionary");
			namesDictionary = new NamesDictionary(surnamesDicFile, givennamesDicFile,
					locationDic.getLocationNameMapping(), seeds[23]);
			namesDictionary.init();
			
			System.out.println("Building email dictionary");
			emailDic = new EmailDictionary(emailDicFile, seeds[32]);
			emailDic.init();
	
			System.out.println("Building Group dictionary");
			browserDic = new BrowserDictionary(browserDicFile, seeds[44], probAnotherBrowser);
			browserDic.init();			
			
			System.out.println("Building university dictionary");
			organizationsDictionary = new OrganizationsDictionary(organizationsDicFile, locationDic,
					seeds[24], probUnCorrelatedOrganization, seeds[45], probTopUniv);
			organizationsDictionary.init();
	
			System.out.println("Building companies dictionary");
			companiesDictionary = new CompanyDictionary(companiesDicFile,locationDic, 
			        seeds[40], probUnCorrelatedCompany);
			companiesDictionary.init();
	
			System.out.println("Building popular places dictionary");
			popularDictionary = new PopularPlacesDictionary(popularPlacesDicFile, 
					locationDic.getLocationNameMapping(), seeds[46]);
			popularDictionary.init();
			
			System.out.println("Building user agents dictionary");
			userAgentDic = new UserAgentDictionary(agentFile, seeds[28], seeds[30], probSentFromAgent);
			userAgentDic.init();
			
			System.out.println("Building photo generator");
            photoGenerator = new PhotoGenerator(dateTimeGenerator,
                    locationDic.getVecLocations(), seeds[17], 
                    popularDictionary, probPopularPlaces);
            
            System.out.println("Building Group generator");
            groupGenerator = new GroupGenerator(dateTimeGenerator, locationDic,
                    mainTagDic, numtotalUser, seeds[35]);


			outputDataWriter = new OutputDataWriter();
			serializer = getSerializer(serializerType, RDF_OUTPUT_FILE);
		}
	}
	
	/**
	 * Generates the user activity (Photos, Posts, Comments, Groups) data 
	 * of (numberCell * numUserPerCell) users correspondingto this hadoop job (machineId)
	 * 
	 * @param inputFile The hadoop file with the user serialization (data and friends)
	 * @param numCell The number of cells the generator will parse.
	 */
	public void generateUserActivity(String inputFile, int numCell){
        startWritingUserData();

        long startPostGeneration = System.currentTimeMillis();
        
        //NOTE: Until this point of the code numtotalUser*2 forums where generated (2 for user) thats
        //the reason behind this forum id assignment.
        groupGenerator.setForumId((numtotalUser + 10) * 2);
        generatePostandPhoto(inputFile, numCell);

        long endPostGeneration = System.currentTimeMillis();
        System.out.println("Post generation takes " + getDurationInMinutes(startPostGeneration, endPostGeneration));

        finishWritingUserData();

        long startGroupGeneration = System.currentTimeMillis();
        generateAllGroup(inputFile, numCell);
        long endGroupGeneration = System.currentTimeMillis();
        System.out.println("Group generation takes " + getDurationInMinutes(startGroupGeneration, endGroupGeneration));

        serializer.serialize();

        System.out.println("Number of generated triples " + serializer.triplesGenerated());
        System.out.println("Number of popular users " + numPopularUser);
        System.out.println("Writing the data for test driver ");
        
        writeDataForQGEN();
    }
	
	/**
	 * Generates the post and photo activity for all users.
	 * 
	 * @param inputFile The hadoop file with the user serialization (data and friends)
	 * @param numCells The number of cells the generator will parse.
	 */
	public void generatePostandPhoto(String inputFile, int numCells) {
        
	    reducedUserProfilesCell = new ReducedUserProfile[cellSize];
        StorageManager storeManager = new StorageManager(cellSize, windowSize, outUserProfile, sibOutputDir);
        
        storeManager.initDeserialization(inputFile);
        System.out.println("Generating the posts & comments ");
        System.out.println("Number of cells in file : " + numCells);
        
        for (int i = 0; i < numCells; i++) {
            storeManager.deserializeOneCellUserProfile(reducedUserProfilesCell);
            
            for (int j = 0; j < cellSize; j++) {
                UserExtraInfo extraInfo = new UserExtraInfo();
                setInfoFromUserProfile(reducedUserProfilesCell[j], extraInfo);
                serializer.gatherData(reducedUserProfilesCell[j], extraInfo);

                generatePosts(reducedUserProfilesCell[j], extraInfo);
                generatePhoto(reducedUserProfilesCell[j], extraInfo);
            }
        }
        
        storeManager.endDeserialization();
        System.out.println("Done generating the posts and photos....");
        System.out.println("Number of deserialized objects is " + storeManager.getNumberDeSerializedObject());
    }
	
	/**
	 * Generates the Groups and it's posts and comments for all users.
	 * 
	 * @param inputFile The user information serialization file from previous jobs
	 * @param numCell The number of cells to process
	 */
	public void generateAllGroup(String inputFile, int numCell) {
		groupStoreManager = new StorageManager(cellSize, windowSize, outUserProfile, sibOutputDir);
		
		groupStoreManager.initDeserialization(inputFile);
		
		for (int i = 0; i < numCell; i++) {
		    generateGroups(4, i, numCell);
		}

		groupStoreManager.endDeserialization();
		
		System.out.println("Done generating user groups and groups' posts");
		System.out.println("Number of deserialized objects for group is " + groupStoreManager.getNumberDeSerializedObject());
	}
	
	/**
	 * Generates the groups for a single user.
	 * 
	 * @param pass The algorithm number step.
	 * @param cellPos The cell position of the user.
	 * @param numCell The total number of cells.
	 */
	public void generateGroups(int pass, int cellPos, int numCell) {

        int newCellPosInWindow = cellPos % numberOfCellPerWindow;
        int newIdxInWindow = newCellPosInWindow * cellSize;
        int newStartIndex = (cellPos % numberOfCellPerWindow) * cellSize;
        
        groupStoreManager.deserializeOneCellUserProfile(newIdxInWindow, cellSize, reducedUserProfiles);
        
        for (int i = 0; i < cellSize; i++) {
            int curIdxInWindow = newStartIndex + i;
            double moderatorProb = randGroupModerator.nextDouble();
            
            if (moderatorProb > groupModeratorProb) {
                continue;
            }

            Friend firstLevelFriends[] = reducedUserProfiles[curIdxInWindow].getFriendList();
            Vector<Friend> secondLevelFriends = new Vector<Friend>();
            //TODO: Include friends of friends a.k.a second level friends.

            int numGroup = randNumberGroup.nextInt(maxNumGroupCreatedPerUser);
            for (int j = 0; j < numGroup; j++) { 
                createGroupForUser(reducedUserProfiles[curIdxInWindow],
                        firstLevelFriends, secondLevelFriends);
            }
        }
    }
	
	public void pushUserProfile(ReducedUserProfile reduceUser, int pass, 
			Reducer<IntWritable, ReducedUserProfile,IntWritable, ReducedUserProfile>.Context context, 
			boolean isContext, ObjectOutputStream oos){

		numUserProfilesRead++;
		ReducedUserProfile userObject = new ReducedUserProfile();
		userObject.copyFields(reduceUser);
		
		if (numUserProfilesRead < windowSize) {
			if (reducedUserProfiles[numUserProfilesRead-1] != null){
				reducedUserProfiles[numUserProfilesRead-1].clear();
				reducedUserProfiles[numUserProfilesRead-1] = null;
			}
			reducedUserProfiles[numUserProfilesRead-1] = userObject;
		} else if (numUserProfilesRead == windowSize) {
			if (reducedUserProfiles[numUserProfilesRead-1] != null){
				reducedUserProfiles[numUserProfilesRead-1].clear();
				reducedUserProfiles[numUserProfilesRead-1] = null;
			}
			reducedUserProfiles[numUserProfilesRead-1] = userObject;
			mr2InitFriendShipWindow(pass, context, isContext, oos);
		} else {
			numUserForNewCell++;
			
			if (cellReducedUserProfiles[numUserForNewCell-1] != null){
				cellReducedUserProfiles[numUserForNewCell-1] = null;
			}
			
			cellReducedUserProfiles[numUserForNewCell-1] = userObject;
			if (numUserForNewCell == cellSize){
				mrCurCellPost++;
				mr2SlideFriendShipWindow(pass,mrCurCellPost, context, cellReducedUserProfiles,
						 isContext,  oos);
				
				numUserForNewCell = 0;
			}
		}
		
		if (reduceUser != null){
			reduceUser = null;
		}
	}
	public void pushAllRemainingUser(int pass, 
			Reducer<IntWritable, ReducedUserProfile,IntWritable, ReducedUserProfile>.Context context, 
			boolean isContext, ObjectOutputStream oos){
	    
	    for (int numLeftCell = numberOfCellPerWindow - 1; numLeftCell > 0; numLeftCell--) {
			mr2SlideLastCellsFriendShip(pass, ++mrCurCellPost, numLeftCell, context, isContext,  oos);
		}
	}
	
	public void mrGenerateUserInfo(int pass, Context context, int mapIdx){
		
		int numToGenerateUser = (numMaps == mapIdx) ? numCellInLastFile : numCellPerfile;
		numToGenerateUser = numToGenerateUser * cellSize;
		
		System.out.println("numToGenerateUser in Machine " + mapIdx + " = " + numToGenerateUser);
		int numZeroPopularPlace = 0; 
		
		for (int i = 0; i < numToGenerateUser; i++) {
			UserProfile user = generateGeneralInformation(i + startMapUserIdx); 
			ReducedUserProfile reduceUserProf = new ReducedUserProfile(user, NUM_FRIENDSHIP_HADOOP_JOBS);
			if (reduceUserProf.getNumPopularPlace() == 0) {
			    numZeroPopularPlace++;
			}
			user = null;
			try {
				context.write(new IntWritable(reduceUserProf.getDicElementId(pass)), reduceUserProf);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		System.out.println("Number of Zero popular places: " + numZeroPopularPlace);
	}
	
	public void generateCellOfUsers2(int newStartIndex, ReducedUserProfile[] _cellReduceUserProfiles){
		int curIdxInWindow;
		for (int i = 0; i < cellSize; i++) {
			curIdxInWindow = newStartIndex + i;
			if (reducedUserProfiles[curIdxInWindow] != null){
				reducedUserProfiles[curIdxInWindow].clear();
				reducedUserProfiles[curIdxInWindow] = null;
			}
			reducedUserProfiles[curIdxInWindow] = _cellReduceUserProfiles[i];
		}
	}
	
	public void mr2InitFriendShipWindow(int pass, Reducer.Context context, boolean isContext, ObjectOutputStream oos){
	    
		for (int i = 0; i < cellSize; i++) {
			// From this user, check all the user in the window to create friendship
			for (int j = i + 1; j < windowSize - 1; j++) {
				if (reducedUserProfiles[i].getNumFriendsAdded() 
						== reducedUserProfiles[i].getNumFriends(pass)) {
					break;
				}
				if (reducedUserProfiles[j].getNumFriendsAdded() 
						== reducedUserProfiles[j].getNumFriends(pass)) {
					continue;
				}

                if (reducedUserProfiles[i].isExistFriend(
                		reducedUserProfiles[j].getAccountId())) {
                    continue;
                }

				// Generate a random value
				double randProb = randUniform.nextDouble();
				
				double prob = getFriendCreatePro(i, j, pass);  
				
				if ((randProb < prob) || (randProb < limitProCorrelated)) {
					// add a friendship
					createFriendShip(reducedUserProfiles[i], reducedUserProfiles[j], (byte) pass);
				}

			}
		}

		updateLastPassFriendAdded(0, cellSize, pass);

		mrWriter.writeReducedUserProfiles(0, cellSize, pass, reducedUserProfiles, context, isContext, oos);
		
		exactOutput = exactOutput + cellSize; 
	}

	public void mr2SlideFriendShipWindow(int pass, int cellPos, Reducer.Context context, ReducedUserProfile[] _cellReduceUserProfiles, 
			boolean isContext, ObjectOutputStream oos){

		// In window, position of new cell = the position of last removed cell = cellPos - 1
		int newCellPosInWindow = (cellPos - 1) % numberOfCellPerWindow;

		int newStartIndex = newCellPosInWindow * cellSize;
		
		int curIdxInWindow;

		// Init the number of friends for each user in the new cell
		generateCellOfUsers2(newStartIndex, _cellReduceUserProfiles);

		// Create the friendships
		// Start from each user in the first cell of the window --> at the
		// cellPos, not from the new cell
		newStartIndex = (cellPos % numberOfCellPerWindow) * cellSize;
		for (int i = 0; i < cellSize; i++) {
			curIdxInWindow = newStartIndex + i;
			// Generate set of friends list

			// Here assume that all the users in the window including the new
			// cell have the number of friends
			// and also the number of friends to add

			double randProb;

			if (reducedUserProfiles[curIdxInWindow].getNumFriendsAdded() 
					== reducedUserProfiles[curIdxInWindow].getNumFriends(pass)) {
				continue;
			}

			// From this user, check all the user in the window to create friendship
			for (int j = i + 1; (j < windowSize - 1)
					&& reducedUserProfiles[curIdxInWindow].getNumFriendsAdded() 
					< reducedUserProfiles[curIdxInWindow].getNumFriends(pass); j++) {

				int checkFriendIdx = (curIdxInWindow + j) % windowSize;

				if (reducedUserProfiles[checkFriendIdx].getNumFriendsAdded() 
						== reducedUserProfiles[checkFriendIdx].getNumFriends(pass)) {
					continue;
				}

                if (reducedUserProfiles[curIdxInWindow].isExistFriend(
                		reducedUserProfiles[checkFriendIdx].getAccountId())) {
                    continue;
                }
                
                
                // Generate a random value
				randProb = randUniform.nextDouble();
				
				double prob = getFriendCreatePro(curIdxInWindow, checkFriendIdx, pass);
						
				if ((randProb < prob) || (randProb < limitProCorrelated)) {
					// add a friendship
					createFriendShip(reducedUserProfiles[curIdxInWindow], reducedUserProfiles[checkFriendIdx],
							(byte) pass);
				}
			}

		}

		updateLastPassFriendAdded(newStartIndex, newStartIndex + cellSize, pass);
		mrWriter.writeReducedUserProfiles(newStartIndex, newStartIndex + cellSize, pass, reducedUserProfiles, context, 
				isContext, oos);
		
		exactOutput = exactOutput + cellSize; 
	}
	
	public void mr2SlideLastCellsFriendShip(int pass, int cellPos,	int numleftCell, Reducer.Context context,
			boolean isContext, ObjectOutputStream oos) {

		int newStartIndex;

		int curIdxInWindow;

		newStartIndex = (cellPos % numberOfCellPerWindow) * cellSize;
		
		for (int i = 0; i < cellSize; i++) {
			curIdxInWindow = newStartIndex + i;
			// Generate set of friends list

			// Here assume that all the users in the window including the new
			// cell have the number of friends
			// and also the number of friends to add

			double randProb;

			if (reducedUserProfiles[curIdxInWindow].getNumFriendsAdded() 
					== reducedUserProfiles[curIdxInWindow].getNumFriends(pass)) {
				continue;
			}

			// From this user, check all the user in the window to create friendship
			for (int j = i + 1; (j < numleftCell * cellSize - 1)
					&& reducedUserProfiles[curIdxInWindow].getNumFriendsAdded() 
					   < reducedUserProfiles[curIdxInWindow].getNumFriends(pass); j++) {

				int checkFriendIdx = (curIdxInWindow + j) % windowSize;

				if (reducedUserProfiles[checkFriendIdx].getNumFriendsAdded() 
						== reducedUserProfiles[checkFriendIdx].getNumFriends(pass)) {
					continue;
				}

                if (reducedUserProfiles[curIdxInWindow].isExistFriend(
                		reducedUserProfiles[checkFriendIdx].getAccountId())) {
                    continue;
                }
                
                
				// Generate a random value
				randProb = randUniform.nextDouble();

				double prob = getFriendCreatePro(curIdxInWindow, checkFriendIdx, pass);
						
				if ((randProb < prob) || (randProb < limitProCorrelated)) {
					// add a friendship
					createFriendShip(reducedUserProfiles[curIdxInWindow], reducedUserProfiles[checkFriendIdx],
							(byte) pass);
				}				
			}

		}

		updateLastPassFriendAdded(newStartIndex, newStartIndex + cellSize, pass);
		mrWriter.writeReducedUserProfiles(newStartIndex, newStartIndex + cellSize, pass, reducedUserProfiles, context,
				isContext, oos);
		exactOutput = exactOutput + cellSize; 

	}

	public void generatePosts(ReducedUserProfile user, UserExtraInfo extraInfo){
		// Generate location-related posts
		int numPosts = getNumOfPost(user);
		for (int m = 0; m < numPosts; m++) {
			Post post = tagTextDic.createPost(user, maxNumLikes, userAgentDic, ipAddDictionary, browserDic);
			Integer languageIndex = randUniform.nextInt(extraInfo.getLanguages().size());
			post.setLanguage(extraInfo.getLanguages().get(languageIndex));

			serializer.gatherData(post);
			
			// Generate comments
			int numComment = randNumberComments.nextInt(maxNumComments);
			long lastCommentCreatedDate = post.getCreatedDate();
			long lastCommentId = -1;
			long startCommentId = TagTextDictionary.commentId;
			for (int l = 0; l < numComment; l++) {
				Comment comment = tagTextDic.createComment(post, user, lastCommentCreatedDate, startCommentId, lastCommentId, 
				        userAgentDic, ipAddDictionary, browserDic);
				if (comment.getAuthorId() != -1) {
					serializer.gatherData(comment);
					
					lastCommentCreatedDate = comment.getCreateDate();
					lastCommentId = comment.getCommentId();
				}
			}
		}
	}
	
	public void generatePhoto(ReducedUserProfile user, UserExtraInfo extraInfo){
		// Generate photo Album and photos
		int numOfmonths = (int) dateTimeGenerator.numberOfMonths(user);
		int numPhotoAlbums = randNumberPhotoAlbum.nextInt(maxNumPhotoAlbumsPerMonth);
		if (numOfmonths != 0) {
			numPhotoAlbums = numOfmonths * numPhotoAlbums;
		}
		
		for (int m = 0; m < numPhotoAlbums; m++) {
		    Group album = groupGenerator.createAlbum(user, extraInfo, m, randMembership, joinProbs[0]);
			
			serializer.gatherData(album);
			
			// Generate photos for this album
			int numPhotos = randNumberPhotos.nextInt(maxNumPhotoPerAlbums);
			for (int l = 0; l < numPhotos; l++) {
				Photo photo = photoGenerator.generatePhoto(user, album, l, maxNumLikes);

				photo.setUserAgent(userAgentDic.getUserAgentName(user.isHaveSmartPhone(), user.getAgentIdx()));
				photo.setBrowserIdx(browserDic.getPostBrowserId(user.getBrowserIdx()));
				ipAddDictionary.setPhotoIPAdress(user.isFrequentChange(), user.getIpAddress(), photo);
				
				serializer.gatherData(photo);
			}
		}
	}

	public void createGroupForUser(ReducedUserProfile user,
			Friend firstLevelFriends[], Vector<Friend> secondLevelFriends) {
		double randLevelProb;
		double randMemberProb;

		Group group = groupGenerator.createGroup(user);

		HashSet<Integer> memberIds = new HashSet<Integer>();

		int numGroupMember = randNumberUserPerGroup.nextInt(maxNumMemberGroup);
		group.initAllMemberships(numGroupMember);
		
		int numLoop = 0;
		while ((group.getNumMemberAdded() < numGroupMember) && (numLoop < windowSize)) {

			numLoop++;
			randLevelProb = randFriendLevelSelect.nextDouble();

			// Select the appropriate friend level
			if (randLevelProb < levelProbs[0]) { // ==> level 1
				// Find a friendIdx
				int friendIdx = randMemberIdxSelector.nextInt(user.getNumFriendsAdded());
				// Note: Use user.getNumFriendsAdded(), do not use
				// firstLevelFriends.length
				// because we allocate a array for friendLists, but do not
				// guarantee that
				// all the element in this array contain values

				int potentialMemberAcc = firstLevelFriends[friendIdx].getFriendAcc();

				randMemberProb = randMembership.nextDouble();
				if (randMemberProb < joinProbs[0]) {
					// Check whether this user has been added and then add to the group
					if (!memberIds.contains(potentialMemberAcc)) {
						memberIds.add(potentialMemberAcc);
						// Assume the earliest membership date is the friendship created date
						GroupMemberShip memberShip = groupGenerator.createGroupMember(
						        potentialMemberAcc, group.getCreatedDate(),
						        firstLevelFriends[friendIdx]);
						group.addMember(memberShip);
					}
				}
			} else if (randLevelProb < levelProbs[1]) { // ==> level 2
				if (secondLevelFriends.size() == 0)
					continue;

				int friendIdx = randMemberIdxSelector.nextInt(secondLevelFriends.size());
				int potentialMemberAcc = secondLevelFriends.get(friendIdx).getFriendAcc();
				randMemberProb = randMembership.nextDouble();
				if (randMemberProb < joinProbs[1]) {
					// Check whether this user has been added and then add to the group
					if (!memberIds.contains(potentialMemberAcc)) {
						memberIds.add(potentialMemberAcc);
						// Assume the earliest membership date is the friendship created date
						GroupMemberShip memberShip = groupGenerator.createGroupMember(
						        potentialMemberAcc, group.getCreatedDate(), 
						        secondLevelFriends.get(friendIdx));
						group.addMember(memberShip);
					}
				}
			} else { // ==> random users
				// Select a user from window
				int friendIdx = randMemberIdxSelector.nextInt(windowSize);
				int potentialMemberAcc = reducedUserProfiles[friendIdx].getAccountId();
				randMemberProb = randMembership.nextDouble();
				if (randMemberProb < joinProbs[2]) {
					// Check whether this user has been added and then add to the group
					if (!memberIds.contains(potentialMemberAcc)) {
						memberIds.add(potentialMemberAcc);
						GroupMemberShip memberShip = groupGenerator.createGroupMember(
						        potentialMemberAcc, group.getCreatedDate(),
						        reducedUserProfiles[friendIdx]);
						group.addMember(memberShip);
					}
				}
			}
		}

		serializer.gatherData(group);
		generatePostForGroup(group);
	}

	public void generatePostForGroup(Group group) {
		int numberGroupPost = getNumOfGroupPost(group);
		for (int i = 0; i < numberGroupPost; i++) {
			Post groupPost = tagTextDic.createPost(group, maxNumLikes, userAgentDic, ipAddDictionary, browserDic);
//			Integer languageIndex = randUniform.nextInt(extraInfo.getLanguages().size());
//            post.setLanguage(extraInfo.getLanguages().get(languageIndex));
			groupPost.setLanguage(-1);

			serializer.gatherData(groupPost);


			int numComment = randNumberComments.nextInt(maxNumComments);
			long lastCommentCreatedDate = groupPost.getCreatedDate();
			long lastCommentId = -1;
			long startCommentId = TagTextDictionary.commentId;

			for (int j = 0; j < numComment; j++) {
				Comment comment = tagTextDic.createComment(groupPost, group, lastCommentCreatedDate, startCommentId, lastCommentId, 
                        userAgentDic, ipAddDictionary, browserDic);
				if (comment.getAuthorId() != -1) { // In case the comment is not reated because of the friendship's createddate
					
					serializer.gatherData(comment);

					lastCommentCreatedDate = comment.getCreateDate();
					lastCommentId = comment.getCommentId();
				}
			}
		}
	}

	// User has more friends will have more posts
	// Thus, the number of post is calculated according
	// to the number of friends and createdDate of a user
	public int getNumOfPost(ReducedUserProfile user) {
		int numOfmonths = (int) dateTimeGenerator.numberOfMonths(user);
		int numberPost;
		if (numOfmonths == 0) {
			numberPost = randNumberPost.nextInt(maxNumPostPerMonth);
		} else {
			numberPost = randNumberPost.nextInt(maxNumPostPerMonth * numOfmonths);
		}

		numberPost = (numberPost * user.getNumFriendsAdded()) / maxNoFriends;

		return numberPost;
	}

	public int getNumOfGroupPost(Group group) {
	    
		int numOfmonths = (int) dateTimeGenerator.numberOfMonths(group.getCreatedDate());

		int numberPost;
		if (numOfmonths == 0) {
			numberPost = randNumberGroupPost.nextInt(maxNumGroupPostPerMonth);
		} else {
			numberPost = randNumberGroupPost.nextInt(maxNumGroupPostPerMonth * numOfmonths);
		}

		numberPost = (numberPost * group.getNumMemberAdded()) / maxNumMemberGroup;

		return numberPost;
	}

	public void seedGenerate(int mapIdx) {
		seeds = new Long[50];
		Random seedRandom = new Random(53223436L + 1234567*mapIdx);
		for (int i = 0; i < 50; i++) {
			seeds[i] = seedRandom.nextLong();
		}
	}

	public UserProfile generateGeneralInformation(int accountId) {
		UserProfile userProf = new UserProfile(accountId);

		userProf.setCreatedDate(dateTimeGenerator.randomDateInMillis());
		
		userProf.setNumFriends((short) randPowerlaw.getValue());
		userProf.allocateFriendListMemory(NUM_FRIENDSHIP_HADOOP_JOBS);
		
		short totalFriendSet = 0; 
		for (int i = 0; i < NUM_FRIENDSHIP_HADOOP_JOBS-1; i++){
			short numPassFriend = (short) Math.floor(friendRatioPerJob[0] * userProf.getNumFriends());
			totalFriendSet = (short) (totalFriendSet + numPassFriend);
			userProf.setNumPassFriends(totalFriendSet,i);
			
		}
		// Prevent the case that the number of friends added exceeds the total number of friends
		userProf.setNumPassFriends(userProf.getNumFriends(),NUM_FRIENDSHIP_HADOOP_JOBS-1);

		userProf.setNumFriendsAdded((short) 0);
		userProf.setLocationIdx(locationDic.getLocation(accountId));
		userProf.setCityIdx(locationDic.getRandomCity(userProf.getLocationIdx()));
		userProf.setLocationZId(locationDic.getZorderID(userProf.getLocationIdx()));
		
		
		int userMainTag = mainTagDic.getaTagByCountry(userProf.getLocationIdx());
		
		userProf.setMainTagId(userMainTag);
		
		userProf.setNumTags((short) randTagPowerlaw.getValue());
		
		userProf.setSetOfTags(topicTagDic.getSetofTags(userMainTag, userProf.getNumTags()));

		userProf.setLocationOrganizationId(organizationsDictionary.getRandomOrganization(userProf.getLocationIdx()));

		userProf.setBirthDay(dateTimeGenerator.getBirthDay(userProf.getCreatedDate()));

		byte gender = (randGender.nextDouble() > 0.5) ? (byte)1 : (byte)0;
		userProf.setGender(gender);
		
		userProf.setForumWallId(accountId * 2); // Each user has an wall
		userProf.setForumStatusId(accountId * 2 + 1);

		// User's Agent
		userProf.setHaveSmartPhone(randUserAgent.nextDouble() > probHavingSmartPhone);
		if (userProf.isHaveSmartPhone()) {
			userProf.setAgentIdx(userAgentDic.getRandomUserAgentIdx());
		}

		// User's browser
		userProf.setBrowserIdx(browserDic.getRandomBrowserId());

		// source IP
		userProf.setIpAddress(ipAddDictionary
				.getRandomIPAddressFromLocation(userProf.getLocationIdx()));

		// Popular places 
		byte numPopularPlaces = (byte) randNumPopularPlaces.nextInt(maxNumPopularPlaces + 1);
		userProf.setNumPopularPlace(numPopularPlaces);
		short popularPlaces[] = new short[numPopularPlaces];
		for (int i = 0; i < numPopularPlaces; i++){
			popularPlaces[i] = popularDictionary.getPopularPlace(userProf.getLocationIdx());
			if (popularPlaces[i] == -1){ 	// no popular place here
				userProf.setNumPopularPlace((byte)0);
				break;
			}
		}
		userProf.setPopularPlaceIds(popularPlaces);
		
		// Get random Idx
		userProf.setRandomIdx(randUserRandomIdx.nextInt(USER_RANDOM_ID_LIMIT));

		return userProf;
	}
	

	public void setInfoFromUserProfile(ReducedUserProfile user,
			UserExtraInfo userExtraInfo) {

		// The country will be always present, but the city can be missing if that data is 
		// not available on the dictionary
		int locationId = (user.getCityIdx() != -1) ? user.getCityIdx() : user.getLocationIdx();
		userExtraInfo.setLocationId(locationId);
		if (user.getCityIdx() != -1) {
		    userExtraInfo.setLocation(locationDic.getCityName(user.getCityIdx()));
		} else {
		    userExtraInfo.setLocation(locationDic.getLocationName(user.getLocationIdx()));
		}
		
		// We consider that the distance from where user is living and 
		double distance = randomExactLongLat.nextDouble() * 2;  
		userExtraInfo.setLatt(locationDic.getLatt(user.getLocationIdx()) + distance);
		userExtraInfo.setLongt(locationDic.getLongt(user.getLocationIdx()) + distance);
		
		userExtraInfo.setOrganization(
				organizationsDictionary.getOrganizationName(user.getLocationOrganizationIdx()));
		
		// Relationship status
		if (randomHaveStatus.nextDouble() > missingStatusRatio) {

		    if (randomStatusSingle.nextDouble() < probSingleStatus) {
		        userExtraInfo.setStatus(RelationshipStatus.SINGLE);
		        userExtraInfo.setSpecialFriendIdx(-1);
		    } else {
		        // The two first status, "NO_STATUS" and "SINGLE", are not included
		        int statusIdx = randomStatus.nextInt(RelationshipStatus.values().length - 2) + 2;
		        userExtraInfo.setStatus(RelationshipStatus.values()[statusIdx]);

		        // Select a special friend
		        Friend friends[] = user.getFriendList();

		        int relationid = -1;
		        if (user.getNumFriendsAdded() > 0) {
		            int specialFriendId = 0;
		            int numFriendCheck = 0;

		            do {
		                specialFriendId = randomHaveStatus.nextInt(user
		                        .getNumFriendsAdded());
		                numFriendCheck++;
		            } while (friends[specialFriendId].getCreatedTime() == -1
		                    && numFriendCheck < friends.length);

		            if (friends[specialFriendId].getCreatedTime() != -1) {
		                relationid = friends[specialFriendId].getFriendAcc();
		            }
		        }
		        userExtraInfo.setSpecialFriendIdx(relationid);
		    }
		} else {
		    userExtraInfo.setStatus(RelationshipStatus.NOSTATUS);
		}


		boolean isMale;
		if (user.getGender() == 1) {
			isMale = true;
			userExtraInfo.setGender(gender[0]); // male
		} else {
			isMale = false;
			userExtraInfo.setGender(gender[1]); // female
		}
		
		userExtraInfo.setFirstName(namesDictionary.getRandomGivenName(
				user.getLocationIdx(),isMale, 
				dateTimeGenerator.getBirthYear(user.getBirthDay())));
		
		userExtraInfo.setLastName(namesDictionary.getRandomSurName(user.getLocationIdx()));

		// email is created by using the user's first name + userId
		int numEmails = randomExtraInfo.nextInt(maxEmails) + 1;
		double prob = randomExtraInfo.nextDouble();
		if (prob >= missingRatio) {
			String base = userExtraInfo.getFirstName().replaceAll(" ", ".");
			for (int i = 0; i < numEmails; i++) {
			    String email = base + "" + user.getAccountId() + "@" + emailDic.getRandomEmail();
			    userExtraInfo.addEmail(email);
			}
		}
		
		// Set class year
		prob = randomExtraInfo.nextDouble();
		if ((prob < missingRatio) || userExtraInfo.getOrganization().equals("")) {
			userExtraInfo.setClassYear(-1);
		} else {
			userExtraInfo.setClassYear(dateTimeGenerator.getClassYear(
					user.getCreatedDate(), user.getBirthDay()));
		}

		// Set company and workFrom
        int numCompanies = randomExtraInfo.nextInt(maxCompanies) + 1;
        prob = randomExtraInfo.nextDouble();
        if (prob >= missingRatio) {
            for (int i = 0; i < numCompanies; i++) {
                if (userExtraInfo.getClassYear() != -1) {
                    long workFrom = dateTimeGenerator.getWorkFromYear(user.getCreatedDate(), user.getBirthDay());
                    userExtraInfo.addCompany(companiesDictionary.getRandomCompany(user.getLocationIdx()), workFrom);
                } else {
                    long workFrom = dateTimeGenerator.getWorkFromYear(userExtraInfo.getClassYear());
                    userExtraInfo.addCompany(companiesDictionary.getRandomCompany(user.getLocationIdx()), workFrom);
                }
            }
        }
        Vector<Integer> userLanguages = languageDic.getLanguages(user.getLocationIdx());
        int nativeLanguage = randomExtraInfo.nextInt(userLanguages.size());
        userExtraInfo.setNativeLanguage(userLanguages.get(nativeLanguage));
        int internationalLang = languageDic.getInternationlLanguage();
        if (internationalLang != -1 && userLanguages.indexOf(internationalLang) == -1) {
            userLanguages.add(internationalLang);
        }
        userExtraInfo.setLanguages(userLanguages);

		// write user data for test driver
		if (user.getNumFriendsAdded() > thresholdPopularUser) {
			outputDataWriter.writeUserData(user.getAccountId(),
					user.getNumFriendsAdded());
			numPopularUser++;
		}
	}


	public double getFriendCreatePro(int i, int j, int pass){
		double prob;
		if (j > i){
			prob = Math.pow(baseProbCorrelated, (j- i));
		} else{
			prob =  Math.pow(baseProbCorrelated, (j + windowSize - i));
		}
		return prob; 

	}
	public void createFriendShip(ReducedUserProfile user1, ReducedUserProfile user2, byte pass) {
		long requestedTime = dateTimeGenerator.randomFriendRequestedDate(user1, user2);
		byte initiator = (byte) randInitiator.nextInt(2);
		long createdTime = -1;
		long declinedTime = -1;
		if (randFriendReject.nextDouble() > friendRejectRatio) {
			createdTime = dateTimeGenerator.randomFriendApprovedDate(requestedTime);
		} else {
			declinedTime = dateTimeGenerator.randomFriendDeclinedDate(requestedTime);
			if (randFriendReapprov.nextDouble() < friendReApproveRatio) {
				createdTime = dateTimeGenerator.randomFriendReapprovedDate(declinedTime);
			}
		}

		user2.addNewFriend(new Friend(user1, requestedTime, declinedTime,
				createdTime, pass, initiator));
		user1.addNewFriend(new Friend(user2, requestedTime, declinedTime,
				createdTime, pass, initiator));

		friendshipNo++;
	}

	
	public void updateLastPassFriendAdded(int from, int to, int pass) {
		if (to > windowSize) {
			for (int i = from; i < windowSize; i++) {
				reducedUserProfiles[i].setPassFriendsAdded(pass, reducedUserProfiles[i].getNumFriendsAdded());
			}
			for (int i = 0; i < to - windowSize; i++) {
				reducedUserProfiles[i].setPassFriendsAdded(pass, reducedUserProfiles[i].getNumFriendsAdded());
			}
		} else {
			for (int i = from; i < to; i++) {
				reducedUserProfiles[i].setPassFriendsAdded(pass, reducedUserProfiles[i].getNumFriendsAdded());
			}
		}
	}

	private Serializer getSerializer(String type, String outputFileName) {
	    SN.setMachineNumber(machineId);
		String t = type.toLowerCase();
		if (t.equals("ttl")) {
            return new Turtle(sibOutputDir + outputFileName, forwardChaining,
                    numRdfOutputFile, true, mainTagDic,
                    browserDic.getvBrowser(), companiesDictionary.getCompanyLocationMap(), 
                    organizationsDictionary.GetOrganizationLocationMap(),
                    ipAddDictionary, locationDic, languageDic);
		} else if (t.equals("n3")) {
            return new Turtle(sibOutputDir + outputFileName, forwardChaining,
                    numRdfOutputFile, false, mainTagDic,
                    browserDic.getvBrowser(), companiesDictionary.getCompanyLocationMap(), 
                    organizationsDictionary.GetOrganizationLocationMap(),
                    ipAddDictionary, locationDic, languageDic);
		} else if (t.equals("csv")) {
			return new CSV(sibOutputDir /*+ outputFileName*/, forwardChaining,
					numRdfOutputFile, mainTagDic,
					browserDic.getvBrowser(), companiesDictionary.getCompanyLocationMap(), 
                    organizationsDictionary.GetOrganizationLocationMap(),
					ipAddDictionary,locationDic, languageDic);
		} else {
		    System.err.println("Unexpected Serializer - Aborting");
		    System.exit(-1);
		    return null;
		}
	}
	
	private void startWritingUserData() {
		outputDataWriter.initWritingUserData();
	}

	private void finishWritingUserData() {
		outputDataWriter.finishWritingUserData();
	}
	
	private void writeDataForQGEN() {
        outputDataWriter.writeGeneralDataForTestDriver(numtotalUser, dateTimeGenerator);
        outputDataWriter.writeGroupDataForTestDriver(groupGenerator);
        outputDataWriter.writeLocationDataForTestDriver(locationDic);
        outputDataWriter.writeNamesDataForTestDriver(namesDictionary);
    }

	public void writeToOutputFile(String filenames[], String outputfile){
	    Writer output = null;
	    File file = new File(outputfile);
	    try {
	        output = new BufferedWriter(new FileWriter(file));
	        for (int i = 0; i < (filenames.length - 1); i++) {
	            output.write(filenames[i] + " " + numCellPerfile + "\n");
	        }

	        output.write(filenames[filenames.length - 1] + " " + numCellInLastFile + "\n");

	        output.close();
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
	}
	
	public void printHeapSize(){
		long heapSize = Runtime.getRuntime().totalMemory();
		long heapMaxSize = Runtime.getRuntime().maxMemory();
		long heapFreeSize = Runtime.getRuntime().freeMemory(); 
		
		System.out.println(" ---------------------- ");
		System.out.println(" Current Heap Size: " + heapSize/(1024*1024));
		System.out.println(" Max Heap Size: " + heapMaxSize/(1024*1024));
		System.out.println(" Free Heap Size: " + heapFreeSize/(1024*1024));
		System.out.println(" ---------------------- ");
	}
}
 
