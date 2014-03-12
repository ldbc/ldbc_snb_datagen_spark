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
import java.util.TreeSet;
import java.util.Properties;
import java.util.Random;
import java.util.Vector;
import java.util.Iterator;
import java.text.Normalizer;

import ldbc.socialnet.dbgen.dictionary.BrowserDictionary;
import ldbc.socialnet.dbgen.dictionary.CompanyDictionary;
import ldbc.socialnet.dbgen.dictionary.EmailDictionary;
import ldbc.socialnet.dbgen.dictionary.IPAddressDictionary;
import ldbc.socialnet.dbgen.dictionary.LanguageDictionary;
import ldbc.socialnet.dbgen.dictionary.LocationDictionary;
import ldbc.socialnet.dbgen.dictionary.NamesDictionary;
import ldbc.socialnet.dbgen.dictionary.UniversityDictionary;
import ldbc.socialnet.dbgen.dictionary.PopularPlacesDictionary;
import ldbc.socialnet.dbgen.dictionary.TagDictionary;
import ldbc.socialnet.dbgen.dictionary.FlashmobTagDictionary;
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
import ldbc.socialnet.dbgen.serializer.CSVMergeForeign;
import ldbc.socialnet.dbgen.serializer.EmptySerializer;
import ldbc.socialnet.dbgen.serializer.Serializer;
import ldbc.socialnet.dbgen.serializer.Turtle;
import ldbc.socialnet.dbgen.serializer.Statistics;
import ldbc.socialnet.dbgen.storage.StorageManager;
import ldbc.socialnet.dbgen.vocabulary.SN;

import ldbc.socialnet.dbgen.generator.PostGenerator;
import ldbc.socialnet.dbgen.generator.UniformPostGenerator;
import ldbc.socialnet.dbgen.generator.FlashmobPostGenerator;
import ldbc.socialnet.dbgen.generator.CommentGenerator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import umontreal.iro.lecuyer.probdist.PowerDist;

public class ScalableGenerator{

    public enum OrganisationType {
        university,
        company
    }

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

    /**
     * The maximum number of likes
     */
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
    private static final String  STATS_FILE           = "testdata.json";
    private              String  RDF_OUTPUT_FILE      = "ldbc_socialnet_dbg"; //Not static or final: Will be modified to add the machine prefix

    // Dictionaries dataset files
    private static final String   browserDictonryFile       = DICTIONARY_DIRECTORY + "browsersDic.txt";
    private static final String   companiesDictionaryFile   = DICTIONARY_DIRECTORY + "companiesByCountry.txt";
    private static final String   countryAbbrMappingFile    = DICTIONARY_DIRECTORY + "countryAbbrMapping.txt";
    private static final String   tagDictionaryFile         = DICTIONARY_DIRECTORY + "dicCelebritiesByCountry.txt";
    private static final String   countryDictionaryFile     = DICTIONARY_DIRECTORY + "dicLocation.txt";
    private static final String   tagNamesFile              = DICTIONARY_DIRECTORY + "dicTopic.txt";
    private static final String   emailDictionaryFile       = DICTIONARY_DIRECTORY + "email.txt";
    private static final String   nameDictionaryFile        = DICTIONARY_DIRECTORY + "givennameByCountryBirthPlace.txt.freq.full";
    private static final String   universityDictionaryFile  = DICTIONARY_DIRECTORY + "institutesCityByCountry.txt";
    private static final String   cityDictionaryFile        = DICTIONARY_DIRECTORY + "citiesByCountry.txt";
    private static final String   languageDictionaryFile    = DICTIONARY_DIRECTORY + "languagesByCountry.txt";
    private static final String   popularDictionaryFile     = DICTIONARY_DIRECTORY + "popularPlacesByCountry.txt";
    private static final String   agentFile                 = DICTIONARY_DIRECTORY + "smartPhonesProviders.txt";
    private static final String   surnamDictionaryFile      = DICTIONARY_DIRECTORY + "surnameByCountryBirthPlace.txt.freq.sort";
    private static final String   tagClassFile              = DICTIONARY_DIRECTORY + "tagClasses.txt";
    private static final String   tagHierarchyFile          = DICTIONARY_DIRECTORY + "tagHierarchy.txt";
    private static final String   tagTextFile               = DICTIONARY_DIRECTORY + "tagText.txt";
    private static final String   tagTopicDictionaryFile    = DICTIONARY_DIRECTORY + "topicMatrixId.txt";
    private static final String   flashmobDistFile          = DICTIONARY_DIRECTORY + "flashmobDist.txt";

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


    private final String MIN_LARGE_POST_SIZE                 = "minLargePostSize";
    private final String MAX_LARGE_POST_SIZE                 = "maxLargePostSize";
    private final String MIN_LARGE_COMMENT_SIZE              = "minLargeCommentSize";
    private final String MAX_LARGE_COMMENT_SIZE              = "maxLargeCommentSize";
    private final String LARGE_POST_RATIO                    = "ratioLargePost";
    private final String LARGE_COMMENT_RATIO                 = "ratioLargeComment";

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

    private final String FLASHMOB_TAGS_PER_MONTH                = "flashmobTagsPerMonth";
    private final String PROB_INTEREST_FLASHMOB_TAG             = "probInterestFlashmobTag";
    private final String PROB_RANDOM_PER_LEVEL                  = "probRandomPerLevel";
    private final String POST_PER_LEVEL_SCALE_FACTOR            = "postPerLevelScaleFactor";
    private final String FLASHMOB_TAG_MIN_LEVEL                 = "flashmobTagMinLevel";
    private final String FLASHMOB_TAG_MAX_LEVEL                 = "flashmobTagMaxLevel";
    private final String FLASHMOB_TAG_DIST_EXP                  = "flashmobTagDistExp";

    /**
     * This array provides a quick way to check if any of the required parameters is missing and throw the appropriate
     * exception in the method loadParamsFromFile()
     */
    private final String[] checkParameters = {CELL_SIZE, NUM_CELL_WINDOW, MIN_FRIENDS, MAX_FRIENDS, FRIEND_REJECT,
        FRIEND_REACCEPT, USER_MIN_TAGS, USER_MAX_TAGS, USER_MAX_POST_MONTH, MAX_COMMENT_POST, LIMIT_CORRELATED,
        BASE_CORRELATED, MAX_EMAIL, MAX_COMPANIES, ENGLISH_RATIO, SECOND_LANGUAGE_RATIO, OTHER_BROWSER_RATIO,
        MIN_TEXT_SIZE, MAX_TEXT_SIZE, MIN_COMMENT_SIZE, MAX_COMMENT_SIZE, REDUCE_TEXT_RATIO,
        MIN_LARGE_POST_SIZE, MAX_LARGE_POST_SIZE, MIN_LARGE_COMMENT_SIZE, MAX_LARGE_COMMENT_SIZE, LARGE_POST_RATIO,
        LARGE_COMMENT_RATIO,MAX_PHOTOALBUM,
        MAX_PHOTO_PER_ALBUM, USER_MAX_GROUP, MAX_GROUP_MEMBERS, GROUP_MODERATOR_RATIO, GROUP_MAX_POST_MONTH, 
        MISSING_RATIO, STATUS_MISSING_RATIO, STATUS_SINGLE_RATIO, SMARTHPHONE_RATIO, AGENT_SENT_RATIO, 
        DIFFERENT_IP_IN_TRAVEL_RATIO, DIFFERENT_IP_NOT_TRAVEL_RATIO, DIFFERENT_IP_TRAVELLER_RATIO, 
        COMPANY_UNCORRELATED_RATIO, UNIVERSITY_UNCORRELATED_RATIO, BEST_UNIVERSTY_RATIO, MAX_POPULAR_PLACES, 
        POPULAR_PLACE_RATIO, TAG_UNCORRELATED_COUNTRY, FLASHMOB_TAGS_PER_MONTH, 
        PROB_INTEREST_FLASHMOB_TAG, PROB_RANDOM_PER_LEVEL, POST_PER_LEVEL_SCALE_FACTOR, FLASHMOB_TAG_MIN_LEVEL, FLASHMOB_TAG_MAX_LEVEL,
        FLASHMOB_TAG_DIST_EXP};


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

    //Stat container
    private Statistics stats;

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
    TreeSet<Integer> 		selectedFileIdx;

    // For friendship generation
    int 				friendshipNum;
    int 				minNumFriends;
    int 				maxNumFriends;
    double 				friendRejectRatio;
    double 				friendReApproveRatio;

    StorageManager	 	storeManager[];
    StorageManager		groupStoreManager; 		

    MRWriter			mrWriter; 

    /*Random 				randomFriendReject;
    Random 				randomFriendAproval;
    Random 				randomInitiator;*/
    double 				baseProbCorrelated;
    double	 			limitProCorrelated; 

    // For each user
    int                 minNumTagsPerUser; 
    int					maxNumTagsPerUser; 
    int 				maxNumPostPerMonth;
    int 				maxNumComments;



    // Random values generators
    PowerDistGenerator 	randomPowerLaw;
    PowerDistGenerator  randomTagPowerLaw;
    Long[] 				seeds;
    Random              randomDate;
    Random              randomBirthDay;
    Random              randomFriendRequest;
    Random 				randomUniform;
    Random 				randomNumInterests;
    Random 				randomNumTags;
    Random 				randomFriendIdx;
    Random 				randomNumComments;
    Random 				randomNumPhotoAlbums;
    Random 				randomNumPhotos;
    Random 				randomNumGroups;
    Random 				randomNumUsersPerGroup;
    Random 				randomGender;
    Random				randomUserRandomIdx;	   // For generating the random dimension
    Random              randomNumPopularPlaces; 
    Random              randomFriendLevelSelect; // For select an level of moderator's friendship
    Random              randomMembership;        // For deciding whether or not a user is joined
    Random              randomMemberIdxSelector;
    Random              randomGroupMemStep;
    Random              randomGroupModerator;    // Decide whether a user can be moderator of groups or not
    Random              randomExtraInfo;
    Random              randomExactLongLat;
    Random              randomHaveStatus;
    Random              randomStatusSingle;
    Random              randomStatus;
    Random              randomUserAgent;
    Random              randomFileSelect;
    Random              randomIdsInWindow;
    Random              randomIP;
    Random              randomBrowser;
    Random              randomCity;
    Random              randomTag;
    Random              randomUniversity;
    Random              randomUserAgent;
    Random              randomPopular;
    Random              randomEmail;
    Random              randomCompany;
    Random              randomLanguage;

    DateGenerator 		dateTimeGenerator;
    int					startYear;
    int                 endYear;

    // Dictionary classes
    LocationDictionary 		locationDictionary;
    LanguageDictionary      languageDictionary;
    TagDictionary 			tagDictionary;
    FlashmobTagDictionary   flashmobTagDictionary;
    TagTextDictionary       tagTextDictionary;
    TagMatrix	 			topicTagDictionary;
    NamesDictionary 		namesDictionary;
    UniversityDictionary    unversityDictionary;
    CompanyDictionary       companiesDictionary;
    UserAgentDictionary     userAgentDictionary;
    EmailDictionary         emailDictionary;
    BrowserDictionary       browserDictonry;
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

    // This parameters configure the amount of large posts are created 
    int                     minLargePostSize;
    int                     maxLargePostSize;
    int                     minLargeCommentSize;
    int                     maxLargeCommentSize;
    double                  ratioLargePost; 
    double                  ratioLargeComment; 

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
    UniformPostGenerator           uniformPostGenerator;
    FlashmobPostGenerator          flashmobPostGenerator;
    CommentGenerator        commentGenerator;
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
    double	       missingRatio;
    double         missingStatusRatio;
    double         probSingleStatus; // Status "Single" has more probability than others'


    double 		   probAnotherBrowser;
    double         probHavingSmartPhone;
    double         probSentFromAgent;

    // The probability that normal user posts from different location
    double         probDiffIPinTravelSeason; // in travel season
    double         probDiffIPnotTravelSeason; // not in travel season

    // The probability that travellers post from different location
    double         probDiffIPforTraveller;

    // Writing data for test driver
    int            thresholdPopularUser = 40;
    int            numPopularUser = 0;

    int            flashmobTagsPerMonth = 0;
    double         probInterestFlashmobTag = 0.0;
    double         probRandomPerLevel = 0.0;
    int            postPerLevelScaleFactor = 0;
    double         flashmobTagMinLevel = 0.0f;
    double         flashmobTagMaxLevel = 0.0f;
    double         flashmobTagDistExp  = 0.0f;

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
        this.stats = new Statistics();

        System.out.println("Map Reduce File Idx is: " + mapreduceFileIdx);
        if (mapreduceFileIdx != -1){
            outUserProfile = "mr" + mapreduceFileIdx + "_" + outUserProfileName;
        }

        System.out.println("Current directory in ScaleGenerator is " + sibHomeDir);
    }

    /**
     * Gets the size of the cell.
     * 
     * @return The size of che cell.
     */
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
        _init(mapId);

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
            minNumFriends = Integer.parseInt(properties.getProperty(MIN_FRIENDS));
            maxNumFriends = Integer.parseInt(properties.getProperty(MAX_FRIENDS));
            thresholdPopularUser = (int) (maxNumFriends * 0.9);
            friendRejectRatio = Double.parseDouble(properties.getProperty(FRIEND_REJECT));
            friendReApproveRatio = Double.parseDouble(properties.getProperty(FRIEND_REACCEPT));
            minNumTagsPerUser= Integer.parseInt(properties.getProperty(USER_MIN_TAGS));
            maxNumTagsPerUser= Integer.parseInt(properties.getProperty(USER_MAX_TAGS));
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
            minLargePostSize = Integer.parseInt(properties.getProperty(MIN_LARGE_POST_SIZE));
            maxLargePostSize = Integer.parseInt(properties.getProperty(MAX_LARGE_POST_SIZE));
            minLargeCommentSize = Integer.parseInt(properties.getProperty(MIN_LARGE_COMMENT_SIZE));
            maxLargeCommentSize = Integer.parseInt(properties.getProperty(MAX_LARGE_COMMENT_SIZE));
            ratioLargePost = Double.parseDouble(properties.getProperty(LARGE_POST_RATIO));
            ratioLargeComment = Double.parseDouble(properties.getProperty(LARGE_COMMENT_RATIO));
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
            flashmobTagsPerMonth = Integer.parseInt(properties.getProperty(FLASHMOB_TAGS_PER_MONTH));
            probInterestFlashmobTag = Double.parseDouble(properties.getProperty(PROB_INTEREST_FLASHMOB_TAG));
            probRandomPerLevel = Double.parseDouble(properties.getProperty(PROB_RANDOM_PER_LEVEL));
            postPerLevelScaleFactor = Integer.parseInt(properties.getProperty(POST_PER_LEVEL_SCALE_FACTOR));
            flashmobTagMinLevel = Double.parseDouble(properties.getProperty(FLASHMOB_TAG_MIN_LEVEL));
            flashmobTagMaxLevel = Double.parseDouble(properties.getProperty(FLASHMOB_TAG_MAX_LEVEL));
            flashmobTagDistExp = Double.parseDouble(properties.getProperty(FLASHMOB_TAG_DIST_EXP));
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
                    !serializerType.equals("csv") && !serializerType.equals("none") && !serializerType.equals("csv_merge_foreign")) {
                throw new IllegalStateException("serializerType must be ttl, n3, csv, csv_merge_foreign");
                    }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(-1);
        }
    }

    /**
     * Initializes the internal variables.
     * @param mapId: The hadoop reduce id.
     */
    private void _init(int mapId) {

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
        randomDate = new Random(seeds[1]);
        randomBirthDay = new Random(seeds[2]);
        randomFriendRequest = new Random(seeds[3]);
        randomPowerLaw    = new PowerDistGenerator(minNumFriends,     maxNumFriends + 1,     alpha, seeds[2]);
        randomTagPowerLaw = new PowerDistGenerator(minNumTagsPerUser, maxNumTagsPerUser + 1, alpha, seeds[2]);
        randomUniform = new Random(seeds[3]);
        randomGender = new Random(seeds[3]);
        randomNumInterests = new Random(seeds[4]);
        randomNumTags = new Random(seeds[4]);
        randomFriendIdx = new Random(seeds[6]);
        randomFileSelect = new Random(seeds[7]);
        randomIdsInWindow = new Random(seeds[8]);
        randomNumComments = new Random(seeds[10]);
        randomNumPhotoAlbums = new Random(seeds[11]);
        randomNumPhotos = new Random(seeds[12]);
        randomNumGroups = new Random(seeds[13]);
        randomNumUsersPerGroup = new Random(seeds[14]);
        randomMemberIdxSelector = new Random(seeds[18]);
        randomGroupMemStep = new Random(seeds[19]);
        randomFriendLevelSelect = new Random(seeds[20]);
        randomMembership = new Random(seeds[21]);
        randomGroupModerator = new Random(seeds[22]);
        randomExtraInfo = new Random(seeds[27]);
        randomExactLongLat = new Random(seeds[27]);
        randomUserAgent = new Random(seeds[29]);
        randomFriendReject = new Random(seeds[37]);
        randomFriendAproval = new Random(seeds[38]);
        randomInitiator = new Random(seeds[39]);
        randomHaveStatus = new Random(seeds[41]);
        randomStatusSingle = new Random(seeds[42]);
        randomStatus = new Random(seeds[43]);
        randomNumPopularPlaces = new Random(seeds[47]);
        randomUserRandomIdx = new Random(seeds[48]);
        randomIP = new Random(seeds[49]);
        randomBrowser = new Random(seeds[49]);
        randomBrowser = new Random(seeds[49]);
        randomCity = new Random(seeds[49]);
        randomTag = new Random(seeds[49]);
        randomUniversity = new Random(seeds[49]);
        randomUserAgent = new Random(seeds[49]);
        randomPopular = new Random(seeds[49]);
        randomEmail = new Random(seeds[49]);
        randomCompany = new Random(seeds[49]);
        randomLanguage = new Random(seeds[49]);

        // Initializing window memory
        reducedUserProfiles = new ReducedUserProfile[windowSize];
        cellReducedUserProfiles = new ReducedUserProfile[cellSize];

        dateTimeGenerator = new DateGenerator( new GregorianCalendar(startYear, startMonth, startDate), 
                                               new GregorianCalendar(endYear, endMonth, endDate), alpha);


        System.out.println("Building location dictionary ");
        locationDictionary = new LocationDictionary(numtotalUser, countryDictionaryFile, cityDictionaryFile);
        locationDictionary.init();

        System.out.println("Building language dictionary ");
        languageDictionary = new LanguageDictionary(languageDictionaryFile, locationDictionary, 
                probEnglish, probSecondLang);
        languageDictionary.init();

        System.out.println("Building Tag dictionary ");
        tagDictionary = new TagDictionary(tagNamesFile, tagDictionaryFile, tagClassFile, tagHierarchyFile,
                locationDictionary.getCountries().size(), tagCountryCorrProb);
        tagDictionary.initialize();

        System.out.println("Building Tag-text dictionary ");
        tagTextDictionary = new TagTextDictionary(tagTextFile, dateTimeGenerator, tagDictionary,
                ratioReduceText);
        tagTextDictionary.initialize();

        System.out.println("Building Tag Matrix dictionary ");
        topicTagDictionary = new TagMatrix(topicTagDictionaryFile, tagDictionary.getNumCelebrity());
        topicTagDictionary.initMatrix();

        System.out.println("Building IP addresses dictionary ");
        ipAddDictionary = new IPAddressDictionary(countryAbbrMappingFile,
                IPZONE_DIRECTORY, locationDictionary, probDiffIPinTravelSeason, probDiffIPnotTravelSeason,
                probDiffIPforTraveller);
        ipAddDictionary.initialize();

        System.out.println("Building Names dictionary");
        namesDictionary = new NamesDictionary(surnamDictionaryFile, nameDictionaryFile,
                locationDictionary);
        namesDictionary.init();

        System.out.println("Building email dictionary");
        emailDictionary = new EmailDictionary(emailDictionaryFile);
        emailDictionary.init();

        System.out.println("Building browser dictionary");
        browserDictonry = new BrowserDictionary(browserDictonryFile, probAnotherBrowser);
        browserDictonry.init();			

        System.out.println("Building university dictionary");
        unversityDictionary = new UniversityDictionary(universityDictionaryFile, locationDictionary,
                 probUnCorrelatedOrganization, probTopUniv);
        unversityDictionary.init();

        System.out.println("Building companies dictionary");
        companiesDictionary = new CompanyDictionary(companiesDictionaryFile,locationDictionary, 
                 probUnCorrelatedCompany);
        companiesDictionary.init();

        System.out.println("Building popular places dictionary");
        popularDictionary = new PopularPlacesDictionary(popularDictionaryFile, 
                locationDictionary);
        popularDictionary.init();

        System.out.println("Building user agents dictionary");
        userAgentDictionary = new UserAgentDictionary(agentFile, probSentFromAgent);
        userAgentDictionary.init();

        // Building generators.
        
        System.out.println("Building photo generator");
        photoGenerator = new PhotoGenerator(dateTimeGenerator,
                locationDictionary, 0, popularDictionary, probPopularPlaces);

        System.out.println("Building Group generator");
        groupGenerator = new GroupGenerator(dateTimeGenerator, locationDictionary,
                tagDictionary, numtotalUser, 0);



        /// IMPORTANT: ratioLargeText is divided 0.083333, the probability 
        /// that SetUserLargePoster returns true.
        System.out.println("Building Uniform Post Generator");
        uniformPostGenerator = new UniformPostGenerator( tagTextDictionary, 
                userAgentDictionary,
                ipAddDictionary,
                browserDictonry,
                minTextSize,
                maxTextSize,
                ratioReduceText,
                minLargePostSize,
                maxLargePostSize,
                ratioLargePost/0.0833333,
                maxNumLikes,
                0,
                0,
                dateTimeGenerator,
                maxNumPostPerMonth,
                maxNumFriends,
                maxNumGroupPostPerMonth,
                maxNumMemberGroup,
                0,
                0,
                0 );
        uniformPostGenerator.initialize();

        System.out.println("Building Flashmob Tag Dictionary");
        flashmobTagDictionary = new FlashmobTagDictionary( tagDictionary, 
                dateTimeGenerator,
                flashmobTagsPerMonth,
                probInterestFlashmobTag,
                probRandomPerLevel,
                flashmobTagMinLevel,
                flashmobTagMaxLevel,
                flashmobTagDistExp,
                0 
                );
        flashmobTagDictionary.initialize();

        stats.flashmobTags = flashmobTagDictionary.getFlashmobTags();

        /// IMPORTANT: ratioLargeText is divided 0.083333, the probability 
        /// that SetUserLargePoster returns true.
        System.out.println("Building Flashmob Post Generator");
        flashmobPostGenerator = new FlashmobPostGenerator( tagTextDictionary, 
                userAgentDictionary,
                ipAddDictionary,
                browserDictonry,
                minTextSize,
                maxTextSize,
                ratioReduceText,
                minLargePostSize,
                maxLargePostSize,
                ratioLargePost/0.0833333, 
                maxNumLikes,
                0,
                0,
                dateTimeGenerator,
                flashmobTagDictionary,
                postPerLevelScaleFactor,
                flashmobDistFile
                );
        flashmobPostGenerator.initialize();

        /// IMPORTANT: ratioLargeText is divided 0.083333, the probability 
        /// that SetUserLargePoster returns true.
        System.out.println("Building Comment Generator");
        commentGenerator = new CommentGenerator(tagTextDictionary, dateTimeGenerator,
                minCommentSize, maxCommentSize, ratioReduceText,
                minLargeCommentSize, maxLargeCommentSize, ratioLargeComment/0.0833333,
                0, 0);
        commentGenerator.initialize();

        serializer = getSerializer(serializerType, RDF_OUTPUT_FILE);
    }

    /**
     * Generates the user activity (Photos, Posts, Comments, Groups) data 
     * of (numberCell * numUserPerCell) users correspondingto this hadoop job (machineId)
     * 
     * @param inputFile The hadoop file with the user serialization (data and friends)
     * @param numCell The number of cells the generator will parse.
     */
    public void generateUserActivity(String inputFile, int numCell) {
        long startPostGeneration = System.currentTimeMillis();
        //NOTE: Until this point of the code numtotalUser*2 forums where generated (2 for user) thats
        //the reason behind this forum id assignment.
        groupGenerator.setForumId((numtotalUser + 10) * 2);
        generatePostandPhoto(inputFile, numCell);
        long endPostGeneration = System.currentTimeMillis();
        System.out.println("Post generation takes " + getDurationInMinutes(startPostGeneration, endPostGeneration));
        long startGroupGeneration = System.currentTimeMillis();
        generateAllGroup(inputFile, numCell);
        long endGroupGeneration = System.currentTimeMillis();
        System.out.println("Group generation takes " + getDurationInMinutes(startGroupGeneration, endGroupGeneration));
        serializer.close();
        writeStatistics();
        System.out.println("Number of generated triples " + serializer.unitsGenerated());
        System.out.println("Number of popular users " + numPopularUser);
        System.out.println("Writing the data for test driver ");
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

                //generatePosts(reducedUserProfilesCell[j], extraInfo);
                //generateFlashmobPosts(reducedUserProfilesCell[j], extraInfo);
                //generatePhoto(reducedUserProfilesCell[j], extraInfo);
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
            double moderatorProb = randomGroupModerator.nextDouble();
            if (moderatorProb <= groupModeratorProb) {
                Friend firstLevelFriends[] = reducedUserProfiles[curIdxInWindow].getFriendList();
                Vector<Friend> secondLevelFriends = new Vector<Friend>();
                //TODO: Include friends of friends a.k.a second level friends?
                int numGroup = randomNumGroups.nextInt(maxNumGroupCreatedPerUser);
                for (int j = 0; j < numGroup; j++) { 
                    createGroupForUser(reducedUserProfiles[curIdxInWindow],
                            firstLevelFriends, secondLevelFriends);
                }
            }
        }
    }

    /**
     * Pushes a user into the generator.
     * 
     * @param reduceUser The user to push.
     * @param pass The pass identifier, which is used to decide the criteria under the edges are created.
     * @param context The map-reduce context.
     * @param isContext
     * @param oos The output stream used to write the data.
     */
    public void pushUserProfile(ReducedUserProfile reduceUser, int pass, 
            Reducer<IntWritable, ReducedUserProfile,IntWritable, ReducedUserProfile>.Context context, 
            boolean isContext, ObjectOutputStream oos){
        numUserProfilesRead++;
        ReducedUserProfile userObject = new ReducedUserProfile();
        userObject.copyFields(reduceUser);
        if (numUserProfilesRead <= windowSize) {
            reducedUserProfiles[numUserProfilesRead-1] = userObject;
            if( numUserProfilesRead == windowSize ) {
                mr2InitFriendShipWindow(pass, context, isContext, oos);
            }
        } else {
            numUserForNewCell++;
            cellReducedUserProfiles[numUserForNewCell-1] = userObject;
            if (numUserForNewCell == cellSize){
                mrCurCellPost++;
                mr2SlideFriendShipWindow(   pass,
                        mrCurCellPost, 
                        context, 
                        cellReducedUserProfiles,
                        isContext,  
                        oos );
                numUserForNewCell = 0;
            }
        }
        reduceUser = null;
    }

    /**
     * Creates the remainder of the edges for the currently inserted nodes.
     * 
     * @param reduceUser The user to push.
     * @param pass The pass identifier, which is used to decide the criteria under the edges are created.
     * @param context The map-reduce context.
     * @param isContext
     * @param oos The output stream used to write the data.
     */
    public void pushAllRemainingUser(int pass, 
            Reducer<IntWritable, ReducedUserProfile,IntWritable, ReducedUserProfile>.Context context, 
            boolean isContext, ObjectOutputStream oos){

        for (int numLeftCell = numberOfCellPerWindow - 1; numLeftCell > 0; numLeftCell--) {
            mr2SlideLastCellsFriendShip(pass, ++mrCurCellPost, numLeftCell, context, isContext,  oos);
        }
    }

    /**
     * Generates the users.
     * 
     * @param pass The pass identifying the current pass.
     * @param context The map-reduce context.
     * @param mapIdx The index of the current map, used to determine how many users to generate.
     */
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
            for (int j = i + 1;  (j < windowSize - 1) && 
                    (reducedUserProfiles[i].getNumFriendsAdded() != reducedUserProfiles[i].getNumFriends(pass));
                    j++) {
                if (!((reducedUserProfiles[j].getNumFriendsAdded() 
                                == reducedUserProfiles[j].getNumFriends(pass)) || 
                            reducedUserProfiles[i].isExistFriend(reducedUserProfiles[j].getAccountId()))) {
                    double randProb = randomUniform.nextDouble();
                    double prob = getFriendCreatePro(i, j, pass);  
                    if ((randProb < prob) || (randProb < limitProCorrelated)) {
                        createFriendShip(reducedUserProfiles[i], reducedUserProfiles[j], (byte) pass);
                    }
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
        // Init the number of friends for each user in the new cell
        generateCellOfUsers2(newStartIndex, _cellReduceUserProfiles);
        // Create the friendships
        // Start from each user in the first cell of the window --> at the
        // cellPos, not from the new cell
        newStartIndex = (cellPos % numberOfCellPerWindow) * cellSize;
        for (int i = 0; i < cellSize; i++) {
            int curIdxInWindow = newStartIndex + i;
            // From this user, check all the user in the window to create friendship
            for (int j = i + 1; (j < windowSize - 1) && reducedUserProfiles[curIdxInWindow].getNumFriendsAdded() < reducedUserProfiles[curIdxInWindow].getNumFriends(pass); j++) {
                int checkFriendIdx = (curIdxInWindow + j) % windowSize;
                if ( !(reducedUserProfiles[checkFriendIdx].getNumFriendsAdded() 
                            == reducedUserProfiles[checkFriendIdx].getNumFriends(pass) || 
                            reducedUserProfiles[curIdxInWindow].isExistFriend(reducedUserProfiles[checkFriendIdx].getAccountId()))) {
                    double randProb = randomUniform.nextDouble();
                    double prob = getFriendCreatePro(curIdxInWindow, checkFriendIdx, pass);
                    if ((randProb < prob) || (randProb < limitProCorrelated)) {
                        createFriendShip(reducedUserProfiles[curIdxInWindow], reducedUserProfiles[checkFriendIdx], (byte) pass);
                    }
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
            // From this user, check all the user in the window to create friendship
            for (int j = i + 1; (j < numleftCell * cellSize - 1)
                    && reducedUserProfiles[curIdxInWindow].getNumFriendsAdded() 
                    < reducedUserProfiles[curIdxInWindow].getNumFriends(pass); j++) {
                int checkFriendIdx = (curIdxInWindow + j) % windowSize;
                if ( !(reducedUserProfiles[checkFriendIdx].getNumFriendsAdded() 
                            == reducedUserProfiles[checkFriendIdx].getNumFriends(pass) || 
                            reducedUserProfiles[curIdxInWindow].isExistFriend(reducedUserProfiles[checkFriendIdx].getAccountId()))) {
                    double randProb = randomUniform.nextDouble();
                    double prob = getFriendCreatePro(curIdxInWindow, checkFriendIdx, pass);
                    if ((randProb < prob) || (randProb < limitProCorrelated)) {
                        createFriendShip(reducedUserProfiles[curIdxInWindow], reducedUserProfiles[checkFriendIdx],
                                (byte) pass);
                    }				
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
        Vector<Post> createdPosts = uniformPostGenerator.createPosts(user, extraInfo );
        Iterator<Post> it = createdPosts.iterator();
        while(it.hasNext()) {
            Post post = it.next();
            String countryName = locationDictionary.getLocationName((ipAddDictionary.getLocation(post.getIpAddress())));
            stats.countries.add(countryName);

            GregorianCalendar date = new GregorianCalendar();
            date.setTimeInMillis(post.getCreationDate());
            String strCreationDate = DateGenerator.formatYear(date);
            if (stats.maxPostCreationDate == null) {
                stats.maxPostCreationDate = strCreationDate;
                stats.minPostCreationDate = strCreationDate;
            } else {
                if (stats.maxPostCreationDate.compareTo(strCreationDate) < 0) {
                    stats.maxPostCreationDate = strCreationDate;
                }
                if (stats.minPostCreationDate.compareTo(strCreationDate) > 0) {
                    stats.minPostCreationDate = strCreationDate;
                }
            }

            serializer.gatherData(post);

            // Generate comments
            int numComment = randomNumComments.nextInt(maxNumComments);
            long lastCommentCreatedDate = post.getCreationDate();
            long lastCommentId = -1;
            long startCommentId = TagTextDictionary.commentId;
            for (int l = 0; l < numComment; l++) {
                Comment comment = commentGenerator.createComment(post, user, lastCommentCreatedDate, startCommentId, lastCommentId, 
                        userAgentDictionary, ipAddDictionary, browserDictonry);
                //				if (comment.getAuthorId() != -1) {
                if ( comment!=null ) {
                    countryName = locationDictionary.getLocationName((ipAddDictionary.getLocation(comment.getIpAddress())));
                    stats.countries.add(countryName);
                    serializer.gatherData(comment);
                    lastCommentCreatedDate = comment.getCreationDate();
                    lastCommentId = comment.getCommentId();
                }
                }
            }
        }

        public void generateFlashmobPosts(ReducedUserProfile user, UserExtraInfo extraInfo){
            // Generate location-related posts
            Vector<Post> createdPosts = flashmobPostGenerator.createPosts(user, extraInfo );
            Iterator<Post> it = createdPosts.iterator();
            while(it.hasNext()){
                Post post = it.next();
                String countryName = locationDictionary.getLocationName((ipAddDictionary.getLocation(post.getIpAddress())));
                stats.countries.add(countryName);

                GregorianCalendar date = new GregorianCalendar();
                date.setTimeInMillis(post.getCreationDate());
                String strCreationDate = DateGenerator.formatYear(date);
                if (stats.maxPostCreationDate == null) {
                    stats.maxPostCreationDate = strCreationDate;
                    stats.minPostCreationDate = strCreationDate;
                } else {
                    if (stats.maxPostCreationDate.compareTo(strCreationDate) < 0) {
                        stats.maxPostCreationDate = strCreationDate;
                    }
                    if (stats.minPostCreationDate.compareTo(strCreationDate) > 0) {
                        stats.minPostCreationDate = strCreationDate;
                    }
                }

                serializer.gatherData(post);

                // Generate comments
                int numComment = randomNumComments.nextInt(maxNumComments);
                long lastCommentCreatedDate = post.getCreationDate();
                long lastCommentId = -1;
                long startCommentId = TagTextDictionary.commentId;
                for (int l = 0; l < numComment; l++) {
                    Comment comment = commentGenerator.createComment(post, user, lastCommentCreatedDate, startCommentId, lastCommentId, 
                            userAgentDictionary, ipAddDictionary, browserDictonry);
                    //              if (comment.getAuthorId() != -1) {
                    if ( comment!=null ) {
                        countryName = locationDictionary.getLocationName((ipAddDictionary.getLocation(comment.getIpAddress())));
                        stats.countries.add(countryName);
                        serializer.gatherData(comment);
                        lastCommentCreatedDate = comment.getCreationDate();
                        lastCommentId = comment.getCommentId();
                    }
                    }
                }
            }

            public void generatePhoto(ReducedUserProfile user, UserExtraInfo extraInfo){
                // Generate photo Album and photos
                int numOfmonths = (int) dateTimeGenerator.numberOfMonths(user);
                int numPhotoAlbums = randomNumPhotoAlbums.nextInt(maxNumPhotoAlbumsPerMonth);
                if (numOfmonths != 0) {
                    numPhotoAlbums = numOfmonths * numPhotoAlbums;
                }

                for (int m = 0; m < numPhotoAlbums; m++) {
                    Group album = groupGenerator.createAlbum(user, extraInfo, m, randomMembership, joinProbs[0]);

                    serializer.gatherData(album);

                    // Generate photos for this album
                    int numPhotos = randomNumPhotos.nextInt(maxNumPhotoPerAlbums);
                    for (int l = 0; l < numPhotos; l++) {
                        Photo photo = photoGenerator.generatePhoto(user, album, l, maxNumLikes);

                        photo.setUserAgent(userAgentDictionary.getUserAgentName(user.isHaveSmartPhone(), user.getAgentIdx()));
                        photo.setBrowserIdx(browserDictonry.getPostBrowserId(user.getBrowserIdx()));
                        photo.setIpAddress(ipAddDictionary.getIP(randomIP,user.getIpAddress(), 
                                    user.isFrequentChange(), photo.getTakenTime(), photo.getLocationId()));
                        String countryName = locationDictionary.getLocationName((ipAddDictionary.getLocation(photo.getIpAddress())));
                        stats.countries.add(countryName);
                        serializer.gatherData(photo);
                    }
                }
            }

            public void createGroupForUser(ReducedUserProfile user,
                    Friend firstLevelFriends[], Vector<Friend> secondLevelFriends) {
                double randLevelProb;
                double randMemberProb;

                Group group = groupGenerator.createGroup(user);

                TreeSet<Long> memberIds = new TreeSet<Long>();

                int numGroupMember = randomNumUsersPerGroup.nextInt(maxNumMemberGroup);
                group.initAllMemberships(numGroupMember);

                int numLoop = 0;
                while ((group.getNumMemberAdded() < numGroupMember) && (numLoop < windowSize)) {

                    numLoop++;
                    randLevelProb = randomFriendLevelSelect.nextDouble();

                    // Select the appropriate friend level
                    if (randLevelProb < levelProbs[0]) { // ==> level 1
                        // Find a friendIdx
                        int friendIdx = randomMemberIdxSelector.nextInt(user.getNumFriendsAdded());
                        // Note: Use user.getNumFriendsAdded(), do not use
                        // firstLevelFriends.length
                        // because we allocate a array for friendLists, but do not
                        // guarantee that
                        // all the element in this array contain values

                        long potentialMemberAcc = firstLevelFriends[friendIdx].getFriendAcc();

                        randMemberProb = randomMembership.nextDouble();
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
                        if (secondLevelFriends.size() != 0) {
                            int friendIdx = randomMemberIdxSelector.nextInt(secondLevelFriends.size());
                            long potentialMemberAcc = secondLevelFriends.get(friendIdx).getFriendAcc();
                            randMemberProb = randomMembership.nextDouble();
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
                        }
                    } else { // ==> random users
                        // Select a user from window
                        int friendIdx = randomMemberIdxSelector.nextInt(windowSize);
                        long potentialMemberAcc = reducedUserProfiles[friendIdx].getAccountId();
                        randMemberProb = randomMembership.nextDouble();
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
                generateFlashmobPostForGroup(group);
            }

            public void generatePostForGroup(Group group) {
                Vector<Post> createdPosts =  uniformPostGenerator.createPosts(group);
                Iterator<Post> it = createdPosts.iterator();
                while(it.hasNext()) {
                    Post groupPost = it.next();
                    String countryName = locationDictionary.getLocationName((ipAddDictionary.getLocation(groupPost.getIpAddress())));
                    stats.countries.add(countryName);
                    serializer.gatherData(groupPost);

                    int numComment = randomNumComments.nextInt(maxNumComments);
                    long lastCommentCreatedDate = groupPost.getCreationDate();
                    long lastCommentId = -1;
                    long startCommentId = TagTextDictionary.commentId;

                    for (int j = 0; j < numComment; j++) {
                        Comment comment = commentGenerator.createComment(groupPost, group, lastCommentCreatedDate, startCommentId, lastCommentId, 
                                userAgentDictionary, ipAddDictionary, browserDictonry);
                        //				if (comment.getAuthorId() != -1) { // In case the comment is not reated because of the friendship's createddate
                        if ( comment!=null ) { // In case the comment is not reated because of the friendship's createddate
                            countryName = locationDictionary.getLocationName((ipAddDictionary.getLocation(comment.getIpAddress())));
                            stats.countries.add(countryName);
                            serializer.gatherData(comment);

                            lastCommentCreatedDate = comment.getCreationDate();
                            lastCommentId = comment.getCommentId();
                        }
                        }
                    }
                }

                public void generateFlashmobPostForGroup(Group group) {
                    Vector<Post> createdPosts = flashmobPostGenerator.createPosts(group);
                    Iterator<Post> it = createdPosts.iterator();
                    while(it.hasNext() ) {
                        Post groupPost = it.next();
                        String countryName = locationDictionary.getLocationName((ipAddDictionary.getLocation(groupPost.getIpAddress())));
                        stats.countries.add(countryName);
                        serializer.gatherData(groupPost);

                        int numComment = randomNumComments.nextInt(maxNumComments);
                        long lastCommentCreatedDate = groupPost.getCreationDate();
                        long lastCommentId = -1;
                        long startCommentId = TagTextDictionary.commentId;

                        for (int j = 0; j < numComment; j++) {
                            Comment comment = commentGenerator.createComment(groupPost, group, lastCommentCreatedDate, startCommentId, lastCommentId, 
                                    userAgentDictionary, ipAddDictionary, browserDictonry);
                            if ( comment!=null ) { // In case the comment is not reated because of the friendship's createddate
                                countryName = locationDictionary.getLocationName((ipAddDictionary.getLocation(comment.getIpAddress())));
                                stats.countries.add(countryName);
                                serializer.gatherData(comment);

                                lastCommentCreatedDate = comment.getCreationDate();
                                lastCommentId = comment.getCommentId();
                            }
                        }
                    }
                }


                public void seedGenerate(int mapIdx) {
                  seeds = new Long[50];
                  Random seedRandom = new Random(53223436L + 1234567*mapIdx);
                  for (int i = 0; i < 50; i++) {
                      seeds[i] = seedRandom.nextLong();
                  }
                }

                public UserProfile generateGeneralInformation(int accountId) {

                    // User Creation
                    long creationDate = dateTimeGenerator.randomDateInMillis( randomDate );
                    int locationId = locationDictionary.getLocation(accountId);
                    UserProfile userProf = new UserProfile( 
                            accountId,
                            creationDate,
                            (randomGender.nextDouble() > 0.5) ? (byte)1 : (byte)0,
                            dateTimeGenerator.getBirthDay(randomBirthDay, creationDate),
                            browserDictonry.getRandomBrowserId(randomBrowser),
                            locationId,
                            locationDictionary.getZorderID(locationId),
                            locationDictionary.getRandomCity(randomCity,locationId),
                            ipAddDictionary.getRandomIPFromLocation(randomIP,locationId),
                            accountId*2);

                    userProf.setNumFriends((short) randomPowerLaw.getValue());
                    userProf.allocateFriendListMemory(NUM_FRIENDSHIP_HADOOP_JOBS);

                    // Setting the number of friends and friends per pass
                    short totalFriendSet = 0; 
                    for (int i = 0; i < NUM_FRIENDSHIP_HADOOP_JOBS-1; i++){
                        short numPassFriend = (short) Math.floor(friendRatioPerJob[0] * userProf.getNumFriends());
                        totalFriendSet = (short) (totalFriendSet + numPassFriend);
                        userProf.setNumPassFriends(totalFriendSet,i);
                    }
                    userProf.setNumPassFriends(userProf.getNumFriends(),NUM_FRIENDSHIP_HADOOP_JOBS-1);

                    int userMainTag = tagDictionary.getaTagByCountry(userProf.getLocationId());
                    userProf.setMainTagId(userMainTag);
                    short numTags = ((short) randomTagPowerLaw.getValue());
                    userProf.setSetOfTags(topicTagDictionary.getSetofTags(userMainTag, numTags));
                    userProf.setUniversityLocationId(unversityDictionary.getRandomUniversity(randomUniversity,userProf.getLocationId()));

                    // Set wether the user has a smartphone or not. 
                    userProf.setHaveSmartPhone(randomUserAgent.nextDouble() > probHavingSmartPhone);
                    if (userProf.isHaveSmartPhone()) {
                        userProf.setAgentId(userAgentDictionary.getRandomUserAgentIdx(randomAgent));
                    }

                    // Compute the popular places the user uses to visit. 
                    byte numPopularPlaces = (byte) randomNumPopularPlaces.nextInt(maxNumPopularPlaces + 1);
                    Vector<Short> auxPopularPlaces = new Vector<Short>();
                    for (int i = 0; i < numPopularPlaces; i++){
                        short aux = popularDictionary.getPopularPlace(userProf.getLocationId());
                        if(aux != -1) {
                            auxPopularPlaces.add(aux);
                        }
                    }
                    short popularPlaces[] = new short[auxPopularPlaces.size()];
                    Iterator<Short> it = auxPopularPlaces.iterator();
                    int i = 0;
                    while(it.hasNext()) {
                        popularPlaces[i] = it.next();
                        ++i;
                    }
                    userProf.setPopularPlaceIds(popularPlaces);

                    // Set random Index used to sort users randomly
                    userProf.setRandomIdx(randomUserRandomIdx.nextInt(USER_RANDOM_ID_LIMIT));

                    // Set whether the user is a large poster or not.
                    userProf.setLargePoster(IsUserALargePoster(userProf));
                    return userProf;
                }

                private boolean IsUserALargePoster(UserProfile user) {
                    if(dateTimeGenerator.getBirthMonth(user.getBirthDay()) == GregorianCalendar.JANUARY) {
                        return true;
                    } 
                    return false;
                }


                public void setInfoFromUserProfile(ReducedUserProfile user,
                        UserExtraInfo userExtraInfo) {

                    // The country will be always present, but the city can be missing if that data is 
                    // not available on the dictionary
                    int locationId = (user.getCityId() != -1) ? user.getCityId() : user.getLocationId();
                    userExtraInfo.setLocationId(locationId);
                    userExtraInfo.setLocation(locationDictionary.getLocationName(locationId));

                    // We consider that the distance from where user is living and 
                    double distance = randomExactLongLat.nextDouble() * 2;  
                    userExtraInfo.setLatt(locationDictionary.getLatt(user.getLocationId()) + distance);
                    userExtraInfo.setLongt(locationDictionary.getLongt(user.getLocationId()) + distance);

                    userExtraInfo.setUniversity(unversityDictionary.getUniversityName(user.getUniversityLocationId()));

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

                            long relationid = -1;
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

                    userExtraInfo.setFirstName(namesDictionary.getRandomGivenName(randomName,
                                user.getLocationId(),isMale, 
                                dateTimeGenerator.getBirthYear(user.getBirthDay())));

                    userExtraInfo.setLastName(namesDictionary.getRandomSurname(randomSurname,user.getLocationId()));

                    // email is created by using the user's first name + userId
                    int numEmails = randomExtraInfo.nextInt(maxEmails) + 1;
                    double prob = randomExtraInfo.nextDouble();
                    if (prob >= missingRatio) {
                        String base = userExtraInfo.getFirstName();
                        base = Normalizer.normalize(base,Normalizer.Form.NFD);
                        base = base.replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
                        base = base.replaceAll(" ", ".");
                        base = base.replaceAll("[.]+", ".");

                        for (int i = 0; i < numEmails; i++) {
                            String email = base + "" + user.getAccountId() + "@" + emailDictionary.getRandomEmail(randomEmail);
                            userExtraInfo.addEmail(email);
                        }
                    }

                    // Set class year
                    prob = randomExtraInfo.nextDouble();
                    if ((prob < missingRatio) || userExtraInfo.getUniversity().equals("")) {
                        userExtraInfo.setClassYear(-1);
                    } else {
                        userExtraInfo.setClassYear(dateTimeGenerator.getClassYear(
                                    user.getCreationDate(), user.getBirthDay()));
                    }

                    // Set company and workFrom
                    int numCompanies = randomExtraInfo.nextInt(maxCompanies) + 1;
                    prob = randomExtraInfo.nextDouble();
                    if (prob >= missingRatio) {
                        for (int i = 0; i < numCompanies; i++) {
                            long workFrom;
                            if (userExtraInfo.getClassYear() != -1) {
                                workFrom = dateTimeGenerator.getWorkFromYear( randomDate, 
                                                                              user.getCreationDate(), 
                                                                              user.getBirthDay());
                            } else {
                                workFrom = dateTimeGenerator.getWorkFromYear(randomDate, userExtraInfo.getClassYear());
                            }
                            String company = companiesDictionary.getRandomCompany(randomCompany,user.getLocationId());
                            userExtraInfo.addCompany(company, workFrom);
                            String countryName = locationDictionary.getLocationName(companiesDictionary.getCountry(company));
                            stats.countries.add(countryName);

                            GregorianCalendar date = new GregorianCalendar();
                            date.setTimeInMillis(workFrom);
                            String strWorkFrom = DateGenerator.formatYear(date);
                            if (stats.maxWorkFrom == null) {
                                stats.maxWorkFrom = strWorkFrom;
                                stats.minWorkFrom = strWorkFrom;
                            } else {
                                if (stats.maxWorkFrom.compareTo(strWorkFrom) < 0) {
                                    stats.maxWorkFrom = strWorkFrom;
                                }
                                if (stats.minWorkFrom.compareTo(strWorkFrom) > 0) {
                                    stats.minWorkFrom = strWorkFrom;
                                }
                            }
                        }
                    }
                    Vector<Integer> userLanguages = languageDictionary.getLanguages(user.getLocationId());
                    int nativeLanguage = randomExtraInfo.nextInt(userLanguages.size());
                    userExtraInfo.setNativeLanguage(userLanguages.get(nativeLanguage));
                    int internationalLang = languageDictionary.getInternationlLanguage();
                    if (internationalLang != -1 && userLanguages.indexOf(internationalLang) == -1) {
                        userLanguages.add(internationalLang);
                    }
                    userExtraInfo.setLanguages(userLanguages);


                    stats.maxPersonId = Math.max(stats.maxPersonId, user.getAccountId());
                    stats.minPersonId = Math.min(stats.minPersonId, user.getAccountId());
                    stats.firstNames.add(userExtraInfo.getFirstName());
                    String countryName = locationDictionary.getLocationName(user.getLocationId());
                    stats.countries.add(countryName);

                    // NOTE: [2013-08-06] The tags of posts, forums, etc.. all depend of the user ones
                    // if in the future this fact change add those in the statistics also.
                    TreeSet<Integer> tags = user.getSetOfTags();
                    for (Integer tagID : tags) {
                        stats.tagNames.add(tagDictionary.getName(tagID));
                        Integer parent = tagDictionary.getTagClass(tagID);
                        while (parent != -1) {
                            stats.tagClasses.add(tagDictionary.getClassName(parent));
                            parent = tagDictionary.getClassParent(parent);
                        }
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
                    long requestedTime = dateTimeGenerator.randomFriendRequestedDate(randomFriendRequest,user1, user2);
                    byte initiator = (byte) randomInitiator.nextInt(2);
                    long createdTime = -1;
                    long declinedTime = -1;
                    if (randomFriendReject.nextDouble() > friendRejectRatio) {
                        createdTime = dateTimeGenerator.randomFriendApprovedDate(randomFriendApproval,requestedTime);
                    } else {
                        declinedTime = dateTimeGenerator.randomFriendDeclinedDate(randomFriendReject,requestedTime);
                        if (randomFriendAproval.nextDouble() < friendReApproveRatio) {
                            createdTime = dateTimeGenerator.randomFriendReapprovedDate(randomDate,declinedTime);
                        }
                    }

                    user2.addNewFriend(new Friend(user1, requestedTime, declinedTime,
                                createdTime, pass, initiator));
                    user1.addNewFriend(new Friend(user2, requestedTime, declinedTime,
                                createdTime, pass, initiator));

                    friendshipNum++;
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
                    SN.setMachineNumber(machineId, numFiles);
                    String t = type.toLowerCase();
                    if (t.equals("ttl")) {
                        return new Turtle(sibOutputDir +"/" + outputFileName, numRdfOutputFile, true, tagDictionary,
                                browserDictonry, companiesDictionary, 
                                unversityDictionary.GetUniversityLocationMap(),
                                ipAddDictionary, locationDictionary, languageDictionary);
                    } else if (t.equals("n3")) {
                        return new Turtle(sibOutputDir + "/" + outputFileName, numRdfOutputFile, false, tagDictionary,
                                browserDictonry, companiesDictionary, 
                                unversityDictionary.GetUniversityLocationMap(),
                                ipAddDictionary, locationDictionary, languageDictionary);
                    } else if (t.equals("csv")) {
                        return new CSV(sibOutputDir, numRdfOutputFile, tagDictionary,
                                browserDictonry, companiesDictionary, 
                                unversityDictionary.GetUniversityLocationMap(),
                                ipAddDictionary,locationDictionary, languageDictionary);
                    } else if (t.equals("csv_merge_foreign")) {
                        return new CSVMergeForeign(sibOutputDir, numRdfOutputFile, tagDictionary,
                                browserDictonry, companiesDictionary, 
                                unversityDictionary.GetUniversityLocationMap(),
                                ipAddDictionary,locationDictionary, languageDictionary);
                    } else if (t.equals("none")) {
                        return new EmptySerializer();
                    } else {
                        System.err.println("Unexpected Serializer - Aborting");
                        System.exit(-1);
                        return null;
                    }
                }

                private void writeStatistics() {
                    Gson gson = new GsonBuilder().setExclusionStrategies(stats.getExclusion()).disableHtmlEscaping().create();
                    FileWriter writer;
                    try {
                        stats.makeCountryPairs(locationDictionary);
                        writer = new FileWriter(sibOutputDir + "m" + machineId + STATS_FILE);
                        writer.append(gson.toJson(stats));
                        writer.flush();
                        writer.close();
                    } catch (IOException e) {
                        System.err.println("Unable to write stastistics");
                        System.err.println(e.getMessage());
                        e.printStackTrace();
                    }
                }
            }

