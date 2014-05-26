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

import java.io.*;
import java.util.*;
import java.text.Normalizer;
import java.lang.Math;

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
import ldbc.socialnet.dbgen.objects.*;
import ldbc.socialnet.dbgen.serializer.*;
import ldbc.socialnet.dbgen.storage.StorageManager;
import ldbc.socialnet.dbgen.util.ScaleFactor;
import ldbc.socialnet.dbgen.vocabulary.SN;
import ldbc.socialnet.dbgen.util.MapReduceKey;
import ldbc.socialnet.dbgen.util.RandomGeneratorFarm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;


public class ScalableGenerator{

    /** @brief A type of organization.**/
    public enum OrganisationType {
        university,
        company
    }


    public long postId  = 0;                             /**< @brief The post and comment identifier counter.*/
    public long groupId = 0;

    private static final int        NUM_FRIENDSHIP_HADOOP_JOBS = 3;                 /**< @brief The number of hadoop jobs used to generate the friendships.*/
    private static final double     friendRatioPerJob[] = { 0.45, 0.45, 0.1 };      /**< @brief The percentage of friendships generated in each hadoop job.*/
    private static final int        USER_RANDOM_ID_LIMIT = 100;                     /**< @brief The number of different random ids generated in the last hadoop job.*/

    private static final int startMonth = 1;
    private static final int startDate  = 1;
    private static final int endMonth   = 1;
    private static final int endDate    = 1;

    private static final double alpha = 0.4;        /**< @brief PowerLaw distribution alpha parameter.*/
    private static final int maxNumLikes = 1000;      /**< @brief The maximum number of likes per post*/

    private static final double levelProbs[] = { 0.5, 0.8, 1.0 };  /**< @brief Cumulative probability to join a group for the user direct friends, friends of friends and friends of the friends of the user friend.*/
    private static final double joinProbs[] = { 0.7, 0.4, 0.1 }; /**< @brief Probability to join a group for the user direct friends, friends of friends and friends of the friends of the user friend.*/

    //Files and folders
    private static final String  DICTIONARY_DIRECTORY = "/dictionaries/";
    private static final String  IPZONE_DIRECTORY     = "/ipaddrByCountries";
    private static final String  CSV_DIRECTORY        = "/csvSerializers";
    private static final String  PARAMETERS_FILE      = "params.ini";
    private static final String  SCALE_FACTORS_FILE   = "scale_factors.xml";
    private static final String  STATS_FILE           = "testdata.json";
    private static final String  RDF_OUTPUT_FILE      = "ldbc_socialnet_dbg";
    private static final String  PARAM_COUNT_FILE     = "factors.txt";

    // Dictionaries dataset files
    private static final String   browserDictonryFile       = DICTIONARY_DIRECTORY + "browsersDic.txt";
    private static final String   companiesDictionaryFile   = DICTIONARY_DIRECTORY + "companiesByCountry.txt";
    private static final String   countryAbbrMappingFile    = DICTIONARY_DIRECTORY + "countryAbbrMapping.txt";
    private static final String   tagDictionaryFile         = DICTIONARY_DIRECTORY + "dicCelebritiesByCountry.txt";
    private static final String   countryDictionaryFile     = DICTIONARY_DIRECTORY + "dicLocations.txt";
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
    private static final String   fbSocialDegreeFile	    = DICTIONARY_DIRECTORY + "facebookBucket100.dat";

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
    private final String MAX_NUM_FLASHMOB_POST_PER_MONTH        = "maxNumFlashmobPostPerMonth";
    private final String MAX_NUM_GROUP_FLASHMOB_POST_PER_MONTH  = "maxNumGroupFlashmobPostPerMonth";
    private final String MAX_NUM_TAG_PER_FLASHMOB_POST          = "maxNumTagPerFlashmobPost";
    private final String FLASHMOB_TAG_MIN_LEVEL                 = "flashmobTagMinLevel";
    private final String FLASHMOB_TAG_MAX_LEVEL                 = "flashmobTagMaxLevel";
    private final String FLASHMOB_TAG_DIST_EXP                  = "flashmobTagDistExp";
    private final String DELTA_TIME                             = "deltaTime";
    private final String UPDATE_PORTION                         = "updatePortion";

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
            PROB_INTEREST_FLASHMOB_TAG, PROB_RANDOM_PER_LEVEL,MAX_NUM_FLASHMOB_POST_PER_MONTH, MAX_NUM_GROUP_FLASHMOB_POST_PER_MONTH, MAX_NUM_TAG_PER_FLASHMOB_POST, FLASHMOB_TAG_MIN_LEVEL, FLASHMOB_TAG_MAX_LEVEL,
            FLASHMOB_TAG_DIST_EXP, DELTA_TIME, UPDATE_PORTION};

    //final user parameters
    private final String SCALE_FACTOR       = "scaleFactor";
    private final String SERIALIZER_TYPE    = "serializerType";
    private final String EXPORT_TEXT        = "exportText";
    private final String ENABLE_COMPRESSION = "enableCompression";
    private final String NUM_PERSONS        = "numPersons";
    private final String NUM_YEARS          = "numYears";
    private final String START_YEAR         = "startYear";

    /**
     * This array provides a quick way to check if any of the required parameters is missing and throw the appropriate
     * exception in the method loadParamsFromFile()
     */
    private final String[] publicCheckParameters = {SERIALIZER_TYPE, ENABLE_COMPRESSION};

    // Gender string representation, both representations vector/standalone so the string is coherent.
    private final String MALE   = "male";
    private final String FEMALE = "female";
    private final String gender[] = { MALE, FEMALE};

    //Stat container
    private Statistics stats;
    
    // bookkeeping for parameter generation
    private HashMap<Long, ReducedUserProfile.Counts> factorTable;
    private HashMap<Integer, Integer> postsPerCountry;
    private HashMap<Integer, Integer> tagClassCount;
    private HashMap<String, Integer> firstNameCount;
    
    // For blocking
    private static final int  reducerShift[] = { 26, 8, 1 };

    // For sliding window
    int 					cellSize; // Number of user in one cell
    int 					numberOfCellPerWindow;
    int	 					numTotalUser;
    int 					windowSize;
    int						machineId;
    int                     numFiles;

    ReducedUserProfile  	reducedUserProfiles[];
    ReducedUserProfile 		reducedUserProfilesCell[];

    // For friendship generation
    int 				friendshipNum;
    int 				minNumFriends;
    int 				maxNumFriends;
    double 				friendRejectRatio;
    double 				friendReApproveRatio;

    StorageManager		groupStoreManager;

    MRWriter			mrWriter;

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

    DateGenerator 		dateTimeGenerator;
    int					startYear;
    int                 endYear;

    // Dictionary classes
    LocationDictionary 		locationDictionary;
    LanguageDictionary      languageDictionary;
    TagDictionary 			tagDictionary;

    //For facebook-like social degree distribution
    FBSocialDegreeGenerator	fbDegreeGenerator;
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
    DataExporter    dataExporter = null;
    String 			serializerType;
    String 			outUserProfileName = "userProf.ser";
    String 			outUserProfile;
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
    int            maxNumFlashmobPostPerMonth = 0;
    int            maxNumGroupFlashmobPostPerMonth = 0;
    int            maxNumTagPerFlashmobPost = 0;
    double         flashmobTagMinLevel = 0.0f;
    double         flashmobTagMaxLevel = 0.0f;
    double         flashmobTagDistExp  = 0.0f;

    long            deltaTime = 0;
    long            dateThreshold = 0;
    double          updatePortion = 0.0;

    // Data accessed from the hadoop jobs
    private ReducedUserProfile[] cellReducedUserProfiles;
    private int     numUserProfilesRead      = 0;
    public int     totalNumUserProfilesRead = 0;
    private int     numUserForNewCell        = 0;
    private int     mrCurCellPost            = 0;
    public static int     blockId                 = 0;
    public int     exactOutput              = 0;

    public boolean exportText = true;
    public boolean enableCompression = true;

    RandomGeneratorFarm randomFarm;
    public static final int blockSize = 10000;

    TreeMap<Integer, ScaleFactor> scaleFactors;

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
        this.postsPerCountry = new HashMap<Integer, Integer>();
        this.tagClassCount = new HashMap<Integer, Integer>();
        this.firstNameCount = new HashMap<String, Integer>();
        
        System.out.println("Map Reduce File Idx is: " + mapreduceFileIdx);
        if (mapreduceFileIdx != -1){
            outUserProfile = "mr" + mapreduceFileIdx + "_" + outUserProfileName;
        }
        System.out.println("Current directory in ScaleGenerator is " + sibHomeDir);
    }

    /**
     * Initializes the generator reading the private parameter file, the user parameter file
     * and initialize all the internal variables.
     *
     * @param numMaps: How many hadoop reduces are performing the job.
     * @param mapId: The hadoop reduce id.
     */
    public void init(int numMaps, int mapId){
        this.numFiles = numMaps;
        this.machineId = mapId;
        this.factorTable = new HashMap<Long, ReducedUserProfile.Counts>();
        loadParamsFromFile();
        randomFarm = new RandomGeneratorFarm();
        windowSize = cellSize * numberOfCellPerWindow;                          // We compute the size of the window.
        mrWriter = new MRWriter(cellSize, windowSize, sibOutputDir);
        resetWindow();
        randomPowerLaw    = new PowerDistGenerator(minNumFriends,     maxNumFriends + 1,     alpha);
        randomTagPowerLaw = new PowerDistGenerator(minNumTagsPerUser, maxNumTagsPerUser + 1, alpha);

        // Initializing window memory
        reducedUserProfiles = new ReducedUserProfile[windowSize];
        cellReducedUserProfiles = new ReducedUserProfile[cellSize];

        dateTimeGenerator = new DateGenerator( new GregorianCalendar(startYear, startMonth, startDate),
                new GregorianCalendar(endYear, endMonth, endDate), alpha, deltaTime);
        dateThreshold = dateTimeGenerator.getMaxDateTime() - (long)((dateTimeGenerator.getMaxDateTime() - dateTimeGenerator.getStartDateTime())*(updatePortion));
        SN.minDate = dateTimeGenerator.getStartDateTime();
        SN.maxDate = dateTimeGenerator.getMaxDateTime();

        System.out.println("Building location dictionary ");
        locationDictionary = new LocationDictionary(numTotalUser, countryDictionaryFile, cityDictionaryFile);
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
        topicTagDictionary = new TagMatrix(tagTopicDictionaryFile, tagDictionary.getNumCelebrity());
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


        System.out.println("Building companies dictionary");
        companiesDictionary = new CompanyDictionary(companiesDictionaryFile,locationDictionary,
                probUnCorrelatedCompany);
        companiesDictionary.init();

        System.out.println("Building university dictionary");
        unversityDictionary = new UniversityDictionary(universityDictionaryFile, locationDictionary,
                probUnCorrelatedOrganization, probTopUniv, companiesDictionary.getNumCompanies());
        unversityDictionary.init();

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
                locationDictionary, 0, popularDictionary, probPopularPlaces, maxNumLikes, deltaTime, randomFarm);

        System.out.println("Building Group generator");
        groupGenerator = new GroupGenerator(dateTimeGenerator, locationDictionary,
                tagDictionary, numTotalUser);



        /// IMPORTANT: ratioLargeText is divided 0.083333, the probability
        /// that SetUserLargePoster returns true.
        System.out.println("Building Uniform Post Generator");
        uniformPostGenerator = new UniformPostGenerator( dateTimeGenerator,
                tagTextDictionary,
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
                exportText,
                deltaTime,
                maxNumPostPerMonth,
                maxNumFriends,
                maxNumGroupPostPerMonth,
                maxNumMemberGroup
        );
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
        flashmobPostGenerator = new FlashmobPostGenerator(dateTimeGenerator, tagTextDictionary,
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
                exportText,
                deltaTime,
                flashmobTagDictionary,
                topicTagDictionary,
                maxNumFlashmobPostPerMonth,
                maxNumGroupFlashmobPostPerMonth,
                maxNumFriends,
                maxNumMemberGroup,
                maxNumTagPerFlashmobPost,
                flashmobDistFile
        );
        flashmobPostGenerator.initialize();

        /// IMPORTANT: ratioLargeText is divided 0.083333, the probability
        /// that SetUserLargePoster returns true.
        System.out.println("Building Comment Generator");
        commentGenerator = new CommentGenerator( tagDictionary,
                tagTextDictionary,
                topicTagDictionary,
                dateTimeGenerator,
                browserDictonry,
                ipAddDictionary,
                userAgentDictionary,
                minCommentSize,
                maxCommentSize,
                minLargeCommentSize,
                maxLargeCommentSize,
                ratioLargeComment/0.0833333,
                maxNumLikes,exportText,
                deltaTime
        );
        commentGenerator.initialize();

        System.out.println("Building Facebook-like social degree generator");
        fbDegreeGenerator = new FBSocialDegreeGenerator(numTotalUser, fbSocialDegreeFile, 0);
        fbDegreeGenerator.loadFBBuckets();
        fbDegreeGenerator.rebuildBucketRange();
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
            maxNumFlashmobPostPerMonth = Integer.parseInt(properties.getProperty(MAX_NUM_FLASHMOB_POST_PER_MONTH));
            maxNumGroupFlashmobPostPerMonth = Integer.parseInt(properties.getProperty(MAX_NUM_GROUP_FLASHMOB_POST_PER_MONTH));
            maxNumTagPerFlashmobPost = Integer.parseInt(properties.getProperty(MAX_NUM_TAG_PER_FLASHMOB_POST));
            flashmobTagMinLevel = Double.parseDouble(properties.getProperty(FLASHMOB_TAG_MIN_LEVEL));
            flashmobTagMaxLevel = Double.parseDouble(properties.getProperty(FLASHMOB_TAG_MAX_LEVEL));
            flashmobTagDistExp = Double.parseDouble(properties.getProperty(FLASHMOB_TAG_DIST_EXP));
            deltaTime = Long.parseLong(properties.getProperty(DELTA_TIME));
            updatePortion = Double.parseDouble(properties.getProperty(UPDATE_PORTION));
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            System.exit(-1);
        }

        try {
            scaleFactors = new TreeMap<Integer,ScaleFactor>();
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(getClass().getResourceAsStream("/"+SCALE_FACTORS_FILE));
            doc.getDocumentElement().normalize();

            System.out.println("Reading scale factors..");
            NodeList nodes = doc.getElementsByTagName("scale_factor");
            for (int i = 0; i < nodes.getLength(); i++) {
                Node node = nodes.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    Element element = (Element) node;
                    Integer num = Integer.parseInt(element.getAttribute("number"));
                    ScaleFactor scaleFactor = new ScaleFactor();
                    NodeList files = element.getElementsByTagName("num_persons");
                    scaleFactor.numPersons = Integer.parseInt(files.item(0).getTextContent());
                    files = element.getElementsByTagName("start_year");
                    scaleFactor.startYear = Integer.parseInt(files.item(0).getTextContent());
                    files = element.getElementsByTagName("num_years");
                    scaleFactor.numYears = Integer.parseInt(files.item(0).getTextContent());
                    scaleFactors.put(num,scaleFactor);
                }
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(-1);
        }

        try {
            System.out.println("Reading parameters file ...");
            Properties properties = new Properties();
            properties.load(new InputStreamReader(new FileInputStream(sibHomeDir + "/"+PARAMETERS_FILE), "UTF-8"));
            for (int i = 0; i < publicCheckParameters.length; i++) {
                if (properties.getProperty(publicCheckParameters[i]) == null) {
                    throw new IllegalStateException("Missing " + publicCheckParameters[i] + " parameter");
                }
            }

            boolean scaleFactorSet = properties.getProperty(SCALE_FACTOR) != null;
            boolean numPersonsSet = properties.getProperty(NUM_PERSONS) != null;
            boolean numYearsSet = properties.getProperty(NUM_YEARS) != null;
            boolean startYearSet = properties.getProperty(START_YEAR) != null;

            if(scaleFactorSet) {
                int scaleFactorId = Integer.parseInt(properties.getProperty(SCALE_FACTOR));
                ScaleFactor scaleFactor = scaleFactors.get(scaleFactorId);
                System.out.println("Executin with scale factor " + scaleFactorId);
                System.out.println(" ... Num Persons " + scaleFactor.numPersons);
                System.out.println(" ... Start Year " + scaleFactor.startYear);
                System.out.println(" ... Num Years " + scaleFactor.numYears);
                numTotalUser = scaleFactor.numPersons;
                startYear = scaleFactor.startYear;
                int numYears = scaleFactor.numYears;
                endYear = startYear + numYears;
            } else if( numPersonsSet && numYearsSet && startYearSet ) {
                numTotalUser = Integer.parseInt(properties.getProperty(NUM_PERSONS));
                startYear = Integer.parseInt(properties.getProperty(START_YEAR));
                int numYears = Integer.parseInt(properties.getProperty(NUM_YEARS));
                endYear = startYear + numYears;
            } else {
                throw new IllegalStateException("Missing parameters. Please specify scale factor or num persons, start year and num years");
            }

            serializerType = properties.getProperty(SERIALIZER_TYPE);
//            exportText = Boolean.parseBoolean(properties.getProperty(EXPORT_TEXT));
            exportText = true;
            enableCompression = Boolean.parseBoolean(properties.getProperty(ENABLE_COMPRESSION));
            if (!serializerType.equals("ttl") && !serializerType.equals("n3") &&
                    !serializerType.equals("csv") && !serializerType.equals("none") && !serializerType.equals("csv_merge_foreign")) {
                throw new IllegalStateException("serializerType must be ttl, n3, csv, csv_merge_foreign");
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(-1);
        }
    }

    public void openSerializer() {
        dataExporter = getSerializer(serializerType, RDF_OUTPUT_FILE);
    }

    public void closeSerializer() {
        dataExporter.close();
        writeStatistics();
        writeFactorTable();
        System.out.println("Number of generated triples " + dataExporter.unitsGenerated());
        System.out.println("Number of popular users " + numPopularUser);
        System.out.println("Writing the data for test driver ");
    }

    public void generateUserActivity( ReducedUserProfile userProfile, Reducer<MapReduceKey, ReducedUserProfile,MapReduceKey, ReducedUserProfile>.Context context) {
        int index = numUserProfilesRead%windowSize;
        numUserProfilesRead++;
        reducedUserProfiles[index] = userProfile;
        UserExtraInfo extraInfo = new UserExtraInfo();
        reducedUserProfiles[index].setForumWallId(SN.composeId(groupId, reducedUserProfiles[index].getCreationDate()));
        groupId++;
        setInfoFromUserProfile(reducedUserProfiles[index], extraInfo);
        UserInfo userInfo = new UserInfo();
        userInfo.user = reducedUserProfiles[index];
        userInfo.extraInfo = extraInfo;
        dataExporter.export(userInfo);
        int nameCount = firstNameCount.containsKey(extraInfo.getFirstName())? firstNameCount.get(extraInfo.getFirstName()):0;
        firstNameCount.put(extraInfo.getFirstName(), nameCount+1);
        generatePosts(uniformPostGenerator,reducedUserProfiles[index], extraInfo);
        generatePosts(flashmobPostGenerator, reducedUserProfiles[index], extraInfo);
        generatePhotos(reducedUserProfiles[index], extraInfo);
        generateUserGroups(reducedUserProfiles[index], extraInfo);
        if( numUserProfilesRead % 100 == 0) context.setStatus("Generated post and photo for "+numUserProfilesRead+" users");
        factorTable.get(userProfile.getAccountId()).numberOfFriends = userProfile.getFriendIds().size();
        factorTable.get(userProfile.getAccountId()).numberOfWorkPlaces = userProfile.getNumOfWorkPlaces();
    }

    private void generateUserGroups(ReducedUserProfile userProfile, UserExtraInfo extraInfo ) {
        double moderatorProb = randomFarm.get(RandomGeneratorFarm.Aspect.GROUP_MODERATOR).nextDouble();
        if (moderatorProb <= groupModeratorProb) {
            Friend firstLevelFriends[] = userProfile.getFriendList();
            Vector<Friend> secondLevelFriends = new Vector<Friend>();
            //TODO: Include friends of friends a.k.a second level friends?
            int numGroup = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_GROUP).nextInt(maxNumGroupCreatedPerUser)+1;
            for (int j = 0; j < numGroup; j++) {
                createGroupForUser(userProfile, firstLevelFriends, secondLevelFriends);
            }
        }
    }

    public void resetWindow() {
        numUserProfilesRead = 0;
        numUserForNewCell = 0;
        mrCurCellPost = 0;
    }

    public void pushUserProfile(ReducedUserProfile reduceUser, int pass, int outputDimension, Reducer<MapReduceKey, ReducedUserProfile,MapReduceKey, ReducedUserProfile>.Context context){
        ReducedUserProfile userObject = new ReducedUserProfile();
        userObject.copyFields(reduceUser);
        totalNumUserProfilesRead++;
        if (numUserProfilesRead < windowSize) {                             // Push the user into the window if there is enought space. 
            reducedUserProfiles[numUserProfilesRead] = userObject;
            numUserProfilesRead++;
        } else {                                                            // If the window is full, push the user into the backup cell. 
            cellReducedUserProfiles[numUserForNewCell] = userObject;
            numUserForNewCell++;
            if (numUserForNewCell == cellSize) {                            // Once the backup cell is full, create friendships and slide the window.
                mr2SlideFriendShipWindow( pass,
                        mrCurCellPost,
                        context,
                        cellReducedUserProfiles,
                        outputDimension);
                mrCurCellPost++;
                numUserForNewCell = 0;
            }
        }
    }

    public void pushAllRemainingUser(int pass, int outputDimension, Reducer<MapReduceKey, ReducedUserProfile,MapReduceKey, ReducedUserProfile>.Context context){

        // For each remianing cell in the window, we create the edges.
        for (int numLeftCell = Math.min(numberOfCellPerWindow, numUserProfilesRead/cellSize); numLeftCell > 0; --numLeftCell, ++mrCurCellPost) {
            mr2SlideLastCellsFriendShip(pass, mrCurCellPost, numLeftCell, context, outputDimension);
        }

        // We write to the context the users that might have been left into not fully filled cell.
            mrWriter.writeReducedUserProfiles(0, numUserForNewCell, outputDimension, cellReducedUserProfiles, context);
        exactOutput+=numUserForNewCell;
    }


    public void resetState(int seed) {
        blockId = seed;
        postId = 0;
        groupId = 0;
        SN.setMachineNumber(blockId, (int)Math.ceil(numTotalUser / (double) (blockSize)) );
        fbDegreeGenerator.resetState(seed);
        resetWindow();
        randomFarm.resetRandomGenerators((long)seed);
        if( dataExporter != null ) {
            dataExporter.resetState(seed);
        }
    }


    /**
     * Generates the users. The user generation process is divided in blocks of size four times the
     * size of the window. At the beginning of each block, the seeds of the random number generators
     * are reset. This is for the sake of determinism, so independently of the mapper that receives a
     * block, the seeds are set deterministically, and therefore, we make this generation phase 
     * deterministic. This implies that the different mappers have to process blocks of full size,
     * that is, a block have to be fully processed by a mapper.
     *
     * @param pass The pass identifying the current pass.
     * @param context The map-reduce context.
     * @param mapIdx The index of the current map, used to determine how many users to generate.
     */
    public void mrGenerateUserInfo(int pass, Context context, int mapIdx){

        if (numTotalUser % cellSize != 0) {
            System.err.println("Number of users should be a multiple of the cellsize");
            System.exit(-1);
        }

        // Here we determine the blocks in the "block space" that this mapper is responsible for.
        int numBlocks = (int) (Math.ceil(numTotalUser / (double)blockSize));
        int initBlock = (int) (Math.ceil((numBlocks / (double)numFiles) * mapIdx ));
        int endBlock  = (int) (Math.ceil((numBlocks / (double)numFiles) * (mapIdx+1)));

        int numUsersToGenerate = 0;
        for( int i = initBlock; i < endBlock;++i) {
            // Setting the state for the block
            resetState(i);
            locationDictionary.advanceToUser(i*blockSize);      // We location dictionary has to be advanced to the specific point.
            for (int j = i*blockSize; j < (i+1)*blockSize && j < numTotalUser; ++j) {
                ReducedUserProfile reduceUserProf = generateGeneralInformation(j);
                ++numUsersToGenerate;
                try {
                    int block =  0;                                                                  // The mapreduce group this university will be assigned.
                    int key = reduceUserProf.getDicElementId(pass);                                  // The key used to sort within the block.
                    long id = reduceUserProf.getAccountId();                                         // The id used to sort within the key, to guarantee determinism.
                    MapReduceKey mpk = new MapReduceKey( block, key, id );
                    context.write(mpk, reduceUserProf);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println("Number of generated users: "+numUsersToGenerate);
    }

    private void generateCellOfUsers2(int newStartIndex, ReducedUserProfile[] _cellReduceUserProfiles){
        int curIdxInWindow;
        for (int i = 0; i < cellSize; i++) {
            curIdxInWindow = newStartIndex + i;
            reducedUserProfiles[curIdxInWindow] = _cellReduceUserProfiles[i];
        }
    }


    private void mr2SlideFriendShipWindow(int pass, int cellPos, Reducer.Context context, ReducedUserProfile[] _cellReduceUserProfiles,
                                         int outputDimension){

        int cellPosInWindow = (cellPos) % numberOfCellPerWindow;
        int startIndex = cellPosInWindow * cellSize;
        for (int i = 0; i < cellSize; i++) {
            int curIdxInWindow = startIndex + i;
            for (int j = i + 1; (j < windowSize - 1) &&
                    reducedUserProfiles[curIdxInWindow].getNumFriendsAdded() < reducedUserProfiles[curIdxInWindow].getNumPassFriends(pass);
                 j++) {
                int checkFriendIdx = (curIdxInWindow + j) % windowSize;
                if ( !(reducedUserProfiles[checkFriendIdx].getNumFriendsAdded() == reducedUserProfiles[checkFriendIdx].getNumPassFriends(pass) ||
                        reducedUserProfiles[curIdxInWindow].isExistFriend(reducedUserProfiles[checkFriendIdx].getAccountId()))) {
                    double randProb = randomFarm.get(RandomGeneratorFarm.Aspect.UNIFORM).nextDouble();
                    double prob = getFriendCreatePro(curIdxInWindow, checkFriendIdx, pass);
                    if ((randProb < prob) || (randProb < limitProCorrelated)) {
                        createFriendShip(reducedUserProfiles[curIdxInWindow], reducedUserProfiles[checkFriendIdx], (byte) pass);
                    }
                }
            }
        }
        updateLastPassFriendAdded(startIndex, startIndex + cellSize, pass);
        mrWriter.writeReducedUserProfiles(startIndex, startIndex + cellSize, outputDimension, reducedUserProfiles, context);
        generateCellOfUsers2(startIndex, _cellReduceUserProfiles);
        exactOutput = exactOutput + cellSize;
    }

    private void mr2SlideLastCellsFriendShip(int pass, int cellPos,	int numleftCell, Reducer.Context context, int outputDimension) {

        int startIndex = (cellPos % numberOfCellPerWindow) * cellSize;
        for (int i = 0; i < cellSize; i++) {
            int curIdxInWindow = startIndex + i;
            // From this user, check all the user in the window to create friendship
            for (int j = i + 1; (j < numleftCell * cellSize )
                    && reducedUserProfiles[curIdxInWindow].getNumFriendsAdded()
                    < reducedUserProfiles[curIdxInWindow].getNumPassFriends(pass);
                 j++) {
                int checkFriendIdx = (curIdxInWindow + j) % windowSize;
                if ( !(reducedUserProfiles[checkFriendIdx].getNumFriendsAdded() ==
                        reducedUserProfiles[checkFriendIdx].getNumPassFriends(pass) ||
                        reducedUserProfiles[curIdxInWindow].isExistFriend(reducedUserProfiles[checkFriendIdx].getAccountId()))) {
                    double randProb = randomFarm.get(RandomGeneratorFarm.Aspect.UNIFORM).nextDouble();
                    double prob = getFriendCreatePro(curIdxInWindow, checkFriendIdx, pass);
                    if ((randProb < prob) || (randProb < limitProCorrelated)) {
                        createFriendShip(reducedUserProfiles[curIdxInWindow], reducedUserProfiles[checkFriendIdx],
                                (byte) pass);
                    }
                }
            }
        }
        updateLastPassFriendAdded(startIndex, startIndex + cellSize, pass);
        mrWriter.writeReducedUserProfiles(startIndex, startIndex + cellSize, outputDimension, reducedUserProfiles, context);
        exactOutput = exactOutput + cellSize;
    }

    private void generatePosts(PostGenerator postGenerator, ReducedUserProfile user, UserExtraInfo extraInfo){
        Vector<Post> createdPosts = postGenerator.createPosts( randomFarm, user, extraInfo, postId );
        postId+=createdPosts.size();
        if (!factorTable.containsKey(user.getAccountId()))
        	factorTable.put(user.getAccountId(), new ReducedUserProfile.Counts());
        factorTable.get(user.getAccountId()).numberOfPosts += createdPosts.size();

        Iterator<Post> it = createdPosts.iterator();
        GregorianCalendar c = new GregorianCalendar();
        
        while(it.hasNext()) {
        	
            Post post = it.next();
            if (post.getLikes() != null){
            	factorTable.get(user.getAccountId()).numberOfLikes += post.getLikes().length;
            }
            if (post.getTags() != null) {
            	factorTable.get(user.getAccountId()).numberOfTagsOfPosts += post.getTags().size();
            	for (Integer t: post.getTags()){
            		Integer tagClass = tagDictionary.getTagClass(t);
                	Integer tagCount = tagClassCount.containsKey(tagClass) ? tagClassCount.get(tagClass) : 0;
                	tagClassCount.put(tagClass, tagCount+1);
            	}
            }
            c.setTimeInMillis(post.getCreationDate());
            int bucket = DateGenerator.getNumberOfMonths(c, startMonth, startYear);
            if (bucket < factorTable.get(user.getAccountId()).numberOfPostsPerMonth.length){
            	factorTable.get(user.getAccountId()).numberOfPostsPerMonth[bucket]++;
            }
            
            int locationID = ipAddDictionary.getLocation(post.getIpAddress());
            String countryName = locationDictionary.getLocationName(locationID);
            stats.countries.add(countryName);
                       
            int postCount = postsPerCountry.containsKey(locationID) ? postsPerCountry.get(locationID) : 0;
            postsPerCountry.put(locationID, postCount+1);

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
            dataExporter.export(post);
            // Generate comments
            int numComment = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_COMMENT).nextInt(maxNumComments+1);
            ArrayList<Message> replyCandidates = new ArrayList<Message>();
            replyCandidates.add(post);
            for (int l = 0; l < numComment; l++) {
                int replyIndex = randomFarm.get(RandomGeneratorFarm.Aspect.REPLY_TO).nextInt(replyCandidates.size());
                Comment comment = commentGenerator.createComment(randomFarm, postId, post, replyCandidates.get(replyIndex),user);
                if ( comment!=null ) {
                    if (!factorTable.containsKey(comment.getAuthorId())) {
                    	factorTable.put(comment.getAuthorId(), new ReducedUserProfile.Counts());
                    }
                	factorTable.get(comment.getAuthorId()).numberOfPosts++;
                	
                    c.setTimeInMillis(comment.getCreationDate());
                    bucket = DateGenerator.getNumberOfMonths(c, startMonth, startYear);
                    if (bucket < factorTable.get(comment.getAuthorId()).numberOfPostsPerMonth.length){
                    	factorTable.get(comment.getAuthorId()).numberOfPostsPerMonth[bucket]++;
                    }

                	if (comment.getLikes() != null) {
                		factorTable.get(comment.getAuthorId()).numberOfLikes += comment.getLikes().length;
                	}
                	if (comment.getTags() != null) {
                		factorTable.get(comment.getAuthorId()).numberOfTagsOfPosts += comment.getTags().size();
                    	for (Integer t: comment.getTags()){
                    		Integer tagClass = tagDictionary.getTagClass(t);
                        	Integer tagCount = tagClassCount.containsKey(tagClass) ? tagClassCount.get(tagClass) : 0;
                        	tagClassCount.put(tagClass, tagCount+1);
                    	}
                	}
                	locationID = ipAddDictionary.getLocation(comment.getIpAddress());
                	countryName = locationDictionary.getLocationName(locationID);
                	postCount = postsPerCountry.containsKey(locationID) ? postsPerCountry.get(locationID) : 0;
                	postsPerCountry.put(locationID, postCount+1);
                    stats.countries.add(countryName);
                    dataExporter.export(comment);
                    if( comment.getTextSize() > 10 ) replyCandidates.add(comment);
                    postId++;
                }
            }
        }
    }

    private void generatePhotos(ReducedUserProfile user, UserExtraInfo extraInfo){
        // Generate photo Album and photos
        int numOfmonths = (int) dateTimeGenerator.numberOfMonths(user);
        int numPhotoAlbums = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_PHOTO_ALBUM).nextInt(maxNumPhotoAlbumsPerMonth+1);
        if (numOfmonths != 0) {
            numPhotoAlbums = numOfmonths * numPhotoAlbums;
        }
        GregorianCalendar c = new GregorianCalendar();

        for (int m = 0; m < numPhotoAlbums; m++) {
            Group album = groupGenerator.createAlbum(randomFarm, groupId, user, extraInfo, m, joinProbs[0]);
            if( album != null ) {
                groupId++;
                dataExporter.export(album);
                GroupMemberShip memberships[] = album.getMemberShips();
                int numMembers = album.getNumMemberAdded();
                for( int i = 0; i < numMembers; ++i ) {
            		if (!factorTable.containsKey(memberships[i].getUserId()))
            			factorTable.put(memberships[i].getUserId(), new ReducedUserProfile.Counts());
            		factorTable.get(memberships[i].getUserId()).numberOfGroups++;
            		System.out.println("Adding user "+memberships[i].getUserId()+" to an album "+album.getGroupId());
                }

                // Generate photos for this album
                int numPhotos = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_PHOTO).nextInt(maxNumPhotoPerAlbums+1);
                if (!factorTable.containsKey(user.getAccountId()))
                	factorTable.put(user.getAccountId(), new ReducedUserProfile.Counts());
                factorTable.get(user.getAccountId()).numberOfPosts += numPhotos;
                
                for (int l = 0; l < numPhotos; l++) {
                    Photo photo = photoGenerator.generatePhoto(user, album, l, postId);
                    if( photo != null ) {
                        postId++;
                        photo.setUserAgent(userAgentDictionary.getUserAgentName(randomFarm.get(RandomGeneratorFarm.Aspect.USER_AGENT_SENT),user.isHaveSmartPhone(), user.getAgentId()));
                        photo.setBrowserIdx(browserDictonry.getPostBrowserId(randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_BROWSER),randomFarm.get(RandomGeneratorFarm.Aspect.BROWSER), user.getBrowserIdx()));
                        photo.setIpAddress(ipAddDictionary.getIP(randomFarm.get(RandomGeneratorFarm.Aspect.IP), randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP), randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP_FOR_TRAVELER), user.getIpAddress(),
                                user.isFrequentChange(), photo.getCreationDate(), photo.getLocationId()));
                        int locationID = ipAddDictionary.getLocation(photo.getIpAddress());
                        String countryName = locationDictionary.getLocationName(locationID);
                        int postCount = postsPerCountry.containsKey(locationID) ? postsPerCountry.get(locationID) : 0;
                        postsPerCountry.put(locationID, postCount+1);
                        stats.countries.add(countryName);
                        dataExporter.export(photo);
                        
                        c.setTimeInMillis(photo.getCreationDate());
                        int bucket = DateGenerator.getNumberOfMonths(c, startMonth, startYear);
                        if (bucket < factorTable.get(photo.getAuthorId()).numberOfPostsPerMonth.length){
                        	factorTable.get(photo.getAuthorId()).numberOfPostsPerMonth[bucket]++;
                        }

                    	if (photo.getLikes() != null) {
                    		Like[] likesPhoto = photo.getLikes();
                        	factorTable.get(photo.getAuthorId()).numberOfLikes += photo.getLikes().length;
                    	}
                    	if (photo.getTags() != null) {
                    		factorTable.get(photo.getAuthorId()).numberOfTagsOfPosts += photo.getTags().size();
                        	for (Integer t: photo.getTags()){
                        		Integer tagClass = tagDictionary.getTagClass(t);
                            	Integer tagCount = tagClassCount.containsKey(tagClass) ? tagClassCount.get(tagClass) : 0;
                            	tagClassCount.put(tagClass, tagCount+1);
                        	}
                    	}

                    }
                }
            }
        }
    }

    private void createGroupForUser(ReducedUserProfile user,
                                   Friend firstLevelFriends[], Vector<Friend> secondLevelFriends) {
        double randLevelProb;
        double randMemberProb;

        Group group = groupGenerator.createGroup(randomFarm,groupId,user);
        if( group != null ) {
            groupId++;
            TreeSet<Long> memberIds = new TreeSet<Long>();

            int numGroupMember = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_USERS_PER_GROUP).nextInt(maxNumMemberGroup);
            group.initAllMemberships(numGroupMember);

            int numLoop = 0;
            while ((group.getNumMemberAdded() < numGroupMember) && (numLoop < windowSize)) {
                randLevelProb = randomFarm.get(RandomGeneratorFarm.Aspect.FRIEND_LEVEL).nextDouble();
                // Select the appropriate friend level
                if (randLevelProb < levelProbs[0] && user.getNumFriendsAdded() > 0 ) { // ==> level 1
                    int friendIdx = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX).nextInt(user.getNumFriendsAdded());
                    long potentialMemberAcc = firstLevelFriends[friendIdx].getFriendAcc();
                    randMemberProb = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP).nextDouble();
                    if (randMemberProb < joinProbs[0]) {
                        // Check whether this user has been added and then add to the group
                        if (!memberIds.contains(potentialMemberAcc)) {
                            memberIds.add(potentialMemberAcc);
                            // Assume the earliest membership date is the friendship created date
                            GroupMemberShip memberShip = groupGenerator.createGroupMember(randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX),
                                    potentialMemberAcc, group.getCreatedDate(),
                                    firstLevelFriends[friendIdx]);
                            if( memberShip != null ) {
                                memberShip.setGroupId(group.getGroupId());
                                group.addMember(memberShip);
                            }
                        }
                    }
                } else if (randLevelProb < levelProbs[1]) { // ==> level 2
                    if (secondLevelFriends.size() != 0) {
                        int friendIdx = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX).nextInt(secondLevelFriends.size());
                        long potentialMemberAcc = secondLevelFriends.get(friendIdx).getFriendAcc();
                        randMemberProb = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP).nextDouble();
                        if (randMemberProb < joinProbs[1]) {
                            // Check whether this user has been added and then add to the group
                            if (!memberIds.contains(potentialMemberAcc)) {
                                memberIds.add(potentialMemberAcc);
                                // Assume the earliest membership date is the friendship created date
                                GroupMemberShip memberShip = groupGenerator.createGroupMember(randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX),
                                        potentialMemberAcc, group.getCreatedDate(),
                                        secondLevelFriends.get(friendIdx));
                                if( memberShip != null ) {
                                    memberShip.setGroupId(group.getGroupId());
                                    group.addMember(memberShip);
                                }
                            }
                        }
                    }
                } else { // ==> random users
                    // Select a user from window
                    int friendIdx = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX).nextInt(Math.min(numUserProfilesRead,windowSize));
                    long potentialMemberAcc = reducedUserProfiles[friendIdx].getAccountId();
                    randMemberProb = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP).nextDouble();
                    if (randMemberProb < joinProbs[2]) {
                        // Check whether this user has been added and then add to the group
                        if (!memberIds.contains(potentialMemberAcc)) {
                            memberIds.add(potentialMemberAcc);
                            GroupMemberShip memberShip = groupGenerator.createGroupMember(randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX),
                                    potentialMemberAcc, group.getCreatedDate(),
                                    reducedUserProfiles[friendIdx]);
                            if( memberShip != null ) {
                                memberShip.setGroupId(group.getGroupId());
                                group.addMember(memberShip);
                            }
                        }
                    }
                }
                numLoop++;
            }

            dataExporter.export(group);
            GroupMemberShip memberships[] = group.getMemberShips();
            int numMembers = group.getNumMemberAdded();
            if (memberships != null)
            for( int i = 0; i < numMembers; ++i ) {
        		if (!factorTable.containsKey(memberships[i].getUserId()))
        			factorTable.put(memberships[i].getUserId(), new ReducedUserProfile.Counts());
        		factorTable.get(memberships[i].getUserId()).numberOfGroups++;
            }
            
            generatePostForGroup(uniformPostGenerator,group);
            generatePostForGroup(flashmobPostGenerator,group);
        }
    }

    private void generatePostForGroup(PostGenerator postGenerator, Group group) {
        Vector<Post> createdPosts =  postGenerator.createPosts( randomFarm, group, postId);
        postId+=createdPosts.size();
        Iterator<Post> it = createdPosts.iterator();
        GregorianCalendar c = new GregorianCalendar();

        while(it.hasNext()) {
            Post groupPost = it.next();
            if (!factorTable.containsKey(groupPost.getAuthorId())){	
            	factorTable.put(groupPost.getAuthorId(), new ReducedUserProfile.Counts());
            }
            factorTable.get(groupPost.getAuthorId()).numberOfPosts++;
            
            c.setTimeInMillis(groupPost.getCreationDate());
            int bucket = DateGenerator.getNumberOfMonths(c, startMonth, startYear);
            if (bucket < factorTable.get(groupPost.getAuthorId()).numberOfPostsPerMonth.length){
            	factorTable.get(groupPost.getAuthorId()).numberOfPostsPerMonth[bucket]++;
            }

            if (groupPost.getLikes() != null){
            	factorTable.get(groupPost.getAuthorId()).numberOfLikes += groupPost.getLikes().length;
            }
            if (groupPost.getTags() != null) {
            	factorTable.get(groupPost.getAuthorId()).numberOfTagsOfPosts += groupPost.getTags().size();
            	for (Integer t: groupPost.getTags()){
            		Integer tagClass = tagDictionary.getTagClass(t);
                	Integer tagCount = tagClassCount.containsKey(tagClass) ? tagClassCount.get(tagClass) : 0;
                	tagClassCount.put(tagClass, tagCount+1);
            	}
            }

            int locationID = ipAddDictionary.getLocation(groupPost.getIpAddress());
            String countryName = locationDictionary.getLocationName(locationID);
            int postCount = postsPerCountry.containsKey(locationID) ? postsPerCountry.get(locationID) : 0;
            postsPerCountry.put(locationID, postCount+1);
            stats.countries.add(countryName);
            dataExporter.export(groupPost);

            int numComment = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_COMMENT).nextInt(maxNumComments+1);
            ArrayList<Message> replyCandidates = new ArrayList<Message>();
            replyCandidates.add(groupPost);
            for (int j = 0; j < numComment; j++) {
                int replyIndex = randomFarm.get(RandomGeneratorFarm.Aspect.REPLY_TO).nextInt(replyCandidates.size());
                Comment comment = commentGenerator.createComment(randomFarm, postId, groupPost, replyCandidates.get(replyIndex), group);
                if ( comment!=null ) {
                	locationID = ipAddDictionary.getLocation(comment.getIpAddress());
                	countryName = locationDictionary.getLocationName(locationID);
                	postCount = postsPerCountry.containsKey(locationID) ? postsPerCountry.get(locationID) : 0;
                	postsPerCountry.put(locationID, postCount+1);
                    stats.countries.add(countryName);
                    dataExporter.export(comment);
                    if( comment.getTextSize() > 10 ) replyCandidates.add(comment);
                    
                    if (!factorTable.containsKey(comment.getAuthorId())) {
                    	factorTable.put(comment.getAuthorId(), new ReducedUserProfile.Counts());
                    }
                	factorTable.get(comment.getAuthorId()).numberOfPosts++;
                	
                    c.setTimeInMillis(comment.getCreationDate());
                    bucket = DateGenerator.getNumberOfMonths(c, startMonth, startYear);
                    if (bucket < factorTable.get(comment.getAuthorId()).numberOfPostsPerMonth.length){
                    	factorTable.get(comment.getAuthorId()).numberOfPostsPerMonth[bucket]++;
                    }

                	if (comment.getLikes() != null) {
                		factorTable.get(comment.getAuthorId()).numberOfLikes += comment.getLikes().length;
                	}
                	if (comment.getTags() != null) {
                		factorTable.get(comment.getAuthorId()).numberOfTagsOfPosts += comment.getTags().size();
                    	for (Integer t: comment.getTags()){
                    		Integer tagClass = tagDictionary.getTagClass(t);
                        	Integer tagCount = tagClassCount.containsKey(tagClass) ? tagClassCount.get(tagClass) : 0;
                        	tagClassCount.put(tagClass, tagCount+1);
                    	}
                	}

                    postId++;
                }
            }
        }
    }

    private long composeUserId( long id, long date, long spid ) {
        long spidMask = ~(0xFFFFFFFFFFFFFFFFL << 7);
        long idMask = ~(0xFFFFFFFFFFFFFFFFL << 33);
//        long dateMask = ~(0xFFFFFFFFFFFFFFFFL << 20);
        //return (((date >> 20) & dateMask) << 40) | ((id & idMask) << 7) | (spid & spidMask);
        long bucket = (long)(256*(date - SN.minDate) / (double)SN.maxDate);
        return (  bucket << 40) | ((id & idMask) << 7) | (spid & spidMask);
    }

    private ReducedUserProfile generateGeneralInformation(int accountId) {
        // User Creation
        long creationDate = dateTimeGenerator.randomDateInMillis( randomFarm.get(RandomGeneratorFarm.Aspect.DATE) );
        int locationId = locationDictionary.getLocationForUser(accountId);
        ReducedUserProfile userProf = new ReducedUserProfile();
        userProf.setCreationDate(creationDate);
        userProf.setGender((randomFarm.get(RandomGeneratorFarm.Aspect.GENDER).nextDouble() > 0.5) ? (byte) 1 : (byte) 0);
        userProf.setBirthDay(dateTimeGenerator.getBirthDay(randomFarm.get(RandomGeneratorFarm.Aspect.BIRTH_DAY), creationDate));
        userProf.setBrowserId(browserDictonry.getRandomBrowserId(randomFarm.get(RandomGeneratorFarm.Aspect.BROWSER)));
        userProf.setLocationId(locationId);
        userProf.setCityId(locationDictionary.getRandomCity(randomFarm.get(RandomGeneratorFarm.Aspect.CITY), locationId));
        userProf.setIpAddress(ipAddDictionary.getRandomIPFromLocation(randomFarm.get(RandomGeneratorFarm.Aspect.IP), locationId));
        userProf.setNumFriends(fbDegreeGenerator.getSocialDegree());
        userProf.setSdpId(fbDegreeGenerator.getIDByPercentile());  	//Generate Id from its percentile in the social degree distribution
        userProf.setNumDimensions(NUM_FRIENDSHIP_HADOOP_JOBS);
        userProf.setAccountId(composeUserId(accountId, creationDate, userProf.getSdpId()));

        // Setting the number of friends and friends per pass
        short totalFriendSet = 0;
        for (int i = 0; i < NUM_FRIENDSHIP_HADOOP_JOBS-1; i++){
            short numPassFriend = (short) Math.floor(friendRatioPerJob[i] * userProf.getNumFriends());
            totalFriendSet = (short) (totalFriendSet + numPassFriend);
            userProf.setNumPassFriends(i,totalFriendSet);
        }
        userProf.setNumPassFriends(NUM_FRIENDSHIP_HADOOP_JOBS-1,userProf.getNumFriends());
        // Setting tags
        int userMainTag = tagDictionary.getaTagByCountry(randomFarm.get(RandomGeneratorFarm.Aspect.TAG_OTHER_COUNTRY), randomFarm.get(RandomGeneratorFarm.Aspect.TAG),userProf.getLocationId());
        userProf.setMainTag(userMainTag);
        short numTags = ((short) randomTagPowerLaw.getValue(randomFarm.get(RandomGeneratorFarm.Aspect.NUM_TAG)));
        userProf.setSetOfTags(topicTagDictionary.getSetofTags(randomFarm.get(RandomGeneratorFarm.Aspect.TOPIC), randomFarm.get(RandomGeneratorFarm.Aspect.TAG_OTHER_COUNTRY), userMainTag, numTags));


        userProf.setUniversityLocationId(unversityDictionary.getRandomUniversity(randomFarm, userProf.getLocationId()));

        // Set whether the user has a smartphone or not.
        userProf.setHaveSmartPhone(randomFarm.get(RandomGeneratorFarm.Aspect.USER_AGENT).nextDouble() > probHavingSmartPhone);
        if (userProf.isHaveSmartPhone()) {
            userProf.setAgentId(userAgentDictionary.getRandomUserAgentIdx(randomFarm.get(RandomGeneratorFarm.Aspect.USER_AGENT)));
        }

        // Compute the popular places the user uses to visit.
        byte numPopularPlaces = (byte) randomFarm.get(RandomGeneratorFarm.Aspect.NUM_POPULAR).nextInt(maxNumPopularPlaces + 1);
        Vector<Short> auxPopularPlaces = new Vector<Short>();
        for (int i = 0; i < numPopularPlaces; i++){
            short aux = popularDictionary.getPopularPlace(randomFarm.get(RandomGeneratorFarm.Aspect.POPULAR),userProf.getLocationId());
            if(aux != -1) {
                auxPopularPlaces.add(aux);
            }
        }

        // Setting popular places
        short popularPlaces[] = new short[auxPopularPlaces.size()];
        Iterator<Short> it = auxPopularPlaces.iterator();
        int i = 0;
        while(it.hasNext()) {
            popularPlaces[i] = it.next();
            ++i;
        }
        userProf.setPopularPlaceIds(popularPlaces);

        // Set random Index used to sort users randomly
        userProf.setRandomId(randomFarm.get(RandomGeneratorFarm.Aspect.RANDOM).nextInt(USER_RANDOM_ID_LIMIT));

        // Set whether the user is a large poster or not.
        userProf.setLargePoster(IsUserALargePoster(userProf));
        return userProf;
    }

    private boolean IsUserALargePoster(ReducedUserProfile user) {
        if(dateTimeGenerator.getBirthMonth(user.getBirthDay()) == GregorianCalendar.JANUARY) {
            return true;
        }
        return false;
    }


    private void setInfoFromUserProfile(ReducedUserProfile user,
                                       UserExtraInfo userExtraInfo) {

        int locationId = (user.getCityId() != -1) ? user.getCityId() : user.getLocationId();
        userExtraInfo.setLocationId(locationId);
        userExtraInfo.setLocation(locationDictionary.getLocationName(locationId));
        double distance = randomFarm.get(RandomGeneratorFarm.Aspect.EXACT_LONG_LAT).nextDouble() * 2;
        userExtraInfo.setLatt(locationDictionary.getLatt(user.getLocationId()) + distance);
        userExtraInfo.setLongt(locationDictionary.getLongt(user.getLocationId()) + distance);
        userExtraInfo.setUniversity(unversityDictionary.getUniversityFromLocation(user.getUniversityLocationId()));

        boolean isMale;
        if (user.getGender() == 1) {
            isMale = true;
            userExtraInfo.setGender(gender[0]); // male
        } else {
            isMale = false;
            userExtraInfo.setGender(gender[1]); // female
        }

        userExtraInfo.setFirstName(namesDictionary.getRandomGivenName(randomFarm.get(RandomGeneratorFarm.Aspect.NAME),
                user.getLocationId(),isMale, dateTimeGenerator.getBirthYear(user.getBirthDay())));
        userExtraInfo.setLastName(namesDictionary.getRandomSurname(randomFarm.get(RandomGeneratorFarm.Aspect.SURNAME),user.getLocationId()));

        // email is created by using the user's first name + userId
        int numEmails = randomFarm.get(RandomGeneratorFarm.Aspect.EXTRA_INFO).nextInt(maxEmails) + 1;
        double prob = randomFarm.get(RandomGeneratorFarm.Aspect.EXTRA_INFO).nextDouble();
        if (prob >= missingRatio) {
            String base = userExtraInfo.getFirstName();
            base = Normalizer.normalize(base,Normalizer.Form.NFD);
            base = base.replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
            base = base.replaceAll(" ", ".");
            base = base.replaceAll("[.]+", ".");
            for (int i = 0; i < numEmails; i++) {
                String email = base + "" + user.getAccountId() + "@" + emailDictionary.getRandomEmail(randomFarm.get(RandomGeneratorFarm.Aspect.TOP_EMAIL),randomFarm.get(RandomGeneratorFarm.Aspect.EMAIL));
                userExtraInfo.addEmail(email);
            }
        }

        // Set class year
        prob = randomFarm.get(RandomGeneratorFarm.Aspect.EXTRA_INFO).nextDouble();
        if ((prob < missingRatio) || userExtraInfo.getUniversity() == -1 ) {
            userExtraInfo.setClassYear(-1);
        } else {
            userExtraInfo.setClassYear(dateTimeGenerator.getClassYear(randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                    user.getCreationDate(), user.getBirthDay()));
        }

        // Set company and workFrom
        int numCompanies = randomFarm.get(RandomGeneratorFarm.Aspect.EXTRA_INFO).nextInt(maxCompanies) + 1;
        prob = randomFarm.get(RandomGeneratorFarm.Aspect.EXTRA_INFO).nextDouble();
        if (prob >= missingRatio) {
            for (int i = 0; i < numCompanies; i++) {
                long workFrom;
                if (userExtraInfo.getClassYear() != -1) {
                    workFrom = dateTimeGenerator.getWorkFromYear( randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                            user.getCreationDate(),
                            user.getBirthDay());
                } else {
                    workFrom = dateTimeGenerator.getWorkFromYear(randomFarm.get(RandomGeneratorFarm.Aspect.DATE), userExtraInfo.getClassYear());
                }
                long company = companiesDictionary.getRandomCompany(randomFarm, user.getLocationId());
                userExtraInfo.addCompany(company, workFrom);
                user.addNumOfWorkPlaces(1);
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
        Vector<Integer> userLanguages = languageDictionary.getLanguages(randomFarm.get(RandomGeneratorFarm.Aspect.LANGUAGE),user.getLocationId());
        int internationalLang = languageDictionary.getInternationlLanguage(randomFarm.get(RandomGeneratorFarm.Aspect.LANGUAGE));
        if (internationalLang != -1 && userLanguages.indexOf(internationalLang) == -1) {
            userLanguages.add(internationalLang);
        }
        userExtraInfo.setLanguages(userLanguages);

        stats.maxPersonId = Math.max(stats.maxPersonId, user.getAccountId());
        stats.minPersonId = Math.min(stats.minPersonId, user.getAccountId());
        stats.firstNames.add(userExtraInfo.getFirstName());
        String countryName = locationDictionary.getLocationName(user.getLocationId());
        stats.countries.add(countryName);

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

    private void createFriendShip(ReducedUserProfile user1, ReducedUserProfile user2, byte pass) {

        long requestedTime = dateTimeGenerator.randomFriendRequestedDate(randomFarm.get(RandomGeneratorFarm.Aspect.DATE),user1, user2);
        byte initiator = (byte) randomFarm.get(RandomGeneratorFarm.Aspect.INITIATOR).nextInt(2);
        long createdTime = -1;
        long declinedTime = -1;
        if (randomFarm.get(RandomGeneratorFarm.Aspect.FRIEND_REJECT).nextDouble() > friendRejectRatio) {
            createdTime = dateTimeGenerator.randomFriendApprovedDate(randomFarm.get(RandomGeneratorFarm.Aspect.DATE),requestedTime);
        } else {
            declinedTime = dateTimeGenerator.randomFriendDeclinedDate(randomFarm.get(RandomGeneratorFarm.Aspect.DATE),requestedTime);
            if (randomFarm.get(RandomGeneratorFarm.Aspect.FRIEND_APROVAL).nextDouble() < friendReApproveRatio) {
                createdTime = dateTimeGenerator.randomFriendReapprovedDate(randomFarm.get(RandomGeneratorFarm.Aspect.DATE),declinedTime);
            }
        }
        createdTime = createdTime - user1.getCreationDate() >= deltaTime ? createdTime : createdTime + (deltaTime - (createdTime - user1.getCreationDate() ));
        createdTime = createdTime - user2.getCreationDate() >= deltaTime ? createdTime : createdTime + (deltaTime - (createdTime - user2.getCreationDate() ));
        if( createdTime <= dateTimeGenerator.getEndDateTime() ) {
            user2.addNewFriend(new Friend(user2, user1, requestedTime, declinedTime,
                    createdTime, pass, initiator));
            user1.addNewFriend(new Friend(user1, user2, requestedTime, declinedTime,
                    createdTime, pass, initiator));
            friendshipNum++;
        }
    }


    private void updateLastPassFriendAdded(int from, int to, int pass) {
        if (to > windowSize) {
            for (int i = from; i < windowSize; i++) {
                reducedUserProfiles[i].setNumPassFriendsAdded(pass, reducedUserProfiles[i].getNumFriendsAdded());
            }
            for (int i = 0; i < to - windowSize; i++) {
                reducedUserProfiles[i].setNumPassFriendsAdded(pass, reducedUserProfiles[i].getNumFriendsAdded());
            }
        } else {
            for (int i = from; i < to; i++) {
                reducedUserProfiles[i].setNumPassFriendsAdded(pass, reducedUserProfiles[i].getNumFriendsAdded());
            }
        }
    }

    private DataExporter getSerializer(String type, String outputFileName) {
        String t = type.toLowerCase();
        DataExporter.DataFormat format;
        String configFile = new String("");
        if (t.equals("ttl")) {
            format = DataExporter.DataFormat.TURTLE;
        } else if (t.equals("n3")) {
            format = DataExporter.DataFormat.N3;
        } else if (t.equals("csv")) {
            format = DataExporter.DataFormat.CSV;
 //           configFile = new String(CSV_DIRECTORY+"/csv.xml");
        } else if (t.equals("csv_merge_foreign")) {
            format = DataExporter.DataFormat.CSV_MERGE_FOREIGN;
//            configFile = new String(CSV_DIRECTORY+"/csvMergeForeign.xml");
        } else if (t.equals("none")) {
            format = DataExporter.DataFormat.NONE;
        } else {
            System.err.println("Unexpected Serializer - Aborting");
            System.exit(-1);
            return null;
        }
        return new DataExporter(format,sibOutputDir,this.machineId,dateThreshold,
                exportText,enableCompression,tagDictionary,browserDictonry,companiesDictionary,
                unversityDictionary,ipAddDictionary,locationDictionary,languageDictionary, configFile, stats);
    }

    private void writeFactorTable(){
        Configuration conf = new Configuration();
        try {            
        	FileSystem fs = FileSystem.get(conf);
            OutputStream writer = fs.create(new Path(sibOutputDir+"/"+ "m" + machineId + PARAM_COUNT_FILE));
            writer.write(Integer.toString(factorTable.size()).getBytes());
            writer.write("\n".getBytes());

            for (Map.Entry<Long, ReducedUserProfile.Counts> c: factorTable.entrySet()){
            	ReducedUserProfile.Counts count = c.getValue();
            	// correct the group counts
            	count.numberOfGroups += count.numberOfFriends;
            	StringBuffer strbuf = new StringBuffer();
            	strbuf.append(c.getKey()); strbuf.append(",");
            	strbuf.append(count.numberOfFriends); 		strbuf.append(",");
            	strbuf.append(count.numberOfPosts); 		strbuf.append(",");
            	strbuf.append(count.numberOfLikes); 		strbuf.append(",");
            	strbuf.append(count.numberOfTagsOfPosts); 	strbuf.append(",");
            	strbuf.append(count.numberOfGroups); 		strbuf.append(",");
            	strbuf.append(count.numberOfWorkPlaces); 	strbuf.append(",");

            	int numBuckets = count.numberOfPostsPerMonth.length;
            	strbuf.append(count.numberOfPostsPerMonth[0]);
            	for (int i = 1; i < numBuckets; i++){
            		strbuf.append(",");
            		strbuf.append(count.numberOfPostsPerMonth[i]);
            	}
            	strbuf.append("\n");
            	writer.write(strbuf.toString().getBytes());
            }
            writer.write(Integer.toString(postsPerCountry.size()).getBytes());
            writer.write("\n".getBytes());
            for (Map.Entry<Integer, Integer> c: postsPerCountry.entrySet()){
            	StringBuffer strbuf = new StringBuffer();
            	strbuf.append(locationDictionary.getLocationName(c.getKey()));
            	strbuf.append(",");
            	strbuf.append(c.getValue());
            	strbuf.append("\n");
            	writer.write(strbuf.toString().getBytes());
            }
            
            writer.write(Integer.toString(tagClassCount.size()).getBytes());
            writer.write("\n".getBytes());
            for (Map.Entry<Integer, Integer> c: tagClassCount.entrySet()){
            	StringBuffer strbuf = new StringBuffer();
            	strbuf.append(tagDictionary.getClassName(c.getKey()));
            	strbuf.append(",");
            	strbuf.append(tagDictionary.getClassName(c.getKey()));
            	strbuf.append(",");
            	strbuf.append(c.getValue());
            	strbuf.append("\n");
            	writer.write(strbuf.toString().getBytes());
            }
            writer.write(Integer.toString(firstNameCount.size()).getBytes());
            writer.write("\n".getBytes());
            for (Map.Entry<String, Integer> c: firstNameCount.entrySet()){
            	StringBuffer strbuf = new StringBuffer();
            	strbuf.append(c.getKey());
            	strbuf.append(",");
            	strbuf.append(c.getValue());
            	strbuf.append("\n");
            	writer.write(strbuf.toString().getBytes());
            }

            writer.flush();
            writer.close();
        } catch (IOException e) {
            System.err.println("Unable to write parameter counts");
            System.err.println(e.getMessage());
            e.printStackTrace();
        }
    }
    
    private void writeStatistics() {
        Gson gson = new GsonBuilder().setExclusionStrategies(stats.getExclusion()).disableHtmlEscaping().create();
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(conf);
            stats.makeCountryPairs(locationDictionary);
            stats.deltaTime = deltaTime;
            OutputStream writer = fs.create(new Path(sibOutputDir+"/"+ "m" + machineId + STATS_FILE));
            //writer = new FileWriter(sibOutputDir + "m" + machineId + STATS_FILE);
            writer.write(gson.toJson(stats).getBytes("UTF8"));
            writer.flush();
            writer.close();
        } catch (IOException e) {
            System.err.println("Unable to write stastistics");
            System.err.println(e.getMessage());
            e.printStackTrace();
        }
    }

}

