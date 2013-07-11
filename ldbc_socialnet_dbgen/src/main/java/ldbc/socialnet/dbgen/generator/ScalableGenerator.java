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

import java.io.BufferedReader;
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

    
    public static long postId = -1;
    
	// For sliding window
	int 					cellSize = 1; // Number of user in one cell


	int 					numberOfCellPerWindow = 200;
	int	 					numtotalUser = 10000;
	int 					windowSize;
	int 					lastCellPos;
	int 					lastCell;
	
	int 					lastMapCellPos;
	int 					lastMapCell;
	int 					startMapUserIdx;
	int						machineId;
	
	ReducedUserProfile  	reducedUserProfiles[];
	ReducedUserProfile 		reducedUserProfilesCell[]; 	// Store new cell of user profiles
	ReducedUserProfile 		removedUserProfiles[];
	
	// For (de-)serialization
	FileOutputStream 		ofUserProfiles;
	ObjectOutputStream 		oosUserProfile;

	FileInputStream 		ifUserProfile;
	ObjectInputStream 		oisUserProfile;

	// For multiple files
	boolean 				isMultipleFile = true;
	int 					numFiles = 10;
	int 					numCellPerfile;
	int 					numCellInLastFile;
	int 					numCellRead = 0;
	Random 					randomFileSelect;
	Random 					randomIdxInWindow;
	HashSet<Integer> 		selectedFileIdx;
	
	// For friendship generation
	int 				friendshipNo = 0;
	int 				minNoFriends = 5;
	int 				maxNoFriends = 50;
	double 				friendRejectRatio = 0.02;
	double 				friendReApproveRatio = 0.5;
	
	int					numCorrDimensions = 3;			// Run 3 passes for friendship generation
	StorageManager	 	storeManager[];
	StorageManager		groupStoreManager; 		
	
	MRWriter			mrWriter; 
	
	double 				friendsRatioPerPass[] = { 0.45, 0.45, 0.1 }; // Indicate how many
											// friends will be created for each pass
											// e.g., 30% in the first pass with
											// location information
	Random 				randFriendReject;
	Random 				randFriendReapprov;
	Random 				randInitiator;
	double 				alpha = 0.4;							// Alpha value for power-law distribution
	double	 			baseProbLocationCorrelated = 0.8; 	// Higher probability, faster
														// sliding along this pass

	double 				baseProbCorrelated = 0.9;  	  // Probability that two user having the same 
												      // atrributes value become friends;
	double	 			limitProCorrelated = 0.2; 
	
	// For each user
	int                 minNoTagsPerUser; 
	int					maxNoTagsPerUser; 
	int 				maxNumPostPerMonth;
	int 				maxNumComments;
	

	// Random values generators
	PowerDistGenerator 	randPowerlaw;
	PowerDistGenerator  randTagPowerlaw;
	//Random 				seedRandom = new Random(53223436L);
	Long[] 				seeds;
	Random 				randUniform;
	Random 				randNumInterest;
	Random 				randNumTags;
	Random 				randomFriendIdx; // For third pass
	Random 				randNumberPost;
	Random 				randNumberComments; // Generate number of comments per post
	Random 				randNumberPhotoAlbum;
	Random 				randNumberPhotos;
	Random 				randNumberGroup;
	Random 				randNumberUserPerGroup;
	Random 				randGender;
	
	Random				randUserRandomIdx;	// For generating the 
											// random dimension
	int					maxUserRandomIdx = 100;  //SHOULE BE IMPORTANT PARAM
	
	DateGenerator 		dateTimeGenerator;
	int					startYear;
	int                 endYear; 
	private static final int startMonth = 1; 
	private static final int startDate  = 1;
	private static final int endMonth   = 1;
	private static final int endDate    = 1;

	// Dictionaries
	private static final String   countryDicFile         = "dicLocation.txt";
	private static final String   languageDicFile        = "languagesByCountry.txt";
	private static final String   cityDicFile            = "institutesCityByCountry.txt";
	private static final String   tagNamesFile           = "dicTopic.txt";
	private static final String   mainTagDicFile         = "dicCelebritiesByCountry.txt";
	private static final String   topicTagDicFile        = "topicMatrixId.txt";
	private static final String   givennamesDicFile      = "givennameByCountryBirthPlace.txt.freq.full";
	private static final String   surnamesDicFile        = "surnameByCountryBirthPlace.txt.freq.sort";
	private static final String   organizationsDicFile   = "institutesCityByCountry.txt";
	private static final String   companiesDicFile       = "companiesByCountry.txt";
	private static final String   popularPlacesDicFile   = "popularPlacesByCountry.txt";
	private static final String   agentFile              = "smartPhonesProviders.txt";
	private static final String   emailDicFile           = "email.txt";
	private static final String   browserDicFile         = "browsersDic.txt";
	private static final String   countryAbbrMappingFile = "countryAbbrMapping.txt";
	private static final String   tagTextFile            = "tagText.txt";
	
	LocationDictionary 		locationDic;
	
	LanguageDictionary      languageDic;
	
	TagDictionary 			mainTagDic;
	TagTextDictionary       tagTextDic;
	double 					tagCountryCorrProb; 

	TagMatrix	 			topicTagDic;

	NamesDictionary 		namesDictionary;
	double					geometricProb = 0.2;

	OrganizationsDictionary	organizationsDictionary;
	double 					probUnCorrelatedOrganization = 0.005;
	double 					probTopUniv = 0.9; // 90% users go to top university

	CompanyDictionary 		companiesDictionary;
	double 					probUnCorrelatedCompany = 0.05;
	
	UserAgentDictionary 	userAgentDic;
	
	EmailDictionary 		emailDic;
	
	BrowserDictionary 		browserDic;
	
	PopularPlacesDictionary	popularDictionary; 
	int  					maxNumPopularPlaces;
	Random 					randNumPopularPlaces; 
	double 					probPopularPlaces;		//probability of taking a photo at popular place 
	
	IPAddressDictionary 	ipAddDictionary;

	int locationIdx = 0; 	//Which is current index of the location in dictionary

	// For generating texts of posts and comments
	int 					maxNumLikes = 10;
	
	int                     maxEmails = 5;
	int                     maxCompanies = 3;
	double                  probEnglish = 0.6;
    double                  probSecondLang = 0.2;

	int 					numArticles = 3606; // May be set -1 if do not know
	int 					minTextSize = 20;
	int 					maxTextSize = 200;
	int 					minCommentSize = 20;
	int 					maxCommentSize = 60;
	double 					ratioReduceText = 0.8; // 80% text has size less than 1/2 max size

	// For photo generator
	PhotoGenerator 			photoGenerator;
	int 					maxNumUserTags = 10;
	int 					maxNumPhotoAlbums = 5;	// This is number of photo album per month
	int 					maxNumPhotoPerAlbums = 100;

	// For generating groups
	GroupGenerator 			groupGenerator;
	int 					maxNumGroupCreatedPerUser = 4;
	int 					maxNumMemberGroup = 100;
	double 					groupModeratorProb = 0.05;
	double 					levelProbs[] = { 0.5, 0.8, 1.0 }; 	// Cumulative 'join' probability
																// for friends of level 1, 2 and 3
																// of a user
	double 					joinProbs[] = { 0.7, 0.4, 0.1 };
	
	Random 					randFriendLevelSelect; 	// For select an level of moderator's friendship
	Random 					randMembership; // For deciding whether or not a user is joined

	Random 					randMemberIdxSelector;
	Random 					randGroupMemStep;

	Random 					randGroupModerator; // Decide whether a user can be moderator of groups or not

	// For group posts
	int 					maxNumGroupPostPerMonth = 20;
	Random 					randNumberGroupPost;


	// For serialize to RDF format
	private String rdfOutputFileName = "ldbc_socialnet_dbg";
	
	Serializer 		serializer;
	String 			serializerType = "ttl";
	String 			outUserProfileName = "userProf.ser";
	String 			outUserProfile;
	int 			numRdfOutputFile = 1;
	boolean 		forwardChaining = false;
	int				mapreduceFileIdx; 
	String 			sibOutputDir;
	String 			sibHomeDir;
	String 			sibDicDataDir = "/dictionaries/";
	String          ipZoneDir = "/ipaddrByCountries";
	
	// For user's extra info
	String 					gender[] = { "male", "female" };
	Random					randomExtraInfo;
	Random					randomExactLongLat;
	double 					missingRatio = 0.2;
	Random 					randomHaveStatus;
	Random 					randomStatusSingle;
	Random 					randomStatus;
	double 					missingStatusRatio = 0.5;
	double 					probSingleStatus = 0.8; // Status "Single" has more probability than
													// others'
	
	
	double 		probAnotherBrowser;

	double 		probHavingSmartPhone = 0.5;
	Random 		randUserAgent;
	double 		probSentFromAgent = 0.2;
	Random 		randIsFrequent;
	double 		probUnFrequent = 0.01; // Whether or not a user frequently changes

	// The probability that normal user posts from different location
	double 		probDiffIPinTravelSeason = 0.1; // in travel season
	double 		probDiffIPnotTravelSeason = 0.02; // not in travel season

	// The probability that travellers post from different location
	double 		probDiffIPforTraveller = 0.3;

	// For loading parameters;
	String 			paramFileName = "params.ini";
	static String	shellArgs[];

	// For MapReduce 
	public static int			numMaps = -1;
	
	MFStoreManager 		mfStore;
	
	// Writing data for test driver
	OutputDataWriter 	outputDataWriter;
	int 				thresholdPopularUser = 40;
	int 				numPopularUser = 0;

	public ScalableGenerator(int _mapreduceFileIdx, String _sibOutputDir, String _sibHomeDir){
		mapreduceFileIdx = _mapreduceFileIdx;
		System.out.println("Map Reduce File Idx is: " + mapreduceFileIdx);
		if (mapreduceFileIdx != -1){
			outUserProfile = "mr" + mapreduceFileIdx + "_" + outUserProfileName;
		}
		
		sibOutputDir = _sibOutputDir;
		sibHomeDir = _sibHomeDir; 
		System.out.println("Current directory in ScaleGenerator is " + _sibHomeDir);
	}
	
	public void initAllParams(String args[], int _numMaps, int mapIdx){
		
		this.machineId = mapIdx;
		
		loadParamsFromFile();

		numFiles = _numMaps;
		
		rdfOutputFileName = "mr" + mapreduceFileIdx + "_" + rdfOutputFileName;
		
		init(mapIdx, true);
		
		System.out.println("Number of files " + numFiles);
		System.out.println("Number of cells per file " + numCellPerfile);
		System.out.println("Number of cells in last file " + numCellInLastFile);
		
		mrWriter = new MRWriter(cellSize, windowSize, sibOutputDir); 

	}
	
	public void mapreduceTask(String inputFile, int numberCell){
		startWritingUserData();

		long startPostGeneration = System.currentTimeMillis();
		
		groupGenerator.setForumId((numtotalUser + 10) * 2);
		
		generatePostandPhoto(inputFile, numberCell);

		long endPostGeneration = System.currentTimeMillis();
		System.out.println("Post generation takes " + getDuration(startPostGeneration, endPostGeneration));

		finishWritingUserData();

		long startGroupGeneration = System.currentTimeMillis();
		generateGroupAll(inputFile, numberCell);
		long endGroupGeneration = System.currentTimeMillis();
		System.out.println("Group generation takes " + getDuration(startGroupGeneration, endGroupGeneration));

		if (mapreduceFileIdx != -1) {
			serializer.serialize();
		}

		System.out.println("Number of generated triples " + serializer.triplesGenerated());
		
		// Write data for testdrivers
		System.out.println("Writing the data for test driver ");
		System.out.println("Number of popular users " + numPopularUser);
	}
	
	public void loadParamsFromFile() {
		try {
		    //First read the internal params.ini
		    BufferedReader paramFile;
			paramFile = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream("/"+paramFileName), "UTF-8"));
			String line;
			while ((line = paramFile.readLine()) != null) {
				String infos[] = line.split(": ");
				if (infos[0].startsWith("cellSize")) {
					cellSize = Short.parseShort(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("numberOfCellPerWindow")) {
					numberOfCellPerWindow = Integer.parseInt(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("minNoFriends")) {
					minNoFriends = Integer.parseInt(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("maxNoFriends")) {
					maxNoFriends = Integer.parseInt(infos[1].trim());
					thresholdPopularUser = (int) (maxNoFriends * 0.9);
					continue;
				} else if (infos[0].startsWith("friendRejectRatio")) {
					friendRejectRatio = Double.parseDouble(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("friendReApproveRatio")) {
					friendReApproveRatio = Double.parseDouble(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("minNumTagsPerUser")) {
                    minNoTagsPerUser= Integer.parseInt(infos[1].trim());
                    continue;
                } else if (infos[0].startsWith("maxNumTagsPerUser")) {
					maxNoTagsPerUser= Integer.parseInt(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("maxNumPostPerMonth")) {
				    maxNumPostPerMonth = Integer.parseInt(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("maxNumComments")) {
					maxNumComments = Integer.parseInt(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("limitProCorrelated")) {
                    limitProCorrelated = Double.parseDouble(infos[1].trim());
                    continue;
                } else if (infos[0].startsWith("baseProbCorrelated")) {
                    baseProbCorrelated = Double.parseDouble(infos[1].trim());
                    continue;
				} else if (infos[0].startsWith("maxEmails")) {
				    maxEmails = Integer.parseInt(infos[1].trim());
                    continue;
                } else if (infos[0].startsWith("maxCompanies")) {
                    maxCompanies = Integer.parseInt(infos[1].trim());
                    continue;
                }  else if (infos[0].startsWith("probEnglish")) {
                    probEnglish = Double.parseDouble(infos[1].trim());
                    continue;
                } else if (infos[0].startsWith("probSecondLang")) {
                    probSecondLang = Double.parseDouble(infos[1].trim());
                    continue;
                } else if (infos[0].startsWith("probAnotherBrowser")) {
					probAnotherBrowser = Double.parseDouble(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("minTextSize")) {
					minTextSize = Integer.parseInt(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("maxTextSize")) {
					maxTextSize = Integer.parseInt(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("minCommentSize")) {
					minCommentSize = Integer.parseInt(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("maxCommentSize")) {
					maxCommentSize = Integer.parseInt(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("ratioReduceText")) {
					ratioReduceText = Double.parseDouble(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("maxNumUserTags")) {
					maxNumUserTags = Integer.parseInt(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("maxNumPhotoAlbums")) {
					maxNumPhotoAlbums = Integer.parseInt(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("maxNumPhotoPerAlbums")) {
					maxNumPhotoPerAlbums = Integer.parseInt(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("maxNumGroupCreatedPerUser")) {
					maxNumGroupCreatedPerUser = Integer.parseInt(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("maxNumMemberGroup")) {
					maxNumMemberGroup = Integer.parseInt(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("groupModeratorProb")) {
					groupModeratorProb = Double.parseDouble(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("maxNumGroupPostPerMonth")) {
					maxNumGroupPostPerMonth = Integer.parseInt(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("serializerType")) {
					serializerType = infos[1].trim();
					continue;
				}else if (infos[0].startsWith("missingRatio")) {
					missingRatio = Double.parseDouble(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("missingStatusRatio")) {
					missingStatusRatio = Double.parseDouble(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("probSingleStatus")) {
					probSingleStatus = Double.parseDouble(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("probHavingSmartPhone")) {
					probHavingSmartPhone = Double.parseDouble(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("probSentFromAgent")) {
					probSentFromAgent = Double.parseDouble(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("probUnFrequent")) {
					probUnFrequent = Double.parseDouble(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("probDiffIPinTravelSeason")) {
					probDiffIPinTravelSeason = Double.parseDouble(infos[1]
							.trim());
					continue;
				} else if (infos[0].startsWith("probDiffIPnotTravelSeason")) {
					probDiffIPnotTravelSeason = Double.parseDouble(infos[1]
							.trim());
					continue;
				} else if (infos[0].startsWith("probDiffIPforTraveller")) {
					probDiffIPforTraveller = Double.parseDouble(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("probUnCorrelatedCompany")) {
					probUnCorrelatedCompany = Double.parseDouble(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("probUnCorrelatedOrganization")) {
					probUnCorrelatedOrganization = Double.parseDouble(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("probTopUniv")) {
					probTopUniv = Double.parseDouble(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("maxNumPopularPlaces")) {
					maxNumPopularPlaces = Integer.parseInt(infos[1].trim());
					continue;					
				} else if (infos[0].startsWith("probPopularPlaces")) {
					probPopularPlaces = Double.parseDouble(infos[1].trim());
					continue;	
				} else if (infos[0].startsWith("tagCountryCorrProb")) {
					tagCountryCorrProb = Double.parseDouble(infos[1].trim());
					continue;	
				}
			}

			paramFile.close();
		} catch (IOException e) {
            e.printStackTrace();
        }
			
		try {
			//read the user param file
		    BufferedReader paramFile;
			paramFile = new BufferedReader(new InputStreamReader(new FileInputStream(sibHomeDir + paramFileName), "UTF-8"));
			String line;
			numtotalUser = -1;
			int numYears = -1;
			startYear = -1;
			serializerType = "";
			while ((line = paramFile.readLine()) != null) {
                String infos[] = line.split(": ");
                if (infos[0].startsWith("numtotalUser")) {
                    numtotalUser = Integer.parseInt(infos[1].trim());
                    continue;
                } else if (infos[0].startsWith("startYear")) {
                    startYear = Integer.parseInt(infos[1].trim());
                    continue;   
                } else if (infos[0].startsWith("numYears")) {
                    numYears = Integer.parseInt(infos[1].trim());
                    continue;   
                } else if (infos[0].startsWith("serializerType")) {
                    serializerType = infos[1].trim();
                    continue;
                } else {
                    System.out.println("This param " + line + " does not match any option");
                }
			}
			paramFile.close();
			if (numtotalUser == -1) {
			    throw new Exception("No numtotalUser parameter provided");
			}
			if (startYear == -1) {
			    throw new Exception("No startYears parameter provided");
			}
			if (numYears == -1) {
			    throw new Exception("No numYears parameter provided");
			}
			if (!serializerType.equals("ttl") && !serializerType.equals("n3") && 
			        !serializerType.equals("csv")) {
                throw new Exception("serializerType must be ttl, nt or csv");
            }
			
			endYear = startYear + numYears;
		} catch (Exception e) {
		    System.out.println(e.getMessage());
		    System.exit(-1);
		}
	}

	// Init the data for the first window of cells
	public void init(int mapId, boolean isFullLoad) {
		seedGenerate(mapId);

		windowSize = (int) cellSize * numberOfCellPerWindow;
		randPowerlaw    = new PowerDistGenerator(minNoFriends,     maxNoFriends + 1,     alpha, seeds[2]);
		randTagPowerlaw = new PowerDistGenerator(minNoTagsPerUser, maxNoTagsPerUser + 1, alpha, seeds[2]);
		randUniform = new Random(seeds[3]);
		randGender = new Random(seeds[3]);
		randNumInterest = new Random(seeds[4]);
		randNumTags = new Random(seeds[4]);
		randomFriendIdx = new Random(seeds[6]);
		randomFileSelect = new Random(seeds[7]);
		randomIdxInWindow = new Random(seeds[8]);
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
		randIsFrequent = new Random(seeds[34]);
		randNumberGroupPost = new Random(seeds[36]);
		randFriendReject = new Random(seeds[37]);
		randFriendReapprov = new Random(seeds[38]);
		randInitiator = new Random(seeds[39]);
		randomHaveStatus = new Random(seeds[41]);
		randomStatusSingle = new Random(seeds[42]);
		randomStatus = new Random(seeds[43]);
		randNumPopularPlaces = new Random(seeds[47]);
		randUserRandomIdx = new Random(seeds[48]);

		reducedUserProfiles = new ReducedUserProfile[windowSize];	//// Collect of reduced user profile
		cellReducedUserProfiles = new ReducedUserProfile[cellSize];

		// Number of users should be a multiple of the cellsize
		if (numtotalUser % cellSize != 0) {
			System.out.println("Number of users should be a multiple of the cellsize ");
			System.exit(-1);
		}
		
		dateTimeGenerator = new DateGenerator(new GregorianCalendar(startYear, startMonth, startDate), 
		        new GregorianCalendar(endYear, endMonth, endDate), seeds[0], seeds[1], alpha);

		lastCellPos = (int) (numtotalUser - windowSize) / cellSize;
		lastCell = (int) numtotalUser / cellSize - 1; // The last cell of the sliding process
		
		lastMapCellPos = (int)(numtotalUser/numMaps - windowSize) / cellSize;
		lastMapCell = (int)numtotalUser/(numMaps * cellSize) - 1;
		startMapUserIdx = (numtotalUser/numMaps) * mapId; 
		
		// For multiple output files
		numCellPerfile = (lastCell + 1) / numFiles;
		numCellInLastFile = (lastCell + 1) - numCellPerfile * (numFiles - 1);

		if (numCellPerfile < numberOfCellPerWindow) {
			System.out.println("The number of Cell per file should be greater than that of a window ");
			System.exit(-1);
		}

		if (isFullLoad) {
			System.out.println("Building interests dictionary & locations/interests distribution ");

	
			System.out.println("Building locations dictionary ");
	
			locationDic = new LocationDictionary(numtotalUser, seeds[7] ,sibDicDataDir + countryDicFile,
			        sibDicDataDir + cityDicFile);
			locationDic.init();
			
			languageDic = new LanguageDictionary(sibDicDataDir + languageDicFile, 
			        locationDic.getLocationNameMapping(), probEnglish, probSecondLang, seeds[11]);
			languageDic.init();
			
			System.out.println("Building Tags dictionary ");
			
			mainTagDic = new TagDictionary(sibDicDataDir + tagNamesFile,
			        sibDicDataDir + mainTagDicFile, 
					locationDic.getLocationNameMapping().size(), seeds[5], tagCountryCorrProb);
			mainTagDic.extractTags();
			
			tagTextDic = new TagTextDictionary(sibDicDataDir + tagTextFile, dateTimeGenerator, mainTagDic.getTagsNamesMapping(),
			        minTextSize, maxTextSize, minCommentSize, maxCommentSize, ratioReduceText, seeds[15], seeds[16]);
			tagTextDic.initialize();

			System.out.println("Building Tag Matrix dictionary ");
			
			topicTagDic = new TagMatrix(sibDicDataDir + topicTagDicFile, 
					mainTagDic.getNumCelebrity() , seeds[5]);
			
			topicTagDic.initMatrix();
	
			ipAddDictionary = new IPAddressDictionary(sibDicDataDir + countryAbbrMappingFile,
					ipZoneDir, locationDic.getVecLocations(), seeds[33],
					probDiffIPinTravelSeason, probDiffIPnotTravelSeason,
					probDiffIPforTraveller);
			ipAddDictionary.init();
			
			System.out.println("Building dictionary of articles ");
	
			groupGenerator = new GroupGenerator(dateTimeGenerator, locationDic,
					mainTagDic, numtotalUser, seeds[35]);
	
			namesDictionary = new NamesDictionary(sibDicDataDir + surnamesDicFile,
					sibDicDataDir + givennamesDicFile,
					locationDic.getLocationNameMapping(), seeds[23], geometricProb);
			namesDictionary.init();
			
			emailDic = new EmailDictionary(sibDicDataDir + emailDicFile, seeds[32]);
			emailDic.init();
	
			browserDic = new BrowserDictionary(sibDicDataDir + browserDicFile, seeds[44],
					probAnotherBrowser);
			browserDic.init();			
			
			organizationsDictionary = new OrganizationsDictionary(
					sibDicDataDir + organizationsDicFile, locationDic,
					seeds[24], probUnCorrelatedOrganization, seeds[45], probTopUniv);
			organizationsDictionary.init();
	
			companiesDictionary = new CompanyDictionary(sibDicDataDir + companiesDicFile,
					locationDic, seeds[40], probUnCorrelatedCompany);
			companiesDictionary.init();
	
			popularDictionary = new PopularPlacesDictionary(sibDicDataDir + popularPlacesDicFile, 
					locationDic.getLocationNameMapping(), seeds[46]);
			popularDictionary.init();
			
			photoGenerator = new PhotoGenerator(dateTimeGenerator,
					locationDic.getVecLocations(), seeds[17], maxNumUserTags, 
					popularDictionary, probPopularPlaces);

			System.out.println("Building user agents dictionary");
			userAgentDic = new UserAgentDictionary(sibDicDataDir + agentFile, seeds[28], seeds[30],
					probSentFromAgent);
			userAgentDic.init();


			outputDataWriter = new OutputDataWriter();
	
			serializer = getSerializer(serializerType, rdfOutputFileName);
		}

	}
	
	public void generateGroupAll(String inputFile, int numberOfCell) {
		// Fifth pass: Group & group posts generator
		groupStoreManager = new StorageManager(cellSize, windowSize, outUserProfile, sibOutputDir);
		
		groupStoreManager.initDeserialization(inputFile);
		generateGroups();
		
		int curCellPost = 0;
		while (curCellPost < (numberOfCell - numberOfCellPerWindow)){
			curCellPost++;
			generateGroups(4, curCellPost, numberOfCell);
		}

		System.out.println("Done generating user groups and groups' posts");
		
		groupStoreManager.endDeserialization();
		System.out.println("Number of deserialized objects for group is " + groupStoreManager.getNumberDeSerializedObject());
	}	
	
	public int		numUserProfilesRead = 0;
	public int		numUserForNewCell = 0;
	public int		mrCurCellPost = 0;
	public ReducedUserProfile[] cellReducedUserProfiles;
	public int		exactOutput = 0; 
	
	
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
	    
		int numLeftCell = numberOfCellPerWindow - 1;
		while (numLeftCell > 0){
			mrCurCellPost++;
			mr2SlideLastCellsFriendShip(pass, mrCurCellPost, numLeftCell, context,
					 isContext,  oos);
			numLeftCell--;
		}
	}
	
	public void mrGenerateUserInfo(int pass, Context context, int mapIdx){
		
		int numToGenerateUser;
		if (numMaps == mapIdx){
			numToGenerateUser = numCellInLastFile * cellSize;
		}
		else {
			numToGenerateUser = numCellPerfile * cellSize;
		}
		
		System.out.println("numToGenerateUser in Machine " + mapIdx + " = " + numToGenerateUser);
		int numZeroPopularPlace = 0; 
		
		for (int i = 0; i < numToGenerateUser; i++) {
			UserProfile user = generateGeneralInformation(i + startMapUserIdx); 
			ReducedUserProfile reduceUserProf = new ReducedUserProfile(user, numCorrDimensions);
			if (reduceUserProf.getNumPopularPlace() == 0) numZeroPopularPlace++;
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
	
	public void initBasicParams(String args[], int _numMaps, String _mrInputFile, int mapIdx){
		
		loadParamsFromFile();

		numFiles = _numMaps;
		
		rdfOutputFileName = "mr" + mapreduceFileIdx + "_" + rdfOutputFileName;
		rdfOutputFileName = rdfOutputFileName + numtotalUser;
		
		init(mapIdx, false);
		
		System.out.println("Number of files " + numFiles);
		System.out.println("Number of cells per file " + numCellPerfile);
		System.out.println("Number of cells in last file " + numCellInLastFile);
		
		mrWriter = new MRWriter(cellSize, windowSize, sibOutputDir); 

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
	    
	    //Generate the friendship in the first window
		double randProb;
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
				randProb = randUniform.nextDouble();
				
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

		// In window, position of new cell = the position of last removed cell =
		// cellPos - 1
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

			// From this user, check all the user in the window to create
			// friendship
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

			// From this user, check all the user in the window to create
			// friendship
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

	public void generatePostandPhoto(String inputFile, int numOfCells) {
		
		// Init neccessary objects
		StorageManager storeManager = new StorageManager(cellSize, windowSize, outUserProfile, sibOutputDir);
		storeManager.initDeserialization(inputFile);
		reducedUserProfilesCell = new ReducedUserProfile[cellSize];
		
		System.out.println("Generating the posts & comments ");

		// Processing for each cell in the file
		System.out.println("Number of cells in file : " + numOfCells);
		
		for (int j = 0; j < numOfCells; j++) {
			storeManager.deserializeOneCellUserProfile(reducedUserProfilesCell);
			
			for (int k = 0; k < cellSize; k++) {
				// Generate extra info such as names, organization before writing out
			    UserExtraInfo extraInfo = new UserExtraInfo();
				setInfoFromUserProfile(reducedUserProfilesCell[k], extraInfo);
				
				serializer.gatherData(reducedUserProfilesCell[k], extraInfo);

				generatePosts(reducedUserProfilesCell[k], extraInfo);

				generatePhoto(reducedUserProfilesCell[k], extraInfo);
			}
		}
		
		storeManager.endDeserialization();
		System.out.println("Done generating the posts and photos....");
		System.out.println("Number of deserialized objects is " + storeManager.getNumberDeSerializedObject());
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
		int numPhotoAlbums = randNumberPhotoAlbum.nextInt(maxNumPhotoAlbums);
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

	// The group only created from users and their friends in the current
	// sliding window.
	// We do that, because we need the user's created date information for
	// generating the group's joining datetime and group's post created date.
	// For one user, a number of groups are created. For each group, the number
	// of members is first generated.
	// To select a member for joining the group
	// First, select which level of friends that we are considering
	// Second, randomSelect one user in that level
	// Decide whether that user can be a member of the group by their joinProb
	public void generateGroups() {
		System.out.println("Generating user groups ");

		groupStoreManager.deserializeWindowlUserProfile(reducedUserProfiles);

		// Init a window of removed users, now it is empty
		
		removedUserProfiles = new ReducedUserProfile[windowSize];

		double moderatorProb;

		for (int i = 0; i < cellSize; i++) {
			moderatorProb = randGroupModerator.nextDouble();
			if (moderatorProb > groupModeratorProb) {
				continue;
			}

			Friend firstLevelFriends[];
			Vector<Friend> secondLevelFriends = new Vector<Friend>();

			// Get the set of first and second level friends
			firstLevelFriends = reducedUserProfiles[i].getFriendList();
			for (int j = 0; j < reducedUserProfiles[i].getNumFriendsAdded(); j++) {
				int friendId = firstLevelFriends[j].getFriendAcc();

				int friendIdxInWindow = getIdxInWindow(0, 0, friendId);

				if (friendIdxInWindow != -1) {
					Friend friendOfFriends[] = reducedUserProfiles[friendIdxInWindow].getFriendList();

					for (int k = 0; k < friendOfFriends.length; k++) {
						if (friendOfFriends[k] != null) {
							secondLevelFriends.add(friendOfFriends[k]);
						} else {
							break;
						}
					}
				}
			}

			// Create a group whose the moderator is the current user
			int numGroup = randNumberGroup.nextInt(maxNumGroupCreatedPerUser);
			for (int j = 0; j < numGroup; j++) {
				createGroupForUser(reducedUserProfiles[i], firstLevelFriends, secondLevelFriends);
			}

		}
	}

	public void generateGroups(int pass, int cellPos, int numberCellInFile) {

		int newCellPosInWindow = (cellPos - 1) % numberOfCellPerWindow;

		int newIdxInWindow = newCellPosInWindow * cellSize;

		// Store the to-be-removed cell to the window
		storeCellToRemovedWindow(newIdxInWindow, cellSize, pass);

		// Deserialize the cell from file
		
		groupStoreManager.deserializeOneCellUserProfile(newIdxInWindow, cellSize, reducedUserProfiles);
		

		int newStartIndex = (cellPos % numberOfCellPerWindow) * cellSize;
		int curIdxInWindow;
		double moderatorProb;
		for (int i = 0; i < cellSize; i++) {
			moderatorProb = randGroupModerator.nextDouble();
			if (moderatorProb > groupModeratorProb) {
				continue;
			}

			curIdxInWindow = newStartIndex + i;

			Friend firstLevelFriends[];
			Vector<Friend> secondLevelFriends = new Vector<Friend>();

			// Get the set of first and second level friends
			firstLevelFriends = reducedUserProfiles[curIdxInWindow].getFriendList();

			// Create a group whose the moderator is the current user
			int numGroup = randNumberGroup.nextInt(maxNumGroupCreatedPerUser);
			for (int j = 0; j < numGroup; j++) { 
				createGroupForUser(reducedUserProfiles[curIdxInWindow],
						firstLevelFriends, secondLevelFriends);
			}
		}
	}

	public int getIdxInWindow(int startIndex, int startUserId, int userAccId) {
		if (((startUserId + windowSize) <= userAccId) || (startUserId > userAccId)) {
			return -1;
		}
		
		return (startIndex + (userAccId - startUserId)) % windowSize;
	}

	public int getIdxInRemovedWindow(int startIndex, int startUserId, int userAccId) {
		if (userAccId >= startUserId|| ((userAccId + windowSize) < startUserId)) {
			return -1;
		}
		
		return (startIndex + (userAccId + windowSize - startUserId)) % windowSize;
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
		UserProfile userProf = new UserProfile();
		userProf.resetUser();
		userProf.setAccountId(accountId);
		// Create date
		userProf.setCreatedDate(dateTimeGenerator.randomDateInMillis());
		
		userProf.setNumFriends((short) randPowerlaw.getValue());
		userProf.allocateFriendListMemory(numCorrDimensions);
		
		short totalFriendSet = 0; 
		for (int i = 0; i < numCorrDimensions-1; i++){
			short numPassFriend = (short) Math.floor(friendsRatioPerPass[0] * userProf.getNumFriends());
			totalFriendSet = (short) (totalFriendSet + numPassFriend);
			//userProf.setNumPassFriends(numPassFriend,i);
			userProf.setNumPassFriends(totalFriendSet,i);
			
		}
		// Prevent the case that the number of friends added exceeds the total number of friends
		userProf.setNumPassFriends(userProf.getNumFriends(),numCorrDimensions-1);

		userProf.setNumFriendsAdded((short) 0);
		userProf.setLocationIdx(locationDic.getLocation(accountId));
		userProf.setCityIdx(locationDic.getRandomCity(userProf.getLocationIdx()));
		userProf.setLocationZId(locationDic.getZorderID(userProf.getLocationIdx()));
		
		
		int userMainTag = mainTagDic.getaTagByCountry(userProf.getLocationIdx());
		
		userProf.setMainTagId(userMainTag);
		
//		userProf.setNumTags((short) (randNumTags.nextInt(maxNoTagsPerUser) + 1));
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
		userProf.setRandomIdx(randUserRandomIdx.nextInt(maxUserRandomIdx));

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

		        if (user.getNumFriendsAdded() > 0) {
		            int specialFriendId = 0;
		            int numFriendCheck = 0;

		            do {
		                specialFriendId = randomHaveStatus.nextInt(user
		                        .getNumFriendsAdded());
		                numFriendCheck++;
		            } while (friends[specialFriendId].getCreatedTime() == -1
		                    && numFriendCheck < friends.length);

		            if (friends[specialFriendId].getCreatedTime() == -1) {// In case do not find any friendId
		                userExtraInfo.setSpecialFriendIdx(-1);
		            } else {
		                userExtraInfo
		                .setSpecialFriendIdx(friends[specialFriendId]
		                        .getFriendAcc());
		            }
		        } else {
		            userExtraInfo.setSpecialFriendIdx(-1);
		        }
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
		
		userExtraInfo.setLastName(namesDictionary.getRandomSurName(user
				.getLocationIdx()));

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

	public void storeCellToRemovedWindow(int startIdex, int cellSize, int pass) {
		for (int i = 0; i < cellSize; i++)
			removedUserProfiles[startIdex + i] = reducedUserProfiles[startIdex + i];
	}

	public double getFriendCreatePro(int i, int j, int pass){
		
		double prob;
		if (j > i){
			prob = Math.pow(baseProbCorrelated, (j- i));
		}
		else{
			prob =  Math.pow(baseProbCorrelated, (j + windowSize - i));
		}
		
		return prob; 

	}
	public void createFriendShip(ReducedUserProfile user1, ReducedUserProfile user2, byte pass) {
		long requestedTime = dateTimeGenerator.randomFriendRequestedDate(user1,
				user2);
		byte initiator = (byte) randInitiator.nextInt(2);
		long createdTime = -1;
		long declinedTime = -1;
		if (randFriendReject.nextDouble() > friendRejectRatio) {
			createdTime = dateTimeGenerator
					.randomFriendApprovedDate(requestedTime);
		} else {
			declinedTime = dateTimeGenerator
					.randomFriendDeclinedDate(requestedTime);
			if (randFriendReapprov.nextDouble() < friendReApproveRatio) {
				createdTime = dateTimeGenerator
						.randomFriendReapprovedDate(declinedTime);
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
                    numRdfOutputFile, true, mainTagDic.getTagsNamesMapping(),
                    browserDic.getvBrowser(), companiesDictionary.getCompanyCityMap(), 
                    organizationsDictionary.GetOrganizationLocationMap(),
                    ipAddDictionary, locationDic, languageDic);
		} else if (t.equals("n3")) {
            return new Turtle(sibOutputDir + outputFileName, forwardChaining,
                    numRdfOutputFile, false, mainTagDic.getTagsNamesMapping(),
                    browserDic.getvBrowser(), companiesDictionary.getCompanyCityMap(), 
                    organizationsDictionary.GetOrganizationLocationMap(),
                    ipAddDictionary, locationDic, languageDic);
		} else if (t.equals("csv")) {
			return new CSV(sibOutputDir /*+ outputFileName*/, forwardChaining,
					numRdfOutputFile, mainTagDic.getTagsNamesMapping(),
					browserDic.getvBrowser(), companiesDictionary.getCompanyCityMap(), 
                    organizationsDictionary.GetOrganizationLocationMap(),
					ipAddDictionary,locationDic, languageDic);
		} else {
			return null;
		}
	}
	
	private void startWritingUserData() {
		outputDataWriter.initWritingUserData();
	}

	private void finishWritingUserData() {
		outputDataWriter.finishWritingUserData();
	}

	public void writeToOutputFile(String filenames[], String outputfile){
		 	Writer output = null;
		 	File file = new File(outputfile);
		 	try {
				output = new BufferedWriter(new FileWriter(file));
				for (int i = 0; i < (filenames.length - 1); i++)
					output.write(filenames[i] + " " + numCellPerfile + "\n");
				
				output.write(filenames[filenames.length - 1] + " " + numCellInLastFile + "\n");
				
				output.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
	public String getDuration(long startTime, long endTime){
		String duration = (endTime - startTime)
		/ 60000 + " minutes and " + ((endTime - startTime) % 60000)
		/ 1000 + " seconds";
		
		return duration; 
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
	public int getCellSize() {
		return cellSize;
	}
	public int getMapId() {
		return machineId;
	}

	public void setMapId(int mapId) {
		this.machineId = mapId;
	}


}
 
