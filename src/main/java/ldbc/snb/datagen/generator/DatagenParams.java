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


package ldbc.snb.datagen.generator;

import ldbc.snb.datagen.util.ScaleFactor;
import org.apache.hadoop.conf.Configuration;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.util.TreeMap;

public class DatagenParams {

    //Files and folders
    public static final String DICTIONARY_DIRECTORY    = "/dictionaries/";
    public static final String IPZONE_DIRECTORY        = "/ipaddrByCountries";
    public static final String STATS_FILE              = "testdata.json";
    public static final String RDF_OUTPUT_FILE         = "ldbc_socialnet_dbg";
    public static final String PARAM_COUNT_FILE        = "factors.txt";

    // Dictionaries dataset files
    public static final String browserDictonryFile         = DICTIONARY_DIRECTORY + "browsersDic.txt";
    public static final String companiesDictionaryFile     = DICTIONARY_DIRECTORY + "companiesByCountry.txt";
    public static final String countryAbbrMappingFile      = DICTIONARY_DIRECTORY + "countryAbbrMapping.txt";
    public static final String popularTagByCountryFile     = DICTIONARY_DIRECTORY + "popularTagByCountry.txt";
    public static final String countryDictionaryFile       = DICTIONARY_DIRECTORY + "dicLocations.txt";
    public static final String tagsFile                    = DICTIONARY_DIRECTORY + "tags.txt";
    public static final String emailDictionaryFile         = DICTIONARY_DIRECTORY + "email.txt";
    public static final String nameDictionaryFile          = DICTIONARY_DIRECTORY + "givennameByCountryBirthPlace.txt.freq.full";
    public static final String universityDictionaryFile    = DICTIONARY_DIRECTORY + "universities.txt";
    public static final String cityDictionaryFile          = DICTIONARY_DIRECTORY + "citiesByCountry.txt";
    public static final String languageDictionaryFile      = DICTIONARY_DIRECTORY + "languagesByCountry.txt";
    public static final String popularDictionaryFile       = DICTIONARY_DIRECTORY + "popularPlacesByCountry.txt";
    public static final String agentFile                   = DICTIONARY_DIRECTORY + "smartPhonesProviders.txt";
    public static final String surnamDictionaryFile        = DICTIONARY_DIRECTORY + "surnameByCountryBirthPlace.txt.freq.sort";
    public static final String tagClassFile                = DICTIONARY_DIRECTORY + "tagClasses.txt";
    public static final String tagClassHierarchyFile       = DICTIONARY_DIRECTORY + "tagClassHierarchy.txt";
    public static final String tagTextFile                 = DICTIONARY_DIRECTORY + "tagText.txt";
    public static final String tagMatrixFile               = DICTIONARY_DIRECTORY + "tagMatrix.txt";
    public static final String flashmobDistFile            = DICTIONARY_DIRECTORY + "flashmobDist.txt";
    public static final String fbSocialDegreeFile          = DICTIONARY_DIRECTORY + "facebookBucket100.dat";

    //private parameters
    private enum ParameterNames {
        BASE_CORRELATED ("ldbc.snb.datagen.baseProbCorrelated"),
        BEST_UNIVERSTY_RATIO ("ldbc.snb.datagen.probTopUniv"),
        BLOCK_SIZE ("ldbc.snb.datagen.blockSize"),
        CELL_SIZE ("ldbc.snb.datagen.cellSize"),
        COMPANY_UNCORRELATED_RATIO ("ldbc.snb.datagen.probUnCorrelatedCompany"),
        DIFFERENT_IP_IN_TRAVEL_RATIO ("ldbc.snb.datagen.probDiffIPinTravelSeason"),
        DIFFERENT_IP_NOT_TRAVEL_RATIO ("ldbc.snb.datagen.probDiffIPnotTravelSeason"),
        ENGLISH_RATIO ("ldbc.snb.datagen.probEnglish"),
        FLASHMOB_TAGS_PER_MONTH ("ldbc.snb.datagen.flashmobTagsPerMonth"),
        FLASHMOB_TAG_DIST_EXP ("ldbc.snb.datagen.flashmobTagDistExp"),
        FLASHMOB_TAG_MAX_LEVEL ("ldbc.snb.datagen.flashmobTagMaxLevel"),
        FLASHMOB_TAG_MIN_LEVEL ("ldbc.snb.datagen.flashmobTagMinLevel"),
        FRIEND_REACCEPT ("ldbc.snb.datagen.friendReApproveRatio"),
        FRIEND_REJECT ("ldbc.snb.datagen.friendRejectRatio"),
        GROUP_MAX_POST_MONTH ("ldbc.snb.datagen.maxNumGroupPostPerMonth"),
        GROUP_MODERATOR_RATIO ("ldbc.snb.datagen.groupModeratorProb"),
        LARGE_COMMENT_RATIO ("ldbc.snb.datagen.ratioLargeComment"),
        LARGE_POST_RATIO ("ldbc.snb.datagen.ratioLargePost"),
        LIMIT_CORRELATED ("ldbc.snb.datagen.limitProCorrelated"),
        MAX_COMMENT_POST ("ldbc.snb.datagen.maxNumComments"),
        MAX_COMMENT_SIZE ("ldbc.snb.datagen.maxCommentSize"),
        MAX_COMPANIES ("ldbc.snb.datagen.maxCompanies"),
        MAX_EMAIL ("ldbc.snb.datagen.maxEmails"),
        MAX_FRIENDS ("ldbc.snb.datagen.maxNumFriends"),
        MAX_GROUP_MEMBERS ("ldbc.snb.datagen.maxNumMemberGroup"),
        MAX_LARGE_COMMENT_SIZE ("ldbc.snb.datagen.maxLargeCommentSize"),
        MAX_LARGE_POST_SIZE ("ldbc.snb.datagen.maxLargePostSize"),
        MAX_NUM_FLASHMOB_POST_PER_MONTH ("ldbc.snb.datagen.maxNumFlashmobPostPerMonth"),
        MAX_NUM_GROUP_FLASHMOB_POST_PER_MONTH ("ldbc.snb.datagen.maxNumGroupFlashmobPostPerMonth"),
        MAX_NUM_TAG_PER_FLASHMOB_POST ("ldbc.snb.datagen.maxNumTagPerFlashmobPost"),
        MAX_PHOTOALBUM ("ldbc.snb.datagen.maxNumPhotoAlbumsPerMonth"),
        MAX_PHOTO_PER_ALBUM ("ldbc.snb.datagen.maxNumPhotoPerAlbums"),
        MAX_POPULAR_PLACES ("ldbc.snb.datagen.maxNumPopularPlaces"),
        MAX_TEXT_SIZE ("ldbc.snb.datagen.maxTextSize"),
        MIN_COMMENT_SIZE ("ldbc.snb.datagen.minCommentSize"),
        MIN_FRIENDS ("ldbc.snb.datagen.minNumFriends"),
        MIN_LARGE_COMMENT_SIZE ("ldbc.snb.datagen.minLargeCommentSize"),
        MIN_LARGE_POST_SIZE ("ldbc.snb.datagen.minLargePostSize"),
        MIN_TEXT_SIZE ("ldbc.snb.datagen.minTextSize"),
        MISSING_RATIO ("ldbc.snb.datagen.missingRatio"),
        NUM_CELL_WINDOW ("ldbc.snb.datagen.numberOfCellPerWindow"),
        OTHER_BROWSER_RATIO ("ldbc.snb.datagen.probAnotherBrowser"),
        POPULAR_PLACE_RATIO ("ldbc.snb.datagen.probPopularPlaces"),
        PROB_INTEREST_FLASHMOB_TAG ("ldbc.snb.datagen.probInterestFlashmobTag"),
        PROB_RANDOM_PER_LEVEL ("ldbc.snb.datagen.probRandomPerLevel"),
        REDUCE_TEXT_RATIO ("ldbc.snb.datagen.ratioReduceText"),
        SECOND_LANGUAGE_RATIO ("ldbc.snb.datagen.probSecondLang"),
        STATUS_MISSING_RATIO ("ldbc.snb.datagen.missingStatusRatio"),
        STATUS_SINGLE_RATIO ("ldbc.snb.datagen.probSingleStatus"),
        TAG_UNCORRELATED_COUNTRY ("ldbc.snb.datagen.tagCountryCorrProb"),
        UNIVERSITY_UNCORRELATED_RATIO ("ldbc.snb.datagen.probUnCorrelatedOrganization"),
        MAX_NUM_LIKE ("ldbc.snb.datagen.maxNumLike"),
        UPDATE_PORTION ("ldbc.snb.datagen.serializer.updatePortion"),
        USER_MAX_GROUP ("ldbc.snb.datagen.maxNumGroupCreatedPerUser"),
        USER_MAX_POST_MONTH ("ldbc.snb.datagen.maxNumPostPerMonth"),
        USER_MAX_TAGS ("ldbc.snb.datagen.maxNumTagsPerUser"),
        USER_MIN_TAGS ("ldbc.snb.datagen.minNumTagsPerUser");

        private final String name;

        private ParameterNames( String name ) {
            this.name = name;
        }
        public String toString() {
            return name;
        }
    }

    public static double baseProbCorrelated                = 0.0; // the base probability to create a correlated edge between two persons
    public static double flashmobTagDistExp                = 0.0; // the flashmob tag distribution exponent
    public static double flashmobTagMaxLevel               = 0.0; // the flashmob tag max activity volume level
    public static double flashmobTagMinLevel               = 0.0; // the flashmob tag min activity volume level
    public static double friendReApproveRatio              = 0.0;  
    public static double friendRejectRatio                 = 0.0;
    public static double groupModeratorProb                = 0.0;
    public static double limitProCorrelated                = 0.0;
    public static double missingRatio                      = 0.0;
    public static double missingStatusRatio                = 0.0;
    public static double probAnotherBrowser                = 0.0;
    public static double probDiffIPinTravelSeason          = 0.0; // in travel season
    public static double probDiffIPnotTravelSeason         = 0.0; // not in travel season
    public static double probEnglish                       = 0.0;
    public static double probInterestFlashmobTag           = 0.0;
    public static double probPopularPlaces                 = 0.0; //probability of taking a photo at popular place
    public static double probRandomPerLevel                = 0.0;
    public static double probSecondLang                    = 0.0;
    public static double probSingleStatus                  = 0.0; // Status "Single" has more probability than others'
    public static double probTopUniv                       = 0.0; // 90% users go to top university
    public static double probUnCorrelatedCompany           = 0.0;
    public static double probUnCorrelatedOrganization      = 0.0;
    public static double ratioLargeComment                 = 0.0;
    public static double ratioLargePost                    = 0.0;
    public static double ratioReduceText                   = 0.0; // 80% text has size less than 1/2 max size
    public static double tagCountryCorrProb                = 0.0;
    public static double updatePortion                     = 0.0;
    public static int blockSize                            = 0;
    public static int cellSize                             = 0; // Number of user in one cell
    public static int flashmobTagsPerMonth                 = 0;
    public static int maxCommentSize                       = 0;
    public static int maxCompanies                         = 0;
    public static int maxEmails                            = 0;
    public static int maxLargeCommentSize                  = 0;
    public static int maxLargePostSize                     = 0;
    public static int maxNumComments                       = 0;
    public static int maxNumFlashmobPostPerMonth           = 0;
    public static int maxNumFriends                        = 0;
    public static int maxNumGroupCreatedPerUser            = 0;
    public static int maxNumGroupFlashmobPostPerMonth      = 0;
    public static int maxNumGroupPostPerMonth              = 0;
    public static int maxNumMemberGroup                    = 0;
    public static int maxNumLike                           = 0;
    public static int maxNumPhotoAlbumsPerMonth            = 0;
    public static int maxNumPhotoPerAlbums                 = 0;
    public static int maxNumPopularPlaces                  = 0;
    public static int maxNumPostPerMonth                   = 0;
    public static int maxNumTagPerFlashmobPost             = 0;
    public static int maxNumTagsPerUser                    = 0;
    public static int maxTextSize                          = 0;
    public static int minCommentSize                       = 0;
    public static int minLargeCommentSize                  = 0;
    public static int minLargePostSize                     = 0;
    public static int minNumFriends                        = 0;
    public static int minNumTagsPerUser                    = 0;
    public static int minTextSize                          = 0;
    public static int numberOfCellPerWindow                = 0;

    public static final int startMonth                     = 1;
    public static final int startDate                      = 1;
    public static final int endMonth                       = 1;
    public static final int endDate                        = 1;
    public static final double alpha                       = 0.4;


    public static String outputDir                         = "./";
    public static String hadoopDir                         = "./";
    public static String socialNetworkDir                  = "./";
    public static int    numThreads                        = 1;
    public static int    deltaTime                         = 10000;
    public static int    numPersons                        = 10000;
    public static int    startYear                         = 2010;
    public static int    endYear                           = 2013;
    public static int    numYears                          = 3;
    public static boolean updateStreams                    = false;
    public static boolean exportText                       = true;
    public static boolean compressed                       = false;

    private static TreeMap<Integer, ScaleFactor> scaleFactors;
    private static final String SCALE_FACTORS_FILE      =  "scale_factors.xml";

    public static void readConf( Configuration conf ) {
        try {

            ParameterNames values[] = ParameterNames.values();
            for( int i = 0; i < values.length; ++i ) {
                if (conf.get(values[i].toString()) == null) {
                    throw new IllegalStateException("Missing " + values[i].toString() + " parameter");
                }
            }

            cellSize                        = Short.parseShort(conf.get(ParameterNames.CELL_SIZE.toString()));
            numberOfCellPerWindow           = Integer.parseInt(conf.get(ParameterNames.NUM_CELL_WINDOW.toString()));
            minNumFriends                   = Integer.parseInt(conf.get(ParameterNames.MIN_FRIENDS.toString()));
            maxNumFriends                   = Integer.parseInt(conf.get(ParameterNames.MAX_FRIENDS.toString()));
            friendRejectRatio               = Double.parseDouble(conf.get(ParameterNames.FRIEND_REJECT.toString()));
            friendReApproveRatio            = Double.parseDouble(conf.get(ParameterNames.FRIEND_REACCEPT.toString()));
            minNumTagsPerUser               = Integer.parseInt(conf.get(ParameterNames.USER_MIN_TAGS.toString()));
            maxNumTagsPerUser               = Integer.parseInt(conf.get(ParameterNames.USER_MAX_TAGS.toString()));
            maxNumPostPerMonth              = Integer.parseInt(conf.get(ParameterNames.USER_MAX_POST_MONTH.toString()));
            maxNumComments                  = Integer.parseInt(conf.get(ParameterNames.MAX_COMMENT_POST.toString()));
            limitProCorrelated              = Double.parseDouble(conf.get(ParameterNames.LIMIT_CORRELATED.toString()));
            baseProbCorrelated              = Double.parseDouble(conf.get(ParameterNames.BASE_CORRELATED.toString()));
            maxEmails                       = Integer.parseInt(conf.get(ParameterNames.MAX_EMAIL.toString()));
            maxCompanies                    = Integer.parseInt(conf.get(ParameterNames.MAX_EMAIL.toString()));
            probEnglish                     = Double.parseDouble(conf.get(ParameterNames.MAX_EMAIL.toString()));
            probSecondLang                  = Double.parseDouble(conf.get(ParameterNames.MAX_EMAIL.toString()));
            probAnotherBrowser              = Double.parseDouble(conf.get(ParameterNames.OTHER_BROWSER_RATIO.toString()));
            minTextSize                     = Integer.parseInt(conf.get(ParameterNames.MIN_TEXT_SIZE.toString()));
            maxTextSize                     = Integer.parseInt(conf.get(ParameterNames.MAX_TEXT_SIZE.toString()));
            minCommentSize                  = Integer.parseInt(conf.get(ParameterNames.MIN_COMMENT_SIZE.toString()));
            maxCommentSize                  = Integer.parseInt(conf.get(ParameterNames.MAX_COMMENT_SIZE.toString()));
            ratioReduceText                 = Double.parseDouble(conf.get(ParameterNames.REDUCE_TEXT_RATIO.toString()));
            minLargePostSize                = Integer.parseInt(conf.get(ParameterNames.MIN_LARGE_POST_SIZE.toString()));
            maxLargePostSize                = Integer.parseInt(conf.get(ParameterNames.MAX_LARGE_POST_SIZE.toString()));
            minLargeCommentSize             = Integer.parseInt(conf.get(ParameterNames.MIN_LARGE_COMMENT_SIZE.toString()));
            maxLargeCommentSize             = Integer.parseInt(conf.get(ParameterNames.MAX_LARGE_COMMENT_SIZE.toString()));
            ratioLargePost                  = Double.parseDouble(conf.get(ParameterNames.LARGE_POST_RATIO.toString()));
            ratioLargeComment               = Double.parseDouble(conf.get(ParameterNames.LARGE_COMMENT_RATIO.toString()));
            maxNumLike                      = Integer.parseInt(conf.get(ParameterNames.MAX_NUM_LIKE.toString()));
            maxNumPhotoAlbumsPerMonth       = Integer.parseInt(conf.get(ParameterNames.MAX_PHOTOALBUM.toString()));
            maxNumPhotoPerAlbums            = Integer.parseInt(conf.get(ParameterNames.MAX_PHOTO_PER_ALBUM.toString()));
            maxNumGroupCreatedPerUser       = Integer.parseInt(conf.get(ParameterNames.USER_MAX_GROUP.toString()));
            maxNumMemberGroup               = Integer.parseInt(conf.get(ParameterNames.MAX_GROUP_MEMBERS.toString()));
            groupModeratorProb              = Double.parseDouble(conf.get(ParameterNames.GROUP_MODERATOR_RATIO.toString()));
            maxNumGroupPostPerMonth         = Integer.parseInt(conf.get(ParameterNames.GROUP_MAX_POST_MONTH.toString()));
            missingRatio                    = Double.parseDouble(conf.get(ParameterNames.MISSING_RATIO.toString()));
            missingStatusRatio              = Double.parseDouble(conf.get(ParameterNames.STATUS_MISSING_RATIO.toString()));
            probSingleStatus                = Double.parseDouble(conf.get(ParameterNames.STATUS_SINGLE_RATIO.toString()));
            probDiffIPinTravelSeason        = Double.parseDouble(conf.get(ParameterNames.DIFFERENT_IP_IN_TRAVEL_RATIO.toString()));
            probDiffIPnotTravelSeason       = Double.parseDouble(conf.get(ParameterNames.DIFFERENT_IP_NOT_TRAVEL_RATIO.toString()));
            probUnCorrelatedCompany         = Double.parseDouble(conf.get(ParameterNames.COMPANY_UNCORRELATED_RATIO.toString()));
            probUnCorrelatedOrganization    = Double.parseDouble(conf.get(ParameterNames.UNIVERSITY_UNCORRELATED_RATIO.toString()));
            probTopUniv                     = Double.parseDouble(conf.get(ParameterNames.BEST_UNIVERSTY_RATIO.toString()));
            maxNumPopularPlaces             = Integer.parseInt(conf.get(ParameterNames.MAX_POPULAR_PLACES.toString()));
            probPopularPlaces               = Double.parseDouble(conf.get(ParameterNames.POPULAR_PLACE_RATIO.toString()));
            tagCountryCorrProb              = Double.parseDouble(conf.get(ParameterNames.TAG_UNCORRELATED_COUNTRY.toString()));
            flashmobTagsPerMonth            = Integer.parseInt(conf.get(ParameterNames.FLASHMOB_TAGS_PER_MONTH.toString()));
            probInterestFlashmobTag         = Double.parseDouble(conf.get(ParameterNames.PROB_INTEREST_FLASHMOB_TAG.toString()));
            probRandomPerLevel              = Double.parseDouble(conf.get(ParameterNames.PROB_RANDOM_PER_LEVEL.toString()));
            maxNumFlashmobPostPerMonth      = Integer.parseInt(conf.get(ParameterNames.MAX_NUM_FLASHMOB_POST_PER_MONTH.toString()));
            maxNumGroupFlashmobPostPerMonth = Integer.parseInt(conf.get(ParameterNames.MAX_NUM_GROUP_FLASHMOB_POST_PER_MONTH.toString()));
            maxNumTagPerFlashmobPost        = Integer.parseInt(conf.get(ParameterNames.MAX_NUM_TAG_PER_FLASHMOB_POST.toString()));
            flashmobTagMinLevel             = Double.parseDouble(conf.get(ParameterNames.FLASHMOB_TAG_MIN_LEVEL.toString()));
            flashmobTagMaxLevel             = Double.parseDouble(conf.get(ParameterNames.FLASHMOB_TAG_MAX_LEVEL.toString()));
            flashmobTagDistExp              = Double.parseDouble(conf.get(ParameterNames.FLASHMOB_TAG_DIST_EXP.toString()));
            updatePortion                   = Double.parseDouble(conf.get(ParameterNames.UPDATE_PORTION.toString()));
            blockSize                       = Integer.parseInt(conf.get(ParameterNames.BLOCK_SIZE.toString()));
            scaleFactors = new TreeMap<Integer, ScaleFactor>();
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(LDBCDatagen.class.getResourceAsStream("/" + SCALE_FACTORS_FILE));
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
                    scaleFactors.put(num, scaleFactor);
                }
            }
            System.out.println("Number of scale factors read "+scaleFactors.size());
        } catch (Exception e) {
            System.out.println("Error reading scale factors");
            System.err.println(e.getMessage());
            System.exit(-1);
        }

        try {
            if (conf.get("ldbc.snb.datagen.numPersons") != null && conf.get("ldbc.snb.datagen.numYears") != null && conf.get("ldbc.snb.datagen.startYear") != null) {
                numPersons = Integer.parseInt(conf.get("ldbc.snb.datagen.numPersons"));
                startYear = Integer.parseInt(conf.get("ldbc.snb.datagen.startYear"));
                numYears = Integer.parseInt(conf.get("ldbc.snb.datagen.numYears"));
                endYear = startYear + numYears;
            } else {
                int scaleFactorId = Integer.parseInt(conf.get("ldbc.snb.datagen.scaleFactor"));
                ScaleFactor scaleFactor = scaleFactors.get(scaleFactorId);
                System.out.println("Executing with scale factor " + scaleFactorId);
                System.out.println(" ... Num Persons " + scaleFactor.numPersons);
                System.out.println(" ... Start Year " + scaleFactor.startYear);
                System.out.println(" ... Num Years " + scaleFactor.numYears);
                numPersons = scaleFactor.numPersons;
                startYear = scaleFactor.startYear;
                numYears = scaleFactor.numYears;
                endYear = startYear + numYears;
            }

            exportText = conf.getBoolean("ldbc.snb.datagen.exportText",true);
            compressed = conf.getBoolean("ldbc.snb.datagen.serializer.compressed",false);
            numThreads = conf.getInt("ldbc.snb.datagen.numThreads",1);
            updateStreams = conf.getBoolean("ldbc.snb.datagen.serializer.updateStreams",false);
            deltaTime = conf.getInt("ldbc.snb.datagen.deltaTime",10000);
            outputDir = conf.get("ldbc.snb.datagen.serializer.outputDir","./");
            hadoopDir = conf.get("ldbc.snb.datagen.serializer.hadoopDir","./");
            socialNetworkDir = conf.get("ldbc.snb.datagen.serializer.socialNetworkDir","./");
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(-1);
        }
    }
}
