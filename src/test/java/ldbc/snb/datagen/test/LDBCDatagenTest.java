package ldbc.snb.datagen.test;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.generator.LDBCDatagen;
import ldbc.snb.datagen.test.csv.*;
import ldbc.snb.datagen.util.ConfigParser;
import org.apache.hadoop.conf.Configuration;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Created by aprat on 18/12/15.
 */
public class LDBCDatagenTest {

    private final String dir = "./test_data/social_network";
    private final String sdir = "./test_data/substitution_parameters";

    @BeforeClass
    public static void generateData() throws Exception {
        Configuration conf = ConfigParser.initialize();
        ConfigParser.readConfig(conf, "./test_params.ini");
        ConfigParser.readConfig(conf, LDBCDatagen.class.getResourceAsStream("/params.ini"));
        try {
            LDBCDatagen.prepareConfiguration(conf);
            LDBCDatagen.initializeContext(conf);
            LDBCDatagen datagen = new LDBCDatagen();
            datagen.runGenerateJob(conf);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void personTest() {
        testIdUniqueness(dir+"/person_0_0.csv", 0);
        testStringLength(dir+"/person_0_0.csv", 1, 40);
        testStringLength(dir+"/person_0_0.csv", 2, 40);
        testStringLength(dir+"/person_0_0.csv", 3, 40);
        testStringLength(dir+"/person_0_0.csv", 6, 40);
        testStringLength(dir+"/person_0_0.csv", 7, 40);
        assertTrue("Everything ok",true);
    }

    @Test
    public void postTest() {
        testIdUniqueness(dir+"/post_0_0.csv", 0);
        testLongBetween(dir+"/post_0_0.csv",7,0,2001);
        testStringLength(dir+"/post_0_0.csv", 1, 40);
        testStringLength(dir+"/post_0_0.csv", 3, 40);
        testStringLength(dir+"/post_0_0.csv", 4, 40);
        testStringLength(dir+"/post_0_0.csv", 5, 40);
        assertTrue("Everything ok",true);
    }

    @Test
    public void forumTest() {
        testIdUniqueness(dir+"/forum_0_0.csv", 0);
        testStringLength(dir+"/forum_0_0.csv", 1, 256);
        assertTrue("Everything ok",true);
    }

    @Test
    public void commentTest() {
        testIdUniqueness(dir+"/comment_0_0.csv", 0);
        testLongBetween(dir+"/comment_0_0.csv",5,0,2001);
        testStringLength(dir+"/comment_0_0.csv", 2, 40);
        testStringLength(dir+"/comment_0_0.csv", 3, 40);
        assertTrue("Everything ok",true);
    }

    @Test
    public void organisationTest() {
        testIdUniqueness(dir+"/organisation_0_0.csv", 0);
        testStringLength(dir+"/organisation_0_0.csv", 2, 256);
        assertTrue("Everything ok",true);
    }

    @Test
    public void placeTest() {
        testIdUniqueness(dir+"/place_0_0.csv", 0);
        testStringLength(dir+"/place_0_0.csv", 1, 256);
        assertTrue("Everything ok",true);
    }

    @Test
    public void tagTest() {
        testIdUniqueness(dir+"/tag_0_0.csv", 0);
        testStringLength(dir+"/tag_0_0.csv", 1, 256);
        assertTrue("Everything ok",true);
    }

    @Test
    public void tagclassTest() {
        testIdUniqueness(dir+"/tagclass_0_0.csv", 0);
        testStringLength(dir+"/tagclass_0_0.csv", 1, 256);
        assertTrue("Everything ok",true);
    }

    @Test
    public void personKnowsPersonTest() {
        testPairUniquenessPlusExistance(dir+"/person_knows_person_0_0.csv",0,1,dir+"/person_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void organisationIsLocatedInPlaceTest() {
        testPairUniquenessPlusExistance(dir+"/organisation_isLocatedIn_place_0_0.csv",0,1,dir+"/organisation_0_0.csv",0,dir+"/place_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void placeIsPartOfPlaceTest() {
        testPairUniquenessPlusExistance(dir+"/place_isPartOf_place_0_0.csv",0,1,dir+"/place_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void tagClassIsSubclassOfTest() {
        testPairUniquenessPlusExistance(dir+"/tagclass_isSubclassOf_tagclass_0_0.csv",0,1,dir+"/tagclass_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void tagHasTypeTagclassCheck() {
        testPairUniquenessPlusExistance(dir+"/tag_hasType_tagclass_0_0.csv",0,1,dir+"/tag_0_0.csv",0,dir+"/tagclass_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void personStudyAtOrganisationCheck() {
        testPairUniquenessPlusExistance(dir+"/person_studyAt_organisation_0_0.csv",0,1,dir+"/person_0_0.csv",0,dir+"/organisation_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void personWorkAtOrganisationCheck() {
        testPairUniquenessPlusExistance(dir+"/person_workAt_organisation_0_0.csv",0,1,dir+"/person_0_0.csv",0,dir+"/organisation_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void personHasInterestTagCheck() {
        testPairUniquenessPlusExistance(dir+"/person_hasInterest_tag_0_0.csv",0,1,dir+"/person_0_0.csv",0,dir+"/tag_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void personIsLocatedInPlaceCheck() {
        testPairUniquenessPlusExistance(dir+"/person_isLocatedIn_place_0_0.csv",0,1,dir+"/person_0_0.csv",0,dir+"/place_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void forumHasTagCheck() {
        testPairUniquenessPlusExistance(dir+"/forum_hasTag_tag_0_0.csv",0,1,dir+"/forum_0_0.csv",0,dir+"/tag_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void forumHasModeratorPersonCheck() {
        testPairUniquenessPlusExistance(dir+"/forum_hasModerator_person_0_0.csv",0,1,dir+"/forum_0_0.csv",0,dir+"/person_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void forumHasMemberPersonCheck() {
        testPairUniquenessPlusExistance(dir+"/forum_hasMember_person_0_0.csv",0,1,dir+"/forum_0_0.csv",0,dir+"/person_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void forumContainerOfPostCheck() {
        testPairUniquenessPlusExistance(dir+"/forum_containerOf_post_0_0.csv",0,1,dir+"/forum_0_0.csv",0,dir+"/post_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void commentHasCreatorPersonCheck() {
        testPairUniquenessPlusExistance(dir+"/comment_hasCreator_person_0_0.csv",0,1,dir+"/comment_0_0.csv",0,dir+"/person_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void commentHasTagTagCheck() {
        testPairUniquenessPlusExistance(dir+"/comment_hasTag_tag_0_0.csv",0,1,dir+"/comment_0_0.csv",0,dir+"/tag_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void commentIsLocatedInPlaceCheck() {
        testPairUniquenessPlusExistance(dir+"/comment_isLocatedIn_place_0_0.csv",0,1,dir+"/comment_0_0.csv",0,dir+"/place_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void commentReplyOfCommentCheck() {
        testPairUniquenessPlusExistance(dir+"/comment_replyOf_comment_0_0.csv",0,1,dir+"/comment_0_0.csv",0,dir+"/comment_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void commentReplyOfPostCheck() {
        testPairUniquenessPlusExistance(dir+"/comment_replyOf_post_0_0.csv",0,1,dir+"/comment_0_0.csv",0,dir+"/post_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void postHasCreatorPersonCheck() {
        testPairUniquenessPlusExistance(dir+"/post_hasCreator_person_0_0.csv",0,1,dir+"/post_0_0.csv",0,dir+"/person_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void postIsLocatedInPlaceCheck() {
        testPairUniquenessPlusExistance(dir+"/post_isLocatedIn_place_0_0.csv",0,1,dir+"/post_0_0.csv",0,dir+"/place_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void personLikesCommentCheck() {
        testPairUniquenessPlusExistance(dir+"/person_likes_comment_0_0.csv",0,1,dir+"/person_0_0.csv",0,dir+"/comment_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void personLikesPostCheck() {
        testPairUniquenessPlusExistance(dir+"/person_likes_post_0_0.csv",0,1,dir+"/person_0_0.csv",0,dir+"/post_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void personEmailAddressCheck() {
        testIdExistance(dir+"/person_0_0.csv",0,dir+"/person_email_emailaddress_0_0.csv",0);
        testStringLength(dir+"/person_email_emailaddress_0_0.csv", 1, 256);
        assertTrue("Everything ok",true);
    }

    // test update stream  time consistency
    @Test
    public void updateStreamForumsConsistencyCheck() {
        testLongPair(dir+"/updateStream_0_0_forum.csv",0,1,NumericPairCheck.NumericCheckType.GE, -10000,0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void queryParamsTest() {
        //Creating person id check
        LongParser parser = new LongParser();
        ColumnSet<Long> persons = new ColumnSet<Long>(parser,new File(dir+"/person_0_0.csv"),0,1);
        List<ColumnSet<Long>> personsRef = new ArrayList<ColumnSet<Long>>();
        personsRef.add(persons);
        List<Integer> personIndex = new ArrayList<Integer>();
        personIndex.add(0);
        ExistsCheck<Long> existsPersonCheck = new ExistsCheck<Long>(parser,personIndex, personsRef);

        //Creating name check
        StringParser strParser = new StringParser();
        ColumnSet<String> names = new ColumnSet<String>(strParser,new File(dir+"/person_0_0.csv"),1,1);
        List<ColumnSet<String>> namesRef = new ArrayList<ColumnSet<String>>();
        namesRef.add(names);
        List<Integer> namesIndex = new ArrayList<Integer>();
        namesIndex.add(1);
        ExistsCheck<String> existsNameCheck = new ExistsCheck<String>(strParser,namesIndex, namesRef);



        FileChecker fileChecker = new FileChecker(sdir+"/interactive_1_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        fileChecker.addCheck(existsNameCheck);
        assertTrue("ERROR PASSING TEST QUERY 1 PERSON AND NAME EXISTS ",fileChecker.run(1));

        //Crating date interval check
        fileChecker = new FileChecker(sdir+"/interactive_2_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        assertTrue("ERROR PASSING TEST QUERY 2 PERSON EXISTS ",fileChecker.run(1));
        testLongGE(sdir+"/interactive_2_param.txt",1, Dictionaries.dates.getStartDateTime());

        //Creating country check
        ColumnSet<String> places = new ColumnSet<String>(strParser,new File(dir+"/place_0_0.csv"),1,1);
        List<ColumnSet<String>> placesRef = new ArrayList<ColumnSet<String>>();
        placesRef.add(places);
        List<Integer> countriesIndex = new ArrayList<Integer>();
        countriesIndex.add(3);
        countriesIndex.add(4);
        ExistsCheck<String> countryExists = new ExistsCheck<String>(strParser,countriesIndex, placesRef);

        //Date duration check
        //DateDurationCheck dateDurationCheck = new DateDurationCheck("Date duration check",1,2,Dictionaries.dates
         //       .getStartDateTime(), Dictionaries.dates.getEndDateTime());

        fileChecker = new FileChecker(sdir+"/interactive_3_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        fileChecker.addCheck(countryExists);
        assertTrue("ERROR PASSING TEST QUERY 3 PERSON EXISTS ",fileChecker.run(1));
        testLongGE(sdir+"/interactive_3_param.txt",1, Dictionaries.dates.getStartDateTime());

        fileChecker = new FileChecker(sdir+"/interactive_4_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        assertTrue("ERROR PASSING TEST QUERY 4 PERSON EXISTS ",fileChecker.run(1));
        testLongGE(sdir+"/interactive_4_param.txt",1, Dictionaries.dates.getStartDateTime());

        fileChecker = new FileChecker(sdir+"/interactive_5_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        assertTrue("ERROR PASSING TEST QUERY 5 PERSON EXISTS ",fileChecker.run(1));
        testLongGE(sdir+"/interactive_5_param.txt",1, Dictionaries.dates.getStartDateTime());

        //Creating tag check
        ColumnSet<String> tags = new ColumnSet<String>(strParser,new File(dir+"/tag_0_0.csv"),1,1);
        List<ColumnSet<String>> tagsRef = new ArrayList<ColumnSet<String>>();
        tagsRef.add(tags);
        List<Integer> tagsIndex = new ArrayList<Integer>();
        tagsIndex.add(1);
        ExistsCheck<String> tagExists = new ExistsCheck<String>(strParser,tagsIndex, tagsRef);

        fileChecker = new FileChecker(sdir+"/interactive_6_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        fileChecker.addCheck(tagExists);
        assertTrue("ERROR PASSING TEST QUERY 6 PERSON EXISTS ",fileChecker.run(1));

        fileChecker = new FileChecker(sdir+"/interactive_7_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        assertTrue("ERROR PASSING TEST QUERY 7 PERSON EXISTS ",fileChecker.run(1));

        fileChecker = new FileChecker(sdir+"/interactive_8_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        assertTrue("ERROR PASSING TEST QUERY 8 PERSON EXISTS ",fileChecker.run(1));

        fileChecker = new FileChecker(sdir+"/interactive_9_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        assertTrue("ERROR PASSING TEST QUERY 9 PERSON EXISTS ",fileChecker.run(1));
        testLongGE(sdir+"/interactive_9_param.txt",1, Dictionaries.dates.getStartDateTime());

        fileChecker = new FileChecker(sdir+"/interactive_10_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        assertTrue("ERROR PASSING TEST QUERY 10 PERSON EXISTS ",fileChecker.run(1));
        testLongBetween(sdir+"/interactive_10_param.txt",1, 1, 13);

        //Creating country check
        countriesIndex.clear();
        countriesIndex.add(1);
        countryExists = new ExistsCheck<String>(strParser,countriesIndex, placesRef);

        fileChecker = new FileChecker(sdir+"/interactive_11_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        fileChecker.addCheck(countryExists);
        assertTrue("ERROR PASSING TEST QUERY 11 PERSON EXISTS ",fileChecker.run(1));

        //Creating tagClass check
        ColumnSet<String> tagClass = new ColumnSet<String>(strParser,new File(dir+"/tagclass_0_0.csv"),1,1);
        List<ColumnSet<String>> tagClassRef = new ArrayList<ColumnSet<String>>();
        tagClassRef.add(tagClass);
        List<Integer> tagClassIndex = new ArrayList<Integer>();
        tagClassIndex.add(1);
        ExistsCheck<String> tagClassExists = new ExistsCheck<String>(strParser,tagClassIndex, tagClassRef);

        fileChecker = new FileChecker(sdir+"/interactive_12_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        fileChecker.addCheck(tagClassExists);
        assertTrue("ERROR PASSING TEST QUERY 12 PERSON EXISTS ",fileChecker.run(1));

        personIndex.add(1);
        ExistsCheck<Long> exists2PersonCheck = new ExistsCheck<Long>(parser,personIndex, personsRef);

        fileChecker = new FileChecker(sdir+"/interactive_13_param.txt");
        fileChecker.addCheck(exists2PersonCheck);
        assertTrue("ERROR PASSING TEST QUERY 13 PERSON EXISTS ",fileChecker.run(1));

        fileChecker = new FileChecker(sdir+"/interactive_14_param.txt");
        fileChecker.addCheck(exists2PersonCheck);
        assertTrue("ERROR PASSING TEST QUERY 14 PERSON EXISTS ",fileChecker.run(1));

    }

    public void testLongPair(String fileName, Integer columnA, Integer columnB, NumericPairCheck.NumericCheckType type, long offsetA, long offsetB) {
        FileChecker fileChecker = new FileChecker(fileName);
        LongParser parser = new LongParser();
        LongPairCheck check = new LongPairCheck(parser, " Long check ", columnA, columnB, type, offsetA, offsetB);
        fileChecker.addCheck(check);
        assertTrue("ERROR PASSING TEST LONG PAIR FOR FILE "+fileName,fileChecker.run(0));
    }

    public void testIdUniqueness(String fileName, int column) {
        FileChecker fileChecker = new FileChecker(fileName);
        UniquenessCheck check = new UniquenessCheck(column);
        fileChecker.addCheck(check);
        assertTrue("ERROR PASSING TEST ID UNIQUENESS FOR FILE "+fileName,fileChecker.run(1));
    }

    public void testLongGE(String fileName, int column, long a) {
        FileChecker fileChecker = new FileChecker(fileName);
        LongParser parser = new LongParser();
        LongCheck longcheck = new LongCheck(parser, "Date Test",column, NumericCheck.NumericCheckType.GE, a,0L);
        fileChecker.addCheck(longcheck);
        assertTrue("ERROR PASSING GE TEST FOR FILE "+fileName+" column "+column+" greater or equal "+a,fileChecker.run(1));
    }

    public void testLongBetween(String fileName, int column, long a, long b) {
        FileChecker fileChecker = new FileChecker(fileName);
        LongParser parser = new LongParser();
        LongCheck longcheck = new LongCheck(parser, "Date Test",column, NumericCheck.NumericCheckType.BETWEEN, a,b);
        fileChecker.addCheck(longcheck);
        assertTrue("ERROR PASSING BETWEENS TEST FOR FILE "+fileName+" column "+column+" between "+a+" and "+b,fileChecker.run(1));
    }

    public void testPairUniquenessPlusExistance(String relationFileName, int columnA, int columnB, String entityFileNameA, int entityColumnA, String entityFileNameB, int entityColumnB) {
        LongParser parser = new LongParser();
        ColumnSet<Long> entitiesA = new ColumnSet<Long>(parser,new File(entityFileNameA),entityColumnA,1);
        ColumnSet<Long> entitiesB = new ColumnSet<Long>(parser,new File(entityFileNameB),entityColumnB,1);
        FileChecker fileChecker = new FileChecker(relationFileName);
        PairUniquenessCheck pairUniquenessCheck = new PairUniquenessCheck<Long,Long>(parser,parser,columnA,columnB);
        fileChecker.addCheck(pairUniquenessCheck);
        List<ColumnSet<Long>> entityARefColumns = new ArrayList<ColumnSet<Long>>();
        entityARefColumns.add(entitiesA);
        List<ColumnSet<Long>> entityBRefColumns = new ArrayList<ColumnSet<Long>>();
        entityBRefColumns.add(entitiesB);
        List<Integer> organisationIndices = new ArrayList<Integer>();
        organisationIndices.add(columnA);
        List<Integer> placeIndices = new ArrayList<Integer>();
        placeIndices.add(columnB);
        ExistsCheck<Long> existsEntityACheck = new ExistsCheck<Long>(parser,organisationIndices, entityARefColumns);
        ExistsCheck<Long> existsEntityBCheck = new ExistsCheck<Long>(parser,placeIndices, entityBRefColumns);
        fileChecker.addCheck(existsEntityACheck);
        fileChecker.addCheck(existsEntityBCheck);
        assertTrue("ERROR PASSING ORGANISATION_ISLOCATEDIN_PLACE TEST",fileChecker.run(1));

    }

    public void testPairUniquenessPlusExistance(String relationFileName, int columnA, int columnB, String entityFileName, int entityColumn) {
        LongParser parser = new LongParser();
        ColumnSet<Long> entities = new ColumnSet<Long>(parser,new File(entityFileName),entityColumn,1);
        FileChecker fileChecker = new FileChecker(relationFileName);
        PairUniquenessCheck pairUniquenessCheck = new PairUniquenessCheck<Long,Long>(parser,parser,columnA,columnB);
        fileChecker.addCheck(pairUniquenessCheck);
        List<ColumnSet<Long>> refcolumns = new ArrayList<ColumnSet<Long>>();
        refcolumns.add(entities);
        List<Integer> columnIndices = new ArrayList<Integer>();
        columnIndices.add(columnA);
        columnIndices.add(columnB);
        ExistsCheck existsCheck = new ExistsCheck<Long>(parser,columnIndices, refcolumns);
        fileChecker.addCheck(existsCheck);
        assertTrue("ERROR PASSING "+relationFileName+" TEST",fileChecker.run(1));
    }

    public void testIdExistance(String fileToCheckExistanceOf, int columnToCheckExistanceOf, String fileToCheckExistanceAgainst, int columnToCheckExistanceAgainst) {
        LongParser parser = new LongParser();
        ColumnSet<Long> checkAgainstEntities = new ColumnSet<Long>(parser,new File(fileToCheckExistanceAgainst),columnToCheckExistanceAgainst,1);
        FileChecker fileChecker = new FileChecker(fileToCheckExistanceOf);
        List<ColumnSet<Long>> refcolumns = new ArrayList<ColumnSet<Long>>();
        refcolumns.add(checkAgainstEntities);
        List<Integer> columnIndices = new ArrayList<Integer>();
        columnIndices.add(columnToCheckExistanceOf);
        ExistsCheck existsCheck = new ExistsCheck<Long>(parser,columnIndices, refcolumns);
        fileChecker.addCheck(existsCheck);
        assertTrue("ERROR PASSING "+fileToCheckExistanceOf+" ID EXISTANCE TEST",fileChecker.run(1));
    }

    public void testStringLength(String fileToCheckExistanceOf, int columnToCheckExistanceOf, int length) {
        FileChecker fileChecker = new FileChecker(fileToCheckExistanceOf);
        StringLengthCheck lengthCheck = new StringLengthCheck(columnToCheckExistanceOf, length);
        fileChecker.addCheck(lengthCheck);
        assertTrue("ERROR PASSING "+fileToCheckExistanceOf+" ID EXISTANCE TEST",fileChecker.run(1));
    }

}
