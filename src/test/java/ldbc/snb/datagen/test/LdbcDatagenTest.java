package ldbc.snb.datagen.test;

import ldbc.snb.datagen.hadoop.HadoopConfiguration;
import ldbc.snb.datagen.hadoop.LdbcDatagen;
import ldbc.snb.datagen.test.csv.ColumnSet;
import ldbc.snb.datagen.test.csv.ExistsCheck;
import ldbc.snb.datagen.test.csv.FileChecker;
import ldbc.snb.datagen.test.csv.LongCheck;
import ldbc.snb.datagen.test.csv.LongPairCheck;
import ldbc.snb.datagen.test.csv.LongParser;
import ldbc.snb.datagen.test.csv.NumericCheck;
import ldbc.snb.datagen.test.csv.NumericPairCheck;
import ldbc.snb.datagen.test.csv.PairUniquenessCheck;
import ldbc.snb.datagen.test.csv.StringLengthCheck;
import ldbc.snb.datagen.test.csv.UniquenessCheck;
import ldbc.snb.datagen.util.ConfigParser;
import ldbc.snb.datagen.util.LdbcConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class LdbcDatagenTest {

    private final String dataDir = "./social_network";

    @BeforeClass
    public static void generateData() throws Exception {
        Map<String, String> confMap = ConfigParser.defaultConfiguration();
        confMap.putAll(ConfigParser.readConfig("./test_params.ini"));
        confMap.putAll(ConfigParser.readConfig(LdbcDatagen.class.getResourceAsStream("/params_default.ini")));
        LdbcConfiguration conf = new LdbcConfiguration(confMap);
        Configuration hadoopConf = HadoopConfiguration.prepare(conf);
        LdbcDatagen datagen = new LdbcDatagen(conf, hadoopConf);
        datagen.runGenerateJob();
    }

    @Test
    public void personTest() {
        testIdUniqueness(dataDir + "/dynamic/person_0_0.csv", 1);
        testStringLength(dataDir + "/dynamic/person_0_0.csv", 2, 40);
        testStringLength(dataDir + "/dynamic/person_0_0.csv", 3, 40);
        testStringLength(dataDir + "/dynamic/person_0_0.csv", 4, 40);
        testStringLength(dataDir + "/dynamic/person_0_0.csv", 5, 40);
        testStringLength(dataDir + "/dynamic/person_0_0.csv", 6, 40);
        assertTrue("Person attributes OK", true);
    }

    @Test
    public void postTest() {
        testIdUniqueness(dataDir + "/dynamic/post_0_0.csv", 1);
        testLongBetween(dataDir + "/dynamic/post_0_0.csv", 7, 0, 2001);
        testStringLength(dataDir + "/dynamic/post_0_0.csv", 2, 40);
        testStringLength(dataDir + "/dynamic/post_0_0.csv", 3, 40);
        testStringLength(dataDir + "/dynamic/post_0_0.csv", 4, 40);
        testStringLength(dataDir + "/dynamic/post_0_0.csv", 5, 40);

        assertTrue("Post attributes OK", true);
    }

    @Test
    public void forumTest() {
        testIdUniqueness(dataDir + "/dynamic/forum_0_0.csv", 1);
        testStringLength(dataDir + "/dynamic/forum_0_0.csv", 2, 256);
        assertTrue("Forum attributes OK", true);
    }

    @Test
    public void commentTest() {
        testIdUniqueness(dataDir + "/dynamic/comment_0_0.csv", 1);
        testLongBetween(dataDir + "/dynamic/comment_0_0.csv", 5, 0, 2001);
        testStringLength(dataDir + "/dynamic/comment_0_0.csv", 2, 40);
        testStringLength(dataDir + "/dynamic/comment_0_0.csv", 3, 40);
        assertTrue("Everything ok", true);
    }

    @Test
    public void organisationTest() {
        testIdUniqueness(dataDir + "/static/organisation_0_0.csv", 0);
        testStringLength(dataDir + "/static/organisation_0_0.csv", 2, 256);
        assertTrue("Everything ok", true);
    }

    @Test
    public void placeTest() {
        testIdUniqueness(dataDir + "/static/place_0_0.csv", 0);
        testStringLength(dataDir + "/static/place_0_0.csv", 1, 256);
        assertTrue("Everything ok", true);
    }

    @Test
    public void tagTest() {
        testIdUniqueness(dataDir + "/static/tag_0_0.csv", 0);
        testStringLength(dataDir + "/static/tag_0_0.csv", 1, 256);
        assertTrue("Everything ok", true);
    }

    @Test
    public void tagclassTest() {
        testIdUniqueness(dataDir + "/static/tagclass_0_0.csv", 0);
        testStringLength(dataDir + "/static/tagclass_0_0.csv", 1, 256);
        assertTrue("Everything ok", true);
    }

    @Test
    public void personKnowsPersonTest() {
        testPairUniquenessPlusExistence(
                dataDir + "/dynamic/person_knows_person_0_0.csv", 1, 2,
                dataDir + "/dynamic/person_0_0.csv", 1); // TODO: double check
        assertTrue("Everything ok", true);
    }
    @Test
    public void organisationIsLocatedInPlaceTest() {
        testPairUniquenessPlusExistence(dataDir + "/static/organisation_isLocatedIn_place_0_0.csv", 0, 1, dataDir + "/static/organisation_0_0.csv", 0, dataDir + "/static/place_0_0.csv", 0);
        assertTrue("Everything ok", true);
    }

    @Test
    public void placeIsPartOfPlaceTest() {
        testPairUniquenessPlusExistence(dataDir + "/static/place_isPartOf_place_0_0.csv", 0, 1, dataDir + "/static/place_0_0.csv", 0);
        assertTrue("Everything ok", true);
    }

    @Test
    public void tagClassIsSubclassOfTest() {
        testPairUniquenessPlusExistence(dataDir + "/static/tagclass_isSubclassOf_tagclass_0_0.csv", 0, 1, dataDir + "/static/tagclass_0_0.csv", 0);
        assertTrue("Everything ok", true);
    }

    @Test
    public void tagHasTypeTagclassCheck() {
        testPairUniquenessPlusExistence(dataDir + "/static/tag_hasType_tagclass_0_0.csv", 0, 1, dataDir + "/static/tag_0_0.csv", 0, dataDir + "/static/tagclass_0_0.csv", 0);
        assertTrue("Everything ok", true);
    }

    @Test
    public void personStudyAtOrganisationCheck() {
        testPairUniquenessPlusExistence(
                dataDir + "/dynamic/person_studyAt_organisation_0_0.csv", 1, 2,
                dataDir + "/dynamic/person_0_0.csv", 1,
                dataDir + "/static/organisation_0_0.csv", 0);
        assertTrue("Everything ok", true);
    }

    @Test
    public void personWorkAtOrganisationCheck() {
        testPairUniquenessPlusExistence(
                dataDir + "/dynamic/person_workAt_organisation_0_0.csv", 1, 2,
                dataDir + "/dynamic/person_0_0.csv", 1,
                dataDir + "/static/organisation_0_0.csv", 0);
        assertTrue("Everything ok", true);
    }

    @Test
    public void personHasInterestTagCheck() {
        testPairUniquenessPlusExistence(dataDir + "/dynamic/person_hasInterest_tag_0_0.csv", 1, 2,
                dataDir + "/dynamic/person_0_0.csv", 1,
                dataDir + "/static/tag_0_0.csv", 0);
        assertTrue("Everything ok", true);
    }

    @Test
    public void personIsLocatedInPlaceCheck() {
        testPairUniquenessPlusExistence(
                dataDir + "/dynamic/person_isLocatedIn_place_0_0.csv", 1, 2,
                dataDir + "/dynamic/person_0_0.csv", 1,
                dataDir + "/static/place_0_0.csv", 0);
        assertTrue("Everything ok", true);
    }

    @Test
    public void forumHasTagCheck() {
        testPairUniquenessPlusExistence(
                dataDir + "/dynamic/forum_hasTag_tag_0_0.csv", 1, 2,
                dataDir + "/dynamic/forum_0_0.csv", 1,
                dataDir + "/static/tag_0_0.csv", 0);
        assertTrue("Everything ok", true);
    }

//    @Test
//    public void forumHasModeratorPersonCheck() {
//        testPairUniquenessPlusExistence(
//                dataDir + "/dynamic/forum_hasModerator_person_0_0.csv", 1, 2,
//                dataDir + "/dynamic/forum_0_0.csv", 1,
//                dataDir + "/dynamic/person_0_0.csv", 1);
//        assertTrue("Everything ok", true);
//    }

    @Test
    public void forumHasMemberPersonCheck() {
        testPairUniquenessPlusExistence(
                dataDir + "/dynamic/forum_hasMember_person_0_0.csv", 1, 2,
                dataDir + "/dynamic/forum_0_0.csv", 1,
                dataDir + "/dynamic/person_0_0.csv", 1);
        assertTrue("Everything ok", true);
    }

    @Test
    public void forumContainerOfPostCheck() {
        testPairUniquenessPlusExistence(
                dataDir + "/dynamic/forum_containerOf_post_0_0.csv", 1, 2,
                dataDir + "/dynamic/forum_0_0.csv", 1,
                dataDir + "/dynamic/post_0_0.csv", 1);
        assertTrue("Everything ok", true);
    }

    @Test
    public void commentHasCreatorPersonCheck() {
        testPairUniquenessPlusExistence(
                dataDir + "/dynamic/comment_hasCreator_person_0_0.csv", 1, 2,
                dataDir + "/dynamic/comment_0_0.csv", 1,
                dataDir + "/dynamic/person_0_0.csv", 1);
        assertTrue("Everything ok", true);
    }

    @Test
    public void commentHasTagTagCheck() {
        testPairUniquenessPlusExistence(
                dataDir + "/dynamic/comment_hasTag_tag_0_0.csv", 1, 2,
                dataDir + "/dynamic/comment_0_0.csv", 1,
                dataDir + "/static/tag_0_0.csv", 0);
        assertTrue("Everything ok", true);
    }

    @Test
    public void commentIsLocatedInPlaceCheck() {
        testPairUniquenessPlusExistence(
                dataDir + "/dynamic/comment_isLocatedIn_place_0_0.csv", 1, 2,
                dataDir + "/dynamic/comment_0_0.csv", 1,
                dataDir + "/static/place_0_0.csv", 0);
        assertTrue("Everything ok", true);
    }

    @Test
    public void commentReplyOfCommentCheck() {
        testPairUniquenessPlusExistence(
                dataDir + "/dynamic/comment_replyOf_comment_0_0.csv", 1, 2,
                dataDir + "/dynamic/comment_0_0.csv", 1,
                dataDir + "/dynamic/comment_0_0.csv", 1);
        assertTrue("Everything ok", true);
    }

    @Test
    public void commentReplyOfPostCheck() {
        testPairUniquenessPlusExistence(
                dataDir + "/dynamic/comment_replyOf_post_0_0.csv", 1, 2,
                dataDir + "/dynamic/comment_0_0.csv", 1,
                dataDir + "/dynamic/post_0_0.csv", 1);
        assertTrue("Everything ok", true);
    }

    @Test
    public void postHasCreatorPersonCheck() {
        testPairUniquenessPlusExistence(
                dataDir + "/dynamic/post_hasCreator_person_0_0.csv", 1, 2,
                dataDir + "/dynamic/post_0_0.csv", 1,
                dataDir + "/dynamic/person_0_0.csv", 1);
        assertTrue("Everything ok", true);
    }

    @Test
    public void postIsLocatedInPlaceCheck() {
        testPairUniquenessPlusExistence(
                dataDir + "/dynamic/post_isLocatedIn_place_0_0.csv", 1, 2,
                dataDir + "/dynamic/post_0_0.csv", 1,
                dataDir + "/static/place_0_0.csv", 0);
        assertTrue("Everything ok", true);
    }

    @Test
    public void personLikesCommentCheck() {
        testPairUniquenessPlusExistence(
                dataDir + "/dynamic/person_likes_comment_0_0.csv", 1, 2,
                dataDir + "/dynamic/person_0_0.csv", 1,
                dataDir + "/dynamic/comment_0_0.csv", 1);
        assertTrue("Everything ok", true);
    }

    @Test
    public void personLikesPostCheck() {
        testPairUniquenessPlusExistence(
                dataDir + "/dynamic/person_likes_post_0_0.csv", 1, 2,
                dataDir + "/dynamic/person_0_0.csv", 1,
                dataDir + "/dynamic/post_0_0.csv", 1);
        assertTrue("Everything ok", true);
    }

    @Test
    public void personEmailAddressCheck() {
        testIdExistence(
                dataDir + "/dynamic/person_0_0.csv", 1,
                dataDir + "/dynamic/person_email_emailaddress_0_0.csv", 1);
        testStringLength(dataDir + "/dynamic/person_email_emailaddress_0_0.csv", 1, 256);
        assertTrue("Everything ok", true);
    }

//    @Test
//    public void queryParamsTest() {
//        //Creating person id check
//        LongParser parser = new LongParser();
//        ColumnSet<Long> persons = new ColumnSet<>(parser, new File(dataDir + "/dynamic/person_0_0.csv"), 2, 1);
//        List<ColumnSet<Long>> personsRef = new ArrayList<>();
//        personsRef.add(persons);
//        List<Integer> personIndex = new ArrayList<>();
//        personIndex.add(0);
//        ExistsCheck<Long> existsPersonCheck = new ExistsCheck<>(parser, personIndex, personsRef);
//
//        //Creating name check
//        StringParser strParser = new StringParser();
//        ColumnSet<String> names = new ColumnSet<>(strParser, new File(dataDir + "/dynamic/person_0_0.csv"), 3, 1);
//        List<ColumnSet<String>> namesRef = new ArrayList<>();
//        namesRef.add(names);
//        List<Integer> namesIndex = new ArrayList<>();
//        namesIndex.add(1);
//        ExistsCheck<String> existsNameCheck = new ExistsCheck<>(strParser, namesIndex, namesRef);
//
//
//        String subParamsDir = "./test_data/substitution_parameters";
//        FileChecker fileChecker = new FileChecker(subParamsDir + "/interactive_1_param.txt");
//        fileChecker.addCheck(existsPersonCheck);
//        fileChecker.addCheck(existsNameCheck);
//        assertTrue("ERROR PASSING TEST QUERY 1 PERSON AND NAME EXISTS ", fileChecker.run(1));
//
//        //Creating date interval check
//        fileChecker = new FileChecker(subParamsDir + "/interactive_2_param.txt");
//        fileChecker.addCheck(existsPersonCheck);
//        assertTrue("ERROR PASSING TEST QUERY 2 PERSON EXISTS ", fileChecker.run(1));
//        testLongGE(subParamsDir + "/interactive_2_param.txt", 1, Dictionaries.dates.getSimulationStart());
//
//        //Creating country check
//        ColumnSet<String> places = new ColumnSet<>(strParser, new File(dataDir + "/static/place_0_0.csv"), 1, 1);
//        List<ColumnSet<String>> placesRef = new ArrayList<>();
//        placesRef.add(places);
//        List<Integer> countriesIndex = new ArrayList<>();
//        countriesIndex.add(3);
//        countriesIndex.add(4);
//        ExistsCheck<String> countryExists = new ExistsCheck<>(strParser, countriesIndex, placesRef);
//
//        //Date duration check
//        //DateDurationCheck dateDurationCheck = new DateDurationCheck("Date duration check",1,2,Dictionaries.dates
//        //       .getStartDateTime(), Dictionaries.dates.getEndDateTime());
//
//        fileChecker = new FileChecker(subParamsDir + "/interactive_3_param.txt");
//        fileChecker.addCheck(existsPersonCheck);
//        fileChecker.addCheck(countryExists);
//        assertTrue("ERROR PASSING TEST QUERY 3 PERSON EXISTS ", fileChecker.run(1));
//        testLongGE(subParamsDir + "/interactive_3_param.txt", 1, Dictionaries.dates.getSimulationStart());
//
//        fileChecker = new FileChecker(subParamsDir + "/interactive_4_param.txt");
//        fileChecker.addCheck(existsPersonCheck);
//        assertTrue("ERROR PASSING TEST QUERY 4 PERSON EXISTS ", fileChecker.run(1));
//        testLongGE(subParamsDir + "/interactive_4_param.txt", 1, Dictionaries.dates.getSimulationStart());
//
//        fileChecker = new FileChecker(subParamsDir + "/interactive_5_param.txt");
//        fileChecker.addCheck(existsPersonCheck);
//        assertTrue("ERROR PASSING TEST QUERY 5 PERSON EXISTS ", fileChecker.run(1));
//        testLongGE(subParamsDir + "/interactive_5_param.txt", 1, Dictionaries.dates.getSimulationStart());
//
//        //Creating tag check
//        ColumnSet<String> tags = new ColumnSet<>(strParser, new File(dataDir + "/static/tag_0_0.csv"), 1, 1);
//        List<ColumnSet<String>> tagsRef = new ArrayList<>();
//        tagsRef.add(tags);
//        List<Integer> tagsIndex = new ArrayList<>();
//        tagsIndex.add(1);
//        ExistsCheck<String> tagExists = new ExistsCheck<>(strParser, tagsIndex, tagsRef);
//
//        fileChecker = new FileChecker(subParamsDir + "/interactive_6_param.txt");
//        fileChecker.addCheck(existsPersonCheck);
//        fileChecker.addCheck(tagExists);
//        assertTrue("ERROR PASSING TEST QUERY 6 PERSON EXISTS ", fileChecker.run(1));
//
//        fileChecker = new FileChecker(subParamsDir + "/interactive_7_param.txt");
//        fileChecker.addCheck(existsPersonCheck);
//        assertTrue("ERROR PASSING TEST QUERY 7 PERSON EXISTS ", fileChecker.run(1));
//
//        fileChecker = new FileChecker(subParamsDir + "/interactive_8_param.txt");
//        fileChecker.addCheck(existsPersonCheck);
//        assertTrue("ERROR PASSING TEST QUERY 8 PERSON EXISTS ", fileChecker.run(1));
//
//        fileChecker = new FileChecker(subParamsDir + "/interactive_9_param.txt");
//        fileChecker.addCheck(existsPersonCheck);
//        assertTrue("ERROR PASSING TEST QUERY 9 PERSON EXISTS ", fileChecker.run(1));
//        testLongGE(subParamsDir + "/interactive_9_param.txt", 1, Dictionaries.dates.getSimulationStart());
//
//        fileChecker = new FileChecker(subParamsDir + "/interactive_10_param.txt");
//        fileChecker.addCheck(existsPersonCheck);
//        assertTrue("ERROR PASSING TEST QUERY 10 PERSON EXISTS ", fileChecker.run(1));
//        testLongBetween(subParamsDir + "/interactive_10_param.txt", 1, 1, 13);
//
//        //Creating country check
//        countriesIndex.clear();
//        countriesIndex.add(1);
//        countryExists = new ExistsCheck<>(strParser, countriesIndex, placesRef);
//
//        fileChecker = new FileChecker(subParamsDir + "/interactive_11_param.txt");
//        fileChecker.addCheck(existsPersonCheck);
//        fileChecker.addCheck(countryExists);
//        assertTrue("ERROR PASSING TEST QUERY 11 PERSON EXISTS ", fileChecker.run(1));
//
//        //Creating tagClass check
//        ColumnSet<String> tagClass = new ColumnSet<>(strParser, new File(dataDir + "/static/tagclass_0_0.csv"), 1, 1);
//        List<ColumnSet<String>> tagClassRef = new ArrayList<>();
//        tagClassRef.add(tagClass);
//        List<Integer> tagClassIndex = new ArrayList<>();
//        tagClassIndex.add(1);
//        ExistsCheck<String> tagClassExists = new ExistsCheck<>(strParser, tagClassIndex, tagClassRef);
//
//        fileChecker = new FileChecker(subParamsDir + "/interactive_12_param.txt");
//        fileChecker.addCheck(existsPersonCheck);
//        fileChecker.addCheck(tagClassExists);
//        assertTrue("ERROR PASSING TEST QUERY 12 PERSON EXISTS ", fileChecker.run(1));
//
//        personIndex.add(1);
//        ExistsCheck<Long> exists2PersonCheck = new ExistsCheck<>(parser, personIndex, personsRef);
//
//        fileChecker = new FileChecker(subParamsDir + "/interactive_13_param.txt");
//        fileChecker.addCheck(exists2PersonCheck);
//        assertTrue("ERROR PASSING TEST QUERY 13 PERSON EXISTS ", fileChecker.run(1));
//
//        fileChecker = new FileChecker(subParamsDir + "/interactive_14_param.txt");
//        fileChecker.addCheck(exists2PersonCheck);
//        assertTrue("ERROR PASSING TEST QUERY 14 PERSON EXISTS ", fileChecker.run(1));
//
//    }

    public void testLongPair(String fileName, Integer columnA, Integer columnB, NumericPairCheck.NumericCheckType type, long offsetA, long offsetB) {
        FileChecker fileChecker = new FileChecker(fileName);
        LongParser parser = new LongParser();
        LongPairCheck check = new LongPairCheck(parser, " Long check ", columnA, columnB, type, offsetA, offsetB);
        fileChecker.addCheck(check);
        assertTrue("ERROR PASSING TEST LONG PAIR FOR FILE " + fileName, fileChecker.run(0));
    }

    public void testIdUniqueness(String fileName, int column) {
        FileChecker fileChecker = new FileChecker(fileName);
        UniquenessCheck check = new UniquenessCheck(column);
        fileChecker.addCheck(check);
        assertTrue("ERROR PASSING TEST ID UNIQUENESS FOR FILE " + fileName, fileChecker.run(1));
    }

    public void testLongGE(String fileName, int column, long a) {
        FileChecker fileChecker = new FileChecker(fileName);
        LongParser parser = new LongParser();
        LongCheck longcheck = new LongCheck(parser, "Date Test", column, NumericCheck.NumericCheckType.GE, a, 0L);
        fileChecker.addCheck(longcheck);
        assertTrue("ERROR PASSING GE TEST FOR FILE " + fileName + " column " + column + " greater or equal " + a, fileChecker.run(1));
    }

    public void testLongBetween(String fileName, int column, long a, long b) {
        FileChecker fileChecker = new FileChecker(fileName);
        LongParser parser = new LongParser();
        LongCheck longcheck = new LongCheck(parser, "Date Test", column, NumericCheck.NumericCheckType.BETWEEN, a, b);
        fileChecker.addCheck(longcheck);
        assertTrue("Error passing betweenness test for file " + fileName + " column " + column + " between " + a + " and " + b, fileChecker.run(1));
    }

    public void testPairUniquenessPlusExistence(
            String relationFileName, int columnA, int columnB,
            String entityFileNameA, int entityColumnA,
            String entityFileNameB, int entityColumnB) {

        LongParser parser = new LongParser();
        ColumnSet<Long> entitiesA = new ColumnSet<>(parser, new File(entityFileNameA), entityColumnA, 1);
        ColumnSet<Long> entitiesB = new ColumnSet<>(parser, new File(entityFileNameB), entityColumnB, 1);
        FileChecker fileChecker = new FileChecker(relationFileName);
        PairUniquenessCheck pairUniquenessCheck = new PairUniquenessCheck<>(parser, parser, columnA, columnB);
        fileChecker.addCheck(pairUniquenessCheck);
        List<ColumnSet<Long>> entityARefColumns = new ArrayList<>();
        entityARefColumns.add(entitiesA);
        List<ColumnSet<Long>> entityBRefColumns = new ArrayList<>();
        entityBRefColumns.add(entitiesB);
        List<Integer> organisationIndices = new ArrayList<>();
        organisationIndices.add(columnA);
        List<Integer> placeIndices = new ArrayList<>();
        placeIndices.add(columnB);
        ExistsCheck<Long> existsEntityACheck = new ExistsCheck<>(parser, organisationIndices, entityARefColumns);
        ExistsCheck<Long> existsEntityBCheck = new ExistsCheck<>(parser, placeIndices, entityBRefColumns);
        fileChecker.addCheck(existsEntityACheck);
        fileChecker.addCheck(existsEntityBCheck);
        assertTrue("Error passing uniqueness and existence test for file " + relationFileName, fileChecker.run(1));

    }

    public void testPairUniquenessPlusExistence(String relationFileName, int columnA, int columnB, String entityFileName, int entityColumn) {
        LongParser parser = new LongParser();
        ColumnSet<Long> entities = new ColumnSet<>(parser, new File(entityFileName), entityColumn, 1);
        FileChecker fileChecker = new FileChecker(relationFileName);
        PairUniquenessCheck pairUniquenessCheck = new PairUniquenessCheck<>(parser, parser, columnA, columnB);
        fileChecker.addCheck(pairUniquenessCheck);
        List<ColumnSet<Long>> refcolumns = new ArrayList<>();
        refcolumns.add(entities);
        List<Integer> columnIndices = new ArrayList<>();
        columnIndices.add(columnA);
        columnIndices.add(columnB);
        ExistsCheck existsCheck = new ExistsCheck<>(parser, columnIndices, refcolumns);
        fileChecker.addCheck(existsCheck);
        assertTrue("ERROR PASSING " + relationFileName + " TEST", fileChecker.run(1));
    }

    public void testIdExistence(String fileToCheckExistenceOf, int columnToCheckExistenceOf, String fileToCheckExistenceAgainst, int columnToCheckExistenceAgainst) {
        LongParser parser = new LongParser();
        ColumnSet<Long> checkAgainstEntities = new ColumnSet<>(parser, new File(fileToCheckExistenceAgainst), columnToCheckExistenceAgainst, 1);
        FileChecker fileChecker = new FileChecker(fileToCheckExistenceOf);
        List<ColumnSet<Long>> refcolumns = new ArrayList<>();
        refcolumns.add(checkAgainstEntities);
        List<Integer> columnIndices = new ArrayList<>();
        columnIndices.add(columnToCheckExistenceOf);
        ExistsCheck existsCheck = new ExistsCheck<>(parser, columnIndices, refcolumns);
        fileChecker.addCheck(existsCheck);
        assertTrue("ERROR PASSING " + fileToCheckExistenceOf + " ID Existence TEST", fileChecker.run(1));
    }

    public void testStringLength(String fileToCheckExistenceOf, int columnToCheckExistenceOf, int length) {
        FileChecker fileChecker = new FileChecker(fileToCheckExistenceOf);
        StringLengthCheck lengthCheck = new StringLengthCheck(columnToCheckExistenceOf, length);
        fileChecker.addCheck(lengthCheck);
        assertTrue("ERROR PASSING " + fileToCheckExistenceOf + " ID Existence TEST", fileChecker.run(1));
    }

}
