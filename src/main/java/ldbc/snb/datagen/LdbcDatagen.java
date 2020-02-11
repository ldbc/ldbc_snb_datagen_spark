/* 
 Copyright (c) 2013 LDBC
 Linked Data Benchmark Council (http://www.ldbcouncil.org)
 
 This file is part of ldbc_snb_datagen.
 
 ldbc_snb_datagen is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.
 
 ldbc_snb_datagen is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with ldbc_snb_datagen.  If not, see <http://www.gnu.org/licenses/>.
 
 Copyright (C) 2011 OpenLink Software <bdsmt@openlinksw.com>
 All Rights Reserved.
 
 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation;  only Version 2 of the License dated
 June 1991.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.*/
package ldbc.snb.datagen;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.hadoop.generator.HadoopKnowsGenerator;
import ldbc.snb.datagen.hadoop.generator.HadoopPersonActivityGenerator;
import ldbc.snb.datagen.hadoop.generator.HadoopPersonGenerator;
import ldbc.snb.datagen.hadoop.miscjob.HadoopMergeFriendshipFiles;
import ldbc.snb.datagen.hadoop.serializer.HadoopPersonSerializer;
import ldbc.snb.datagen.hadoop.serializer.HadoopPersonSortAndSerializer;
import ldbc.snb.datagen.hadoop.serializer.HadoopStaticSerializer;
import ldbc.snb.datagen.hadoop.serializer.HadoopUpdateStreamSorterAndSerializer;
import ldbc.snb.datagen.hadoop.sorting.HadoopSorter;
import ldbc.snb.datagen.util.ConfigParser;
import ldbc.snb.datagen.vocabulary.SN;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LdbcDatagen {
    private static boolean initialized = false;

    public static void prepareConfiguration(Configuration conf) throws Exception {
        //set the temp directory to outputdir/hadoop and the final directory to outputdir/social_network
        conf.set("ldbc.snb.datagen.serializer.hadoopDir", conf.get("ldbc.snb.datagen.serializer.outputDir") + "/hadoop");
        conf.set("ldbc.snb.datagen.serializer.socialNetworkDir", conf.get("ldbc.snb.datagen.serializer.outputDir") + "/social_network");
        // Deleting existing files
        FileSystem dfs = FileSystem.get(conf);
        dfs.delete(new Path(conf.get("ldbc.snb.datagen.serializer.hadoopDir")), true);
        dfs.delete(new Path(conf.get("ldbc.snb.datagen.serializer.socialNetworkDir")), true);
        FileUtils.deleteDirectory(new File(conf.get("ldbc.snb.datagen.serializer.outputDir") + "/substitution_parameters"));
        ConfigParser.printConfig(conf);
    }

    public static synchronized void initializeContext(Configuration conf) {
        if (!initialized) {
            DatagenParams.readConf(conf);
            Dictionaries.loadDictionaries(conf);
            SN.initialize();
            try {
                Person.personSimilarity = (Person.PersonSimilarity) Class.forName(conf.get("ldbc.snb.datagen.generator.person.similarity")).newInstance();
            } catch (Exception e) {
                System.err.println("Error while loading person similarity class");
                System.err.println(e.getMessage());
            }
            initialized = true;
        }
    }

    private long personGenerateJob(String hadoopPrefix, Configuration conf) throws Exception {
        printProgress("Starting: Person generation");
        long startPerson = System.currentTimeMillis();
        new HadoopPersonGenerator(conf)
                .run(hadoopPrefix + "/persons",
                        "ldbc.snb.datagen.hadoop.miscjob.keychanger.UniversityKeySetter");
        return(System.currentTimeMillis()-startPerson);
    }

    private long knowsGenerateJob(String hadoopPrefix, Configuration conf,String message,String preKey,String postKey,int step_index,String generator,String outputFile) throws Exception {
        List<Float> percentages = new ArrayList<>(Arrays.asList(0.45f,0.45f,0.1f));
        printProgress(message);
        long startUniversity = System.currentTimeMillis();
        new HadoopKnowsGenerator(conf, preKey, postKey, percentages,step_index,generator)
                .run(hadoopPrefix + "/persons", hadoopPrefix + outputFile);
        return(System.currentTimeMillis()-startUniversity);
    }

    private long mergeKnows(String hadoopPrefix, Configuration conf) throws Exception{
        printProgress("Merging the different edge files");
        long startMerge = System.currentTimeMillis();
        HadoopMergeFriendshipFiles merger = new HadoopMergeFriendshipFiles(conf, "ldbc.snb.datagen.hadoop.miscjob.keychanger.RandomKeySetter");
        merger.run(hadoopPrefix + "/mergedPersons",
                new ArrayList<>(Arrays.asList(hadoopPrefix + "/universityEdges",hadoopPrefix + "/interestEdges",hadoopPrefix + "/randomEdges")));
        return (System.currentTimeMillis()-startMerge);
    }
    private long serializePersons(String hadoopPrefix, Configuration conf) throws Exception {
        printProgress("Serializing persons");
        long startPersonSerializing = System.currentTimeMillis();
        if (conf.getBoolean("ldbc.snb.datagen.serializer.persons.sort", true))
            new HadoopPersonSortAndSerializer(conf).run(hadoopPrefix + "/mergedPersons");
        else
            new HadoopPersonSerializer(conf).run(hadoopPrefix + "/mergedPersons");

        return(System.currentTimeMillis()-startPersonSerializing);
    }
    private void copyToLocal(FileSystem fs,String pathString) throws Exception {
        fs.copyToLocalFile(false, new Path(pathString), new Path("./"));
    }
    private long personActivityJob(String hadoopPrefix, Configuration conf,FileSystem fs) throws Exception {
        long startPersonActivity = System.currentTimeMillis();
        if (conf.getBoolean("ldbc.snb.datagen.generator.activity", true)) {
            printProgress("Generating and serializing person activity");
            HadoopPersonActivityGenerator activityGenerator = new HadoopPersonActivityGenerator(conf);
            activityGenerator.run(hadoopPrefix + "/mergedPersons");
            for (int i = 0; i < DatagenParams.numThreads; ++i) {
                if (i < (int) Math.ceil(DatagenParams.numPersons / (double) DatagenParams.blockSize)) { // i<number of blocks
                    copyToLocal(fs,DatagenParams.hadoopDir + "/m" + i + "personFactors.txt");
                    copyToLocal(fs,DatagenParams.hadoopDir + "/m" + i + "activityFactors.txt");
                    copyToLocal(fs,DatagenParams.hadoopDir + "/m0friendList" + i + ".csv");
                }
            }
        }
        return(System.currentTimeMillis()-startPersonActivity);
    }


    private long updateStreamHelper(FileSystem fs,int fileNum,long[] minMaxDate,String path) throws Exception{
        Path propertiesFile = new Path(DatagenParams.hadoopDir + path + fileNum + ".properties");
        FSDataInputStream file = fs.open(propertiesFile);
        Properties properties = new Properties();
        properties.load(file);
        long aux;
        aux = Long.parseLong(properties.getProperty("ldbc.snb.interactive.min_write_event_start_time"));
        minMaxDate[0]= aux < minMaxDate[0] ? aux : minMaxDate[0];
        aux = Long.parseLong(properties.getProperty("ldbc.snb.interactive.max_write_event_start_time"));
        minMaxDate[1] = aux > minMaxDate[1] ? aux : minMaxDate[1];
        aux = Long.parseLong(properties.getProperty("ldbc.snb.interactive.num_events"));
        file.close();
        fs.delete(propertiesFile, true);
        return(aux); //sending back aux to be added to count
    }
    private long updateStreamSort(Configuration conf,FileSystem fs) throws Exception {
        long startSortingUpdateStreams = System.currentTimeMillis();
        if (conf.getBoolean("ldbc.snb.datagen.serializer.updateStreams", false)) {
            printProgress("Sorting update streams ");
            List<String> personStreamsFileNames = new ArrayList<>();
            List<String> forumStreamsFileNames = new ArrayList<>();
            for (int i = 0; i < DatagenParams.numThreads; ++i) {
                int numPartitions = conf.getInt("ldbc.snb.datagen.serializer.numUpdatePartitions", 1);
                for (int j = 0; j < numPartitions; ++j) {
                    personStreamsFileNames.add(DatagenParams.hadoopDir + "/temp_updateStream_person_" + i + "_" + j);
                    if (conf.getBoolean("ldbc.snb.datagen.generator.activity", false)) {
                        forumStreamsFileNames.add(DatagenParams.hadoopDir + "/temp_updateStream_forum_" + i + "_" + j);
                    }
                }
            }
            HadoopUpdateStreamSorterAndSerializer updateSorterAndSerializer = new HadoopUpdateStreamSorterAndSerializer(conf);
            updateSorterAndSerializer.run(personStreamsFileNames, "person");
            updateSorterAndSerializer.run(forumStreamsFileNames, "forum");
            for (String file : personStreamsFileNames)
                fs.delete(new Path(file), true);
            for (String file : forumStreamsFileNames)
                fs.delete(new Path(file), true);

            long[] minMaxDate = new long[] {Long.MAX_VALUE,Long.MIN_VALUE};
            long count = 0;
            for (int i = 0; i < DatagenParams.numThreads; ++i) {
                count += updateStreamHelper(fs,i,minMaxDate,"/temp_updateStream_person_");
                if (conf.getBoolean("ldbc.snb.datagen.generator.activity", false))
                    count += updateStreamHelper(fs,i,minMaxDate,"/temp_updateStream_forum_");
            }

            OutputStream output = fs.create(new Path(DatagenParams.socialNetworkDir + "/updateStream" + ".properties"), true);
            output.write(("ldbc.snb.interactive.gct_delta_duration:" + DatagenParams.deltaTime + "\n").getBytes());
            output.write(("ldbc.snb.interactive.min_write_event_start_time:" + minMaxDate[0] + "\n").getBytes());
            output.write(("ldbc.snb.interactive.max_write_event_start_time:" + minMaxDate[1] + "\n").getBytes());
            output.write(("ldbc.snb.interactive.update_interleave:" + (minMaxDate[1] - minMaxDate[0]) / count + "\n").getBytes());
            output.write(("ldbc.snb.interactive.num_events:" + count).getBytes());
            output.close();
        }
        return (System.currentTimeMillis()-startSortingUpdateStreams);
    }

    private long serializeStaticGraph(Configuration conf) throws Exception {
        printProgress("Serializing static graph ");
        long startInvariantSerializing = System.currentTimeMillis();
        HadoopStaticSerializer staticSerializer = new HadoopStaticSerializer(conf);
        staticSerializer.run();
        return(System.currentTimeMillis()-startInvariantSerializing);
    }

    private void generateBIParameters(Configuration conf) throws Exception {
        print("Generating BI Parameters");
        ProcessBuilder pb = new ProcessBuilder(conf.get("ldbc.snb.datagen.parametergenerator.python"), "paramgenerator/generateparamsbi.py", "./", conf.get("ldbc.snb.datagen.serializer.outputDir") + "/substitution_parameters");
        pb.directory(new File("./"));
        File logBi = new File("parameters_bi.log");
        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(logBi));
        Process p = pb.start();
        p.waitFor();
        print("Finished BI Parameter Generation");
    }

    private void generateInteractiveParameters(Configuration conf) throws Exception{

        print("Running Parameter Generation");
        print("Generating Interactive Parameters");
        ProcessBuilder pb = new ProcessBuilder("mkdir", "-p", conf.get("ldbc.snb.datagen.serializer.outputDir") + "/substitution_parameters");
        pb.directory(new File("./"));
        Process p = pb.start();
        p.waitFor();

        pb = new ProcessBuilder(conf.get("ldbc.snb.datagen.parametergenerator.python"), "paramgenerator/generateparams.py", "./", conf.get("ldbc.snb.datagen.serializer.outputDir") + "/substitution_parameters");
        pb.directory(new File("./"));
        File logInteractive = new File("parameters_interactive.log");
        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(logInteractive));
        p = pb.start();
        p.waitFor();
        print("Finished Interactive Parameter Generation");
    }

    public int runGenerateJob(Configuration conf) throws Exception {
        String hadoopPrefix = conf.get("ldbc.snb.datagen.serializer.hadoopDir");
        FileSystem fs = FileSystem.get(conf);
        long start = System.currentTimeMillis();

        //create all people in the graph
        long personGenTime = personGenerateJob(hadoopPrefix,conf);
        //generate friendships based on going to the same uni
        long uniKnowsGenTime = knowsGenerateJob(hadoopPrefix, conf,
                "Creating university location correlated edges",
                "ldbc.snb.datagen.hadoop.miscjob.keychanger.UniversityKeySetter",
                "ldbc.snb.datagen.hadoop.miscjob.keychanger.RandomKeySetter",
                0,
                conf.get("ldbc.snb.datagen.generator.knowsGenerator"),
                "/universityEdges");
        //generate second set of friendships based on similar interests
        long interestKnowsGenTime = knowsGenerateJob(hadoopPrefix, conf,
                "Creating main interest correlated edges",
                "ldbc.snb.datagen.hadoop.miscjob.keychanger.InterestKeySetter",
                "ldbc.snb.datagen.hadoop.miscjob.keychanger.RandomKeySetter",
                1,
                conf.get("ldbc.snb.datagen.generator.knowsGenerator"),
                "/interestEdges");
        //generate final set of friendships based on randomness
        long randomKnowsGenTime = knowsGenerateJob(hadoopPrefix, conf,
                "Creating random correlated edges",
                "ldbc.snb.datagen.hadoop.miscjob.keychanger.RandomKeySetter",
                "ldbc.snb.datagen.hadoop.miscjob.keychanger.RandomKeySetter",
                2,
                "ldbc.snb.datagen.generator.generators.knowsgenerators.RandomKnowsGenerator",
                "/randomEdges");
        //delete old persons file as now merged persons is going to be used
        fs.delete(new Path(DatagenParams.hadoopDir + "/persons"), true);
        //merge all friendship edges into a single file
        long mergeKnowsTime = mergeKnows(hadoopPrefix,conf);
        //serialize persons
        long personSerializeTime = serializePersons(hadoopPrefix,conf);
        //generate person activities
        long personActivityTime = personActivityJob(hadoopPrefix,conf,fs);
        //sort update stream (last 10% of generated data)
        long updateSortingTime = updateStreamSort(conf,fs);
        //serialize static graph
        long serializeStaticTime = serializeStaticGraph(conf);
        //total time taken
        print("Person generation time: " + (personGenTime / 1000));
        print("University correlated edge generation time: " + (uniKnowsGenTime / 1000));
        print("Interest correlated edge generation time: " + (interestKnowsGenTime / 1000));
        print("Random correlated edge generation time: " + (randomKnowsGenTime / 1000));
        print("Edges merge time: " + (mergeKnowsTime / 1000));
        print("Person serialization time: " + (personSerializeTime / 1000));
        print("Person activity generation and serialization time: " + (personActivityTime / 1000));
        print("Sorting update streams time: " + (updateSortingTime / 1000));
        print("Invariant schema serialization time: " + (serializeStaticTime / 1000));
        print("Total Execution time: " + ((System.currentTimeMillis() - start) / 1000));

        //are we generating paramerters
        if (conf.getBoolean("ldbc.snb.datagen.parametergenerator.parameters", false) &&
                conf.getBoolean("ldbc.snb.datagen.generator.activity", false)) {
            generateInteractiveParameters(conf);
            generateBIParameters(conf);
        }

        return 0;
    }

    public void individualSortJob(String filename, Configuration conf) {
        try {
            String hadoopPrefix = conf.get("ldbc.snb.datagen.serializer.socialNetworkDir") + "/sorted";
            HadoopSorter hadoopSorter = new HadoopSorter();
            long startSort = System.currentTimeMillis();
            printProgress("Starting: " + filename + " Sorting");
            hadoopSorter.run("social_network/dynamic/", filename + "_[0-9]*_[0-9]*.csv", hadoopPrefix + "/" + filename);
            print(filename + " sorting time: " + ((System.currentTimeMillis() - startSort) / 1000));
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public int runSortJob(Configuration conf)  {
        try {
            FileSystem.get(conf).mkdirs(new Path(conf.get("ldbc.snb.datagen.serializer.socialNetworkDir") + "/sorted"));

            String[] fileNames = {"comment", "comment_hasCreator_person", "comment_hasTag_tag", "comment_isLocatedIn_place",
                    "comment_replyOf_comment", "comment_replyOf_post", "forum", "forum_containerOf_post", "forum_hasMember_person",
                    "forum_hasModerator_person", "forum_hasTag_tag", "person_email_emailaddress", "person", "person_hasInterest_tag",
                    "person_isLocatedIn_place", "person_knows_person", "person_likes_comment", "person_likes_post",
                    "person_speaks_language", "person_studyAt_organisation", "person_workAt_organisation", "post",
                    "post_hasCreator_person", "post_hasTag_tag", "post_isLocatedIn_place"};

            for (int i = 0; i < fileNames.length; i++) {
                individualSortJob(fileNames[i], conf);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return 0;
    }



    public static void main(String[] args) throws Exception {
        try {
            Configuration conf = ConfigParser.initialize();
            ConfigParser.readConfig(conf, args[0]);
            ConfigParser.readConfig(conf, LdbcDatagen.class.getResourceAsStream("/params_default.ini"));

            LdbcDatagen.prepareConfiguration(conf);
            LdbcDatagen.initializeContext(conf);
            LdbcDatagen datagen = new LdbcDatagen();
            datagen.runGenerateJob(conf);
            datagen.runSortJob(conf);
        } catch (Exception e) {
            System.err.println("Error during execution");
            System.err.println(e.getMessage());
            throw e;
        }
    }

    private static void print(String message) {System.out.println(message);}
    private void printProgress(String message) {
        print("************************************************");
        print("* " + String.format("%1$-44s", message)    + " *");
        print("************************************************");
    }

}
