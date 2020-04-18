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
package ldbc.snb.datagen.hadoop;

import com.google.common.collect.Lists;
import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.hadoop.generator.HadoopKnowsGenerator;
import ldbc.snb.datagen.hadoop.generator.HadoopPersonActivityGenerator;
import ldbc.snb.datagen.hadoop.generator.HadoopPersonGenerator;
import ldbc.snb.datagen.hadoop.miscjob.HadoopMergeFriendshipFiles;
import ldbc.snb.datagen.hadoop.serializer.*;
import ldbc.snb.datagen.hadoop.sorting.HadoopCreationTimeSorter;
import ldbc.snb.datagen.hadoop.sorting.HadoopDeletionTimeSorter;
import ldbc.snb.datagen.hadoop.writer.HdfsCsvWriter;
import ldbc.snb.datagen.serializer.DynamicActivitySerializer;
import ldbc.snb.datagen.serializer.DynamicPersonSerializer;
import ldbc.snb.datagen.serializer.snb.csv.FileName;
import ldbc.snb.datagen.util.LdbcConfiguration;
import ldbc.snb.datagen.util.ConfigParser;
import ldbc.snb.datagen.vocabulary.SN;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.OutputStream;
import java.util.*;

import static ldbc.snb.datagen.DatagenMode.*;

public class LdbcDatagen {
    private static boolean initialized = false;

    public static synchronized void initializeContext(Configuration hadoopConf) {
        try {
            if (!initialized) {
                LdbcConfiguration conf = HadoopConfiguration.extractLdbcConfig(hadoopConf);
                DatagenParams.readConf(conf);
                Dictionaries.loadDictionaries(conf);
                SN.initialize();
                Person.personSimilarity = DatagenParams.getPersonSimularity();
                initialized = true;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private long personGenerateJob(String hadoopPrefix, Configuration conf) throws Exception {
        printProgress("Starting: Person generation");
        long startPerson = System.currentTimeMillis();
        new HadoopPersonGenerator(conf)
                .run(hadoopPrefix + "/persons",
                        "ldbc.snb.datagen.hadoop.miscjob.keychanger.UniversityKeySetter");
        return (System.currentTimeMillis() - startPerson);
    }

    private long knowsGenerateJob(String hadoopPrefix, Configuration conf, String message, String preKey, String postKey, int step_index, String generator, String outputFile) throws Exception {
        List<Float> percentages = new ArrayList<>(Arrays.asList(0.45f, 0.45f, 0.1f));
        printProgress(message);
        long startUniversity = System.currentTimeMillis();
        new HadoopKnowsGenerator(conf, preKey, postKey, percentages, step_index, generator)
                .run(hadoopPrefix + "/persons", hadoopPrefix + outputFile);
        return (System.currentTimeMillis() - startUniversity);
    }

    private long mergeKnows(String hadoopPrefix, Configuration conf) throws Exception {
        printProgress("Merging the different edge files");
        long startMerge = System.currentTimeMillis();
        HadoopMergeFriendshipFiles merger = new HadoopMergeFriendshipFiles(conf, "ldbc.snb.datagen.hadoop.miscjob.keychanger.RandomKeySetter");
        merger.run(hadoopPrefix + "/mergedPersons",
                new ArrayList<>(Arrays.asList(hadoopPrefix + "/universityEdges", hadoopPrefix + "/interestEdges", hadoopPrefix + "/randomEdges")));
        return (System.currentTimeMillis() - startMerge);
    }

    private long serializePersons(String hadoopPrefix, Configuration conf) throws Exception {
        printProgress("Serializing persons");
        long startPersonSerializing = System.currentTimeMillis();
        if (conf.getBoolean("ldbc.snb.datagen.serializer.persons.sort", true)) {
            // set true in config parser
            new HadoopPersonSortAndSerializer(conf).run(hadoopPrefix + "/mergedPersons");
        }
        // TODO: check if this is ever needed
//        else {
//            new HadoopPersonSerializer(conf).run(hadoopPrefix + "/mergedPersons");
//        }

        return (System.currentTimeMillis() - startPersonSerializing);
    }

    private void copyToLocal(FileSystem fs, String pathString) throws Exception {
        fs.copyToLocalFile(false, new Path(pathString), new Path("./"));
    }

    private long personActivityJob(String hadoopPrefix, Configuration conf, FileSystem fs) throws Exception {
        long startPersonActivity = System.currentTimeMillis();
        if (DatagenParams.getDatagenMode() != GRAPHALYTICS) {
            printProgress("Generating and serializing person activity");
            HadoopPersonActivityGenerator activityGenerator = new HadoopPersonActivityGenerator(conf);
            activityGenerator.run(hadoopPrefix + "/mergedPersons");
            for (int i = 0; i < HadoopConfiguration.getNumThreads(conf); ++i) {
                if (i < (int) Math.ceil(DatagenParams.numPersons / (double) DatagenParams.blockSize)) { // i<number of blocks
                    copyToLocal(fs, HadoopConfiguration.getHadoopDir(conf) + "/m" + i + "personFactors.txt");
                    copyToLocal(fs, HadoopConfiguration.getHadoopDir(conf) + "/m" + i + "activityFactors.txt");
                    copyToLocal(fs, HadoopConfiguration.getHadoopDir(conf) + "/m0friendList" + i + ".csv");
                }
            }
        }
        return (System.currentTimeMillis() - startPersonActivity);
    }

    private long serializeStaticGraph(Configuration conf) {
        printProgress("Serializing static graph ");
        long startInvariantSerializing = System.currentTimeMillis();
        HadoopStaticSerializer staticSerializer = new HadoopStaticSerializer(conf);
        staticSerializer.run();
        return (System.currentTimeMillis() - startInvariantSerializing);
    }

    private void generateBIParameters(Configuration conf) throws Exception {
        print("Generating BI Parameters");
        ProcessBuilder pb = new ProcessBuilder("python", "paramgenerator/generateparamsbi.py", "./", HadoopConfiguration.getOutputDir(conf) + "/substitution_parameters");
        pb.directory(new File("./"));
        File logBi = new File("parameters_bi.log");
        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(logBi));
        Process p = pb.start();
        p.waitFor();
        print("Finished BI Parameter Generation");
    }

    private void generateInteractiveParameters(Configuration conf) throws Exception {

        print("Running Parameter Generation");
        print("Generating Interactive Parameters");
        ProcessBuilder pb = new ProcessBuilder("mkdir", "-p", HadoopConfiguration.getOutputDir(conf) + "/substitution_parameters");
        pb.directory(new File("./"));
        Process p = pb.start();
        p.waitFor();

        pb = new ProcessBuilder("python", "paramgenerator/generateparams.py", "./", HadoopConfiguration.getOutputDir(conf) + "/substitution_parameters");
        pb.directory(new File("./"));
        File logInteractive = new File("parameters_interactive.log");
        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(logInteractive));
        p = pb.start();
        p.waitFor();
        print("Finished Interactive Parameter Generation");
    }

    public void runGenerateJob(Configuration conf) throws Exception {
        String hadoopPrefix = HadoopConfiguration.getHadoopDir(conf);
        FileSystem fs = FileSystem.get(conf);
        long start = System.currentTimeMillis();

        //create all people in the graph
        long personGenTime = personGenerateJob(hadoopPrefix, conf);
        //generate friendships based on going to the same uni
        long uniKnowsGenTime = knowsGenerateJob(hadoopPrefix, conf,
                "Creating university location correlated edges",
                "ldbc.snb.datagen.hadoop.miscjob.keychanger.UniversityKeySetter",
                "ldbc.snb.datagen.hadoop.miscjob.keychanger.RandomKeySetter",
                0,
                DatagenParams.getKnowsGenerator(),
                "/universityEdges");
        //generate second set of friendships based on similar interests
        long interestKnowsGenTime = knowsGenerateJob(hadoopPrefix, conf,
                "Creating main interest correlated edges",
                "ldbc.snb.datagen.hadoop.miscjob.keychanger.InterestKeySetter",
                "ldbc.snb.datagen.hadoop.miscjob.keychanger.RandomKeySetter",
                1,
                DatagenParams.getKnowsGenerator(),
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
        fs.delete(new Path(HadoopConfiguration.getHadoopDir(conf) + "/persons"), true);
        //merge all friendship edges into a single file
        long mergeKnowsTime = mergeKnows(hadoopPrefix, conf);
        //serialize persons
        long personSerializeTime = serializePersons(hadoopPrefix, conf);
        //generate person activities
        long personActivityTime = personActivityJob(hadoopPrefix, conf, fs);
        //serialize static graph
        long serializeStaticTime = serializeStaticGraph(conf);
        // interative generate update stream
        long serializeUpdatesTime = 0;
        if (DatagenParams.getDatagenMode() == INTERACTIVE) {
            printProgress("Serializing update streams and generating parameters");
            serializeUpdatesTime = runSortInsertStream(conf);
            serializeUpdatesTime = serializeUpdatesTime + runSortDeleteStream(conf);
            generateInteractiveParameters(conf);
        }

        // [JACK] ni this sort should merge all insert/delete streams, sort by event time then divide into a specified
        // number of refresh data sets. Also some redundant sorting here I think.
        if (DatagenParams.getDatagenMode() == BI) {
            printProgress("Serializing batches and generating parameters");
            runSortInsertStream(conf);
            runSortDeleteStream(conf);
            runBiSortJob(conf);
            generateBIParameters(conf);
        }

        //total time taken
        print("Person generation time: " + (personGenTime / 1000));
        print("University correlated edge generation time: " + (uniKnowsGenTime / 1000));
        print("Interest correlated edge generation time: " + (interestKnowsGenTime / 1000));
        print("Random correlated edge generation time: " + (randomKnowsGenTime / 1000));
        print("Edges merge time: " + (mergeKnowsTime / 1000));
        print("Person serialization time: " + (personSerializeTime / 1000));
        print("Person activity generation and serialization time: " + (personActivityTime / 1000));
        print("Static schema serialization time: " + (serializeStaticTime / 1000));
        print("Insert stream serialization time: " + (serializeUpdatesTime / 1000));

        print("Total Execution time: " + ((System.currentTimeMillis() - start) / 1000));

    }

    public void individualSortJob(String filename, Configuration conf) throws Exception {
        String creationPrefix = conf.get("ldbc.snb.datagen.serializer.socialNetworkDir") + "/sorted/creation";
        String deletionPrefix = conf.get("ldbc.snb.datagen.serializer.socialNetworkDir") + "/sorted/deletion";
        HadoopCreationTimeSorter creationSorter = new HadoopCreationTimeSorter();
        HadoopDeletionTimeSorter deletionSorter = new HadoopDeletionTimeSorter();
        long startSort = System.currentTimeMillis();
        printProgress("Starting: " + filename + " sorting");
        creationSorter.run("social_network/dynamic/", filename + "_[0-9]*_[0-9]*.csv", creationPrefix + "/" + filename);
        deletionSorter.run("social_network/dynamic/", filename + "_[0-9]*_[0-9]*.csv", deletionPrefix + "/" + filename);
        print(filename + " sorting time: " + ((System.currentTimeMillis() - startSort) / 1000));
    }

    public void runBiSortJob(Configuration conf) throws Exception {
        FileSystem.get(conf).mkdirs(new Path(conf.get("ldbc.snb.datagen.serializer.socialNetworkDir") + "/sorted"));
        FileSystem.get(conf).mkdirs(new Path(conf.get("ldbc.snb.datagen.serializer.socialNetworkDir") + "/sorted/creation"));
        FileSystem.get(conf).mkdirs(new Path(conf.get("ldbc.snb.datagen.serializer.socialNetworkDir") + "/sorted/deletion"));

        DynamicActivitySerializer<HdfsCsvWriter> dynamicActivitySerializer = HadoopConfiguration.getDynamicActivitySerializer(conf);
        DynamicPersonSerializer<HdfsCsvWriter> dynamicPersonSerializer = HadoopConfiguration.getDynamicPersonSerializer(conf);


        List<FileName> filenames = Lists.newArrayList();
        filenames.addAll(dynamicActivitySerializer.getFileNames());
        filenames.addAll(dynamicPersonSerializer.getFileNames());

        for (FileName f : filenames) {
            individualSortJob(f.toString(), conf);
        }
    }

    public long runSortInsertStream(Configuration conf) throws Exception {

        long startSortingInsertStreams = System.currentTimeMillis();

        List<String> personStreamsFileNames = new ArrayList<>();
        List<String> forumStreamsFileNames = new ArrayList<>();
        for (int i = 0; i < HadoopConfiguration.getNumThreads(conf); ++i) {
            int numPartitions = DatagenParams.numUpdateStreams;
            for (int j = 0; j < numPartitions; ++j) {
                personStreamsFileNames.add(HadoopConfiguration.getHadoopDir(conf) + "/temp_insertStream_person_" + i + "_" + j);
                forumStreamsFileNames.add(HadoopConfiguration.getHadoopDir(conf) + "/temp_insertStream_forum_" + i + "_" + j);
            }
        }

        HadoopInsertStreamSorterAndSerializer insertSorterAndSerializer = new HadoopInsertStreamSorterAndSerializer(conf);
        insertSorterAndSerializer.run(personStreamsFileNames, "person");
        insertSorterAndSerializer.run(forumStreamsFileNames, "forum");
        for (String file : personStreamsFileNames) {
            FileSystem.get(conf).delete(new Path(file), true);
        }

        for (String file : forumStreamsFileNames) {
            FileSystem.get(conf).delete(new Path(file), true);
        }

        long minDate = Long.MAX_VALUE;
        long maxDate = Long.MIN_VALUE;
        long count = 0;
        for (int i = 0; i < HadoopConfiguration.getNumThreads(conf); ++i) {
            Path propertiesFile = new Path(HadoopConfiguration.getHadoopDir(conf) + "/temp_insertStream_person_" + i + ".properties");
            FSDataInputStream file = FileSystem.get(conf).open(propertiesFile);
            Properties properties = new Properties();
            properties.load(file);
            long aux;
            aux = Long.parseLong(properties.getProperty("ldbc.snb.interactive.insert.min_write_event_start_time"));
            minDate = Math.min(aux, minDate);
            aux = Long.parseLong(properties.getProperty("ldbc.snb.interactive.insert.max_write_event_start_time"));
            maxDate = Math.max(aux, maxDate);
            aux = Long.parseLong(properties.getProperty("ldbc.snb.interactive.insert.num_events"));
            count += aux;
            file.close();
            FileSystem.get(conf).delete(propertiesFile, true);

            propertiesFile = new Path(HadoopConfiguration.getHadoopDir(conf) + "/temp_insertStream_forum_" + i + ".properties");
            file = FileSystem.get(conf).open(propertiesFile);
            properties = new Properties();
            properties.load(file);
            aux = Long.parseLong(properties.getProperty("ldbc.snb.interactive.insert.min_write_event_start_time"));
            minDate = Math.min(aux, minDate);
            aux = Long.parseLong(properties.getProperty("ldbc.snb.interactive.insert.max_write_event_start_time"));
            maxDate = Math.max(aux, maxDate);
            aux = Long.parseLong(properties.getProperty("ldbc.snb.interactive.insert.num_events"));
            count += aux;
            file.close();
            FileSystem.get(conf).delete(propertiesFile, true);

        }

        OutputStream output = FileSystem.get(conf)
                .create(new Path(HadoopConfiguration.getSocialNetworkDir(conf) + "/insertStream" + ".properties"), true);
        output.write(("ldbc.snb.interactive.insert.gct_delta_duration:" + DatagenParams.delta + "\n")
                .getBytes());
        output.write(("ldbc.snb.interactive.insert.min_write_event_start_time:" + minDate + "\n").getBytes());
        output.write(("ldbc.snb.interactive.insert.max_write_event_start_time:" + maxDate + "\n").getBytes());
        output.write(("ldbc.snb.interactive.insert_interleave:" + (maxDate - minDate) / count + "\n")
                .getBytes());
        output.write(("ldbc.snb.interactive.insert.num_events:" + count).getBytes());
        output.close();

        long endSortingInsertStreams = System.currentTimeMillis();

        return ((endSortingInsertStreams - startSortingInsertStreams) / 1000);
    }

    public long runSortDeleteStream(Configuration conf) throws Exception {

        printProgress("Sorting delete streams ");

        long startSortingDeleteStreams = System.currentTimeMillis();

        List<String> personStreamsFileNames = new ArrayList<>();
        List<String> forumStreamsFileNames = new ArrayList<>();
        for (int i = 0; i < HadoopConfiguration.getNumThreads(conf); ++i) {
            int numPartitions = DatagenParams.numUpdateStreams;
            for (int j = 0; j < numPartitions; ++j) {
                personStreamsFileNames.add(HadoopConfiguration.getHadoopDir(conf) + "/temp_deleteStream_person_" + i + "_" + j);
                forumStreamsFileNames.add(HadoopConfiguration.getHadoopDir(conf) + "/temp_deleteStream_forum_" + i + "_" + j);
            }
        }

        HadoopDeleteStreamSorterAndSerializer deleteSorterAndSerializer = new HadoopDeleteStreamSorterAndSerializer(conf);
        deleteSorterAndSerializer.run(personStreamsFileNames, "person");
        deleteSorterAndSerializer.run(forumStreamsFileNames, "forum");
        for (String file : personStreamsFileNames) {
            FileSystem.get(conf).delete(new Path(file), true);
        }

        for (String file : forumStreamsFileNames) {
            FileSystem.get(conf).delete(new Path(file), true);
        }

        long minDate = Long.MAX_VALUE;
        long maxDate = Long.MIN_VALUE;
        long count = 0;
        for (int i = 0; i < HadoopConfiguration.getNumThreads(conf); ++i) {
            Path propertiesFile = new Path(HadoopConfiguration.getHadoopDir(conf) + "/temp_deleteStream_person_" + i + ".properties");
            FSDataInputStream file = FileSystem.get(conf).open(propertiesFile);
            Properties properties = new Properties();
            properties.load(file);
            long aux;
            aux = Long.parseLong(properties.getProperty("ldbc.snb.interactive.delete.min_write_event_start_time"));
            minDate = Math.min(aux, minDate);
            aux = Long.parseLong(properties.getProperty("ldbc.snb.interactive.delete.max_write_event_start_time"));
            maxDate = Math.max(aux, maxDate);
            aux = Long.parseLong(properties.getProperty("ldbc.snb.interactive.delete.num_events"));
            count += aux;
            file.close();
            FileSystem.get(conf).delete(propertiesFile, true);

            propertiesFile = new Path(HadoopConfiguration.getHadoopDir(conf) + "/temp_deleteStream_forum_" + i + ".properties");
            file = FileSystem.get(conf).open(propertiesFile);
            properties = new Properties();
            properties.load(file);
            aux = Long.parseLong(properties.getProperty("ldbc.snb.interactive.delete.min_write_event_start_time"));
            minDate = Math.min(aux, minDate);
            aux = Long.parseLong(properties.getProperty("ldbc.snb.interactive.delete.max_write_event_start_time"));
            maxDate = Math.max(aux, maxDate);
            aux = Long.parseLong(properties.getProperty("ldbc.snb.interactive.delete.num_events"));
            count += aux;
            file.close();
            FileSystem.get(conf).delete(propertiesFile, true);

        }

        OutputStream output = FileSystem.get(conf)
                .create(new Path(HadoopConfiguration.getSocialNetworkDir(conf) + "/deleteStream" + ".properties"), true);
        output.write(("ldbc.snb.interactive.delete.gct_delta_duration:" + DatagenParams.delta + "\n")
                .getBytes());
        output.write(("ldbc.snb.interactive.delete.min_write_event_start_time:" + minDate + "\n").getBytes());
        output.write(("ldbc.snb.interactive.delete.max_write_event_start_time:" + maxDate + "\n").getBytes());
        output.write(("ldbc.snb.interactive.delete_interleave:" + (maxDate - minDate) / count + "\n")
                .getBytes());
        output.write(("ldbc.snb.interactive.delete.num_events:" + count).getBytes());
        output.close();

        long endSortingDeleteStreams = System.currentTimeMillis();

        return ((endSortingDeleteStreams - startSortingDeleteStreams) / 1000);
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> conf = ConfigParser.defaultConfiguration();

        conf.putAll(ConfigParser.readConfig(args[0]));
        conf.putAll(ConfigParser.readConfig(LdbcDatagen.class.getResourceAsStream("/params_default.ini")));

        Configuration hadoopConf = HadoopConfiguration.prepare(conf);
        System.out.println(HadoopConfiguration.getHadoopDir(hadoopConf));
        System.out.println(HadoopConfiguration.getSocialNetworkDir(hadoopConf));
        System.out.println(HadoopConfiguration.getOutputDir(hadoopConf));

        LdbcDatagen.initializeContext(hadoopConf);
        LdbcDatagen datagen = new LdbcDatagen();

        datagen.runGenerateJob(hadoopConf);

    }

    private static void print(String message) {
        System.out.println(message);
    }

    private void printProgress(String message) {
        print("*************************************************");
        print("* " + String.format("%1$-45s", message) + " *");
        print("*************************************************");
    }

}
