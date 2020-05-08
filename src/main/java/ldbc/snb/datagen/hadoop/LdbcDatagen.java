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
import ldbc.snb.datagen.DatagenContext;
import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.hadoop.generator.HadoopKnowsGenerator;
import ldbc.snb.datagen.hadoop.generator.HadoopPersonActivityGenerator;
import ldbc.snb.datagen.hadoop.generator.HadoopPersonGenerator;
import ldbc.snb.datagen.hadoop.miscjob.HadoopMergeFriendshipFiles;
import ldbc.snb.datagen.hadoop.serializer.HadoopDeleteStreamSorterAndSerializer;
import ldbc.snb.datagen.hadoop.serializer.HadoopInsertStreamSorterAndSerializer;
import ldbc.snb.datagen.hadoop.serializer.HadoopPersonSortAndSerializer;
import ldbc.snb.datagen.hadoop.serializer.HadoopStaticSerializer;
import ldbc.snb.datagen.hadoop.sorting.HadoopCreationTimeSorter;
import ldbc.snb.datagen.hadoop.sorting.HadoopDeletionTimeSorter;
import ldbc.snb.datagen.hadoop.writer.HdfsCsvWriter;
import ldbc.snb.datagen.serializer.DynamicActivitySerializer;
import ldbc.snb.datagen.serializer.DynamicPersonSerializer;
import ldbc.snb.datagen.serializer.snb.csv.FileName;
import ldbc.snb.datagen.util.ConfigParser;
import ldbc.snb.datagen.util.LdbcConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.OutputStream;
import java.util.*;

import static ldbc.snb.datagen.DatagenMode.*;

public class LdbcDatagen extends DatagenHadoopJob {

    public LdbcDatagen(LdbcConfiguration conf, Configuration hadoopConf) {
        super(conf, hadoopConf);
    }

    private long personGenerateJob(String hadoopPrefix) throws Exception {
        printProgress("Starting: Person generation");
        long startPerson = System.currentTimeMillis();
        new HadoopPersonGenerator(conf, hadoopConf)
                .run(hadoopPrefix + "/persons",
                        "ldbc.snb.datagen.hadoop.miscjob.keychanger.UniversityKeySetter");
        return (System.currentTimeMillis() - startPerson);
    }

    private long knowsGenerateJob(String hadoopPrefix, String message, String preKey, String postKey, int step_index, String generator, String outputFile) throws Exception {
        List<Float> percentages = new ArrayList<>(Arrays.asList(0.45f, 0.45f, 0.1f));
        printProgress(message);
        long startUniversity = System.currentTimeMillis();
        new HadoopKnowsGenerator(conf, hadoopConf, preKey, postKey, percentages, step_index, generator)
                .run(hadoopPrefix + "/persons", hadoopPrefix + outputFile);
        return (System.currentTimeMillis() - startUniversity);
    }

    private long mergeKnows(String hadoopPrefix) throws Exception {
        printProgress("Merging the different edge files");
        long startMerge = System.currentTimeMillis();
        HadoopMergeFriendshipFiles merger = new HadoopMergeFriendshipFiles(hadoopConf, "ldbc.snb.datagen.hadoop.miscjob.keychanger.RandomKeySetter");
        merger.run(hadoopPrefix + "/mergedPersons",
                new ArrayList<>(Arrays.asList(hadoopPrefix + "/universityEdges", hadoopPrefix + "/interestEdges", hadoopPrefix + "/randomEdges")));
        return (System.currentTimeMillis() - startMerge);
    }

    private long serializePersons(String hadoopPrefix) throws Exception {
        printProgress("Serializing persons");
        long startPersonSerializing = System.currentTimeMillis();
        if (hadoopConf.getBoolean("ldbc.snb.datagen.serializer.persons.sort", true)) {
            // set true in config parser
            new HadoopPersonSortAndSerializer(conf, hadoopConf).run(hadoopPrefix + "/mergedPersons");
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

    private long personActivityJob(String hadoopPrefix, FileSystem fs) throws Exception {
        long startPersonActivity = System.currentTimeMillis();
        printProgress("Generating and serializing person activity");
        HadoopPersonActivityGenerator activityGenerator = new HadoopPersonActivityGenerator(conf, hadoopConf);
        activityGenerator.run(hadoopPrefix + "/mergedPersons");
        for (int i = 0; i < HadoopConfiguration.getNumThreads(hadoopConf); ++i) {
            if (i < (int) Math.ceil(DatagenParams.numPersons / (double) DatagenParams.blockSize)) { // i<number of blocks
                copyToLocal(fs, conf.getBuildDir() + "/m" + i + "personFactors.txt");
                copyToLocal(fs, conf.getBuildDir() + "/m" + i + "activityFactors.txt");
                copyToLocal(fs, conf.getBuildDir() + "/m0friendList" + i + ".csv");
            }
        }
        return (System.currentTimeMillis() - startPersonActivity);
    }

    private long serializeStaticGraph() {
        printProgress("Serializing static graph ");
        long startInvariantSerializing = System.currentTimeMillis();
        HadoopStaticSerializer staticSerializer = new HadoopStaticSerializer(
                conf,
                hadoopConf,
                HadoopConfiguration.getNumThreads(hadoopConf)

        );
        staticSerializer.run();
        return (System.currentTimeMillis() - startInvariantSerializing);
    }

    private void generateBIParameters() throws Exception {
        print("Generating BI Parameters");
        ProcessBuilder pb = new ProcessBuilder("python", "paramgenerator/generateparamsbi.py", "./", conf.getBuildDir() + "/substitution_parameters");
        pb.directory(new File("./"));
        File logBi = new File("parameters_bi.log");
        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(logBi));
        Process p = pb.start();
        p.waitFor();
        print("Finished BI Parameter Generation");
    }

    private void generateInteractiveParameters() throws Exception {

        print("Running Parameter Generation");
        print("Generating Interactive Parameters");
        ProcessBuilder pb = new ProcessBuilder("mkdir", "-p", conf.getBuildDir() + "/substitution_parameters");
        pb.directory(new File("./"));
        Process p = pb.start();
        p.waitFor();

        pb = new ProcessBuilder("python", "paramgenerator/generateparams.py", "./", conf.getBuildDir() + "/substitution_parameters");
        pb.directory(new File("./"));
        File logInteractive = new File("parameters_interactive.log");
        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(logInteractive));
        p = pb.start();
        p.waitFor();
        print("Finished Interactive Parameter Generation");
    }

    public void runGenerateJob() throws Exception {
        String hadoopPrefix = conf.getBuildDir();
        FileSystem fs = FileSystem.get(hadoopConf);
        long start = System.currentTimeMillis();

        //create all people in the graph
        long personGenTime = personGenerateJob(hadoopPrefix);
        //generate friendships based on going to the same uni
        long uniKnowsGenTime = knowsGenerateJob(hadoopPrefix,
                "Creating university location correlated edges",
                "ldbc.snb.datagen.hadoop.miscjob.keychanger.UniversityKeySetter",
                "ldbc.snb.datagen.hadoop.miscjob.keychanger.RandomKeySetter",
                0,
                DatagenParams.getKnowsGenerator(),
                "/universityEdges");
        //generate second set of friendships based on similar interests
        long interestKnowsGenTime = knowsGenerateJob(hadoopPrefix,
                "Creating main interest correlated edges",
                "ldbc.snb.datagen.hadoop.miscjob.keychanger.InterestKeySetter",
                "ldbc.snb.datagen.hadoop.miscjob.keychanger.RandomKeySetter",
                1,
                DatagenParams.getKnowsGenerator(),
                "/interestEdges");
        //generate final set of friendships based on randomness
        long randomKnowsGenTime = knowsGenerateJob(hadoopPrefix,
                "Creating random correlated edges",
                "ldbc.snb.datagen.hadoop.miscjob.keychanger.RandomKeySetter",
                "ldbc.snb.datagen.hadoop.miscjob.keychanger.RandomKeySetter",
                2,
                "ldbc.snb.datagen.generator.generators.knowsgenerators.RandomKnowsGenerator",
                "/randomEdges");
        //delete old persons file as now merged persons is going to be used
        fs.delete(new Path(conf.getBuildDir() + "/persons"), true);
        //merge all friendship edges into a single file
        long mergeKnowsTime = mergeKnows(hadoopPrefix);
        //serialize persons
        long personSerializeTime = serializePersons(hadoopPrefix);
        //generate person activities
        long personActivityTime = personActivityJob(hadoopPrefix, fs);
        //serialize static graph
        long serializeStaticTime = serializeStaticGraph();
        // interative generate update stream
        long serializeUpdatesTime = 0;
        if (DatagenParams.getDatagenMode() == INTERACTIVE) {
            printProgress("Serializing update streams and generating parameters");
            serializeUpdatesTime = runSortInsertStream();
            serializeUpdatesTime = serializeUpdatesTime + runSortDeleteStream();
            generateInteractiveParameters();
        }

        // [JACK] ni this sort should merge all insert/delete streams, sort by event time then divide into a specified
        // number of refresh data sets. Also some redundant sorting here I think.
        if (DatagenParams.getDatagenMode() == BI) {
            printProgress("Serializing batches and generating parameters");
            runSortInsertStream();
            runSortDeleteStream();
            runBiSortJob();
            generateBIParameters();
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

    public void individualSortJob(String filename) throws Exception {
        String creationPrefix = hadoopConf.get("ldbc.snb.datagen.serializer.socialNetworkDir") + "/sorted/creation";
        String deletionPrefix = hadoopConf.get("ldbc.snb.datagen.serializer.socialNetworkDir") + "/sorted/deletion";
        HadoopCreationTimeSorter creationSorter = new HadoopCreationTimeSorter();
        HadoopDeletionTimeSorter deletionSorter = new HadoopDeletionTimeSorter();
        long startSort = System.currentTimeMillis();
        printProgress("Starting: " + filename + " sorting");
        creationSorter.run("social_network/dynamic/", filename + "_[0-9]*_[0-9]*.csv", creationPrefix + "/" + filename);
        deletionSorter.run("social_network/dynamic/", filename + "_[0-9]*_[0-9]*.csv", deletionPrefix + "/" + filename);
        print(filename + " sorting time: " + ((System.currentTimeMillis() - startSort) / 1000));
    }

    public void runBiSortJob() throws Exception {
        FileSystem.get(hadoopConf).mkdirs(new Path(hadoopConf.get("ldbc.snb.datagen.serializer.socialNetworkDir") + "/sorted"));
        FileSystem.get(hadoopConf).mkdirs(new Path(hadoopConf.get("ldbc.snb.datagen.serializer.socialNetworkDir") + "/sorted/creation"));
        FileSystem.get(hadoopConf).mkdirs(new Path(hadoopConf.get("ldbc.snb.datagen.serializer.socialNetworkDir") + "/sorted/deletion"));

        DynamicActivitySerializer<HdfsCsvWriter> dynamicActivitySerializer = conf.getDynamicActivitySerializer();
        DynamicPersonSerializer<HdfsCsvWriter> dynamicPersonSerializer = conf.getDynamicPersonSerializer();


        List<FileName> filenames = Lists.newArrayList();
        filenames.addAll(dynamicActivitySerializer.getFileNames());
        filenames.addAll(dynamicPersonSerializer.getFileNames());

        for (FileName f : filenames) {
            individualSortJob(f.toString());
        }
    }

    public long runSortInsertStream() throws Exception {

        long startSortingInsertStreams = System.currentTimeMillis();

        List<String> personStreamsFileNames = new ArrayList<>();
        List<String> forumStreamsFileNames = new ArrayList<>();
        for (int i = 0; i < HadoopConfiguration.getNumThreads(hadoopConf); ++i) {
            int numPartitions = DatagenParams.numUpdateStreams;
            for (int j = 0; j < numPartitions; ++j) {
                personStreamsFileNames.add(conf.getBuildDir() + "/temp_insertStream_person_" + i + "_" + j);
                forumStreamsFileNames.add(conf.getBuildDir() + "/temp_insertStream_forum_" + i + "_" + j);
            }
        }

        HadoopInsertStreamSorterAndSerializer insertSorterAndSerializer = new HadoopInsertStreamSorterAndSerializer(conf, hadoopConf);
        insertSorterAndSerializer.run(personStreamsFileNames, "person");
        insertSorterAndSerializer.run(forumStreamsFileNames, "forum");
        for (String file : personStreamsFileNames) {
            FileSystem.get(hadoopConf).delete(new Path(file), true);
        }

        for (String file : forumStreamsFileNames) {
            FileSystem.get(hadoopConf).delete(new Path(file), true);
        }

        long minDate = Long.MAX_VALUE;
        long maxDate = Long.MIN_VALUE;
        long count = 0;
        for (int i = 0; i < HadoopConfiguration.getNumThreads(hadoopConf); ++i) {
            Path propertiesFile = new Path(conf.getBuildDir() + "/temp_insertStream_person_" + i + ".properties");
            FSDataInputStream file = FileSystem.get(hadoopConf).open(propertiesFile);
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
            FileSystem.get(hadoopConf).delete(propertiesFile, true);

            propertiesFile = new Path(conf.getBuildDir() + "/temp_insertStream_forum_" + i + ".properties");
            file = FileSystem.get(hadoopConf).open(propertiesFile);
            properties = new Properties();
            properties.load(file);
            aux = Long.parseLong(properties.getProperty("ldbc.snb.interactive.insert.min_write_event_start_time"));
            minDate = Math.min(aux, minDate);
            aux = Long.parseLong(properties.getProperty("ldbc.snb.interactive.insert.max_write_event_start_time"));
            maxDate = Math.max(aux, maxDate);
            aux = Long.parseLong(properties.getProperty("ldbc.snb.interactive.insert.num_events"));
            count += aux;
            file.close();
            FileSystem.get(hadoopConf).delete(propertiesFile, true);

        }

        OutputStream output = FileSystem.get(hadoopConf)
                .create(new Path(conf.getBuildDir() + "/insertStream" + ".properties"), true);
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

    public long runSortDeleteStream() throws Exception {

        printProgress("Sorting delete streams ");

        long startSortingDeleteStreams = System.currentTimeMillis();

        List<String> personStreamsFileNames = new ArrayList<>();
        List<String> forumStreamsFileNames = new ArrayList<>();
        for (int i = 0; i < HadoopConfiguration.getNumThreads(hadoopConf); ++i) {
            int numPartitions = DatagenParams.numUpdateStreams;
            for (int j = 0; j < numPartitions; ++j) {
                personStreamsFileNames.add(conf.getBuildDir() + "/temp_deleteStream_person_" + i + "_" + j);
                forumStreamsFileNames.add(conf.getBuildDir() + "/temp_deleteStream_forum_" + i + "_" + j);
            }
        }

        HadoopDeleteStreamSorterAndSerializer deleteSorterAndSerializer = new HadoopDeleteStreamSorterAndSerializer(conf, hadoopConf);
        deleteSorterAndSerializer.run(personStreamsFileNames, "person");
        deleteSorterAndSerializer.run(forumStreamsFileNames, "forum");
        for (String file : personStreamsFileNames) {
            FileSystem.get(hadoopConf).delete(new Path(file), true);
        }

        for (String file : forumStreamsFileNames) {
            FileSystem.get(hadoopConf).delete(new Path(file), true);
        }

        long minDate = Long.MAX_VALUE;
        long maxDate = Long.MIN_VALUE;
        long count = 0;
        for (int i = 0; i < HadoopConfiguration.getNumThreads(hadoopConf); ++i) {
            Path propertiesFile = new Path(conf.getBuildDir() + "/temp_deleteStream_person_" + i + ".properties");
            FSDataInputStream file = FileSystem.get(hadoopConf).open(propertiesFile);
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
            FileSystem.get(hadoopConf).delete(propertiesFile, true);

            propertiesFile = new Path(conf.getBuildDir() + "/temp_deleteStream_forum_" + i + ".properties");
            file = FileSystem.get(hadoopConf).open(propertiesFile);
            properties = new Properties();
            properties.load(file);
            aux = Long.parseLong(properties.getProperty("ldbc.snb.interactive.delete.min_write_event_start_time"));
            minDate = Math.min(aux, minDate);
            aux = Long.parseLong(properties.getProperty("ldbc.snb.interactive.delete.max_write_event_start_time"));
            maxDate = Math.max(aux, maxDate);
            aux = Long.parseLong(properties.getProperty("ldbc.snb.interactive.delete.num_events"));
            count += aux;
            file.close();
            FileSystem.get(hadoopConf).delete(propertiesFile, true);

        }

        OutputStream output = FileSystem.get(hadoopConf)
                .create(new Path(conf.getSocialNetworkDir() + "/deleteStream" + ".properties"), true);
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
        Map<String, String> confMap = ConfigParser.defaultConfiguration();

        confMap.putAll(ConfigParser.readConfig(args[0]));
        confMap.putAll(ConfigParser.readConfig(LdbcDatagen.class.getResourceAsStream("/params_default.ini")));

        LdbcConfiguration conf = new LdbcConfiguration(confMap);

        Configuration hadoopConf = HadoopConfiguration.prepare(conf);

        System.out.println(conf.getBuildDir());
        System.out.println(conf.getSocialNetworkDir());
        System.out.println(conf.getOutputDir());

        DatagenContext.initialize(conf);
        LdbcDatagen datagen = new LdbcDatagen(conf, hadoopConf);

        datagen.runGenerateJob();
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
