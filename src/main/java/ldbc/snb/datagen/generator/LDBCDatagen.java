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

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.hadoop.*;
import ldbc.snb.datagen.util.ConfigParser;
import ldbc.snb.datagen.vocabulary.SN;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class LDBCDatagen {

	static boolean initialized = false;
	public static synchronized void init (Configuration conf) {
		if(!initialized) {
			DatagenParams.readConf(conf);
			Dictionaries.loadDictionaries();
			SN.initialize();
			initialized = true;
		}
	}

    private void printProgress(String message) {
        System.out.println("************************************************");
        System.out.println("* " + message + " *");
        System.out.println("************************************************");
    }

    public int runGenerateJob(Configuration conf) throws Exception {

        String personsFileName1 = conf.get("ldbc.snb.datagen.serializer.hadoopDir") + "/persons1";
        String personsFileName2 = conf.get("ldbc.snb.datagen.serializer.hadoopDir") + "/persons2";
        FileSystem fs = FileSystem.get(conf);

        long start = System.currentTimeMillis();
        printProgress("Starting: Person generation");
        long startPerson = System.currentTimeMillis();
        HadoopPersonGenerator personGenerator = new HadoopPersonGenerator( conf );
        personGenerator.run(personsFileName1);
        long endPerson = System.currentTimeMillis();


        printProgress("Creating university location correlated edges");
        long startUniversity = System.currentTimeMillis();
        HadoopKnowsGenerator knowsGenerator = new HadoopKnowsGenerator(conf,"ldbc.snb.datagen.hadoop.UniversityKeySetter", 0.45f);
        knowsGenerator.run(personsFileName1,personsFileName2);
        fs.delete(new Path(personsFileName1), true);
        long endUniversity = System.currentTimeMillis();

        printProgress("Creating main interest correlated edges");
        long startInterest= System.currentTimeMillis();
        knowsGenerator = new HadoopKnowsGenerator(conf,"ldbc.snb.datagen.hadoop.InterestKeySetter", 0.90f);
        knowsGenerator.run(personsFileName2,personsFileName1);
        fs.delete(new Path(personsFileName2), true);
        long endInterest = System.currentTimeMillis();

        printProgress("Creating random correlated edges");
        long startRandom= System.currentTimeMillis();
        knowsGenerator = new HadoopKnowsGenerator(conf,"ldbc.snb.datagen.hadoop.RandomKeySetter", 1.0f);
        knowsGenerator.run(personsFileName1,personsFileName2);
        fs.delete(new Path(personsFileName1), true);
        long endRandom= System.currentTimeMillis();

        printProgress("Serializing persons");
        long startPersonSerializing= System.currentTimeMillis();
        HadoopPersonSerializer serializer = new HadoopPersonSerializer(conf);
        serializer.run(personsFileName2);
        long endPersonSerializing= System.currentTimeMillis();

        long startPersonActivity= System.currentTimeMillis();
	if(conf.getBoolean("ldbc.snb.dagaten.generator.activity", true)) {
		printProgress("Generating and serializing person activity");
		HadoopPersonActivityGenerator activityGenerator = new HadoopPersonActivityGenerator(conf);
		activityGenerator.run(personsFileName2);
	}
        long endPersonActivity= System.currentTimeMillis();

	long startSortingUpdateStreams= System.currentTimeMillis();
	if(conf.getBoolean("ldbc.snb.datagen.serializer.updateStreams", false)) {
		printProgress("Sorting update streams ");
	}
	for( int i = 0; i < DatagenParams.numThreads; ++i) {
		int numPartitions = conf.getInt("ldbc.snb.datagen.serializer.numPartitions", 1);
		for( int j = 0; j < numPartitions; ++j ) {
			if(conf.getBoolean("ldbc.snb.datagen.serializer.updateStreams", false)) {
				HadoopFileSorter updateStreamSorter = new HadoopFileSorter(conf,LongWritable.class,Text.class);
				updateStreamSorter.run(DatagenParams.hadoopDir+"/temp_updateStream_person_"+i+"_"+j, DatagenParams.hadoopDir+"/updateStream_person_"+i+"_"+j);
				updateStreamSorter.run(DatagenParams.hadoopDir+"/temp_updateStream_forum_"+i+"_"+j, DatagenParams.hadoopDir+"/updateStream_forum_"+i+"_"+j);
			}

			//fs.delete(new Path(DatagenParams.hadoopDir+"/temp_updateStream_person_"+i+"_"+j), true);
			//fs.delete(new Path(DatagenParams.hadoopDir+"/temp_updateStream_forum_"+i+"_"+j), true);
			
			if(conf.getBoolean("ldbc.snb.datagen.serializer.updateStreams", false)) {
				HadoopUpdateStreamSerializer updateSerializer = new HadoopUpdateStreamSerializer(conf);
				updateSerializer.run(DatagenParams.hadoopDir+"/updateStream_person_"+i+"_"+j, i, j, "person");
				updateSerializer.run(DatagenParams.hadoopDir+"/updateStream_forum_"+i+"_"+j, i, j, "forum");
				
			//	fs.delete(new Path(DatagenParams.hadoopDir+"/updateStream_person_"+i+"_"+j), true);
			//	fs.delete(new Path(DatagenParams.hadoopDir+"/updateStream_forum_"+i+"_"+j), true);
			}
		}
	}
    long endSortingUpdateStreams= System.currentTimeMillis();

        printProgress("Serializing invariant schema ");
        long startInvariantSerializing= System.currentTimeMillis();
        HadoopInvariantSerializer invariantSerializer = new HadoopInvariantSerializer(conf);
        invariantSerializer.run();
        long endInvariantSerializing= System.currentTimeMillis();



        long end = System.currentTimeMillis();

	

        System.out.println(((end - start) / 1000)
                + " total seconds");
        System.out.println("Person generation time: "+((endPerson - startPerson) / 1000));
        System.out.println("University correlated edge generation time: "+((endUniversity - startUniversity) / 1000));
        System.out.println("Interest correlated edge generation time: "+((endInterest - startInterest) / 1000));
        System.out.println("Random correlated edge generation time: "+((endRandom - startRandom) / 1000));
        System.out.println("Person serialization time: "+((endPersonSerializing - startPersonSerializing) / 1000));
        System.out.println("Person activity generation and serialization time: "+((endPersonActivity - startPersonActivity) / 1000));
        System.out.println("Invariant schema serialization time: "+((endInvariantSerializing - startInvariantSerializing) / 1000));
        System.out.println("Total Execution time: "+((end - start) / 1000));
        return 0;
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = ConfigParser.initialize();
        ConfigParser.readConfig(conf,"./src/main/resources/params.ini");
        ConfigParser.readConfig(conf,args[0]);
        conf.set("ldbc.snb.datagen.serializer.hadoopDir",conf.get("ldbc.snb.datagen.serializer.outputDir")+"/hadoop");
        conf.set("ldbc.snb.datagen.serializer.socialNetworkDir",conf.get("ldbc.snb.datagen.serializer.outputDir")+"/social_network");
        ConfigParser.printConfig(conf);

        // Deleting exisging files
        FileSystem dfs = FileSystem.get(conf);
        dfs.delete(new Path(conf.get("ldbc.snb.datagen.serializer.hadoopDir")), true);
        dfs.delete(new Path(conf.get("ldbc.snb.datagen.serializer.socialNetworkDir")), true);

        // Create input text file in HDFS
        LDBCDatagen datagen = new LDBCDatagen();
	LDBCDatagen.init(conf);
        datagen.runGenerateJob(conf);
    }
}
