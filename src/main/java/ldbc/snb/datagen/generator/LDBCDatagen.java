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

import ldbc.snb.datagen.hadoop.HadoopFileRanker;
import ldbc.snb.datagen.hadoop.HadoopFileSorter;
import ldbc.snb.datagen.hadoop.HadoopPersonGenerator;
import ldbc.snb.datagen.hadoop.HadoopPersonSerializer;
import ldbc.snb.datagen.objects.Person;
import ldbc.snb.datagen.util.ConfigParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

public class LDBCDatagen {

    private void printProgress(String message) {
        System.out.println("************************************************");
        System.out.println("* " + message + " *");
        System.out.println("************************************************");
    }

    public int runGenerateJob(Configuration conf) throws Exception {

        String personsFileName = conf.get("hadoopDir") + "/users";

        long start = System.currentTimeMillis();
        printProgress("Starting: Person generation");
        HadoopPersonGenerator personGenerator = new HadoopPersonGenerator( conf );
        personGenerator.run(personsFileName);

        printProgress("Ranking Persons by Key");
        String sortedPersonsFileName = conf.get("hadoopDir") + "/sorted_users";

//        HadoopFileSorter hadoopFileSorter = new HadoopFileSorter( conf, LongWritable.class, Person.class );
 //       hadoopFileSorter.run(personsFileName,sortedPersonsFileName);

        String rankedPersonsFileName = conf.get("hadoopDir") + "/ranked_users";
        HadoopFileRanker hadoopFileRanker = new HadoopFileRanker( conf, LongWritable.class, Person.class );
        hadoopFileRanker.run(personsFileName,rankedPersonsFileName);

        printProgress("Serializing persons");
        HadoopPersonSerializer serializer = new HadoopPersonSerializer(conf);
        serializer.run(rankedPersonsFileName);

        long end = System.currentTimeMillis();
        System.out.println(((end - start) / 1000)
                + " total seconds");
        return 0;
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = ConfigParser.initialize();
        ConfigParser.readConfig(conf,args[0]);
        ConfigParser.printConfig(conf);

        // Deleting exisging files
        FileSystem dfs = FileSystem.get(conf);
        dfs.delete(new Path(conf.get("hadoopDir")), true);
        dfs.delete(new Path(conf.get("socialNetworkDir")), true);

        // Create input text file in HDFS
        LDBCDatagen datagen = new LDBCDatagen();
        datagen.runGenerateJob(conf);
    }
}
