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

import ldbc.snb.datagen.hadoop.*;
import ldbc.snb.datagen.objects.Knows;
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
        FileSystem fs = FileSystem.get(conf);

        long start = System.currentTimeMillis();
        printProgress("Starting: Person generation");
        HadoopPersonGenerator personGenerator = new HadoopPersonGenerator( conf );
        personGenerator.run(personsFileName);


        printProgress("Changing Persons key to University location");
        String universityPersonsFileName = conf.get("hadoopDir") + "/university_users";
        HadoopFileKeyChanger keyChanger = new HadoopFileKeyChanger(conf, LongWritable.class,Person.class,"ldbc.snb.datagen.hadoop.UniversityKeySetter");
        keyChanger.run(personsFileName,universityPersonsFileName);
        fs.delete(new Path(personsFileName),true);

        printProgress("Ranking Persons by Key");
        String rankedPersonsFileName = conf.get("hadoopDir") + "/ranked_users";
        HadoopFileRanker hadoopFileRanker = new HadoopFileRanker( conf, LongWritable.class, Person.class );
        hadoopFileRanker.run(universityPersonsFileName,rankedPersonsFileName);
        fs.delete(new Path(universityPersonsFileName), true);

        printProgress("Creating university location correlated edges");
        HadoopKnowsGenerator knowsGenerator = new HadoopKnowsGenerator(conf);
        knowsGenerator.run(rankedPersonsFileName,personsFileName);
        fs.delete(new Path(rankedPersonsFileName), true);


/*        printProgress("Changing Persons key to Main interest");
        String interestPersonFileName = conf.get("hadoopDir") + "/interest_users";
        keyChanger = new HadoopFileKeyChanger(conf, LongWritable.class,Person.class,"ldbc.snb.datagen.hadoop.InterestKeySetter");
        keyChanger.run(universityPersonsFileName,interestPersonFileName);
        fs.delete(new Path(universityPersonsFileName),true);

        printProgress("Changing Persons key to Random id");
        String randomPersonFileName = conf.get("hadoopDir") + "/random_users";
        keyChanger = new HadoopFileKeyChanger(conf, LongWritable.class,Person.class,"ldbc.snb.datagen.hadoop.RandomKeySetter");
        keyChanger.run(interestPersonFileName,randomPersonFileName);
        fs.delete(new Path(interestPersonFileName),true);
        */


        printProgress("Serializing persons");
        HadoopPersonSerializer serializer = new HadoopPersonSerializer(conf);
        serializer.run(personsFileName);

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
