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

import ldbc.snb.datagen.util.ConfigParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;


public class LDBCDatagen {

    private void printProgress(String message) {
        System.out.println("************************************************");
        System.out.println("* " + message + " *");
        System.out.println("************************************************");
    }

    public int runGenerateJob(Configuration conf) throws Exception {
        FileSystem fs = FileSystem.get(conf);
        String hadoopDir = new String(conf.get("outputDir") + "/hadoop");
        String socialNetDir = new String(conf.get("outputDir") + "/social_network");
        int numThreads = Integer.parseInt(conf.get("numThreads"));
        System.out.println("NUMBER OF THREADS " + numThreads);

        long start = System.currentTimeMillis();
        ScalableGenerator.init( conf );

        printProgress("Starting: Person generation and friendship generation 1");
        PersonGenerator personGenerator = new PersonGenerator();
        personGenerator.run(conf);

        printProgress("Starting: Friendship generation 2");
        FriendshipGenerator friendGenerator = new FriendshipGenerator();
        friendGenerator.run(conf,hadoopDir + "/sib",hadoopDir + "/sib2",1,2);
        fs.delete(new Path(hadoopDir + "/sib"), true);

        printProgress("Starting: Friendship generation 3");
        friendGenerator.run(conf,hadoopDir + "/sib2",hadoopDir + "/sib3",2,2);
        fs.delete(new Path(hadoopDir + "/sib2"), true);

        printProgress("Starting: Generating person activity");
        ActivityGenerator activityGenerator = new ActivityGenerator();
        activityGenerator.run(conf, hadoopDir + "/sib3","");
        fs.delete(new Path(hadoopDir + "/sib3"), true);

        printProgress("Starting: Sorting update streams");
        UpdateStreamSorter updateStreamSorter = new UpdateStreamSorter();
        updateStreamSorter.run( conf );

        long end = System.currentTimeMillis();
        System.out.println(((end - start) / 1000)
                + " total seconds");
        for (int i = 0; i < numThreads; ++i) {
            fs.copyToLocalFile(new Path(socialNetDir + "/m" + i + "factors.txt"), new Path("./"));
            fs.copyToLocalFile(new Path(socialNetDir + "/m0friendList" + i + ".csv"), new Path("./"));
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = ConfigParser.GetConfig(args[0]);
        ConfigParser.PringConfig(conf);

        // Deleting exisging files
        FileSystem dfs = FileSystem.get(conf);
        dfs.delete(new Path(conf.get("outputDir") + "/hadoop"), true);
        dfs.delete(new Path(conf.get("outputDir") + "/social_network"), true);

        // Create input text file in HDFS
        writeToOutputFile(conf.get("outputDir") + "/hadoop/mrInputFile", Integer.parseInt(conf.get("numThreads")), conf);
        LDBCDatagen datagen = new LDBCDatagen();
        datagen.runGenerateJob(conf);

    }

    public static void writeToOutputFile(String filename, int numMaps, Configuration conf) {
        try {
            FileSystem dfs = FileSystem.get(conf);
            OutputStream output = dfs.create(new Path(filename));
            for (int i = 0; i < numMaps; i++)
                output.write((new String(i + "\n").getBytes()));
            output.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
