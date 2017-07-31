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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

/**
 * Created by aprat on 10/14/14.
 */
public class HadoopFileSorter {
    private Configuration conf;
    private Class<?> K;
    private Class<?> V;

    /**
     * @param conf The configuration object.
     * @param K    The Key class of the hadoop sequence file.
     * @param V    The Value class of the hadoop sequence file.
     */
    public HadoopFileSorter(Configuration conf, Class<?> K, Class<?> V) {
        this.conf = new Configuration(conf);
        this.K = K;
        this.V = V;
    }

    /**
     * Sorts a hadoop sequence file
     *
     * @param inputFileName  The name of the file to sort.
     * @param outputFileName The name of the sorted file.
     * @throws Exception
     */
    public void run(String inputFileName, String outputFileName) throws Exception {
        int numThreads = conf.getInt("ldbc.snb.datagen.generator.numThreads", 1);
        Job job = Job.getInstance(conf, "Sorting " + inputFileName);

        FileInputFormat.setInputPaths(job, new Path(inputFileName));
        FileOutputFormat.setOutputPath(job, new Path(outputFileName));

        job.setMapOutputKeyClass(K);
        job.setMapOutputValueClass(V);
        job.setOutputKeyClass(K);
        job.setOutputValueClass(V);
        job.setNumReduceTasks(numThreads);

        job.setJarByClass(V);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        InputSampler.Sampler sampler = new InputSampler.RandomSampler(0.1, 1000);
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path(inputFileName + "_partition.lst"));
        InputSampler.writePartitionFile(job, sampler);
        job.setPartitionerClass(TotalOrderPartitioner.class);
        if (!job.waitForCompletion(true)) {
            throw new Exception();
        }
    }
}
