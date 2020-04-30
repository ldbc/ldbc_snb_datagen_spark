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
package ldbc.snb.datagen.hadoop.miscjob;

import ldbc.snb.datagen.DatagenContext;
import ldbc.snb.datagen.hadoop.DatagenHadoopJob;
import ldbc.snb.datagen.hadoop.DatagenReducer;
import ldbc.snb.datagen.hadoop.HadoopConfiguration;
import ldbc.snb.datagen.hadoop.key.TupleKey;
import ldbc.snb.datagen.hadoop.key.blockkey.BlockKey;
import ldbc.snb.datagen.hadoop.key.blockkey.BlockKeyComparator;
import ldbc.snb.datagen.hadoop.miscjob.keychanger.HadoopFileKeyChanger;
import ldbc.snb.datagen.util.LdbcConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class HadoopFileRanker extends DatagenHadoopJob {
    private Class<?> K;
    private Class<?> V;
    private String keySetterName;

    /**
     * @param conf The configuration object.
     * @param K    The Key class of the hadoop sequence file.
     * @param V    The Value class of the hadoop sequence file.
     */
    public HadoopFileRanker(LdbcConfiguration conf, Configuration hadoopConf, Class<?> K, Class<?> V, String keySetter) {
        super(conf, hadoopConf);
        this.keySetterName = keySetter;
        this.K = K;
        this.V = V;
    }

    public static class HadoopFileRankerSortMapper<K, V> extends Mapper<K, V, K, V> {

        private HadoopFileKeyChanger.KeySetter<TupleKey> keySetter;

        @Override
        public void setup(Context context) {
            Configuration hadoopConf = context.getConfiguration();
            try {
                DatagenContext.initialize(HadoopConfiguration.extractLdbcConfig(hadoopConf));
                String className = context.getConfiguration().get("keySetterClassName");
                keySetter = (HadoopFileKeyChanger.KeySetter) Class.forName(className).newInstance();
            } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
                System.out.print(e.getMessage());
                throw new RuntimeException(e);
            }
        }

        @Override
        public void map(K key, V value,
                        Context context) throws IOException, InterruptedException {
            context.write((K) keySetter.getKey(value), value);
        }
    }

    public static class HadoopFileRankerSortReducer<K, V, T extends BlockKey> extends DatagenReducer<K, V, BlockKey, V> {

        private int reducerId;
        /**
         * The id of the reducer.
         **/
        private long counter = 0;

        /**
         * Counter of the number of elements received by this reducer.
         */

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            reducerId = context.getTaskAttemptID().getTaskID().getId();
        }

        @Override
        public void reduce(K key, Iterable<V> valueSet,
                           Context context) throws IOException, InterruptedException {

            for (V v : valueSet) {
                context.write(new BlockKey(reducerId, new TupleKey(counter++, 0)), v);
            }
        }

        @Override
        public void cleanup(Context context) {
            try {
                FileSystem fs = FileSystem.get(hadoopConf);
                DataOutputStream output = fs
                        .create(new Path(conf.getBuildDir() + "/rank_" + reducerId));
                output.writeLong(counter);
                output.close();
            } catch (IOException e) {
                System.err.println(e.getMessage());
            }
        }
    }

    public static class HadoopFileRankerPartitioner<V> extends Partitioner<BlockKey, V> {

        @Override
        public int getPartition(BlockKey key, V value,
                                int numReduceTasks) {
            return (int) (key.block % numReduceTasks);
        }
    }

    public static class HadoopFileRankerFinalReducer<BlockKey, V, T extends LongWritable> extends DatagenReducer<BlockKey, V, LongWritable, V> {

        private int reducerId;
        /**
         * The id of the reducer.
         **/
        private int numReduceTasks;
        /**
         * The number of reducer tasks.
         **/
        private long[] counters;
        /**
         * The number of elements processed by each reducer in the previous step.
         **/
        private int i = 0;

        /**
         * The number of elements read by this reducer.
         **/

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            reducerId = context.getTaskAttemptID().getTaskID().getId();
            numReduceTasks = context.getNumReduceTasks();
            counters = new long[numReduceTasks];
            DatagenContext.initialize(conf);
            try {
                FileSystem fs = FileSystem.get(hadoopConf);
                for (int i = 0; i < (numReduceTasks - 1); ++i) {
                    DataInputStream inputFile = fs
                            .open(new Path(conf.getBuildDir() + "/rank_" + i));
                    counters[i + 1] = inputFile.readLong();
                    inputFile.close();
                }
            } catch (IOException e) {
                System.err.println(e.getMessage());
                throw new RuntimeException(e);
            }

            counters[0] = 0;
            for (int i = 1; i < numReduceTasks; ++i) {
                counters[i] += counters[i - 1];
            }
        }

        @Override
        public void reduce(BlockKey key, Iterable<V> valueSet,
                           Context context) throws IOException, InterruptedException {

            for (V v : valueSet) {
                long rank = counters[reducerId] + i;
                context.write(new LongWritable(rank), v);
                i++;
            }
        }
    }


    /**
     * Sorts a hadoop sequence file
     *
     * @param inputFileName  The name of the file to sort.
     * @param outputFileName The name of the sorted file.
     * @throws Exception exception
     */
    public void run(String inputFileName, String outputFileName) throws Exception {
        int numThreads = HadoopConfiguration.getNumThreads(hadoopConf);

        if (keySetterName != null) {
            hadoopConf.set("keySetterClassName", keySetterName);
        }

        // First Job to sort the key-value pairs and to count the number of elements processed by each reducer.
        Job jobSort = Job.getInstance(hadoopConf, "Sorting " + inputFileName);

        FileInputFormat.setInputPaths(jobSort, new Path(inputFileName));
        FileOutputFormat
                .setOutputPath(jobSort, new Path(conf.getBuildDir() + "/rankIntermediate"));

        if (keySetterName != null) {
            jobSort.setMapperClass(HadoopFileRankerSortMapper.class);
        }
        jobSort.setMapOutputKeyClass(K);
        jobSort.setMapOutputValueClass(V);
        jobSort.setOutputKeyClass(BlockKey.class);
        jobSort.setOutputValueClass(V);
        jobSort.setNumReduceTasks(numThreads);
        jobSort.setReducerClass(HadoopFileRankerSortReducer.class);
        jobSort.setJarByClass(V);
        jobSort.setInputFormatClass(SequenceFileInputFormat.class);
        jobSort.setOutputFormatClass(SequenceFileOutputFormat.class);
        InputSampler.Sampler sampler = new InputSampler.RandomSampler(0.1, 1000);
        TotalOrderPartitioner.setPartitionFile(jobSort.getConfiguration(), new Path(inputFileName + "_partition.lst"));
        InputSampler.writePartitionFile(jobSort, sampler);
        jobSort.setPartitionerClass(TotalOrderPartitioner.class);
        if (!jobSort.waitForCompletion(true)) {
            throw new IllegalStateException("HadoopFileRanker / SortReducer failed");
        }

        // Second Job to assign the rank to each element.
        Job jobRank = Job.getInstance(hadoopConf, "Sorting " + inputFileName);
        FileInputFormat
                .setInputPaths(jobRank, new Path(conf.getBuildDir() + "/rankIntermediate"));
        FileOutputFormat.setOutputPath(jobRank, new Path(outputFileName));

        jobRank.setMapOutputKeyClass(BlockKey.class);
        jobRank.setMapOutputValueClass(V);
        jobRank.setOutputKeyClass(LongWritable.class);
        jobRank.setOutputValueClass(V);
        jobRank.setSortComparatorClass(BlockKeyComparator.class);
        jobRank.setNumReduceTasks(numThreads);
        jobRank.setReducerClass(HadoopFileRankerFinalReducer.class);
        jobRank.setJarByClass(V);
        jobRank.setInputFormatClass(SequenceFileInputFormat.class);
        jobRank.setOutputFormatClass(SequenceFileOutputFormat.class);
        jobRank.setPartitionerClass(HadoopFileRankerPartitioner.class);
        if (!jobRank.waitForCompletion(true)) {
            throw new IllegalStateException("HadoopFileRanker / FinelReducer failed");
        }

        try {
            FileSystem fs = FileSystem.get(hadoopConf);
            for (int i = 0; i < numThreads; ++i) {
                fs.delete(new Path(conf.getBuildDir() + "/rank_" + i), true);
            }
            fs.delete(new Path(conf.getBuildDir() + "/rankIntermediate"), true);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }
}
