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
package ldbc.snb.datagen.hadoop.generator;

import ldbc.snb.datagen.DatagenMode;
import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.entities.dynamic.relations.Knows;
import ldbc.snb.datagen.generator.generators.GenActivity;
import ldbc.snb.datagen.generator.generators.PersonActivityGenerator;
import ldbc.snb.datagen.hadoop.HadoopBlockMapper;
import ldbc.snb.datagen.hadoop.HadoopBlockPartitioner;
import ldbc.snb.datagen.hadoop.HadoopConfiguration;
import ldbc.snb.datagen.hadoop.LdbcDatagen;
import ldbc.snb.datagen.hadoop.key.TupleKey;
import ldbc.snb.datagen.hadoop.key.blockkey.BlockKey;
import ldbc.snb.datagen.hadoop.key.blockkey.BlockKeyComparator;
import ldbc.snb.datagen.hadoop.key.blockkey.BlockKeyGroupComparator;
import ldbc.snb.datagen.hadoop.miscjob.HadoopFileRanker;
import ldbc.snb.datagen.hadoop.writer.HdfsCsvWriter;
import ldbc.snb.datagen.serializer.DeleteEventSerializer;
import ldbc.snb.datagen.serializer.DynamicActivitySerializer;
import ldbc.snb.datagen.serializer.InsertEventSerializer;
import ldbc.snb.datagen.serializer.PersonActivityExporter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class HadoopPersonActivityGenerator {

    private Configuration conf;

    public static class HadoopPersonActivityGeneratorReducer extends Reducer<BlockKey, Person, LongWritable, Person> {

        /**
         * The id of the reducer.
         **/
        private PersonActivityGenerator personActivityGenerator;
        private PersonActivityExporter personActivityExporter;
        private OutputStream personFactors;
        private OutputStream activityFactors;
        private OutputStream friends;

        protected void setup(Context context) {
            System.out.println("Setting up reducer for person activity generation");
            Configuration conf = context.getConfiguration();
            int reducerId = context.getTaskAttemptID().getTaskID().getId();
            LdbcDatagen.initializeContext(conf);
            try {
                DynamicActivitySerializer<HdfsCsvWriter> dynamicActivitySerializer = HadoopConfiguration.getDynamicActivitySerializer(conf);
                dynamicActivitySerializer.initialize(conf, reducerId);
                InsertEventSerializer insertEventSerializer = null;
                DeleteEventSerializer deleteEventSerializer = null;
                if (DatagenParams.getDatagenMode() != DatagenMode.RAW_DATA) {
                    insertEventSerializer = new InsertEventSerializer(conf, HadoopConfiguration.getHadoopDir(conf)  + "/temp_insertStream_forum_" + reducerId, reducerId, DatagenParams.numUpdateStreams);
                    deleteEventSerializer = new DeleteEventSerializer(conf, HadoopConfiguration.getHadoopDir(conf)  + "/temp_deleteStream_forum_" + reducerId, reducerId, DatagenParams.numUpdateStreams);
                }
                personActivityGenerator = new PersonActivityGenerator();
                personActivityExporter =
                        new PersonActivityExporter(dynamicActivitySerializer, insertEventSerializer, deleteEventSerializer);

                FileSystem fs = FileSystem.get(context.getConfiguration());
                personFactors = fs
                        .create(new Path(HadoopConfiguration.getHadoopDir(conf)  + "/" + "m" + reducerId + DatagenParams.PERSON_COUNTS_FILE));
                activityFactors = fs
                        .create(new Path(HadoopConfiguration.getHadoopDir(conf)  + "/" + "m" + reducerId + DatagenParams.ACTIVITY_FILE));
                friends = fs.create(new Path(HadoopConfiguration.getHadoopDir(conf)  + "/" + "m0friendList" + reducerId + ".csv"));

            } catch (Exception e) {
                System.err.println(e.getMessage());
                throw new RuntimeException(e);
            }
        }

        @Override
        public void reduce(BlockKey key, Iterable<Person> valueSet, Context context)
                throws IOException {
            System.out.println("Reducing block " + key.block);
            List<Person> persons = new ArrayList<>();
            for (Person p : valueSet) {
                persons.add(new Person(p));

                StringBuilder strbuf = new StringBuilder();
                strbuf.append(p.getAccountId());
                for (Knows k : p.getKnows()) {
                    strbuf.append(",");
                    strbuf.append(k.to().getAccountId());
//                        TODO: moved this to HadoopPersonSerializer/HadoopPersonSortAndSerializer
//                    if (k.getCreationDate() >= Dictionaries.dates.getBulkLoadThreshold() && DatagenParams.updateStreams) {
//                        insertEventSerializer.export(p, k);
//                        deleteEventSerializer.export(p, k);
//                    }
                }
//                if (DatagenParams.updateStreams) {
//                    insertEventSerializer.changePartition();
//                    deleteEventSerializer.changePartition();
//                }

                strbuf.append("\n");
                friends.write(strbuf.toString().getBytes(StandardCharsets.UTF_8));
            }
            System.out.println("Starting generation of block: " + key.block);
            final int[] counter = { 0 };
            final float[] personGenerationTime = { 0.0f };
            Stream<GenActivity> genActivities = personActivityGenerator.generateActivityForBlock((int) key.block, persons);

            genActivities.forEach(genActivity -> {
                long start = System.currentTimeMillis();
                personActivityExporter.export(genActivity);
                if (counter[0] % 1000 == 0) {
                    context.setStatus("Generating activity of person " + counter[0] + " of block" + key.block);
                    context.progress();
                }
                float time = (System.currentTimeMillis() - start) / 1000.0f;
                personGenerationTime[0] += time;
                counter[0]++;
            });

            System.out.println("Average person activity generation time " + personGenerationTime[0] / (float) persons.size());

            System.out.println("Writing person factors for block: " + key.block);
            personActivityGenerator.writePersonFactors(personFactors);
        }

        protected void cleanup(Context context) throws IOException {
            System.out.println("Cleaning up");
            personActivityGenerator.writeActivityFactors(activityFactors);
            activityFactors.close();
            personFactors.close();
            friends.close();
            personActivityExporter.close();
        }
    }

    public HadoopPersonActivityGenerator(Configuration conf) {
        this.conf = conf;
    }

    public void run(String inputFileName) throws AssertionError, Exception {

        FileSystem fs = FileSystem.get(conf);

        System.out.println("Ranking Persons");
        String rankedFileName = HadoopConfiguration.getHadoopDir(conf) + "/ranked";
        HadoopFileRanker hadoopFileRanker = new HadoopFileRanker(conf, TupleKey.class, Person.class, null);
        hadoopFileRanker.run(inputFileName, rankedFileName);

        System.out.println("Running activity generator");
        int numThreads = HadoopConfiguration.getNumThreads(conf);
        Job job = Job.getInstance(conf, "Person Activity Generator/Serializer");
        job.setMapOutputKeyClass(BlockKey.class);
        job.setMapOutputValueClass(Person.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Person.class);
        job.setJarByClass(HadoopBlockMapper.class);
        job.setMapperClass(HadoopBlockMapper.class);
        job.setReducerClass(HadoopPersonActivityGeneratorReducer.class);
        job.setNumReduceTasks(numThreads);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setSortComparatorClass(BlockKeyComparator.class);
        job.setGroupingComparatorClass(BlockKeyGroupComparator.class);
        job.setPartitionerClass(HadoopBlockPartitioner.class);

        // PROFILING OPTIONS //
        //job.setProfileEnabled(true);
        //job.setProfileParams("-agentlib:hprof=cpu=samples,heap=sites,depth=4,thread=y,format=b,file=%s");
        //job.setProfileTaskRange(true,"0-1");
        //job.setProfileTaskRange(false,"0-1");
        //

        FileInputFormat.setInputPaths(job, new Path(rankedFileName));
        FileOutputFormat.setOutputPath(job, new Path(HadoopConfiguration.getHadoopDir(conf) + "/aux"));
        long start = System.currentTimeMillis();
        if (!job.waitForCompletion(true)) {
            throw new IllegalStateException("HadoopPersonActivityGenerator failed");
        }
        System.out.println("Real time to generate activity: " + (System.currentTimeMillis() - start) / 1000.0f);

        try {
            fs.delete(new Path(rankedFileName), true);
            fs.delete(new Path(HadoopConfiguration.getHadoopDir(conf) + "/aux"), true);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            throw new RuntimeException(e);
        }
    }

}
