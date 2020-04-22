///*
// Copyright (c) 2013 LDBC
// Linked Data Benchmark Council (http://www.ldbcouncil.org)
//
// This file is part of ldbc_snb_datagen.
//
// ldbc_snb_datagen is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// ldbc_snb_datagen is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with ldbc_snb_datagen.  If not, see <http://www.gnu.org/licenses/>.
//
// Copyright (C) 2011 OpenLink Software <bdsmt@openlinksw.com>
// All Rights Reserved.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation;  only Version 2 of the License dated
// June 1991.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.*/
//package ldbc.snb.datagen.hadoop.serializer;
//
//import ldbc.snb.datagen.DatagenMode;
//import ldbc.snb.datagen.DatagenParams;
//import ldbc.snb.datagen.LdbcDatagen;
//import ldbc.snb.datagen.dictionary.Dictionaries;
//import ldbc.snb.datagen.entities.dynamic.person.Person;
//import ldbc.snb.datagen.entities.dynamic.relations.Knows;
//import ldbc.snb.datagen.hadoop.HadoopBlockMapper;
//import ldbc.snb.datagen.hadoop.HadoopTuplePartitioner;
//import ldbc.snb.datagen.hadoop.key.TupleKey;
//import ldbc.snb.datagen.hadoop.writer.HdfsCsvWriter;
//import ldbc.snb.datagen.serializer.DeleteEventSerializer;
//import ldbc.snb.datagen.serializer.DynamicPersonSerializer;
//import ldbc.snb.datagen.serializer.InsertEventSerializer;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
//
//import java.io.IOException;
//
//public class HadoopPersonSerializer {
//
//    private Configuration conf;
//
//    public static class HadoopDynamicPersonSerializerReducer extends Reducer<TupleKey, Person, LongWritable, Person> {
//
//        private DynamicPersonSerializer<HdfsCsvWriter> dynamicPersonSerializer;
//        private InsertEventSerializer insertEventSerializer;
//        private DeleteEventSerializer deleteEventSerializer;
//
//        @Override
//        protected void setup(Context context) {
//            Configuration conf = context.getConfiguration();
//            int reducerId = context.getTaskAttemptID().getTaskID().getId();
//            DatagenContext.initialize(conf);
//            try {
//                dynamicPersonSerializer = DatagenParams.getDynamicPersonSerializer();
//                dynamicPersonSerializer.initialize(conf, reducerId);
//                if (DatagenParams.getDatagenMode() == DatagenMode.INTERACTIVE || DatagenParams.getDatagenMode() == DatagenMode.BI) {
//                    insertEventSerializer = new InsertEventSerializer(conf, DatagenParams.hadoopDir + "/temp_insertStream_person_" + reducerId, reducerId, DatagenParams.numUpdatePartitions);
//                    deleteEventSerializer = new DeleteEventSerializer(conf, DatagenParams.hadoopDir + "/temp_deleteStream_person_" + reducerId, reducerId, DatagenParams.numUpdatePartitions);
//
//                }
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//        }
//
//        @Override
//        public void reduce(TupleKey key, Iterable<Person> valueSet, Context context) throws IOException {
//
//            for (Person p : valueSet) {
//
//                if ((p.getCreationDate() < Dictionaries.dates.getBulkLoadThreshold() &&
//                        p.getDeletionDate() >= Dictionaries.dates.getBulkLoadThreshold())
//                        || !DatagenParams.updateStreams) {
//                    dynamicPersonSerializer.export(p);
//                    if (p.getDeletionDate() != Dictionaries.dates.getNetworkCollapse()) {
//                        deleteEventSerializer.export(p);
//                        deleteEventSerializer.changePartition();
//                    }
//                } else if (p.getCreationDate() >= Dictionaries.dates.getBulkLoadThreshold()) {
//                    insertEventSerializer.export(p);
//                    insertEventSerializer.changePartition();
//                    if (p.getDeletionDate() != Dictionaries.dates.getNetworkCollapse()) {
//                        deleteEventSerializer.export(p);
//                        deleteEventSerializer.changePartition();
//                    }
//                }
//
//                //TODO: moved knows export here was split with HadoopPersonActivityGenerator
//                for (Knows k : p.getKnows()) {
//                    if ((p.getCreationDate() < Dictionaries.dates.getBulkLoadThreshold() &&
//                            p.getDeletionDate() >= Dictionaries.dates.getBulkLoadThreshold())
//                            || !DatagenParams.updateStreams) {
//                        dynamicPersonSerializer.export(p, k);
//                        if (p.getDeletionDate() != Dictionaries.dates.getNetworkCollapse()) {
//                            deleteEventSerializer.export(p, k);
//                            deleteEventSerializer.changePartition();
//                        }
//                    } else if (p.getCreationDate() >= Dictionaries.dates.getBulkLoadThreshold()) {
//                        insertEventSerializer.export(p,k);
//                        insertEventSerializer.changePartition();
//                        if (p.getDeletionDate() != Dictionaries.dates.getNetworkCollapse()) {
//                            deleteEventSerializer.export(p,k);
//                            deleteEventSerializer.changePartition();
//                        }
//                    }
//                }
//            }
//        }
//
//        @Override
//        protected void cleanup(Context context) {
//            dynamicPersonSerializer.close();
//            if (DatagenParams.updateStreams) {
//                try {
//                    insertEventSerializer.close();
//                    deleteEventSerializer.close();
//                } catch (IOException e) {
//                    throw new RuntimeException(e.getMessage());
//                }
//            }
//        }
//    }
//
//    public HadoopPersonSerializer(Configuration conf) {
//        this.conf = new Configuration(conf);
//    }
//
//    public void run(String inputFileName) throws Exception {
//        FileSystem fs = FileSystem.get(conf);
//
//        int numThreads = Integer.parseInt(conf.get("ldbc.snb.datagen.generator.numThreads"));
//        Job job = Job.getInstance(conf, "Person Serializer");
//        job.setMapOutputKeyClass(TupleKey.class);
//        job.setMapOutputValueClass(Person.class);
//        job.setOutputKeyClass(LongWritable.class);
//        job.setOutputValueClass(Person.class);
//        job.setJarByClass(HadoopBlockMapper.class);
//        job.setReducerClass(HadoopDynamicPersonSerializerReducer.class);
//        job.setNumReduceTasks(numThreads);
//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
//
//        job.setPartitionerClass(HadoopTuplePartitioner.class);
//
//        FileInputFormat.setInputPaths(job, new Path(inputFileName));
//        FileOutputFormat.setOutputPath(job, new Path(conf.get("ldbc.snb.datagen.serializer.hadoopDir") + "/aux"));
//        if (!job.waitForCompletion(true)) {
//            throw new IllegalStateException("HadoopPersonSerializer failed");
//        }
//
//        try {
//            fs.delete(new Path(conf.get("ldbc.snb.datagen.serializer.hadoopDir") + "/aux"), true);
//        } catch (IOException e) {
//            System.err.println(e.getMessage());
//        }
//    }
//}
