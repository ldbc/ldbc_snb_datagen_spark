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

import ldbc.snb.datagen.DatagenContext;
import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.generator.generators.knowsgenerators.KnowsGenerator;
import ldbc.snb.datagen.hadoop.HadoopBlockMapper;
import ldbc.snb.datagen.hadoop.HadoopBlockPartitioner;
import ldbc.snb.datagen.hadoop.HadoopConfiguration;
import ldbc.snb.datagen.hadoop.key.TupleKey;
import ldbc.snb.datagen.hadoop.key.blockkey.BlockKey;
import ldbc.snb.datagen.hadoop.key.blockkey.BlockKeyComparator;
import ldbc.snb.datagen.hadoop.key.blockkey.BlockKeyGroupComparator;
import ldbc.snb.datagen.hadoop.miscjob.HadoopFileRanker;
import ldbc.snb.datagen.hadoop.miscjob.keychanger.HadoopFileKeyChanger;
import ldbc.snb.datagen.util.LdbcConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HadoopKnowsGenerator {

    private Configuration conf;
    private String preKeySetterName;
    private String postKeySetterName;
    private String knowsGeneratorName;
    private List<Float> percentages;
    private int stepIndex;


    public static class HadoopKnowsGeneratorReducer extends Reducer<BlockKey, Person, TupleKey, Person> {

        private KnowsGenerator knowsGenerator;
        /**
         * The person serializer
         **/
        private Configuration hadoopConf;
        private HadoopFileKeyChanger.KeySetter<TupleKey> keySetter = null;
        private List<Float> percentages;
        private int step_index;
        private int numGeneratedEdges = 0;
        private Person.PersonSimilarity personSimilarity;

        protected void setup(Context context) {
            this.hadoopConf = context.getConfiguration();
            try {
                LdbcConfiguration conf = HadoopConfiguration.extractLdbcConfig(hadoopConf);
                DatagenContext.initialize(conf);
                this.knowsGenerator = (KnowsGenerator) Class.forName(hadoopConf.get("knowsGeneratorName")).newInstance();
                this.knowsGenerator.initialize(conf);
                personSimilarity = DatagenParams.getPersonSimularity();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
            this.percentages = new ArrayList<>();
            this.step_index = hadoopConf.getInt("stepIndex", 0);
            float p = hadoopConf.getFloat("percentage0", 0.0f);
            int index = 1;
            while (p != 0.0f) {
                this.percentages.add(p);
                p = hadoopConf.getFloat("percentage" + index, 0.0f);
                ++index;
            }
            try {
                this.keySetter = (HadoopFileKeyChanger.KeySetter) Class.forName(hadoopConf.get("postKeySetterName")).newInstance();
            } catch (Exception e) {
                System.out.println(e.getMessage());
                throw new RuntimeException(e);
            }
        }

        @Override
        public void reduce(BlockKey key, Iterable<Person> valueSet, Context context)
                throws IOException, InterruptedException {
            List<Person> persons = new ArrayList<>();
            for (Person p : valueSet) {
                persons.add(new Person(p));
            }
            this.knowsGenerator.generateKnows(persons, (int) key.block, percentages, step_index, personSimilarity);
            for (Person p : persons) {
                context.write(keySetter.getKey(p), p);
                numGeneratedEdges += p.getKnows().size();
            }
        }

        @Override
        public void cleanup(Context context) {
            System.out.println("Number of generated edges: " + numGeneratedEdges / 2);
        }
    }


    public HadoopKnowsGenerator(Configuration conf, String preKeySetterName, String postKeySetterName, List<Float> percentages, int stepIndex, String knowsGeneratorName) {
        this.conf = new Configuration(conf);
        this.preKeySetterName = preKeySetterName;
        this.postKeySetterName = postKeySetterName;
        this.percentages = percentages;
        this.stepIndex = stepIndex;
        this.knowsGeneratorName = knowsGeneratorName;
    }

    public void run(String inputFileName, String outputFileName) throws Exception {
        FileSystem fs = FileSystem.get(conf);

        System.out.println("Ranking persons");
        long start = System.currentTimeMillis();
        String rankedFileName = HadoopConfiguration.getHadoopDir(conf) + "/ranked";
        HadoopFileRanker hadoopFileRanker = new HadoopFileRanker(conf, TupleKey.class, Person.class, preKeySetterName);
        hadoopFileRanker.run(inputFileName, rankedFileName);

        System.out.println("... Time to rank persons: " + (System.currentTimeMillis() - start) + " ms");

        conf.setInt("stepIndex", stepIndex);
        int index = 0;
        for (float p : percentages) {
            conf.setFloat("percentage" + index, p);
            ++index;
        }
        conf.set("postKeySetterName", postKeySetterName);
        conf.set("knowsGeneratorName", knowsGeneratorName);
        int numThreads = HadoopConfiguration.getNumThreads(conf);
        Job job = Job.getInstance(conf, "Knows generator");
        job.setMapOutputKeyClass(BlockKey.class);
        job.setMapOutputValueClass(Person.class);
        job.setOutputKeyClass(TupleKey.class);
        job.setOutputValueClass(Person.class);
        job.setJarByClass(HadoopBlockMapper.class);
        job.setMapperClass(HadoopBlockMapper.class);
        job.setReducerClass(HadoopKnowsGeneratorReducer.class);
        job.setNumReduceTasks(numThreads);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setSortComparatorClass(BlockKeyComparator.class);
        job.setGroupingComparatorClass(BlockKeyGroupComparator.class);
        job.setPartitionerClass(HadoopBlockPartitioner.class);

        FileInputFormat.setInputPaths(job, new Path(rankedFileName));
        FileOutputFormat.setOutputPath(job, new Path(outputFileName));

        System.out.println("Generating knows relations");
        start = System.currentTimeMillis();
        if (!job.waitForCompletion(true)) {
            throw new IllegalStateException("HadoopKnowsGenerator failed");
        }
        System.out.println("... Time to generate knows relations: " + (System.currentTimeMillis() - start) + " ms");

        fs.delete(new Path(rankedFileName), true);
    }
}
