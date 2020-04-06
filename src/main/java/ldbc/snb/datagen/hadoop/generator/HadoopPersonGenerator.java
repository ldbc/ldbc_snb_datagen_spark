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

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.generator.generators.PersonGenerator;
import ldbc.snb.datagen.hadoop.key.TupleKey;
import ldbc.snb.datagen.hadoop.miscjob.keychanger.HadoopFileKeyChanger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HadoopPersonGenerator {

    private Configuration conf = null;

    private JavaSparkContext ctx = null;

    public HadoopPersonGenerator(JavaSparkContext ctx) {
        this.conf = ctx.hadoopConfiguration();
        this.ctx = ctx;
    }

    /**
     * Generates a Person hadoop sequence file containing key-value paiers
     * where the key is the person id and the value is the person itself.
     *
     * @throws Exception
     */
    public void run(String outputFileName, String postKeySetterName) throws Exception {

        int numBlocks = (int) (Math.ceil(DatagenParams.numPersons / (double) DatagenParams.blockSize));

        PairFlatMapFunction<Iterator<Integer>, TupleKey, Person> personPartitionGenerator = (PairFlatMapFunction<Iterator<Integer>, TupleKey, Person>) tIterator -> {
            PersonGenerator personGenerator = new PersonGenerator(conf, conf
                    .get("ldbc.snb.datagen.generator.distribution.degreeDistribution"));

            HadoopFileKeyChanger.KeySetter<TupleKey> keySetter = null;

            try {
                keySetter = (HadoopFileKeyChanger.KeySetter) Class.forName(conf.get("postKeySetterName")).newInstance();
            } catch (Exception e) {
                System.err.println("Error when setting key setter");
                System.err.println(e.getMessage());
                throw new RuntimeException(e);
            }

            // Normally you would like exactly one block/partition, but let's support the general case

            if (!tIterator.hasNext()) {
                return Collections.emptyIterator();
            }

            int startBlock = tIterator.next();
            int endBlock = startBlock + 1;

            while (tIterator.hasNext()) {
                tIterator.next();
                endBlock++;
            }

            int blocksInPartition = endBlock - startBlock;

            Tuple2<TupleKey, Person>[] blocks = new Tuple2[blocksInPartition * DatagenParams.blockSize];

            for (int i = 0; i < blocksInPartition; ++i) {
                personGenerator.generatePersonBlock(i + startBlock, DatagenParams.blockSize, blocks, i * DatagenParams.blockSize);

                // replace the null keys. this whole thing is done in this unclean way so we don't have to allocate a new array
                for (int j = 0; j < DatagenParams.blockSize; ++j) {
                    int elemIdx = i * DatagenParams.blockSize + j;
                    blocks[elemIdx] = new Tuple2(keySetter.getKey(blocks[elemIdx]), blocks[elemIdx]);
                }
            }

            return Arrays.stream(blocks).iterator();
        };

        conf.set("postKeySetterName", postKeySetterName);

        JavaPairRDD<TupleKey, Person> persons = ctx
                .parallelize(IntStream.range(0, numBlocks).boxed().collect(Collectors.toList()), numBlocks)
                .mapPartitionsToPair(personPartitionGenerator, true);

        persons.saveAsHadoopFile(outputFileName, TupleKey.class, Person.class, SequenceFileOutputFormat.class);
    }
}
