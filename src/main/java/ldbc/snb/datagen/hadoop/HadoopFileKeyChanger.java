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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * Created by aprat on 11/17/14.
 */
public class HadoopFileKeyChanger {

    private String keySetterName;
    private Configuration conf;
    private Class<?> K;
    private Class<?> V;

    public interface KeySetter<K> {
        public K getKey(Object object);
    }


    public HadoopFileKeyChanger(Configuration conf, Class<?> K, Class<?> V, String keySetterName) {
        this.keySetterName = keySetterName;
        this.conf = conf;
        this.K = K;
        this.V = V;
    }

    public static class HadoopFileKeyChangerReducer<K, V> extends Reducer<K, V, TupleKey, V> {

        private KeySetter<TupleKey> keySetter;

        @Override
        public void setup(Context context) {
            try {
                String className = context.getConfiguration().get("keySetterClassName");
                keySetter = (HadoopFileKeyChanger.KeySetter) Class.forName(className).newInstance();
            } catch (ClassNotFoundException e) {
                System.out.print(e.getMessage());
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                System.out.print(e.getMessage());
                e.printStackTrace();
            } catch (InstantiationException e) {
                System.out.print(e.getMessage());
                e.printStackTrace();
            }
        }

        @Override
        public void reduce(K key, Iterable<V> valueSet,
                           Context context) throws IOException, InterruptedException {
            for (V v : valueSet) {
                context.write(keySetter.getKey(v), v);
            }
        }
    }

    public void run(String inputFileName, String outputFileName) throws Exception {

        int numThreads = conf.getInt("ldbc.snb.datagen.generator.numThreads", 1);
        System.out.println("***************" + numThreads);
        conf.set("keySetterClassName", keySetterName);

        /** First Job to sort the key-value pairs and to count the number of elements processed by each reducer.**/
        Job job = Job.getInstance(conf, "Sorting " + inputFileName);

        FileInputFormat.setInputPaths(job, new Path(inputFileName));
        FileOutputFormat.setOutputPath(job, new Path(outputFileName));

        job.setMapOutputKeyClass(K);
        job.setMapOutputValueClass(V);
        job.setOutputKeyClass(TupleKey.class);
        job.setOutputValueClass(V);
        job.setNumReduceTasks(numThreads);
        job.setReducerClass(HadoopFileKeyChangerReducer.class);
        job.setJarByClass(V);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        if (!job.waitForCompletion(true)) {
            throw new Exception();
        }
    }
}
