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
package ldbc.snb.datagen.hadoop.serializer;

import ldbc.snb.datagen.hadoop.DatagenHadoopJob;
import ldbc.snb.datagen.hadoop.HadoopConfiguration;
import ldbc.snb.datagen.hadoop.generator.HadoopInsertEventKeyPartitioner;
import ldbc.snb.datagen.hadoop.key.updatekey.InsertEventKey;
import ldbc.snb.datagen.hadoop.key.updatekey.InsertEventKeyGroupComparator;
import ldbc.snb.datagen.util.LdbcConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.GZIPOutputStream;

public class HadoopInsertStreamSorterAndSerializer extends DatagenHadoopJob {

    public HadoopInsertStreamSorterAndSerializer(LdbcConfiguration conf, Configuration hadoopConf) {
        super(conf, hadoopConf);
    }

    public static class HadoopInsertStreamSorterAndSerializerReducer extends StreamSorterAndSerializerReducer<InsertEventKey, Text, InsertEventKey, Text> {
        @Override
        public void reduce(InsertEventKey key, Iterable<Text> valueSet, Context context) {
            OutputStream out;
            try {
                FileSystem fs = FileSystem.get(hadoopConf);
                if (compressed) {
                    Path outFile = new Path(
                            conf.getSocialNetworkDir() + "/insertStream_" + key.reducerId + "_" + key.partition + "_" + streamType + ".csv.gz");
                    out = new GZIPOutputStream(fs.create(outFile));
                } else {
                    Path outFile = new Path(conf.getSocialNetworkDir() + "/insertStream_" + key.reducerId + "_" + key.partition + "_" + streamType + ".csv");
                    out = fs.create(outFile);
                }
                for (Text t : valueSet) {
                    out.write(t.toString().getBytes(StandardCharsets.UTF_8));
                }
                out.close();
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }
    }

    public void run(List<String> inputFileNames, String type) throws Exception {

        int numThreads = HadoopConfiguration.getNumThreads(hadoopConf);
        hadoopConf.set("streamType", type);

        Job job = Job.getInstance(hadoopConf, "Insert Stream Serializer");
        job.setMapOutputKeyClass(InsertEventKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(InsertEventKey.class);
        job.setOutputValueClass(Text.class);
        job.setJarByClass(HadoopInsertStreamSorterAndSerializerReducer.class);
        job.setReducerClass(HadoopInsertStreamSorterAndSerializerReducer.class);
        job.setNumReduceTasks(numThreads);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setPartitionerClass(HadoopInsertEventKeyPartitioner.class);
        job.setGroupingComparatorClass(InsertEventKeyGroupComparator.class);
        //job.setSortComparatorClass(UpdateEventKeySortComparator.class);

        for (String s : inputFileNames) {
            FileInputFormat.addInputPath(job, new Path(s));
        }
        FileOutputFormat.setOutputPath(job, new Path(conf.getBuildDir() + "/aux"));
        if (!job.waitForCompletion(true)) {
            throw new Exception();
        }


        try {
            FileSystem fs = FileSystem.get(hadoopConf);
            fs.delete(new Path(conf.getBuildDir() + "/aux"), true);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }
}

