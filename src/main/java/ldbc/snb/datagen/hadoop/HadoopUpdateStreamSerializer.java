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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Created by aprat on 10/15/14.
 */
public class HadoopUpdateStreamSerializer {

    private Configuration conf;

    public static class HadoopUpdateStreamSerializerReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        private OutputStream out;

        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            int reducerId = Integer.parseInt(conf.get("reducerId"));
            int partitionId = Integer.parseInt(conf.get("partitionId"));
            String streamType = conf.get("streamType");
            try {
                FileSystem fs = FileSystem.get(conf);
                if (Boolean.parseBoolean(conf.get("ldbc.snb.datagen.serializer.compressed"))) {
                    Path outFile = new Path(context.getConfiguration()
                                                   .get("ldbc.snb.datagen.serializer.socialNetworkDir") + "/updateStream_" + reducerId + "_" + partitionId + "_" + streamType + ".csv.gz");
                    out = new GZIPOutputStream(fs.create(outFile));
                } else {
                    Path outFile = new Path(context.getConfiguration()
                                                   .get("ldbc.snb.datagen.serializer.socialNetworkDir") + "/updateStream_" + reducerId + "_" + partitionId + "_" + streamType + ".csv");
                    out = fs.create(outFile);
                }
            } catch (Exception e) {
                System.err.println(e.getMessage());
                e.printStackTrace();
            }
        }

        @Override
        public void reduce(LongWritable key, Iterable<Text> valueSet, Context context)
                throws IOException, InterruptedException {
            for (Text t : valueSet) {
                out.write(t.toString().getBytes("UTF8"));
            }

        }

        protected void cleanup(Context context) {
            try {
                out.close();
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }
    }

    public HadoopUpdateStreamSerializer(Configuration conf) {
        this.conf = new Configuration(conf);
    }

    public void run(String inputFileName, int reducer, int partition, String type) throws Exception {

        conf.setInt("reducerId", reducer);
        conf.setInt("partitionId", partition);
        conf.set("streamType", type);

        Job job = Job.getInstance(conf, "Update Stream Serializer");
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setJarByClass(HadoopUpdateStreamSerializerReducer.class);
        job.setReducerClass(HadoopUpdateStreamSerializerReducer.class);
        job.setNumReduceTasks(1);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(inputFileName));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("ldbc.snb.datagen.serializer.hadoopDir") + "/aux"));
        if (!job.waitForCompletion(true)) {
            throw new Exception();
        }


        try {
            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path(conf.get("ldbc.snb.datagen.serializer.hadoopDir") + "/aux"), true);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }
}

