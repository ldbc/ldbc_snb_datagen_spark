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
package ldbc.snb.datagen.serializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

public class HDFSWriter {

    private int numPartitions;
    private int currentPartition = 0;
    private StringBuffer buffer;
    private OutputStream[] fileOutputStream;

    public HDFSWriter(String outputDir, String prefix, int numPartitions, boolean compressed, String extension) throws IOException {
        this.numPartitions = numPartitions;
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            fileOutputStream = new OutputStream[numPartitions];
            if (compressed) {
                for (int i = 0; i < numPartitions; i++) {
                    this.fileOutputStream[i] = new GZIPOutputStream(fs.create(new Path(outputDir + "/" + prefix + "_" + i + "." + extension + ".gz"), true, 131072));
                }
            } else {
                for (int i = 0; i < numPartitions; i++) {
                    this.fileOutputStream[i] = fs
                            .create(new Path(outputDir + "/" + prefix + "_" + i + "." + extension), true, 131072);
                }
            }
            buffer = new StringBuffer(1024);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            throw e;
        }
    }

    public void write(String entry) {
        buffer.setLength(0);
        buffer.append(entry);
        try {
            fileOutputStream[currentPartition].write(buffer.toString().getBytes("UTF8"));
            currentPartition = ++currentPartition % numPartitions;
        } catch (IOException e) {
            System.out.println("Cannot write to output file ");
            e.printStackTrace();
        }
    }

    public void writeAllPartitions(String entry) {
        try {
            for (int i = 0; i < numPartitions; ++i) {
                fileOutputStream[i].write(entry.getBytes("UTF8"));
            }
        } catch (IOException e) {
            System.out.println("Cannot write to output file ");
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            for (int i = 0; i < numPartitions; ++i) {
                fileOutputStream[i].flush();
                fileOutputStream[i].close();
            }
        } catch (IOException e) {
            System.err.println("Exception when closing a file");
            System.err.println(e.getMessage());
        }
    }
}
