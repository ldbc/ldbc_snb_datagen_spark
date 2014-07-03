/*
 * Copyright (c) 2013 LDBC
 * Linked Data Benchmark Council (http://ldbc.eu)
 *
 * This file is part of ldbc_socialnet_dbgen.
 *
 * ldbc_socialnet_dbgen is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ldbc_socialnet_dbgen is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ldbc_socialnet_dbgen.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2011 OpenLink Software <bdsmt@openlinksw.com>
 * All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation;  only Version 2 of the License dated
 * June 1991.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package ldbc.socialnet.dbgen.util;


import org.apache.hadoop.io.WritableComparable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.DataOutput;
import java.io.DataInput;
import java.util.Random;
import java.util.ArrayList;
import java.util.Iterator;

public class MapReduceKey implements WritableComparable<MapReduceKey> {
    public int block;       
    public int key[];
    public long id;

    public MapReduceKey( ) {
        this.key = new int[3];
    }

    public MapReduceKey( int block, int key[], long id) {
        this.block = block;
        this.key = new int[3];
        this.key[0] = key[0];
        this.key[1] = key[1];
        this.key[2] = key[2];
        this.id = id;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(block);
        out.writeInt(key[0]);
        out.writeInt(key[1]);
        out.writeInt(key[2]);
        out.writeLong(id);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        block = in.readInt();
        key[0] = in.readInt();
        key[1] = in.readInt();
        key[2] = in.readInt();
        id = in.readLong();
    }

    @Override
    public int compareTo( MapReduceKey mpk) {
        return block - mpk.block; 
    }
}
