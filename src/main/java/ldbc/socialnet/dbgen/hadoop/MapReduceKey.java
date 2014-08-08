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
package ldbc.socialnet.dbgen.hadoop;


import org.apache.hadoop.io.WritableComparable;
import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;

public class MapReduceKey implements WritableComparable<MapReduceKey> {
    public int block;       
    public int key;
    public long id;

    public MapReduceKey( ) {
    }

    public MapReduceKey( int block, int key, long id) {
        this.block = block;
        this.key = key;
        this.id = id;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(block);
        out.writeInt(key);
        out.writeLong(id);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        block = in.readInt();
        key = in.readInt();
        id = in.readLong();
    }

    @Override
    public int compareTo( MapReduceKey mpk) {
        return block - mpk.block; 
    }
}
