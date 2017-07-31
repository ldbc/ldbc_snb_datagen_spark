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

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by aprat on 11/9/14.
 */

public class BlockKey implements WritableComparable<BlockKey> {
    public long block;
    public TupleKey tk;

    public BlockKey() {
        tk = new TupleKey();
    }

    public BlockKey(BlockKey bK) {
        this.block = bK.block;
        this.tk = new TupleKey(bK.tk);
    }

    public BlockKey(long block, TupleKey tk) {
        this.block = block;
        this.tk = tk;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(block);
        tk.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        block = in.readLong();
        tk.readFields(in);
    }

    public int compareTo(BlockKey mpk) {
        if (block < mpk.block) return -1;
        if (block > mpk.block) return 1;
        return 0;
    }
}
