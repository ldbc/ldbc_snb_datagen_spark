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
 * Created by aprat on 5/01/16.
 */
public class UpdateEventKey implements WritableComparable<UpdateEventKey> {

    public long date;
    public int reducerId;
    public int partition;

    public UpdateEventKey() {
    }

    public UpdateEventKey(UpdateEventKey key) {
        this.date = key.date;
        this.reducerId = key.reducerId;
        this.partition = key.partition;
    }

    public UpdateEventKey(long date, int reducerId, int partition) {

        this.date = date;
        this.reducerId = reducerId;
        this.partition = partition;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(date);
        out.writeInt(reducerId);
        out.writeInt(partition);
    }

    public void readFields(DataInput in) throws IOException {
        date = in.readLong();
        reducerId = in.readInt();
        partition = in.readInt();
    }

    public int compareTo(UpdateEventKey key) {
        if (reducerId != key.reducerId) return reducerId - key.reducerId;
        if (partition != key.partition) return partition - key.partition;
        if (date < key.date) return -1;
        if (date > key.date) return 1;
        return 0;
    }
}
