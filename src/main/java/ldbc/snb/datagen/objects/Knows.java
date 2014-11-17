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
package ldbc.snb.datagen.objects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class Knows implements Serializable, Comparable<Knows> {
    public long from;
    public long to;
    public long creationDate;


    public Knows() {
    }

    public Knows(Person from, Person to, long creationDate ){
        this.from = from.accountId;
        this.to = to.accountId;
        this.creationDate = creationDate;
    }

    public void readFields(DataInput arg0) throws IOException {
        this.from = arg0.readLong();
        this.to = arg0.readLong();
        this.creationDate = arg0.readLong();
    }

    public void write(DataOutput arg0) throws IOException {
        arg0.writeLong(this.from);
        arg0.writeLong(this.to);
        arg0.writeLong(this.creationDate);
    }

    public int compareTo(Knows k) {
        long res = this.from - k.from;
        if(res==0)  res = this.to - k.to;
        return (int)res;
    }
}
