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

package ldbc.socialnet.dbgen.objects;

import org.apache.hadoop.io.Writable;

import java.io.*;

public class UpdateEvent implements Serializable, Writable {

    public enum UpdateEventType {
        ADD_PERSON,
        ADD_LIKE_POST,
        ADD_LIKE_COMMENT,
        ADD_FORUM,
        ADD_FORUM_MEMBERSHIP,
        ADD_POST,
        ADD_COMMENT,
        ADD_FRIENDSHIP,
        NO_EVENT
    }

    public long    date;
    public String eventData;
    public UpdateEventType type;

    public static void writeEvent( OutputStream os, UpdateEvent event ) {
        try{
            StringBuffer string = new StringBuffer();
            string.append(Long.toString(event.date));
            string.append("|");
            string.append(event.type.toString());
            string.append("|");
            string.append(event.eventData);
            string.append("|");
            string.append("\n");
            //fileOutputStream.write(string.toString().getBytes("UTF8"));
            os.write(string.toString().getBytes("UTF8"));
        } catch(IOException e){
            System.err.println(e.getMessage());
            System.exit(-1);
        }
    }

    public static void writeEventKeyValue( OutputStream os, UpdateEvent event ) {
        try{
            StringBuffer string = new StringBuffer();
            string.append(Long.toString(event.date));
            string.append("|");
            string.append(event.type.toString());
            string.append("|");
            string.append(event.eventData);
            string.append("|");
            string.append("\n");
            //fileOutputStream.write(string.toString().getBytes("UTF8"));
            os.write(string.toString().getBytes("UTF8"));
        } catch(IOException e){
            System.err.println(e.getMessage());
            System.exit(-1);
        }
    }

    public UpdateEvent( long date, UpdateEventType type, String eventData) {
        this.date = date;
        this.type = type;
        this.eventData = eventData;
    }

    public void readObject(java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException{
       this.date = stream.readLong();
       this.type = UpdateEventType.values()[stream.readInt()];
       this.eventData = stream.readUTF();
    }

    public void writeObject(java.io.ObjectOutputStream stream)
            throws IOException{
        stream.writeLong(this.date);
        stream.writeInt(type.ordinal());
        stream.writeUTF(eventData);
    }

    public void readFields(DataInput arg0) throws IOException {
        this.date = arg0.readLong();
        this.type = UpdateEventType.values()[arg0.readInt()];
        this.eventData = arg0.readUTF();
    }

    public void write(DataOutput arg0) throws IOException {
        arg0.writeLong(date);
        arg0.writeInt(type.ordinal());
        arg0.writeUTF(eventData);
    }
}
