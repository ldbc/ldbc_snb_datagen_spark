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

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This class represents an insert event
 */
public class InsertEvent implements Writable {

    public enum InsertEventType {
        ADD_PERSON(0),
        ADD_LIKE_POST(1),
        ADD_LIKE_COMMENT(2),
        ADD_FORUM(3),
        ADD_FORUM_MEMBERSHIP(4),
        ADD_POST(5),
        ADD_COMMENT(6),
        ADD_FRIENDSHIP(7),
        NO_EVENT(100);

        private final int type;

        InsertEventType(int type) {
            this.type = type;
        }

        public int getType() {
            return type;
        }
    }

    private long eventDate;
    private long dependantOnDate;
    private String eventData;
    private InsertEventType insertEventType;

    public InsertEvent(long eventDate, long dependantOnDate, InsertEventType insertEventType, String eventData) {
        this.eventDate = eventDate;
        this.insertEventType = insertEventType;
        this.eventData = eventData;
        this.dependantOnDate = dependantOnDate;
    }

    public long getEventDate() {
        return eventDate;
    }

    public void setEventDate(long eventDate) {
        this.eventDate = eventDate;
    }

    public long getDependantOnDate() {
        return dependantOnDate;
    }

    public void setDependantOnDate(long dependantOnDate) {
        this.dependantOnDate = dependantOnDate;
    }

    public String getEventData() {
        return eventData;
    }

    public void setEventData(String eventData) {
        this.eventData = eventData;
    }

    public InsertEventType getInsertEventType() {
        return insertEventType;
    }

    public void setInsertEventType(InsertEventType insertEventType) {
        this.insertEventType = insertEventType;
    }

    public void readFields(DataInput arg0) throws IOException {
        this.eventDate = arg0.readLong();
        this.dependantOnDate = arg0.readLong();
        this.insertEventType = InsertEventType.values()[arg0.readInt()];
        this.eventData = arg0.readUTF();
    }

    public void write(DataOutput arg0) throws IOException {
        arg0.writeLong(eventDate);
        arg0.writeLong(dependantOnDate);
        arg0.writeInt(insertEventType.ordinal());
        arg0.writeUTF(eventData);
    }
}
