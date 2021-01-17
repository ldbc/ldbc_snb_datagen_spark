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

package ldbc.snb.datagen.entities.dynamic.relations;

import ldbc.snb.datagen.entities.dynamic.DynamicActivity;

public class Like implements DynamicActivity {
    public enum LikeType {
        POST,
        COMMENT,
        PHOTO
    }

    private boolean isExplicitlyDeleted;
    private long person;
    private long personCreationDate;
    private long messageId;
    private long creationDate;
    private long deletionDate;
    private LikeType type;

    public boolean isExplicitlyDeleted() {
        return isExplicitlyDeleted;
    }

    public void setExplicitlyDeleted(boolean explicitlyDeleted) {
        isExplicitlyDeleted = explicitlyDeleted;
    }

    public long getPerson() {
        return person;
    }

    public long getPersonCreationDate() {
        return personCreationDate;
    }

    public long getMessageId() {
        return messageId;
    }

    public long getCreationDate() {
        return creationDate;
    }

    public long getDeletionDate() {
        return deletionDate;
    }

    public LikeType getType() {
        return type;
    }

    public void setPerson(long person) {
        this.person = person;
    }

    public void setPersonCreationDate(long personCreationDate) {
        this.personCreationDate = personCreationDate;
    }

    public void setMessageId(long messageId) {
        this.messageId = messageId;
    }

    public void setCreationDate(long creationDate) {
        this.creationDate = creationDate;
    }

    public void setDeletionDate(long deletionDate) {
        this.deletionDate = deletionDate;
    }

    public void setType(LikeType type) {
        this.type = type;
    }
}
