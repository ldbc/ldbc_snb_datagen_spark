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

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.entities.dynamic.DynamicActivity;
import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.person.PersonSummary;

/**
 * This class represents a hasMember edge between a Person and a Forum
 */
public class ForumMembership implements DynamicActivity {

    private boolean isExplicitlyDeleted;
    private long forumId;
    private long creationDate;
    private long deletionDate;
    private PersonSummary person;
    private Forum.ForumType forumType;

    public ForumMembership(long forumId, long creationDate, long deletionDate, PersonSummary p, Forum.ForumType forumType, boolean isExplicitlyDeleted) {
        assert (p.getCreationDate() + DatagenParams.delta) <= creationDate : "Person creation date is larger than membership";
        this.forumId = forumId;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.forumType = forumType;
        person = p.clone();
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public boolean isExplicitlyDeleted() {
        return isExplicitlyDeleted;
    }

    public void setExplicitlyDeleted(boolean explicitlyDeleted) {
        isExplicitlyDeleted = explicitlyDeleted;
    }

    public long getForumId() {
        return forumId;
    }

    public long getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(long creationDate) {
        this.creationDate = creationDate;
    }

    public long getDeletionDate() {
        return deletionDate;
    }

    public void setDeletionDate(long deletionDate) {
        this.deletionDate = deletionDate;
    }

    public PersonSummary getPerson() {
        return person;
    }

    public void setPerson(PersonSummary p) {
        person = p;
    }

    public Forum.ForumType getForumType() {
        return forumType;
    }

    public void setForumType(Forum.ForumType forumType) {
        this.forumType = forumType;
    }
}
