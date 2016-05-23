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
import ldbc.snb.datagen.generator.DatagenParams;

import java.util.ArrayList;

public class Forum {

    private long id_;
    private Person.PersonSummary moderator_;        
    private long creationDate_;
    private String title_;
    private ArrayList<Integer> tags_;
    private int placeId_;            
    private int language_;
    private ArrayList<ForumMembership> memberships_;


    public Forum( long id, long creationDate, Person.PersonSummary moderator, String title, int placeId, int language  ) {
        assert (moderator.creationDate() + DatagenParams.deltaTime) <= creationDate : "Moderator creation date is larger than message creation date";
	    memberships_ = new ArrayList<ForumMembership>();
	    tags_ = new ArrayList<Integer>();
	    id_ = id;
	    creationDate_ = creationDate;
	    title_ = new String(title);
	    placeId_ = placeId;
	    moderator_ = new Person.PersonSummary(moderator);
	    language_ = language;
    }

    public void addMember(ForumMembership member) {
	    memberships_.add(member);
    }

    public long id() {
        return id_;
    }

    public void id(long id) {
	    id_ = id;
    }

    public Person.PersonSummary moderator() {
        return moderator_;
    }

    public void moderator( Person.PersonSummary moderator) {
        moderator_.copy(moderator);
    }

    public long creationDate() {
        return creationDate_;
    }

    public void creationDate(long creationDate) {
        creationDate_ = creationDate;
    }

    public ArrayList<Integer> tags() {
        return tags_;
    }

    public void tags(ArrayList<Integer> tags) {
	    tags_.clear();
	    tags_.addAll(tags);
    }

    public String title() {
        return title_;
    }

    public void title(String title) {
        title = new String(title);
    }

    public ArrayList<ForumMembership> memberships() {
        return memberships_;
    }

    public void membeships(ArrayList<ForumMembership> memberShips) {
        memberships_ = memberShips;
    }

    public int place() {
        return placeId_;
    }

    public void place(int placeId) {
        placeId_ = placeId;
    }
    
    public int language() {
	    return language_;
    }

    public void language( int l ) {
	    language_ = l;
    }
}
