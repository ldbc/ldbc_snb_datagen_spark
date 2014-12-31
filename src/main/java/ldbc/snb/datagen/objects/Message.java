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

import java.util.TreeSet;

abstract public class Message {

    private long messageId_;
    private long creationDate_;
    private long authorId_;
    private long forumId_;
    private String content_;
    private TreeSet<Integer> tags_;
    private IP ipAddress_;
    private int browserId_;
    private int locationId_;

    public Message(long messageId,
                   long creationDate,
                   long authorId,
                   long forumId,
                   String content,
                   TreeSet<Integer> tags,
                   IP ipAddress,
                   int browserId,
                   int locationId) {

        messageId_ = messageId;
        creationDate_ = creationDate;
        authorId_ = authorId;
        forumId_ = forumId;
        content_ = content;
        tags_ = new TreeSet<Integer>(tags);
	ipAddress_ = new IP(ipAddress);
        browserId_ = browserId;
        locationId_ = locationId;
    }

    public long messageId() {
	    return messageId_;
    }

    public void messageId(long id ) {
	    messageId_ = id;
    }

    public long creationDate () {
	    return creationDate_;
    }

    public void creationDate ( long date ) {
	    creationDate_ = date;
    }

    public long authorId () {
	    return authorId_;
    }

    public void authorId (long id ) {
	    authorId_ = id ;
    }

    public long forumId() {
	    return forumId_;
    }
	    
    public void forumId(long id) {
	    forumId_ = id;
    }

    public String content()  {
	    return content_;
    }

    public void content( String s)  {
	    content_ = new String(s);
    }

    public TreeSet<Integer> tags() {
	    return tags_;
    }

    public void tags( TreeSet<Integer> tags )  {
	    tags_.clear();
	    tags_.addAll(tags);
    }

    public IP ipAddress() {
	    return ipAddress_;
    }

    public void ipAddress( IP ip ) {
	    ipAddress_.copy(ip);
    }

    public int browserId () {
	    return browserId_;
    }

    public void browserId ( int browser ) {
	    browserId_ = browser;
    }

    public int locationId() {
	    return locationId_;
    }

    public void locationId ( int l ) {
	    locationId_ = l;
    }
}
