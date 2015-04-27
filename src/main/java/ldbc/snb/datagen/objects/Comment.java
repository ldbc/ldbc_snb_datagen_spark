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
() * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package ldbc.snb.datagen.objects;


import ldbc.snb.datagen.objects.Person.PersonSummary;

import java.util.TreeSet;

public class Comment extends Message {

    private long postId_;
    private long replyOf_;

    public Comment(long commentId,
                   long creationDate,
                   PersonSummary author,
                   long forumId,
                   String content,
                   TreeSet<Integer> tags,
                   IP ipAddress,
                   int browserId,
                   long postId,
                   long replyOf
    ) {

        super(commentId, creationDate, author, forumId, content, tags, ipAddress, browserId);
        postId_ = postId;
        replyOf_ = replyOf;
    }

    public long postId() {
	    return postId_;
    }

    public void postId( long id ) {
	   postId_ = id; 
    }

    public long replyOf() {
	    return replyOf_;
    }

    public void replyOf( long id ) {
	    replyOf_ = id;
    }

}
