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
package ldbc.snb.datagen.objects;


import ldbc.snb.datagen.objects.Person.PersonSummary;

import java.util.TreeSet;

public class Comment extends Message {

    private long postId_;
    private long replyOf_;

    public Comment() {
        super();
    }

    public Comment(Comment comment) {
        super(comment.messageId(), comment.creationDate(), comment.author(), comment.forumId(), comment.content(),
              comment.tags(), comment.countryId(), comment.ipAddress(), comment.browserId());
        postId_ = comment.postId();
        replyOf_ = comment.replyOf();
    }

    public Comment(long commentId,
                   long creationDate,
                   PersonSummary author,
                   long forumId,
                   String content,
                   TreeSet<Integer> tags,
                   int countryId,
                   IP ipAddress,
                   int browserId,
                   long postId,
                   long replyOf
    ) {

        super(commentId, creationDate, author, forumId, content, tags, countryId, ipAddress, browserId);
        postId_ = postId;
        replyOf_ = replyOf;
    }

    public void initialize(long commentId,
                           long creationDate,
                           PersonSummary author,
                           long forumId,
                           String content,
                           TreeSet<Integer> tags,
                           int countryId,
                           IP ipAddress,
                           int browserId,
                           long postId,
                           long replyOf) {
        super.initialize(commentId, creationDate, author, forumId, content, tags, countryId, ipAddress, browserId);
        postId_ = postId;
        replyOf_ = replyOf;
    }

    public long postId() {
        return postId_;
    }

    public void postId(long id) {
        postId_ = id;
    }

    public long replyOf() {
        return replyOf_;
    }

    public void replyOf(long id) {
        replyOf_ = id;
    }

}
