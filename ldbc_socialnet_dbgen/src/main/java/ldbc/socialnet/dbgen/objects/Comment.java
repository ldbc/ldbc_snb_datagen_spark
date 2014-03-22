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
package ldbc.socialnet.dbgen.objects;


import java.util.TreeSet;

public class Comment extends Message {

	private long postId;			/**< @brief The post identifier of the replied post.**/
	private	long replyOf;			/**< @brief The id of the parent post/comment of this comment.**/

	public Comment( long commentId,
                 String content,
                 int textSize,
                 long creationDate,
                 long authorId,
                 long groupId,
                 TreeSet<Integer> tags,
                 IP ipAddress,
                 String userAgent,
                 byte browserIdx,
                 long postId,
                 long replyOf
    ) {

        super(commentId, content, textSize, creationDate, authorId, groupId, tags, ipAddress, userAgent, browserIdx);
        this.postId = postId;
        this.replyOf = replyOf;
	}

	public long getReplyOf() {
		return replyOf;
	}
	public void setReplyOf(long replyOf) {
		this.replyOf = replyOf;
	}
	public long getPostId() {
		return postId;
	}
	public void setPostId(long postId) {
		this.postId = postId;
	}
}
