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

public class Comment {

	private long commentId;			/**< @brief The identifier of the comment.**/
	private String content;			/**< @brief The content of the comment.**/
    private int    textSize;        /**< @brief The size of the comment. Needed in case we do not generate the content.*/
	private long postId;			/**< @brief The post identifier of the replied post.**/
	private	int authorId;			/**< @brief The identifier of the author.**/
	private	long creationDate;		/**< @brief The creation date of the comment.**/
	private	int groupId;			/**< @brief The identifier of the group where the comment has been written.**/
	private	long replyOf;			/**< @brief The id of the parent post/comment of this comment.**/
	private	IP ipAddress;			/**< @brief The ip address from which the comment has been posted.**/
	private	String userAgent;		/**< @brief The type of device used to send the comment.**/
	private	byte browserIdx;		/**< @brief The browser used to send the comment.**/	

	public Comment( long commentId,
					String content,
                    int textSize,
					long postId,
					int authorId,
					long creationDate,
					int groupId,
					long replyOf,
					IP ipAddress,
					String userAgent,
					byte browserIdx ) {

		this.commentId = commentId;
		this.content = content;
		this.postId = postId;
		this.authorId = authorId;
		this.creationDate = creationDate;
		this.groupId = groupId;
		this.replyOf = replyOf;
		this.ipAddress = ipAddress;
		this.userAgent = userAgent;
		this.browserIdx = browserIdx;
        this.textSize = textSize;
	}

	public IP getIpAddress() {
		return ipAddress;
	}
	public void setIpAddress(IP ipAddress) {
		this.ipAddress = ipAddress;
	}
	public String getUserAgent() {
		return userAgent;
	}
	public void setUserAgent(String userAgent) {
		this.userAgent = userAgent;
	}
	public byte getBrowserIdx() {
		return browserIdx;
	}
	public void setBrowserIdx(byte browserIdx) {
		this.browserIdx = browserIdx;
	}
	public long getReplyOf() {
		return replyOf;
	}
	public void setReplyOf(long replyOf) {
		this.replyOf = replyOf;
	}
	public int getGroupId() {
		return groupId;
	}
	public void setGroupId(int groupId) {
		this.groupId = groupId;
	}
	public long getCommentId() {
		return commentId;
	}
	public void setCommentId(long commentId) {
		this.commentId = commentId;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
	public long getPostId() {
		return postId;
	}
	public void setPostId(long postId) {
		this.postId = postId;
	}
	public int getAuthorId() {
		return authorId;
	}
	public void setAuthorId(int authorId) {
		this.authorId = authorId;
	}
	public long getCreationDate() {
		return creationDate;
	}
	public void setCreationDate(long creationDate) {
		this.creationDate = creationDate;
	}
    public int getTextSize() {
        return textSize;
    }
}
