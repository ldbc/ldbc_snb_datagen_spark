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

import java.util.TreeSet;

public class Post {

    private long postId;                        /**< @brief The post identifier.*/ 
    private String content;                     /**< @brief The string containing the content of the post.*/
    private int    textSize;                    /**< @brief The size of the content. Required in order to be able to generate posts without text but with size.*/
    private long creationDate;                   /**< @brief The creation date of the post.*/
    private long authorId;                       /**< @brief The author identifier of the post.*/
    private long groupId;                        /**< @brief The group identifier where the post belongs to.*/
    private int language;                       /**< @brief The language used in the post.*/
    private TreeSet<Integer> tags;              /**< @brief The set of tags related to the post.*/
    private long interestedUserAccs[];		    /**< @brief The list of users who are interested in the post*/
    private long interestedUserAccsTimestamp[]; /**< @brief The timestamps when the interested users where actually interested.*/
    private IP ipAddress;                       /**< @brief The ip from where the post was created.*/
    private String userAgent;				    /**< @brief The media used to send the post.*/
    private byte browserIdx;					/**< @brief The id of the browser used to send the post.*/ 



    public Post( long postId,
                 String content,
                 int textSize,
                 long creationDate,
                 long authorId, 
                 long groupId,
                 int language,
                 TreeSet<Integer> tags,
                 IP ipAddress,
                 String userAgent,
                 byte browserIdx ) {

        this.postId = postId;
        this.content = content;
        this.textSize = textSize;
        this.creationDate = creationDate;
        this.authorId = authorId;
        this.groupId = groupId;
        this.language = language;
        this.tags = tags;
        this.ipAddress = ipAddress;
        this.userAgent = userAgent;
        this.browserIdx = browserIdx;
    }


    public TreeSet<Integer> getTags() {
        return this.tags;
    }
    public void setTags(TreeSet<Integer> tags) {
        this.tags = tags;
    }
    public long[] getInterestedUserAccs() {
        return this.interestedUserAccs;
    }
    public void setInterestedUserAccs(long[] interestedUserAccs) {
        this.interestedUserAccs = interestedUserAccs;
    }
    public long[] getInterestedUserAccsTimestamp() {
        return this.interestedUserAccsTimestamp;
    }
    public void setInterestedUserAccsTimestamp(long[] timestamps) {
        this.interestedUserAccsTimestamp = timestamps;
    }
    public int getTextSize() {
        return textSize;
    }
    public long getPostId() {
        return this.postId;
    }
    public void setPostId(long postId) {
        this.postId = postId;
    }
    public int getLanguage() {
        return this.language;
    }
    public void setLanguage(int language) {
        this.language = language;
    }
    public String getContent() {
        return this.content;
    }
    public void setContent(String content) {
        this.content = content;
    }
    public long getCreationDate() {
        return this.creationDate;
    }
    public void setCreationDate(long creationDate) {
        this.creationDate = creationDate;
    }
    public long getAuthorId() {
        return this.authorId;
    }
    public void setAuthorId(long authorId) {
        this.authorId = authorId;
    }
    public long getGroupId() {
        return this.groupId;
    }
    public void setGroupId(long groupId) {
        this.groupId = groupId;
    }
    public String getUserAgent() {
        return this.userAgent;
    }
    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }
    public IP getIpAddress() {
        return this.ipAddress;
    }
    public void setIpAddress(IP ipAddress) {
        this.ipAddress = ipAddress;
    }
    public byte getBrowserIdx() {
        return this.browserIdx;
    }
    public void setBrowserIdx(byte browserId) {
        this.browserIdx = browserId;
    }		
}
