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
//    private String title;                       /**< @brief The title of the post.*/
    private String content;                     /**< @brief The string containing the content of the post.*/
    private long creationDate;                   /**< @brief The creation date of the post.*/
    private int authorId;                       /**< @brief The author identifier of the post.*/
    private int groupId;                        /**< @brief The group identifier where the post belongs to.*/
    private int language;                       /**< @brief The language used in the post.*/
    //private int articleIdx;					    /**< @brief Index of articles in the set of same region/interest article*/
    //private int interestIdx;				    /**< @brief Index of one interest in user's interests*/
    private TreeSet<Integer> tags;              /**< @brief The set of tags related to the post.*/
    private int interestedUserAccs[];		    /**< @brief The list of users who are interested in the post*/
    private long interestedUserAccsTimestamp[]; /**< @brief The timestamps when the interested users where actually interested.*/
    private IP ipAddress;                       /**< @brief The ip from where the post was created.*/
    private String userAgent;				    /**< @brief The media used to send the post.*/
    private byte browserIdx;					/**< @brief The id of the browser used to send the post.*/ 
    //private boolean isInterestPost; 		    /**< @brief Only use for group's post*/



    public Post( long postId,
  //             String title,
                 String content,
                 long creationDate,
                 int authorId, 
                 int groupId,
                 int language,
                 TreeSet<Integer> tags,
                 IP ipAddress,
                 String userAgent,
                 byte browserIdx ) {

        this.postId = postId;
 //       this.title = title;
        this.content = content;
        this.creationDate = creationDate;
        this.authorId = authorId;
        this.groupId = groupId;
        this.language = language;
        this.tags = tags;
        this.ipAddress = ipAddress;
        this.userAgent = userAgent;
        this.browserIdx = browserIdx;
    }

    /*public int getInterestIdx() {
        return this.interestIdx;
    }
    public void setInterestIdx(int interestIdx) {
        this.interestIdx = interestIdx;
    }
    */

    public TreeSet<Integer> getTags() {
        return this.tags;
    }
    public void setTags(TreeSet<Integer> tags) {
        this.tags = tags;
    }
    public int[] getInterestedUserAccs() {
        return this.interestedUserAccs;
    }
    public void setInterestedUserAccs(int[] interestedUserAccs) {
        this.interestedUserAccs = interestedUserAccs;
    }
    public long[] getInterestedUserAccsTimestamp() {
        return this.interestedUserAccsTimestamp;
    }
    public void setInterestedUserAccsTimestamp(long[] timestamps) {
        this.interestedUserAccsTimestamp = timestamps;
    }

    public long getPostId() {
        return this.postId;
    }
    public void setPostId(long postId) {
        this.postId = postId;
    }
    /*public String getTitle() {
        return this.title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
    */
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
    public int getAuthorId() {
        return this.authorId;
    }
    public void setAuthorId(int authorId) {
        this.authorId = authorId;
    }
    public int getGroupId() {
        return this.groupId;
    }
    public void setGroupId(int groupId) {
        this.groupId = groupId;
    }
/*    public int getArticleIdx() {
        return this.articleIdx;
    }
    public void setArticleIdx(int articleIdx) {
        this.articleIdx = articleIdx;
    }
    */
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
    /*public boolean isInterestPost() {
        return this.isInterestPost;
    }
    public void setInterestPost(boolean isInterestPost) {
        this.isInterestPost = isInterestPost;
    }
    */
    public byte getBrowserIdx() {
        return this.browserIdx;
    }
    public void setBrowserIdx(byte browserId) {
        this.browserIdx = browserId;
    }		
}
