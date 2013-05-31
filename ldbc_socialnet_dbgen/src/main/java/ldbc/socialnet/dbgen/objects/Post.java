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

import java.util.HashSet;

public class Post extends SocialObject{
		long postId; 
		String title; 
		String content; 
		long createdDate; 
		int authorId; 
		int forumId;
		int language;
		
		int articleIdx;					// Index of articles in the set of same region/interest article  					
		int interestIdx;				// Index of one interest in user's interests
		
		HashSet<Integer> tags; 
		int interestedUserAccs[];		//List of users who are interested in the post  
		long interestedUserAccsTimestamp[];
		
		IP ipAddress; 
		String userAgent;				// Send from where e.g., iPhone, Samsung, HTC
		
		byte browserIdx;					// Set browser Idx 
		
		boolean isInterestPost; 		//Only use for group's post
		/*
		public Post(int numOfTags, int numOfLikes){
			tags = new ArrayList<String>(numOfTags);
			interestedUserAccs = new ArrayList<Integer>(numOfLikes);
		}
		*/
		public int getInterestIdx() {
			return interestIdx;
		}
		public void setInterestIdx(int interestIdx) {
			this.interestIdx = interestIdx;
		}

		public HashSet<Integer> getTags() {
			return tags;
		}
		public void setTags(HashSet<Integer> tags) {
			this.tags = tags;
		}
		public int[] getInterestedUserAccs() {
			return interestedUserAccs;
		}
		public void setInterestedUserAccs(int[] interestedUserAccs) {
			this.interestedUserAccs = interestedUserAccs;
		}
		public long[] getInterestedUserAccsTimestamp() {
            return interestedUserAccsTimestamp;
        }
        public void setInterestedUserAccsTimestamp(long[] timestamps) {
            this.interestedUserAccsTimestamp = timestamps;
        }
		
		public long getPostId() {
			return postId;
		}
		public void setPostId(long postId) {
			this.postId = postId;
		}
		public String getTitle() {
			return title;
		}
		public void setTitle(String title) {
			this.title = title;
		}
		public int getLanguage() {
		    return language;
		}
		public void setLanguage(int language) {
		    this.language = language;
		}
		public String getContent() {
			return content;
		}
		public void setContent(String content) {
			this.content = content;
		}
		public long getCreatedDate() {
			return createdDate;
		}
		public void setCreatedDate(long createdDate) {
			this.createdDate = createdDate;
		}
		public int getAuthorId() {
			return authorId;
		}
		public void setAuthorId(int authorId) {
			this.authorId = authorId;
		}
		public int getForumId() {
			return forumId;
		}
		public void setForumId(int forumId) {
			this.forumId = forumId;
		}
		public int getArticleIdx() {
			return articleIdx;
		}
		public void setArticleIdx(int articleIdx) {
			this.articleIdx = articleIdx;
		}
		public String getUserAgent() {
			return userAgent;
		}
		public void setUserAgent(String userAgent) {
			this.userAgent = userAgent;
		}
		public IP getIpAddress() {
			return ipAddress;
		}
		public void setIpAddress(IP ipAddress) {
			this.ipAddress = ipAddress;
		}
		public boolean isInterestPost() {
			return isInterestPost;
		}
		public void setInterestPost(boolean isInterestPost) {
			this.isInterestPost = isInterestPost;
		}
		public byte getBrowserIdx() {
			return browserIdx;
		}
		public void setBrowserIdx(byte browserId) {
			this.browserIdx = browserId;
		}		
}
