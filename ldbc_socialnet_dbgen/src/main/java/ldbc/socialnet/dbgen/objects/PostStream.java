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

import java.io.Serializable;
import java.util.HashSet;

public class PostStream  implements Serializable{
	boolean isaPost = true; 
	
	long postId; 
	String title; 
	String content; 
	long createdDate; 
	int authorId; 
	int forumId;
	
	int articleIdx;					// Index of articles in the set of same region/interest article  					
	int interestIdx;				// Index of one interest in user's interests
	
	HashSet<Integer> tags; 
	int interestedUserAccs[];		//List of users who are interested in the post 
	long interestedUserAccsTimestamp[];
	
	IP ipAddress; 
	String userAgent;				// Send from where e.g., iPhone, Samsung, HTC
	
	byte browserIdx;					// Set browser Idx 
	
	boolean isInterestPost; 		//Only use for group's post
	
	long commentId; 
	
	long reply_of; 			//Id of the parent post/comment of this comment
	
	
	public PostStream(){}
	public PostStream(Post post){
		isaPost = true; 
		postId = post.getPostId(); 
		title = post.getTitle();
		content = post.getContent();
		createdDate = post.getCreatedDate(); 
		authorId = post.getAuthorId();
		forumId = post.getForumId(); 
		articleIdx = post.getArticleIdx();
		interestIdx = post.getInterestIdx();
		
		tags = post.getTags();
		interestedUserAccs = post.getInterestedUserAccs(); 
		ipAddress = post.getIpAddress();
		userAgent = post.getUserAgent(); 
		browserIdx = post.getBrowserIdx(); 
		isInterestPost = post.isInterestPost();
	}
	
	public PostStream(Comment comment){
		isaPost = false; 
		postId = comment.getPostId(); 
		content = comment.getContent();
		createdDate = comment.getCreateDate(); 
		authorId = comment.getAuthorId();
		forumId = comment.getForumId(); 
		
		ipAddress = comment.getIpAddress();
		userAgent = comment.getUserAgent(); 
		browserIdx = comment.getBrowserIdx();
		
		commentId = comment.getCommentId(); 
		reply_of = comment.getReply_of();
	}
	
	public Comment getComment(){
		Comment comment = new Comment();
		comment.setPostId(postId);
		comment.setContent(content);
		comment.setCreateDate(createdDate);
		comment.setAuthorId(authorId);
		comment.setForumId(forumId);
		comment.setIpAddress(ipAddress);
		comment.setUserAgent(userAgent);
		comment.setBrowserIdx(browserIdx);
		comment.setCommentId(commentId);
		comment.setReply_of(reply_of);
		
		return comment; 
	}
	public Post getPost(){
		Post post = new Post();
		post.setPostId(postId);
		post.setTitle(title);
		post.setContent(content);
		post.setCreatedDate(createdDate);
		post.setAuthorId(authorId);
		post.setForumId(forumId);
		post.setArticleIdx(articleIdx);
		post.setInterestIdx(interestIdx);
		post.setTags(tags);
		post.setIpAddress(ipAddress);
		post.setUserAgent(userAgent);
		post.setBrowserIdx(browserIdx);
		post.setInterestedUserAccs(interestedUserAccs);
		post.setInterestedUserAccsTimestamp(interestedUserAccsTimestamp);
		post.setInterestPost(isInterestPost);
		
		return post; 
	}
	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public long getCreatedDate() {
		return createdDate;
	}

	public void setCreatedDate(long createdDate) {
		this.createdDate = createdDate;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
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
    public void setInterestedUserAccsTimestamp(long[] timestamp) {
        this.interestedUserAccsTimestamp = timestamp;
    }
	public boolean isIsaPost() {
		return isaPost;
	}
	public void setIsaPost(boolean isaPost) {
		this.isaPost = isaPost;
	}
	
	public void printPostStream(){
		System.out.println("postId : " + postId );
		System.out.println("title : " + title );
		System.out.println("content : " + content );
		System.out.println("createdDate : " + createdDate );
		System.out.println("authorId : " + authorId );
		System.out.println("forumId : " + forumId );
		System.out.println("articleIdx : " + articleIdx );
		System.out.println("interestIdx : " + interestIdx );
		System.out.println("tags : " + tags.size() );
		System.out.println("interestedUserAccs : " + interestedUserAccs.length );
		System.out.println("interestedUserAccsTimestamp : " + interestedUserAccsTimestamp.length );
		System.out.println("ipAddress : " + ipAddress.toString() );
		System.out.println("userAgent : " + userAgent );
		System.out.println("browserIdx : " + browserIdx );
		System.out.println("commentId : " + commentId );
		System.out.println("reply_of : " + reply_of );
	}

}
