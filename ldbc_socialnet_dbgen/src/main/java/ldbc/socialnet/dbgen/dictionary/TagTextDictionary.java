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
package ldbc.socialnet.dbgen.dictionary;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Vector;

import ldbc.socialnet.dbgen.generator.DateGenerator;
import ldbc.socialnet.dbgen.generator.ScalableGenerator;
import ldbc.socialnet.dbgen.objects.Comment;
import ldbc.socialnet.dbgen.objects.Friend;
import ldbc.socialnet.dbgen.objects.Group;
import ldbc.socialnet.dbgen.objects.GroupMemberShip;
import ldbc.socialnet.dbgen.objects.Post;
import ldbc.socialnet.dbgen.objects.ReducedUserProfile;
import ldbc.socialnet.dbgen.objects.Tag;


public class TagTextDictionary {
	
    public static int commentId = -1;
    
	HashMap<Integer, String> tagText;
	
	String dicFileName;
	
	DateGenerator dateGen;
	Random rand;
	Random randReduceText;
	Random randTextSize;
	Random randReplyTo;
	
	int minSizeOfText;
	int maxSizeOfText;
    int minSizeOfComment;
    int maxSizeOfComment;
    int reduceTextSize;
    double reduceTextRatio;
	
	public TagTextDictionary(String dicFileName, DateGenerator dateGen, int minSizeOfText, int maxSizeOfText,
            int minSizeOfComment, int maxSizeOfComment, double reduceTextRatio, long seed, long seedTextSize){
		this.dicFileName = dicFileName;
		this.tagText = new HashMap<Integer, String>();
		this.dateGen = dateGen;
		rand = new Random(seed);
		randReduceText = new Random(seed);
		randReplyTo = new Random(seed);
		randTextSize = new Random(seedTextSize);
		this.minSizeOfText = minSizeOfText;
		this.maxSizeOfText = maxSizeOfText;
		this.minSizeOfComment = minSizeOfComment;
		this.maxSizeOfComment = maxSizeOfComment;
		this.reduceTextRatio = reduceTextRatio;
		this.reduceTextSize = maxSizeOfText >> 1;
	}
	
	public void initialize() {
	    try {
	        BufferedReader dictionary = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(dicFileName), "UTF-8"));
	        String line;
	        while ((line = dictionary.readLine()) != null){
	            String[] splitted = line.split("  ");
	            Integer id = Integer.valueOf(splitted[0]);
	            tagText.put(id, splitted[1]);
	        }
	        dictionary.close();
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
	}
	
	public String getTagText(int id) {
	    return tagText.get(id);
	}
	
	public String getRandomText(int id) {

        int textSize;
        int startingPos;
        String finalString = "";

        String content = getTagText(id);
        
        // Generate random fragment from the content 
        if (randReduceText.nextDouble() > reduceTextRatio){
            textSize = randTextSize.nextInt(maxSizeOfText - minSizeOfText) + minSizeOfText;
        }
        else{
            textSize = randTextSize.nextInt(reduceTextSize - minSizeOfText) + minSizeOfText;
        }

        if (textSize >= content.length()) {
            return content;
        } else {
            // Get the starting position for the fragment of text
            startingPos = randTextSize.nextInt(content.length() - textSize);
            finalString = content.substring(startingPos, startingPos + textSize - 1);
            
            int posSpace = finalString.indexOf(" ");
            String returnString = (posSpace != -1) ? finalString.substring(posSpace).trim() : finalString;
            posSpace = returnString.lastIndexOf(" ");
            if (posSpace != -1){
                returnString = returnString.substring(0, posSpace);
            }
            
            return returnString;
        }
    }
	
	public int[] getLikeFriends(ReducedUserProfile user, int numberOfLikes) {
	    Friend[] friendList = user.getFriendList();
	    int numFriends = friendList.length;
	    int[] friends;
        if (numberOfLikes >= numFriends){
            friends = new int[numFriends];
            for (int i = 0; i < numFriends; i++) {
                friends[i] = friendList[i].getFriendAcc();
            }
        } else {
            friends = new int[numberOfLikes];
            int startIdx = rand.nextInt(numFriends - numberOfLikes);
            for (int i = 0; i < numberOfLikes; i++) {
                friends[i] = friendList[i+startIdx].getFriendAcc();
            }
        }
        
        return friends;
	}
	
	public int[] getLikeFriends(Group group, int numOfLikes){
        GroupMemberShip groupMembers[] = group.getMemberShips();

        int numAddedMember = group.getNumMemberAdded();
        int friends[];
        if (numOfLikes >= numAddedMember){
            friends = new int[numAddedMember];
            for (int j = 0; j < numAddedMember; j++){
                friends[j] = groupMembers[j].getUserId();
            }
        } else{
            friends = new int[numOfLikes];
            int startIdx = rand.nextInt(numAddedMember - numOfLikes);
            for (int j = 0; j < numOfLikes; j++){
                friends[j] = groupMembers[j+startIdx].getUserId();
            }           
        }
        return friends; 
    }
	
	public Post createPost(ReducedUserProfile user, int maxNumberOfLikes) {
        
        ScalableGenerator.postId++;
        Post post = new Post();
        post.setPostId(ScalableGenerator.postId);
        
        post.setAuthorId(user.getAccountId());
        post.setCreatedDate(dateGen.randomPostCreatedDate(user));
        post.setForumId(user.getAccountId() * 2);
        
        HashSet<Integer> tags = new HashSet<Integer>();
        Iterator<Integer> it = user.getSetOfTags().iterator();
        while (it.hasNext()) {
            Integer value = it.next();
            if (tags.isEmpty()) {
                tags.add(value);
                post.setContent(getRandomText(value));
            } else {
                if (rand.nextDouble() < 0.2) {
                    tags.add(value);
                }
            }
        }
        post.setTags(tags);        
        
        int numberOfLikes = rand.nextInt(maxNumberOfLikes);
        int[] likes = getLikeFriends(user, numberOfLikes);
        post.setInterestedUserAccs(likes);
        long[] likeTimestamp = new long[likes.length];
        for (int i = 0; i < likes.length; i++) {
            likeTimestamp[i] = (long)(rand.nextDouble()*DateGenerator.sevenDayInMillis+post.getCreatedDate());
        }
        post.setInterestedUserAccsTimestamp(likeTimestamp);
        
        return post;
    }
	
public Post createPost(Group group, int maxNumberOfLikes) {
        
        ScalableGenerator.postId++;
        Post post = new Post();
        post.setPostId(ScalableGenerator.postId);
        
        int memberIdx = rand.nextInt(group.getNumMemberAdded());
        GroupMemberShip memberShip = group.getMemberShips()[memberIdx];
        
        post.setAuthorId(memberShip.getUserId());
        post.setCreatedDate(dateGen.randomGroupPostCreatedDate(memberShip.getJoinDate()));
        post.setForumId(group.getForumWallId());
        
        HashSet<Integer> tags = new HashSet<Integer>();
        for (int i = 0; i < group.getTags().length; i++) {
            tags.add(group.getTags()[i]);
        }
        post.setTags(tags); 
        post.setContent(getRandomText(group.getTags()[0]));
        
        int numberOfLikes = rand.nextInt(maxNumberOfLikes);
        
        int[] likes = getLikeFriends(group, numberOfLikes);
        post.setInterestedUserAccs(likes);
        long[] likeTimestamp = new long[likes.length];
        for (int i = 0; i < likes.length; i++) {
            likeTimestamp[i] = (long)(rand.nextDouble()*DateGenerator.sevenDayInMillis+post.getCreatedDate());
        }
        post.setInterestedUserAccsTimestamp(likeTimestamp);
        
        return post;
    }

    public long getReplyToId(long startId, long lastId) {
        int parentId; 
        if (lastId > (startId+1)){
            parentId = randReplyTo.nextInt((int)(lastId - startId));
            if (parentId == 0) return -1; 
            else return (long)(parentId + startId); 
        }

        return -1; 
    }

	public Comment createComment(Post post, ReducedUserProfile user, 
	        long lastCommentCreatedDate, long startCommentId, long lastCommentId,
	        UserAgentDictionary userAgentDic, IPAddressDictionary ipAddDic,
            BrowserDictionary browserDic) {

	    Comment comment = new Comment();

	    // For userId, randomly select from one of the friends
	    int friendIdx = rand.nextInt(user.getNumFriends());
	    
	    // Only friend whose the friendship created before the createdDate of the post gives the comment
	    Friend friend = user.getFriendList()[friendIdx];
	    if (friend == null || (friend.getCreatedTime() > post.getCreatedDate()) || (friend.getCreatedTime() == -1)){
	        comment.setAuthorId(-1);
	        return comment;
	    }

	    commentId++;
	    comment.setAuthorId(friend.getFriendAcc());
	    comment.setCommentId(commentId);
	    comment.setPostId(post.getPostId());
	    comment.setReply_of(getReplyToId(startCommentId, lastCommentId));
	    comment.setForumId(post.getForumId());

	    userAgentDic.setCommentUserAgent(friend, comment);
	    ipAddDic.setCommentIPAdress(friend.isFrequentChange(), friend.getSourceIp(), comment);
	    comment.setBrowserIdx(browserDic.getPostBrowserId(friend.getBrowserIdx()));

	    Iterator<Integer> it = post.getTags().iterator();
	    comment.setContent(getRandomText(it.next()));

	    comment.setCreateDate(dateGen.powerlawCommDateDay(lastCommentCreatedDate));

	    return comment;
	}
	
	public Comment createComment(Post post, Group group, 
            long lastCommentCreatedDate, long startCommentId, long lastCommentId,
            UserAgentDictionary userAgentDic, IPAddressDictionary ipAddDic,
            BrowserDictionary browserDic) {

        Comment comment = new Comment();

        // For userId, randomly select from one of the friends
        int memberIdx = rand.nextInt(group.getNumMemberAdded());
        GroupMemberShip memberShip = group.getMemberShips()[memberIdx];
        
        if (memberShip.getJoinDate() > post.getCreatedDate()){
            comment.setAuthorId(-1);
            return comment;
        }

        commentId++;
        comment.setAuthorId(memberShip.getUserId());
        comment.setCommentId(commentId);
        comment.setPostId(post.getPostId());
        comment.setReply_of(getReplyToId(startCommentId, lastCommentId));
        comment.setForumId(post.getForumId());

//        Unknow right now
//        userAgentDic.setCommentUserAgent(friend, comment);
//        ipAddDic.setCommentIPAdress(friend.isFrequentChange(), friend.getSourceIp(), comment);
//        comment.setBrowserIdx(browserDic.getPostBrowserId(friend.getBrowserIdx()));
        comment.setUserAgent("");
        comment.setIpAddress(null);
        comment.setBrowserIdx((byte) -1);

        Iterator<Integer> it = post.getTags().iterator();
        comment.setContent(getRandomText(it.next()));

        comment.setCreateDate(dateGen.powerlawCommDateDay(lastCommentCreatedDate));

        return comment;
    }
}
