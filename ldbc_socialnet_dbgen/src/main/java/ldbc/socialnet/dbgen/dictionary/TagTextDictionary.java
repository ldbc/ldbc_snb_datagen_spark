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
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.Iterator;
import java.util.Random;

import ldbc.socialnet.dbgen.generator.DateGenerator;
import ldbc.socialnet.dbgen.generator.ScalableGenerator;
import ldbc.socialnet.dbgen.objects.Comment;
import ldbc.socialnet.dbgen.objects.Friend;
import ldbc.socialnet.dbgen.objects.Group;
import ldbc.socialnet.dbgen.objects.GroupMemberShip;
import ldbc.socialnet.dbgen.objects.Post;
import ldbc.socialnet.dbgen.objects.ReducedUserProfile;


public class TagTextDictionary {
	
    public static int commentId = -1;
    
    private static final String SEPARATOR = "  ";
    
    String dicFileName;
    DateGenerator dateGen;
    
    TagDictionary tagDic;
	HashMap<Integer, String> tagText;
	
	Random rand;
	Random randReplyTo;
	Random randTextSize;
	Random randReduceText;
    Random randLargeText;
	
	int minSizeOfText;
	int maxSizeOfText;
	int reduceTextSize;
    int minSizeOfComment;
    int maxSizeOfComment;
    int minLargeSizeOfText;
    int maxLargeSizeOfText;
    int minLargeSizeOfComment;
    int maxLargeSizeOfComment;
    double reduceTextRatio;
    double largeTextRatio;
	
	public TagTextDictionary(String dicFileName, DateGenerator dateGen, TagDictionary tagDic, 
	        int minSizeOfText, int maxSizeOfText, int minSizeOfComment, int maxSizeOfComment, 
	        double reduceTextRatio, int minLargeSizeOfText, int maxLargeSizeOfText, int minLargeSizeOfComment,
            int maxLargeSizeOfComment, double largeTextRatio, long seed, long seedTextSize){
		this.dicFileName = dicFileName;
		this.tagText = new HashMap<Integer, String>();
		this.dateGen = dateGen;
		this.tagDic = tagDic;
		this.rand = new Random(seed);
		this.randReduceText = new Random(seed);
		this.randReplyTo = new Random(seed);
		this.randTextSize = new Random(seedTextSize);
		this.minSizeOfText = minSizeOfText;
		this.maxSizeOfText = maxSizeOfText;
		this.minSizeOfComment = minSizeOfComment;
		this.maxSizeOfComment = maxSizeOfComment;
		this.reduceTextRatio = reduceTextRatio;
		this.reduceTextSize = maxSizeOfText >> 1;
        this.minLargeSizeOfText = minLargeSizeOfText; 
        this.maxLargeSizeOfText = maxLargeSizeOfText; 
        this.minLargeSizeOfComment = minLargeSizeOfComment; 
        this.maxLargeSizeOfComment = maxLargeSizeOfComment; 
        this.randLargeText = new Random(seed);
        this.largeTextRatio = largeTextRatio;
	}
	
	public void initialize() {
	    try {
	        BufferedReader dictionary = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(dicFileName), "UTF-8"));
	        String line;
	        while ((line = dictionary.readLine()) != null){
	            String[] data = line.split(SEPARATOR);
	            Integer id = Integer.valueOf(data[0]);
	            tagText.put(id, data[1]);
	        }
	        dictionary.close();
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
	}
	
	public String getTagText(int id) {
	    return tagText.get(id);
	}
	
	public String getRandomText(TreeSet<Integer> tags) {

        int textSize;
        int startingPos;
        String returnString = "";
        
        // Generate random fragment from the content 
        if (randReduceText.nextDouble() > reduceTextRatio){
            textSize = randTextSize.nextInt(maxSizeOfText - minSizeOfText) + minSizeOfText;
        }
        else{
            textSize = randTextSize.nextInt(reduceTextSize - minSizeOfText) + minSizeOfText;
        }

        int textSizePerTag = textSize / tags.size();
        Iterator<Integer> it = tags.iterator();
        while (it.hasNext()) {
            Integer tag = it.next();
            String content = getTagText(tag);
            if (textSizePerTag >= content.length()) {
                returnString += content;
            } else {
                startingPos = randTextSize.nextInt(content.length() - textSizePerTag);
                String finalString = content.substring(startingPos, startingPos + textSizePerTag - 1);
                
                String tagName = tagDic.getName(tag).replace("_", " ");
                tagName = tagName.replace("\"", "\\\"");
                String prefix = "About " +tagName+ ", ";

                int posSpace = finalString.indexOf(" ");
                returnString += (posSpace != -1) ? prefix + finalString.substring(posSpace).trim() : prefix + finalString;
                posSpace = returnString.lastIndexOf(" ");
                if (posSpace != -1){
                    returnString = returnString.substring(0, posSpace);
                }
            }
            if (!returnString.endsWith(".")) {
                returnString =  returnString + ".";
            }
            if (it.hasNext()) {
                returnString += " ";
            }
        }
        return returnString.replace("|", " ");
    }

    public String getRandomLargeText(TreeSet<Integer> tags) {
            if( randLargeText.nextDouble() > (1.0f-largeTextRatio) ) {
               int textSize = randLargeText.nextInt(maxLargeSizeOfText - minLargeSizeOfText) + minLargeSizeOfText;
               return generateRandomString( tags, textSize );
            } else {
               return getRandomText(tags);
            }
    }

    private String generateRandomString(TreeSet<Integer> tags, int length ) {
/*        Random rand = new Random();
        char[] text = new char[length];
        for (int i = 0; i < length; i++)
        {
            text[i] = (char)rand.nextInt(256);
        }
        return new String(text);
        */
        String content = new String(); 
        Iterator<Integer> it = tags.iterator();
        while(content.length() < length) {
            if (!it.hasNext()){
                it = tags.iterator();
            }
            Integer tag = it.next();
            String tagContent = getTagText(tag);
            if( content.length() + tagContent.length() < length) {
                content = content.concat(tagContent);
            } else {
                content = content.concat(tagContent.substring(0,length - content.length()));
            }
        }
        return content;
    }

	public int[] getLikeFriends(ReducedUserProfile user, int numberOfLikes) {
	    Friend[] friendList = user.getFriendList();
	    int numFriends = user.getNumFriendsAdded();
	    int[] friends;
        if (numberOfLikes >= numFriends){
            friends = new int[numFriends];
            for (int i = 0; i < numFriends; i++) {
                friends[i] = friendList[i].getFriendAcc();
            }
        } else {
            friends = new int[numberOfLikes];
            int startIdx = rand.nextInt(numFriends - numberOfLikes);
            for (int i = 0; i < numberOfLikes ; i++) {
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
	
	public Post createPost(ReducedUserProfile user, int maxNumberOfLikes,
	        UserAgentDictionary userAgentDic, IPAddressDictionary ipAddDic,
            BrowserDictionary browserDic) {
        
        ScalableGenerator.postId++;
        Post post = new Post();
        post.setPostId(ScalableGenerator.postId);
        
        post.setAuthorId(user.getAccountId());
        post.setCreatedDate(dateGen.randomPostCreatedDate(user));
        post.setForumId(user.getAccountId() * 2);
        post.setUserAgent(userAgentDic.getUserAgentName(user.isHaveSmartPhone(), user.getAgentIdx()));
        post.setIpAddress(ipAddDic.getIP(user.getIpAddress(), user.isFrequentChange(), post.getCreatedDate()));
        post.setBrowserIdx(browserDic.getPostBrowserId(user.getBrowserIdx()));
        
        TreeSet<Integer> tags = new TreeSet<Integer>();
        Iterator<Integer> it = user.getSetOfTags().iterator();
        while (it.hasNext()) {
            Integer value = it.next();
            if (tags.isEmpty()) {
                tags.add(value);
            } else {
                if (rand.nextDouble() < 0.2) {
                    tags.add(value);
                }
            }
        }
        post.setTags(tags);    
        if( user.isLargePoster() ) {
            post.setContent(getRandomLargeText(tags));
            /*if(post.getContent().length() > 1000) {
                System.out.println(post.getPostId());
            }*/
        } else {
            post.setContent(getRandomText(tags));
        }
        
        int numberOfLikes = rand.nextInt(maxNumberOfLikes);
        int[] likes = getLikeFriends(user, numberOfLikes);
        post.setInterestedUserAccs(likes);
        long[] likeTimestamp = new long[likes.length];
        for (int i = 0; i < likes.length; i++) {
            likeTimestamp[i] = (long)(rand.nextDouble()*DateGenerator.SEVEN_DAYS+post.getCreatedDate());
        }
        post.setInterestedUserAccsTimestamp(likeTimestamp);
        
        return post;
    }

	public Post createPost(Group group, int maxNumberOfLikes,
	        UserAgentDictionary userAgentDic, IPAddressDictionary ipAddDic,
	        BrowserDictionary browserDic) {

        ScalableGenerator.postId++;
        Post post = new Post();
        post.setPostId(ScalableGenerator.postId);
        
        int memberIdx = rand.nextInt(group.getNumMemberAdded());
        GroupMemberShip memberShip = group.getMemberShips()[memberIdx];
        
        post.setAuthorId(memberShip.getUserId());
        post.setCreatedDate(dateGen.randomGroupPostCreatedDate(memberShip.getJoinDate()));
        post.setForumId(group.getForumWallId());
        post.setUserAgent(userAgentDic.getUserAgentName(memberShip.isHaveSmartPhone(), memberShip.getAgentIdx()));
        post.setIpAddress(ipAddDic.getIP(memberShip.getIP(), memberShip.isFrequentChange(), post.getCreatedDate()));
        post.setBrowserIdx(browserDic.getPostBrowserId(memberShip.getBrowserIdx()));
        
        TreeSet<Integer> tags = new TreeSet<Integer>();
        for (int i = 0; i < group.getTags().length; i++) {
            tags.add(group.getTags()[i]);
        }
        post.setTags(tags); 

        if( memberShip.isLargePoster() ) {
            post.setContent(getRandomLargeText(tags));
/*            if(post.getContent().length() > 1000) {
                System.out.println(post.getPostId());
            }
            */
        } else {
            post.setContent(getRandomText(tags));
        }
        
        int numberOfLikes = rand.nextInt(maxNumberOfLikes);
        
        int[] likes = getLikeFriends(group, numberOfLikes);
        post.setInterestedUserAccs(likes);
        long[] likeTimestamp = new long[likes.length];
        for (int i = 0; i < likes.length; i++) {
            likeTimestamp[i] = (long)(rand.nextDouble()*DateGenerator.SEVEN_DAYS+post.getCreatedDate());
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
	    
	    
	    ArrayList<Integer> validIds = new ArrayList<Integer>();
	    Friend[] friends = user.getFriendList();
	    for (int i = 0; i <user.getNumFriendsAdded(); i++) {
	        if ((friends[i].getCreatedTime() > post.getCreatedDate()) || (friends[i].getCreatedTime() == -1)){
	            validIds.add(i);
	        }
	    }
	    
	    if (validIds.size() == 0) {
	        comment.setAuthorId(-1);
            return comment;
	    }
	    
	    // For userId, randomly select from one of the friends
//	    int friendIdx = rand.nextInt(user.getNumFriendsAdded());
	    int friendIdx = rand.nextInt(validIds.size());

	    // Only friend whose the friendship created before the createdDate of the post gives the comment
	    Friend friend = user.getFriendList()[friendIdx];
//	    if (friend == null || (friend.getCreatedTime() > post.getCreatedDate()) || (friend.getCreatedTime() == -1)){
//	        comment.setAuthorId(-1);
//	        return comment;
//	    }

	    commentId++;
	    comment.setAuthorId(friend.getFriendAcc());
	    comment.setCommentId(commentId);
	    comment.setPostId(post.getPostId());
	    comment.setReply_of(getReplyToId(startCommentId, lastCommentId));
	    comment.setForumId(post.getForumId());
	    comment.setCreateDate(dateGen.powerlawCommDateDay(lastCommentCreatedDate));

	    comment.setUserAgent(userAgentDic.getUserAgentName(friend.isHaveSmartPhone(), friend.getAgentIdx()));
	    comment.setIpAddress(ipAddDic.getIP(friend.getSourceIp(), friend.isFrequentChange(), comment.getCreateDate()));
	    comment.setBrowserIdx(browserDic.getPostBrowserId(friend.getBrowserIdx()));

        if( user.isLargePoster() ) {
            comment.setContent(getRandomLargeText(post.getTags()));
        } else {
            comment.setContent(getRandomText(post.getTags()));
        }

	    return comment;
	}
	
	public Comment createComment(Post post, Group group, 
            long lastCommentCreatedDate, long startCommentId, long lastCommentId,
            UserAgentDictionary userAgentDic, IPAddressDictionary ipAddDic,
            BrowserDictionary browserDic) {

        Comment comment = new Comment();

        ArrayList<Integer> validIds = new ArrayList<Integer>();
        GroupMemberShip[] memberShips = group.getMemberShips();
        for (int i = 0; i <group.getNumMemberAdded(); i++) {
            if (memberShips[i].getJoinDate() > post.getCreatedDate()){
                validIds.add(i);
            }
        }
        
        if (validIds.size() == 0) {
            comment.setAuthorId(-1);
            return comment;
        }
        int memberIdx = rand.nextInt(validIds.size());
        
        // For userId, randomly select from one of the friends
//        int memberIdx = rand.nextInt(group.getNumMemberAdded());
        GroupMemberShip memberShip = group.getMemberShips()[memberIdx];
//        
//        if (memberShip.getJoinDate() > post.getCreatedDate()){
//            comment.setAuthorId(-1);
//            return comment;
//        }

        commentId++;
        comment.setAuthorId(memberShip.getUserId());
        comment.setCommentId(commentId);
        comment.setPostId(post.getPostId());
        comment.setReply_of(getReplyToId(startCommentId, lastCommentId));
        comment.setForumId(post.getForumId());

        comment.setUserAgent(userAgentDic.getUserAgentName(memberShip.isHaveSmartPhone(), memberShip.getAgentIdx()));
        comment.setIpAddress(ipAddDic.getIP(memberShip.getIP(), memberShip.isFrequentChange(), comment.getCreateDate()));
        comment.setBrowserIdx(browserDic.getPostBrowserId(memberShip.getBrowserIdx()));

        if( memberShip.isLargePoster() ) {
            comment.setContent(getRandomLargeText(post.getTags()));
        } else {
            comment.setContent(getRandomText(post.getTags()));
        }

        comment.setCreateDate(dateGen.powerlawCommDateDay(lastCommentCreatedDate));

        return comment;
    }
}
