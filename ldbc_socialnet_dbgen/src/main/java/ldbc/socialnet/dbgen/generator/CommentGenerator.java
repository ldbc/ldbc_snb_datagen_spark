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

package ldbc.socialnet.dbgen.generator;

import java.util.ArrayList;
import java.util.Random;
import java.util.TreeSet;
import java.util.Iterator;

import ldbc.socialnet.dbgen.dictionary.*;
import ldbc.socialnet.dbgen.objects.*;
import ldbc.socialnet.dbgen.util.RandomGeneratorFarm;


public class CommentGenerator {

    private TagDictionary tagDictionary;
    private TagTextDictionary tagTextDic; /**< @brief The TagTextDictionary used to obtain the texts posts/comments.**/
    private TagMatrix tagMatrix;
    private DateGenerator dateGen;

    private int minSizeOfComment;          /**< @brief The minimum size of a comment.*/
    private int maxSizeOfComment;          /**< @brief The maximum size of a comment.*/       
    private int minLargeSizeOfComment;     /**< @brief The minimum size of large comments.*/ 
    private int maxLargeSizeOfComment;     /**< @brief The maximum size of large comments.*/
    private double largeCommentRatio;      /**< @brief The ratio of large comments.*/
    private boolean generateText;          /**< @brief To generate the text of comments.*/
    private int maxNumberOfLikes;
    private long deltaTime;                 /**< @brief The minimum time to span between post creation and its reply.*/

    private String[] shortComments = {"ok", "good", "great", "cool", "thx", "fine", "LOL", "roflol", "no way!", "I see", "right", "yes", "no", "duh", "thanks", "maybe"};
	
	public CommentGenerator( TagDictionary tagDictionary,
                             TagTextDictionary tagTextDic,
                             TagMatrix tagMatrix,
                             DateGenerator dateGen,
                             int minSizeOfComment, 
                             int maxSizeOfComment, 
                             int minLargeSizeOfComment,
                             int maxLargeSizeOfComment,
                             double largeCommentRatio,
                             int maxNumberOfLikes,
                             boolean generateText,
                             long deltaTime
                             ){
        this.tagDictionary = tagDictionary;
        this.tagTextDic = tagTextDic;
        this.tagMatrix = tagMatrix;
        this.dateGen = dateGen;
		this.minSizeOfComment = minSizeOfComment;
		this.maxSizeOfComment = maxSizeOfComment;
        this.minLargeSizeOfComment = minLargeSizeOfComment; 
        this.maxLargeSizeOfComment = maxLargeSizeOfComment; 
        this.largeCommentRatio = largeCommentRatio;
        this.generateText = generateText;
        this.maxNumberOfLikes = maxNumberOfLikes;
        this.deltaTime = deltaTime;
	}
	
	public void initialize() {
	}

    /** @brief Gets an array of likes for a user.
     *  @param[in] user The user.
     *  @return The array of generated likes.*/
    private long[] generateLikeFriends( Random randomNumLikes, ReducedUserProfile user, int numberOfLikes) {
        Friend[] friendList = user.getFriendList();
        int numFriends = user.getNumFriendsAdded();
        long[] friends;
        if (numberOfLikes >= numFriends){
            friends = new long[numFriends];
            for (int i = 0; i < numFriends; i++) {
                friends[i] = friendList[i].getFriendAcc();
            }
        } else {
            friends = new long[numberOfLikes];
            int startIdx = randomNumLikes.nextInt(numFriends - numberOfLikes);
            for (int i = 0; i < numberOfLikes ; i++) {
                friends[i] = friendList[i+startIdx].getFriendAcc();
            }
        }
        return friends;
    }

    /** @brief Gets an array of likes for a group .
     *  @param[in] group The group.
     *  @param[in] numOfLikes The number of likes we want to generate
     *  @return The array of generated likes.*/
    private long[] generateLikeFriends(Random randomNumLikes, Group group, int numOfLikes){
        GroupMemberShip groupMembers[] = group.getMemberShips();

        int numAddedMember = group.getNumMemberAdded();
        long friends[];
        if (numOfLikes >= numAddedMember){
            friends = new long[numAddedMember];
            for (int j = 0; j < numAddedMember; j++){
                friends[j] = groupMembers[j].getUserId();
            }
        } else{
            friends = new long[numOfLikes];
            int startIdx = randomNumLikes.nextInt(numAddedMember - numOfLikes);
            for (int j = 0; j < numOfLikes; j++){
                friends[j] = groupMembers[j+startIdx].getUserId();
            }
        }
        return friends;
    }

    /** @brief Assigns a set of likes to a post created by a user.
     *  @param[in] user The user that created the post.*/
    private void setLikes( Random randomNumLikes, Random randomDate, Message message, ReducedUserProfile user ) {
        int numberOfLikes = randomNumLikes.nextInt(maxNumberOfLikes);
        long[] likes = generateLikeFriends(randomNumLikes,user, numberOfLikes);
        message.setInterestedUserAccs(likes);
        long[] likeTimestamp = new long[likes.length];
        for (int i = 0; i < likes.length; i++) {
            likeTimestamp[i] = (long)(randomDate.nextDouble()*DateGenerator.SEVEN_DAYS+message.getCreationDate()+deltaTime);
        }
        message.setInterestedUserAccsTimestamp(likeTimestamp);
    }

    /** @brief Assigns a set of likes to a post created by a user.
     *  @param[in] group The group where the post was created.*/
    private void setLikes( Random randomNumLikes, Random randomDate, Message message, Group group ) {
        int numberOfLikes = randomNumLikes.nextInt(maxNumberOfLikes);
        long[] likes = generateLikeFriends(randomNumLikes, group, numberOfLikes);
        message.setInterestedUserAccs(likes);
        long[] likeTimestamp = new long[likes.length];
        for (int i = 0; i < likes.length; i++) {
            likeTimestamp[i] = (long)(randomDate.nextDouble()*DateGenerator.SEVEN_DAYS+message.getCreationDate()+deltaTime);
        }
        message.setInterestedUserAccsTimestamp(likeTimestamp);
    }

    public Comment createComment(RandomGeneratorFarm randomFarm, long commentId, Post post, Message replyTo, ReducedUserProfile user,
                                 UserAgentDictionary userAgentDic, IPAddressDictionary ipAddDic,
                                 BrowserDictionary browserDic) {

        ArrayList<Integer> validIds = new ArrayList<Integer>();
        Friend[] friends = user.getFriendList();
        for (int i = 0; i <user.getNumFriendsAdded(); i++) {
            if ((friends[i].getCreatedTime()+deltaTime <= post.getCreationDate()) || (friends[i].getCreatedTime() == -1)){
                validIds.add(i);
            }
        }
        if (validIds.size() == 0) {
            return null;
        }

        int friendIdx = randomFarm.get(RandomGeneratorFarm.Aspect.FRIEND).nextInt(validIds.size());
        Friend friend = user.getFriendList()[friendIdx];

        TreeSet<Integer> tags = new TreeSet<Integer>();
        String content = "";
        int textSize;
        if( randomFarm.get(RandomGeneratorFarm.Aspect.REDUCED_TEXT).nextDouble() > 0.6666) {
            if( user.isLargePoster() && randomFarm.get(RandomGeneratorFarm.Aspect.LARGE_TEXT).nextDouble() > (1.0f-largeCommentRatio) ) {
                textSize = tagTextDic.getRandomLargeTextSize(randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE), minLargeSizeOfComment, maxLargeSizeOfComment);
            } else {
                textSize = tagTextDic.getRandomTextSize( randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE), randomFarm.get(RandomGeneratorFarm.Aspect.REDUCED_TEXT), minSizeOfComment, maxSizeOfComment);
            }

            ArrayList<Integer> currentTags = new ArrayList<Integer>();
            Iterator<Integer> it = replyTo.getTags().iterator();
            while(it.hasNext()) {
                Integer tag = it.next();
                if( randomFarm.get(RandomGeneratorFarm.Aspect.TAG).nextDouble() > 0.5) {
                    tags.add(tag);
                }
                currentTags.add(tag);
            }

            for( int i = 0; i < (int)Math.ceil(replyTo.getTags().size() / 2.0); ++i) {
                int randomTag = currentTags.get(randomFarm.get(RandomGeneratorFarm.Aspect.TAG).nextInt(currentTags.size()));
                tags.add(tagMatrix.getRandomRelated(randomFarm.get(RandomGeneratorFarm.Aspect.TOPIC), randomFarm.get(RandomGeneratorFarm.Aspect.TAG),randomTag));
            }

            if( generateText ) {
                content = tagTextDic.generateText(randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE), tags, textSize );
                if( content.length() != textSize ) {
                    System.out.println("ERROR while generating text - content size: "+ content.length()+", actual size: "+ textSize);
                    System.exit(-1);
                }
            }
        } else {
            int index = randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE).nextInt(shortComments.length);
            textSize = shortComments[index].length();
            if( generateText ) {
                content = shortComments[index];
            }
        }
        long creationDate = dateGen.powerlawCommDateDay(randomFarm.get(RandomGeneratorFarm.Aspect.DATE),replyTo.getCreationDate()+deltaTime);
        Comment comment = new Comment( commentId,
                                       content,
                                       textSize,
                                       creationDate,
                                       friend.getFriendAcc(),
                                       post.getGroupId(),
                                       tags,
                                       ipAddDic.getIP(randomFarm.get(RandomGeneratorFarm.Aspect.IP), randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP), randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP_FOR_TRAVELER),friend.getSourceIp(), friend.isFrequentChange(), creationDate),
                                       userAgentDic.getUserAgentName(randomFarm.get(RandomGeneratorFarm.Aspect.USER_AGENT),friend.isHaveSmartPhone(), friend.getAgentIdx()),
                                       browserDic.getPostBrowserId(randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_BROWSER),randomFarm.get(RandomGeneratorFarm.Aspect.BROWSER),friend.getBrowserIdx()),
                                       post.getMessageId(),
                                       replyTo.getMessageId());
        setLikes(randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LIKE), randomFarm.get(RandomGeneratorFarm.Aspect.DATE),comment, user);
        return comment;
    }
    
    public Comment createComment(RandomGeneratorFarm randomFarm, long commentId, Post post, Message replyTo , Group group,
            UserAgentDictionary userAgentDic, IPAddressDictionary ipAddDic,
            BrowserDictionary browserDic) {

        ArrayList<Integer> validIds = new ArrayList<Integer>();
        GroupMemberShip[] memberShips = group.getMemberShips();
        for (int i = 0; i <group.getNumMemberAdded(); i++) {
            if (memberShips[i].getJoinDate()+deltaTime <= post.getCreationDate()){
                validIds.add(i);
            }
        }
        if (validIds.size() == 0) {
            return null;
        }

        int memberIdx = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX).nextInt(validIds.size());
        GroupMemberShip membership = group.getMemberShips()[memberIdx];

        TreeSet<Integer> tags = new TreeSet<Integer>();
        String content = "";
        int textSize;
        if( randomFarm.get(RandomGeneratorFarm.Aspect.REDUCED_TEXT).nextDouble() > 0.6666) {
            if( membership.isLargePoster() && randomFarm.get(RandomGeneratorFarm.Aspect.LARGE_TEXT).nextDouble() > (1.0f-largeCommentRatio) ) {
                textSize = tagTextDic.getRandomLargeTextSize(randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE), minLargeSizeOfComment, maxLargeSizeOfComment);
            } else {
                textSize = tagTextDic.getRandomTextSize( randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE), randomFarm.get(RandomGeneratorFarm.Aspect.REDUCED_TEXT), minSizeOfComment, maxSizeOfComment);
            }

            ArrayList<Integer> currentTags = new ArrayList<Integer>();
            Iterator<Integer> it = replyTo.getTags().iterator();
            while(it.hasNext()) {
                Integer tag = it.next();
                if( randomFarm.get(RandomGeneratorFarm.Aspect.TAG).nextDouble() > 0.5) {
                    tags.add(tag);
                    tags.add(tagMatrix.getRandomRelated(randomFarm.get(RandomGeneratorFarm.Aspect.TOPIC), randomFarm.get(RandomGeneratorFarm.Aspect.TAG),tag));
                }
                currentTags.add(tag);
            }

            for( int i = 0; i < (int)Math.ceil(replyTo.getTags().size() / 2.0); ++i) {
                int randomTag = currentTags.get(randomFarm.get(RandomGeneratorFarm.Aspect.TAG).nextInt(currentTags.size()));
                tags.add(tagMatrix.getRandomRelated(randomFarm.get(RandomGeneratorFarm.Aspect.TOPIC), randomFarm.get(RandomGeneratorFarm.Aspect.TAG),randomTag));
            }

            if( generateText ) {
                content = tagTextDic.generateText(randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE), tags, textSize );
                if( content.length() != textSize ) {
                    System.out.println("ERROR while generating text - content size: "+ content.length()+", actual size: "+ textSize);
                    System.exit(-1);
                }
            }
        } else {
            int index = randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE).nextInt(shortComments.length);
            textSize = shortComments[index].length();
            if( generateText ) {
                content = shortComments[index];
            }
        }

        long creationDate = dateGen.powerlawCommDateDay(randomFarm.get(RandomGeneratorFarm.Aspect.DATE),replyTo.getCreationDate()+deltaTime);
        Comment comment = new Comment( commentId,
                content,
                textSize,
                creationDate,
                membership.getUserId(),
                post.getGroupId(),
                tags,
                ipAddDic.getIP(randomFarm.get(RandomGeneratorFarm.Aspect.IP), randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP), randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP_FOR_TRAVELER), membership.getIP(), membership.isFrequentChange(), creationDate),
                userAgentDic.getUserAgentName(randomFarm.get(RandomGeneratorFarm.Aspect.USER_AGENT),membership.isHaveSmartPhone(), membership.getAgentIdx()),
                browserDic.getPostBrowserId(randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_BROWSER),randomFarm.get(RandomGeneratorFarm.Aspect.BROWSER),membership.getBrowserIdx()),
                post.getMessageId(),
                replyTo.getMessageId());

        setLikes(randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LIKE), randomFarm.get(RandomGeneratorFarm.Aspect.DATE), comment, group);
        return comment;
    }
}
