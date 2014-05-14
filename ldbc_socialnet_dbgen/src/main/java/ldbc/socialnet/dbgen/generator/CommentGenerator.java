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
import ldbc.socialnet.dbgen.vocabulary.SN;


public class CommentGenerator {

    private TagDictionary tagDictionary;
    private TagTextDictionary tagTextDic; /**< @brief The TagTextDictionary used to obtain the texts posts/comments.**/
    private TagMatrix tagMatrix;
    private DateGenerator dateGen;
    private BrowserDictionary browserDic;
    private IPAddressDictionary ipAddDic;
    private UserAgentDictionary userAgentDic;

    private int minSizeOfComment;          /**< @brief The minimum size of a comment.*/
    private int maxSizeOfComment;          /**< @brief The maximum size of a comment.*/       
    private int minLargeSizeOfComment;     /**< @brief The minimum size of large comments.*/ 
    private int maxLargeSizeOfComment;     /**< @brief The maximum size of large comments.*/
    private double largeCommentRatio;      /**< @brief The ratio of large comments.*/
    private boolean generateText;          /**< @brief To generate the text of comments.*/
    private int maxNumberOfLikes;
    private long deltaTime;                 /**< @brief The minimum time to span between post creation and its reply.*/
    private PowerDistGenerator likesGenerator;

    private String[] shortComments = {"ok", "good", "great", "cool", "thx", "fine", "LOL", "roflol", "no way!", "I see", "right", "yes", "no", "duh", "thanks", "maybe"};
	
	public CommentGenerator( TagDictionary tagDictionary,
                             TagTextDictionary tagTextDic,
                             TagMatrix tagMatrix,
                             DateGenerator dateGen,
                             BrowserDictionary browserDic,
                             IPAddressDictionary ipAddDic,
                             UserAgentDictionary userAgentDic,
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
        this.likesGenerator = new PowerDistGenerator(1,maxNumberOfLikes,0.07);
        this.browserDic = browserDic;
        this.ipAddDic = ipAddDic;
        this.userAgentDic = userAgentDic;
	}
	
	public void initialize() {
	}

    /** @brief Assigns a set of likes to a post created by a user.
     *  @param[in] user The user that created the post.*/
    private void setLikes( Random randomNumLikes, Random randomDate, Message message, ReducedUserProfile user ) {
        int numFriends = user.getNumFriendsAdded();
        int numLikes = likesGenerator.getValue(randomNumLikes);
        numLikes = numLikes >= numFriends ?  numFriends : numLikes;
        Like[] likes = new Like[numLikes];
        Friend[] friendList = user.getFriendList();
        int startIndex = 0;
        if( numLikes < numFriends ) {
            startIndex = randomNumLikes.nextInt(numFriends - numLikes);
        }
        for (int i = 0; i < numLikes; i++) {
            likes[i] = null;
            long minDate = message.getCreationDate() > friendList[startIndex+i].getCreatedTime() ? message.getCreationDate() : friendList[startIndex+i].getCreatedTime();
            long date = Math.max(dateGen.randomSevenDays(randomDate),deltaTime) + minDate;
            if( date <= dateGen.getEndDateTime() ) {
                likes[i] = new Like();
                likes[i].user = friendList[startIndex + i].getFriendAcc();
                likes[i].messageId = message.getMessageId();
                likes[i].date = date;
                likes[i].type = 1;
            }
        }
        message.setLikes(likes);
    }

    /** @brief Assigns a set of likes to a post created by a user.
     *  @param[in] group The group where the post was created.*/
    private void setLikes( Random randomNumLikes, Random randomDate, Message message, Group group ) {
        int numMembers = group.getNumMemberAdded();
        int numLikes = likesGenerator.getValue(randomNumLikes);
        numLikes = numLikes >= numMembers ?  numMembers : numLikes;
        Like[] likes = new Like[numLikes];
        GroupMemberShip groupMembers[] = group.getMemberShips();
        int startIndex = 0;
        if( numLikes < numMembers ) {
            startIndex = randomNumLikes.nextInt(numMembers - numLikes);
        }
        for (int i = 0; i < numLikes; i++) {
            likes[i] = null;
            long minDate = message.getCreationDate() > groupMembers[startIndex+i].getJoinDate() ? message.getCreationDate() : groupMembers[startIndex+i].getJoinDate();
            long date = Math.max(dateGen.randomSevenDays(randomDate),deltaTime) + minDate;
            if( date <= dateGen.getEndDateTime() ) {
                likes[i] = new Like();
                likes[i].user = groupMembers[startIndex + i].getUserId();
                likes[i].messageId = message.getMessageId();
                likes[i].date = date;
                likes[i].type = 1;
            }
        }
        message.setLikes(likes);
    }



    public Comment createComment(RandomGeneratorFarm randomFarm, long commentId, Post post, Message replyTo, ReducedUserProfile user) {

        ArrayList<Integer> validIds = new ArrayList<Integer>();
        Friend[] friends = user.getFriendList();
        for (int i = 0; i <user.getNumFriendsAdded(); i++) {
            if ((friends[i].getCreatedTime()+deltaTime <= replyTo.getCreationDate()) || (friends[i].getCreatedTime() == -1)){
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
        boolean isShort = false;
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
            isShort = true;
            int index = randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE).nextInt(shortComments.length);
            textSize = shortComments[index].length();
            if( generateText ) {
                content = shortComments[index];
            }
        }
        long creationDate = dateGen.powerlawCommDateDay(randomFarm.get(RandomGeneratorFarm.Aspect.DATE),replyTo.getCreationDate()+deltaTime);
        if( creationDate > dateGen.getEndDateTime() ) return null;
       /* IP commentIP = ipAddDic.getIP(randomFarm.get(RandomGeneratorFarm.Aspect.IP), randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP), randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP_FOR_TRAVELER),friend.getSourceIp(), friend.isFrequentChange(), creationDate);
        int commentLocation = user.getCityId();
        if( !commentIP.equals(user.getIpAddress()) ) {
            commentLocation = locationDic.getRandomCity(randomFarm.get(RandomGeneratorFarm.Aspect.CITY),ipAddDic.getLocation(commentIP));
        }
        */
        Comment comment = new Comment( SN.composeId(commentId,creationDate),
                                       content,
                                       textSize,
                                       creationDate,
                                       friend.getFriendAcc(),
                                       post.getGroupId(),
                                       tags,
                                       ipAddDic.getIP(randomFarm.get(RandomGeneratorFarm.Aspect.IP), randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP), randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP_FOR_TRAVELER),friend.getSourceIp(), friend.isFrequentChange(), creationDate),
                                       userAgentDic.getUserAgentName(randomFarm.get(RandomGeneratorFarm.Aspect.USER_AGENT),friend.isHaveSmartPhone(), friend.getAgentIdx()),
                                       browserDic.getPostBrowserId(randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_BROWSER),randomFarm.get(RandomGeneratorFarm.Aspect.BROWSER),friend.getBrowserIdx()),
                                       user.getCityId(),
                                       post.getMessageId(),
                                       replyTo.getMessageId());
        if( !isShort && randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LIKE).nextDouble() <= 0.1 ) {
            setLikes(randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LIKE), randomFarm.get(RandomGeneratorFarm.Aspect.DATE),comment, user);
        }
        return comment;
    }
    
    public Comment createComment(RandomGeneratorFarm randomFarm, long commentId, Post post, Message replyTo , Group group) {

        ArrayList<Integer> validIds = new ArrayList<Integer>();
        GroupMemberShip[] memberShips = group.getMemberShips();
        for (int i = 0; i <group.getNumMemberAdded(); i++) {
            if (memberShips[i].getJoinDate()+deltaTime <= replyTo.getCreationDate()){
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
        boolean isShort = false;
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
            isShort = true;
            int index = randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE).nextInt(shortComments.length);
            textSize = shortComments[index].length();
            if( generateText ) {
                content = shortComments[index];
            }
        }

        long creationDate = dateGen.powerlawCommDateDay(randomFarm.get(RandomGeneratorFarm.Aspect.DATE),replyTo.getCreationDate()+deltaTime);
        if( creationDate > dateGen.getEndDateTime() ) return null;
        Comment comment = new Comment(SN.composeId(commentId,creationDate),
                content,
                textSize,
                creationDate,
                membership.getUserId(),
                post.getGroupId(),
                tags,
                ipAddDic.getIP(randomFarm.get(RandomGeneratorFarm.Aspect.IP), randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP), randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP_FOR_TRAVELER), membership.getIP(), membership.isFrequentChange(), creationDate),
                userAgentDic.getUserAgentName(randomFarm.get(RandomGeneratorFarm.Aspect.USER_AGENT),membership.isHaveSmartPhone(), membership.getAgentIdx()),
                browserDic.getPostBrowserId(randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_BROWSER),randomFarm.get(RandomGeneratorFarm.Aspect.BROWSER),membership.getBrowserIdx()),
                -1,
                post.getMessageId(),
                replyTo.getMessageId());

        if( !isShort && randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LIKE).nextDouble() <= 0.1 ) {
            setLikes(randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LIKE), randomFarm.get(RandomGeneratorFarm.Aspect.DATE), comment, group);
        }
        return comment;
    }
}
