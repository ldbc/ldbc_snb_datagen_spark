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

import ldbc.socialnet.dbgen.generator.DateGenerator;
import ldbc.socialnet.dbgen.generator.ScalableGenerator;
import ldbc.socialnet.dbgen.dictionary.UserAgentDictionary;
import ldbc.socialnet.dbgen.dictionary.TagTextDictionary;
import ldbc.socialnet.dbgen.dictionary.IPAddressDictionary;
import ldbc.socialnet.dbgen.dictionary.BrowserDictionary;
import ldbc.socialnet.dbgen.objects.Comment;
import ldbc.socialnet.dbgen.objects.Friend;
import ldbc.socialnet.dbgen.objects.Group;
import ldbc.socialnet.dbgen.objects.GroupMemberShip;
import ldbc.socialnet.dbgen.objects.Post;
import ldbc.socialnet.dbgen.objects.ReducedUserProfile;
import ldbc.socialnet.dbgen.util.RandomGeneratorFarm;


public class CommentGenerator {
	
    public static int commentId = -1;
    private static final String SEPARATOR = "  ";
    
    private TagTextDictionary tagTextDic; /**< @brief The TagTextDictionary used to obtain the texts posts/comments.**/
    private DateGenerator dateGen;

    private int minSizeOfComment;          /**< @brief The minimum size of a comment.*/
    private int maxSizeOfComment;          /**< @brief The maximum size of a comment.*/       
    private int minLargeSizeOfComment;     /**< @brief The minimum size of large comments.*/ 
    private int maxLargeSizeOfComment;     /**< @brief The maximum size of large comments.*/

    private double largeCommentRatio;      /**< @brief The ratio of large comments.*/
	
	public CommentGenerator( TagTextDictionary tagTextDic, 
                             DateGenerator dateGen,
                             int minSizeOfComment, 
                             int maxSizeOfComment, 
	                         double reducedTextRatio,
                             int minLargeSizeOfComment,
                             int maxLargeSizeOfComment, 
                             double largeCommentRatio
                             ){

        this.tagTextDic = tagTextDic;
        this.dateGen = dateGen;

		this.minSizeOfComment = minSizeOfComment;
		this.maxSizeOfComment = maxSizeOfComment;
        this.minLargeSizeOfComment = minLargeSizeOfComment; 
        this.maxLargeSizeOfComment = maxLargeSizeOfComment; 
        this.largeCommentRatio = largeCommentRatio;
	}
	
	public void initialize() {
	}

    public long getReplyToId(Random randomReplyTo, long startId, long lastId) {
        int parentId; 
        if (lastId > (startId+1)){
            parentId = randomReplyTo.nextInt((int)(lastId - startId));
            if (parentId == 0) return -1; 
            else return (long)(parentId + startId); 
        }

        return -1; 
    }

    public Comment createComment(RandomGeneratorFarm randomFarm, Post post, ReducedUserProfile user,
                                 long lastCommentCreatedDate, long startCommentId, long lastCommentId,
                                 UserAgentDictionary userAgentDic, IPAddressDictionary ipAddDic,
                                 BrowserDictionary browserDic) {

        ArrayList<Integer> validIds = new ArrayList<Integer>();
        Friend[] friends = user.getFriendList();
        for (int i = 0; i <user.getNumFriendsAdded(); i++) {
            if ((friends[i].getCreatedTime() > post.getCreationDate()) || (friends[i].getCreatedTime() == -1)){
                validIds.add(i);
            }
        }
        if (validIds.size() == 0) {
            return null;
        }

        String content;
        if( user.isLargePoster() ) {
            if( randomFarm.get(RandomGeneratorFarm.Aspect.LARGE_TEXT).nextDouble() > (1.0f-largeCommentRatio) ) {
                content = tagTextDic.getRandomLargeText(randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE),post.getTags(), minLargeSizeOfComment, maxLargeSizeOfComment);
            } else {
                content = tagTextDic.getRandomText(randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE), randomFarm.get(RandomGeneratorFarm.Aspect.REDUCED_TEXT),post.getTags(), minSizeOfComment, maxSizeOfComment);
            }
        } else {
            content = tagTextDic.getRandomText(randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE), randomFarm.get(RandomGeneratorFarm.Aspect.REDUCED_TEXT),post.getTags(), minSizeOfComment, maxSizeOfComment);
        }

        commentId++;
        int friendIdx = randomFarm.get(RandomGeneratorFarm.Aspect.FRIEND).nextInt(validIds.size());
        Friend friend = user.getFriendList()[friendIdx];
        long creationDate = dateGen.powerlawCommDateDay(randomFarm.get(RandomGeneratorFarm.Aspect.DATE),lastCommentCreatedDate);
        Comment comment = new Comment( commentId,
                                       content,
                                       post.getPostId(),
                                       friend.getFriendAcc(),
                                       creationDate,
                                       post.getGroupId(),
                                       getReplyToId(randomFarm.get(RandomGeneratorFarm.Aspect.REPLY_TO),startCommentId, lastCommentId),
                                       ipAddDic.getIP(randomFarm.get(RandomGeneratorFarm.Aspect.IP), randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP), randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP_FOR_TRAVELER),friend.getSourceIp(), friend.isFrequentChange(), creationDate),
                                       userAgentDic.getUserAgentName(randomFarm.get(RandomGeneratorFarm.Aspect.USER_AGENT),friend.isHaveSmartPhone(), friend.getAgentIdx()),
                                       browserDic.getPostBrowserId(randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_BROWSER),randomFarm.get(RandomGeneratorFarm.Aspect.BROWSER),friend.getBrowserIdx()) );
        return comment;
    }
    
    public Comment createComment(RandomGeneratorFarm randomFarm,Post post, Group group,
            long lastCommentCreatedDate, long startCommentId, long lastCommentId,
            UserAgentDictionary userAgentDic, IPAddressDictionary ipAddDic,
            BrowserDictionary browserDic) {

        ArrayList<Integer> validIds = new ArrayList<Integer>();
        GroupMemberShip[] memberShips = group.getMemberShips();
        for (int i = 0; i <group.getNumMemberAdded(); i++) {
            if (memberShips[i].getJoinDate() > post.getCreationDate()){
                validIds.add(i);
            }
        }
        if (validIds.size() == 0) {
            return null;
        }

        int memberIdx = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX).nextInt(validIds.size());
        GroupMemberShip membership = group.getMemberShips()[memberIdx];

        String content;
        if( membership.isLargePoster() ) {
            if( randomFarm.get(RandomGeneratorFarm.Aspect.LARGE_TEXT).nextDouble() > (1.0f-largeCommentRatio) ) {
                content = tagTextDic.getRandomLargeText(randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE),post.getTags(), minLargeSizeOfComment, maxLargeSizeOfComment);
            } else {
                content = tagTextDic.getRandomText(randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE), randomFarm.get(RandomGeneratorFarm.Aspect.REDUCED_TEXT), post.getTags(), minSizeOfComment, maxSizeOfComment );
            }
        } else {
            content = tagTextDic.getRandomText(randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE), randomFarm.get(RandomGeneratorFarm.Aspect.REDUCED_TEXT), post.getTags(), minSizeOfComment, maxSizeOfComment );
        }

        commentId++;
        long creationDate = dateGen.powerlawCommDateDay(randomFarm.get(RandomGeneratorFarm.Aspect.DATE),lastCommentCreatedDate);
        Comment comment = new Comment( commentId,
                                       content,
                                       post.getPostId(),
                                       membership.getUserId(),
                                       creationDate,
                                       post.getGroupId(),
                                       getReplyToId(randomFarm.get(RandomGeneratorFarm.Aspect.REPLY_TO),startCommentId, lastCommentId),
                                       ipAddDic.getIP(randomFarm.get(RandomGeneratorFarm.Aspect.IP), randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP), randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP_FOR_TRAVELER), membership.getIP(), membership.isFrequentChange(), creationDate),
                                       userAgentDic.getUserAgentName(randomFarm.get(RandomGeneratorFarm.Aspect.USER_AGENT), membership.isHaveSmartPhone(), membership.getAgentIdx()),
                                       browserDic.getPostBrowserId(randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_BROWSER),randomFarm.get(RandomGeneratorFarm.Aspect.BROWSER), membership.getBrowserIdx()));
        return comment;
    }
}
