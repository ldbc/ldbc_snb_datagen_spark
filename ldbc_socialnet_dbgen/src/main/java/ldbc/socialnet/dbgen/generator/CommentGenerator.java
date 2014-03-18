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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.Iterator;
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


public class CommentGenerator {
	
    public static int commentId = -1;
    private static final String SEPARATOR = "  ";
    
    private TagTextDictionary tagTextDic; /**< @brief The TagTextDictionary used to obtain the texts posts/comments.**/
    private DateGenerator dateGen;

    /* A set of random number generator for different purposes.*/ 
	private Random rand;                   
	private Random randReplyTo;            
	private Random randTextSize ;
    private Random randLargeComment;

    private int minSizeOfComment;          /**< @brief The minimum size of a comment.*/
    private int maxSizeOfComment;          /**< @brief The maximum size of a comment.*/       
    private int minLargeSizeOfComment;     /**< @brief The minimum size of large comments.*/ 
    private int maxLargeSizeOfComment;     /**< @brief The maximum size of large comments.*/
    private double largeCommentRatio;      /**< @brief The ratio of large comments.*/
    private boolean generateText;          /**< @brief To generate the text of comments.*/
	
	public CommentGenerator( TagTextDictionary tagTextDic, 
                             DateGenerator dateGen,
                             int minSizeOfComment, 
                             int maxSizeOfComment, 
	                         double reducedTextRatio,
                             int minLargeSizeOfComment,
                             int maxLargeSizeOfComment, 
                             double largeCommentRatio,
                             boolean generateText,
                             long seed,
                             long seedTextSize){

        this.tagTextDic = tagTextDic;
        this.dateGen = dateGen;
		this.rand = new Random(seed);
		this.randReplyTo = new Random(seed);
        this.randLargeComment = new Random(seed);

		this.minSizeOfComment = minSizeOfComment;
		this.maxSizeOfComment = maxSizeOfComment;
        this.minLargeSizeOfComment = minLargeSizeOfComment; 
        this.maxLargeSizeOfComment = maxLargeSizeOfComment; 
        this.largeCommentRatio = largeCommentRatio;
        this.generateText = generateText;
	}
	
	public void initialize() {
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

        String content = "";
        int textSize = 0;
        if( generateText ) {
            if( user.isLargePoster() ) {
                if( randLargeComment.nextDouble() > (1.0f-largeCommentRatio) ) {
                    content = tagTextDic.getRandomLargeText(post.getTags(), minLargeSizeOfComment, maxLargeSizeOfComment);
                } else {
                    content = tagTextDic.getRandomText(post.getTags(), minSizeOfComment, maxSizeOfComment);
                }
            } else {
                content = tagTextDic.getRandomText(post.getTags(), minSizeOfComment, maxSizeOfComment);
            }
            textSize = content.length();
        } else {
            if( user.isLargePoster() ) {
                if( randLargeComment.nextDouble() > (1.0f-largeCommentRatio) ) {
                    textSize = tagTextDic.getRandomLargeTextSize( minLargeSizeOfComment, maxLargeSizeOfComment );
                } else {
                    textSize = tagTextDic.getRandomTextSize( minSizeOfComment, maxSizeOfComment);
                }
            } else {
                textSize = tagTextDic.getRandomTextSize( minSizeOfComment, maxSizeOfComment);
            }
        }

        commentId++;
        int friendIdx = rand.nextInt(validIds.size());
        Friend friend = user.getFriendList()[friendIdx];
        long creationDate = dateGen.powerlawCommDateDay(lastCommentCreatedDate);
        Comment comment = new Comment( commentId,
                                       content,
                                       textSize,
                                       post.getPostId(),
                                       friend.getFriendAcc(),
                                       creationDate,
                                       post.getGroupId(),
                                       getReplyToId(startCommentId, lastCommentId),
                                       ipAddDic.getIP(friend.getSourceIp(), friend.isFrequentChange(), creationDate),
                                       userAgentDic.getUserAgentName(friend.isHaveSmartPhone(), friend.getAgentIdx()),
                                       browserDic.getPostBrowserId(friend.getBrowserIdx()) );
        return comment;
    }
    
    public Comment createComment(Post post, Group group, 
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

        int memberIdx = rand.nextInt(validIds.size());
        GroupMemberShip membership = group.getMemberShips()[memberIdx];

        String content = "";
        int textSize = 0;
        if( generateText ) {
        if( membership.isLargePoster() ) {
                if( randLargeComment.nextDouble() > (1.0f-largeCommentRatio) ) {
                    content = tagTextDic.getRandomLargeText(post.getTags(), minLargeSizeOfComment, maxLargeSizeOfComment);
                } else {
                    content = tagTextDic.getRandomText(post.getTags(), minSizeOfComment, maxSizeOfComment);
                }
            } else {
                content = tagTextDic.getRandomText(post.getTags(), minSizeOfComment, maxSizeOfComment);
            }
            textSize = content.length();
        } else {
            if( membership.isLargePoster() ) {
                if( randLargeComment.nextDouble() > (1.0f-largeCommentRatio) ) {
                    textSize = tagTextDic.getRandomLargeTextSize( minLargeSizeOfComment, maxLargeSizeOfComment );
                } else {
                    textSize = tagTextDic.getRandomTextSize( minSizeOfComment, maxSizeOfComment);
                }
            } else {
                textSize = tagTextDic.getRandomTextSize( minSizeOfComment, maxSizeOfComment);
            }
        }

        commentId++;
        long creationDate = dateGen.powerlawCommDateDay(lastCommentCreatedDate);
        Comment comment = new Comment( commentId,
                                       content,
                                       textSize,
                                       post.getPostId(),
                                       membership.getUserId(),
                                       creationDate,
                                       post.getGroupId(),
                                       getReplyToId(startCommentId, lastCommentId),
                                       ipAddDic.getIP(membership.getIP(), membership.isFrequentChange(), creationDate),
                                       userAgentDic.getUserAgentName(membership.isHaveSmartPhone(), membership.getAgentIdx()),
                                       browserDic.getPostBrowserId(membership.getBrowserIdx()));
        return comment;
    }
}
