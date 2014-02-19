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
import ldbc.socialnet.dbgen.dictionary.TagTextDictionary;
import ldbc.socialnet.dbgen.dictionary.UserAgentDictionary;
import ldbc.socialnet.dbgen.dictionary.IPAddressDictionary;
import ldbc.socialnet.dbgen.dictionary.BrowserDictionary;
import ldbc.socialnet.dbgen.objects.Comment;
import ldbc.socialnet.dbgen.objects.Friend;
import ldbc.socialnet.dbgen.objects.Group;
import ldbc.socialnet.dbgen.objects.GroupMemberShip;
import ldbc.socialnet.dbgen.objects.Post;
import ldbc.socialnet.dbgen.objects.ReducedUserProfile;


abstract public class PostGenerator {

    private static final String SEPARATOR = "  ";
    
    private TagTextDictionary tagTextDic;       /**< @brief The TagTextDictionary used to obtain the texts posts/comments.**/
    private UserAgentDictionary userAgentDic;   /**< @brief The user agent dictionary used to obtain the user agents.*/
    private IPAddressDictionary ipAddressDic;   /**< @brief The IP dictionary used to obtain the ips from countries.*/
    private BrowserDictionary browserDic;       /**< @brief The Browser dictioanry used to obtain the browsers used by the users.*/

    /* A set of random number generator for different purposes.*/ 
	protected Random rand;                      
	private Random randReplyTo;            
	private Random randTextSize ;
	private Random randReduceText;
    private Random randLargePost;

	private int minSizeOfPost;             /**< @brief The minimum size of a post.*/
	private int maxSizeOfPost;             /**< @brief The maximum size of a post.*/
	private int reduceTextSize;            /**< @brief The size of small sized posts.*/
    private int minLargeSizeOfPost;        /**< @brief The minimum size of large posts.*/ 
    private int maxLargeSizeOfPost;        /**< @brief The maximum size of large posts.*/ 
    private int maxNumberOfLikes;          /**< @brief The maximum number of likes a post can have.*/

    private double reducedTextRatio;       /**< @brief The ratio of reduced texts.*/
    private double largePostRatio;         /**< @brief The ratio of large posts.*/


	public PostGenerator( TagTextDictionary tagTextDic, 
                          UserAgentDictionary userAgentDic,
                          IPAddressDictionary ipAddressDic,
                          BrowserDictionary browserDic,
	                      int minSizeOfPost, 
                          int maxSizeOfPost, 
	                      double reducedTextRatio,
                          int minLargeSizeOfPost, 
                          int maxLargeSizeOfPost, 
                          double largePostRatio,
                          int maxNumberOfLikes,
                          long seed,
                          long seedTextSize){

        this.tagTextDic = tagTextDic;
        this.userAgentDic = userAgentDic;
        this.ipAddressDic = ipAddressDic;
        this.browserDic = browserDic;
		this.rand = new Random(seed);
		this.randReduceText = new Random(seed);
		this.randReplyTo = new Random(seed);
		this.randTextSize = new Random(seedTextSize);
        this.randLargePost = new Random(seed);

		this.minSizeOfPost = minSizeOfPost;
		this.maxSizeOfPost = maxSizeOfPost;
		this.reduceTextSize = maxSizeOfPost >> 1;
        this.minLargeSizeOfPost = minLargeSizeOfPost; 
        this.maxLargeSizeOfPost = maxLargeSizeOfPost; 

        this.reducedTextRatio = reducedTextRatio;
        this.largePostRatio = largePostRatio;
        this.maxNumberOfLikes = maxNumberOfLikes;
	}
	
	public void initialize() {
	}
	

	private int[] getLikeFriends(ReducedUserProfile user, int numberOfLikes) {
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
	
	private int[] getLikeFriends(Group group, int numOfLikes){
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
	
    private void setLikes( Post post, ReducedUserProfile user ) {
        int numberOfLikes = rand.nextInt(maxNumberOfLikes);
        int[] likes = getLikeFriends(user, numberOfLikes);
        post.setInterestedUserAccs(likes);
        long[] likeTimestamp = new long[likes.length];
        for (int i = 0; i < likes.length; i++) {
            likeTimestamp[i] = (long)(rand.nextDouble()*DateGenerator.SEVEN_DAYS+post.getCreationDate());
        }
        post.setInterestedUserAccsTimestamp(likeTimestamp);
    }

    private void setLikes( Post post, Group group ) {
        int numberOfLikes = rand.nextInt(maxNumberOfLikes);
        int[] likes = getLikeFriends(group, numberOfLikes);
        post.setInterestedUserAccs(likes);
        long[] likeTimestamp = new long[likes.length];
        for (int i = 0; i < likes.length; i++) {
            likeTimestamp[i] = (long)(rand.nextDouble()*DateGenerator.SEVEN_DAYS+post.getCreationDate());
        }
        post.setInterestedUserAccsTimestamp(likeTimestamp);
    }


    
    public Vector<Post> createPosts(ReducedUserProfile user, int language){

        Vector<Post> result = new Vector<Post>();
        int numPosts = getNumOfPost(user);
        for( int i = 0; i < numPosts; ++i ) {
            TreeSet<Integer> tags = GenerateTags(user);

        // Create the content of the post from its tags.
            String content;
            if( user.isLargePoster() ) {
                if( randLargePost.nextDouble() > (1.0f-largePostRatio) ) {
                    content = tagTextDic.getRandomLargeText( tags, minLargeSizeOfPost, maxLargeSizeOfPost );
                } else {
                    content = tagTextDic.getRandomText(tags, minSizeOfPost, maxSizeOfPost);
                }
            } else {
                content = tagTextDic.getRandomText(tags, minSizeOfPost, maxSizeOfPost);
            }

//        long creationDate = dateGen.randomPostCreatedDate(user);
            long creationDate = GeneratePostDate(user, tags);
            ScalableGenerator.postId++;
            Post post = new Post( ScalableGenerator.postId, 
              content,
              creationDate,
              user.getAccountId(),
              user.getAccountId() * 2,
              language,
              tags,
              ipAddressDic.getIP(user.getIpAddress(), user.isFrequentChange(), creationDate),
              userAgentDic.getUserAgentName(user.isHaveSmartPhone(), user.getAgentIdx()),
              browserDic.getPostBrowserId(user.getBrowserIdx()));

        // Create post likes.
            setLikes(post, user);
            result.add(post);
        }
        return post;
    }

    public Vector<Post> createPosts(Group group, int language) {

        Vector<Post> result = new Vector<Post>();
        int numPosts = getNumOfPost(group);
        for( int i = 0; i < numPosts; ++i ) {

        // Create the set of tags of the post.
            TreeSet<Integer> tags = new TreeSet<Integer>();
            for (int i = 0; i < group.getTags().length; i++) {
                tags.add(group.getTags()[i]);
            }

        // Obtain the membership information about the creator of the post.
            int memberIdx = rand.nextInt(group.getNumMemberAdded());
            GroupMemberShip memberShip = group.getMemberShips()[memberIdx];

        // Create the content of the post from its tags.
            String content;
            if( memberShip.isLargePoster() ) {
                if( randLargePost.nextDouble() > (1.0f-largePostRatio) ) {
                    content = tagTextDic.getRandomLargeText(tags, minLargeSizeOfPost, maxLargeSizeOfPost);
                } else {
                    content = tagTextDic.getRandomText(tags, minSizeOfPost, maxSizeOfPost);
                }
            } else {
                content = tagTextDic.getRandomText(tags, minSizeOfPost, maxSizeOfPost);
            }

//        long creationDate = dateGen.randomGroupPostCreatedDate(memberShip.getJoinDate());
            long creationDate = GeneratePostDate(group, membership, tags);
            ScalableGenerator.postId++;
            Post post = new Post( ScalableGenerator.postId, 
              content,
              creationDate,
              memberShip.getUserId(),
              group.getForumWallId(),
              language,
              tags,
              ipAddressDic.getIP(memberShip.getIP(), memberShip.isFrequentChange(), creationDate),
              userAgentDic.getUserAgentName(memberShip.isHaveSmartPhone(), memberShip.getAgentIdx()),
              browserDic.getPostBrowserId(memberShip.getBrowserIdx()));

        // Create the post likes
            setLikes(post, group)   ;
            result.add(post);
        }
        return post;
    }

    protected abstract long GeneratePostDate( ReducedUserProfile user, TreeSet<Integer> tags );

    protected abstract long GeneratePostDate( Group group, GroupMemberShip membership, TreeSet<Integer> tags );

    protected abstract TreeSet<Integer> GenerateTags( ReducedUserProfile user );
        
    protected abstract int getNumOfPost(ReducedUserProfile user);

    protected abstract int getNumOfPost(Group group);
}
