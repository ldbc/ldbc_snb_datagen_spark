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

import java.util.TreeSet;
import java.util.Random;
import java.util.Vector;

import cern.jet.random.engine.RandomGenerator;
import ldbc.socialnet.dbgen.dictionary.TagTextDictionary;
import ldbc.socialnet.dbgen.dictionary.UserAgentDictionary;
import ldbc.socialnet.dbgen.dictionary.IPAddressDictionary;
import ldbc.socialnet.dbgen.dictionary.BrowserDictionary;
import ldbc.socialnet.dbgen.objects.Friend;
import ldbc.socialnet.dbgen.objects.Group;
import ldbc.socialnet.dbgen.objects.GroupMemberShip;
import ldbc.socialnet.dbgen.objects.Post;
import ldbc.socialnet.dbgen.objects.ReducedUserProfile;
import ldbc.socialnet.dbgen.objects.UserExtraInfo;
import ldbc.socialnet.dbgen.util.RandomGeneratorFarm;


abstract public class PostGenerator {

    protected class PostInfo {
        public TreeSet<Integer> tags;
        public long             date;
        public PostInfo() {
            this.tags = new TreeSet<Integer>();
        }
    }

    private static final String SEPARATOR = "  ";
    
    private TagTextDictionary tagTextDic;       /**< @brief The TagTextDictionary used to obtain the texts posts/comments.**/
    private UserAgentDictionary userAgentDic;   /**< @brief The user agent dictionary used to obtain the user agents.*/
    private IPAddressDictionary ipAddressDic;   /**< @brief The IP dictionary used to obtain the ips from countries.*/
    private BrowserDictionary browserDic;       /**< @brief The Browser dictioanry used to obtain the browsers used by the users.*/

    /* A set of random number generator for different purposes.*/ 

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
                          int maxNumberOfLikes
                          ){

        this.tagTextDic = tagTextDic;
        this.userAgentDic = userAgentDic;
        this.ipAddressDic = ipAddressDic;
        this.browserDic = browserDic;

		this.minSizeOfPost = minSizeOfPost;
		this.maxSizeOfPost = maxSizeOfPost;
		this.reduceTextSize = maxSizeOfPost >> 1;
        this.minLargeSizeOfPost = minLargeSizeOfPost; 
        this.maxLargeSizeOfPost = maxLargeSizeOfPost; 

        this.reducedTextRatio = reducedTextRatio;
        this.largePostRatio = largePostRatio;
        this.maxNumberOfLikes = maxNumberOfLikes;
	}
	
    /** @brief Initializes the post generator.*/
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
     *  @param[in] post The post to which we want to assign the likes.
     *  @param[in] user The user that created the post.*/ 
    private void setLikes( Random randomNumLikes, Random randomDate, Post post, ReducedUserProfile user ) {
        int numberOfLikes = randomNumLikes.nextInt(maxNumberOfLikes);
        long[] likes = generateLikeFriends(randomNumLikes,user, numberOfLikes);
        post.setInterestedUserAccs(likes);
        long[] likeTimestamp = new long[likes.length];
        for (int i = 0; i < likes.length; i++) {
            likeTimestamp[i] = (long)(randomDate.nextDouble()*DateGenerator.SEVEN_DAYS+post.getCreationDate());
        }
        post.setInterestedUserAccsTimestamp(likeTimestamp);
    }

    /** @brief Assigns a set of likes to a post created by a user.
     *  @param[in] post The post to which we want to assign the likes.
     *  @param[in] group The group where the post was created.*/ 
    private void setLikes( Random randomNumLikes, Random randomDate, Post post, Group group ) {
        int numberOfLikes = randomNumLikes.nextInt(maxNumberOfLikes);
        long[] likes = generateLikeFriends(randomNumLikes,group, numberOfLikes);
        post.setInterestedUserAccs(likes);
        long[] likeTimestamp = new long[likes.length];
        for (int i = 0; i < likes.length; i++) {
            likeTimestamp[i] = (long)(randomDate.nextDouble()*DateGenerator.SEVEN_DAYS+post.getCreationDate());
        }
        post.setInterestedUserAccsTimestamp(likeTimestamp);
    }


    /** @brief Creates a set of posts for a user..
     *  @param[in] user The user which we want to create the posts..
     *  @param[in] extraInfo The extra information of the user.
     *  @return The set of posts.*/ 
    public Vector<Post> createPosts(RandomGeneratorFarm randomFarm, ReducedUserProfile user, UserExtraInfo extraInfo ){
        Vector<Post> result = new Vector<Post>();
        int numPosts = generateNumOfPost(randomFarm.get(RandomGeneratorFarm.Aspect.NUM_POST), user);
        for( int i = 0; i < numPosts; ++i ) {
            //Generate the post info.
            PostInfo postInfo = generatePostInfo(randomFarm.get(RandomGeneratorFarm.Aspect.TAG), randomFarm.get(RandomGeneratorFarm.Aspect.DATE), user);
            if( postInfo != null ) {
                // Create the content of the post from its tags.
                String content;
                if( user.isLargePoster() ) {
                    if( randomFarm.get(RandomGeneratorFarm.Aspect.LARGE_TEXT).nextDouble() > (1.0f-largePostRatio) ) {
                        content = tagTextDic.getRandomLargeText( randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE), postInfo.tags, minLargeSizeOfPost, maxLargeSizeOfPost );
                    } else {
                        content = tagTextDic.getRandomText(randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE),randomFarm.get(RandomGeneratorFarm.Aspect.REDUCED_TEXT), postInfo.tags, minSizeOfPost, maxSizeOfPost);
                    }
                } else {
                    content = tagTextDic.getRandomText( randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE),randomFarm.get(RandomGeneratorFarm.Aspect.REDUCED_TEXT), postInfo.tags, minSizeOfPost, maxSizeOfPost);
                }

                Integer languageIndex = randomFarm.get(RandomGeneratorFarm.Aspect.LANGUAGE).nextInt(extraInfo.getLanguages().size());
                Post post = new Post( ScalableGenerator.postId,
                  content,
                  postInfo.date,
                  user.getAccountId(),
                  user.getAccountId() * 2,
                  extraInfo.getLanguages().get(languageIndex),
                  postInfo.tags,
                  ipAddressDic.getIP(randomFarm.get(RandomGeneratorFarm.Aspect.IP), randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP), randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP_FOR_TRAVELER),user.getIpAddress(), user.isFrequentChange(), postInfo.date),
                  userAgentDic.getUserAgentName(randomFarm.get(RandomGeneratorFarm.Aspect.USER_AGENT), user.isHaveSmartPhone(), user.getAgentIdx()),
                  browserDic.getPostBrowserId(randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_BROWSER),randomFarm.get(RandomGeneratorFarm.Aspect.BROWSER), user.getBrowserIdx()));
                ScalableGenerator.postId++;

            // Create post likes.
                setLikes(randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LIKE), randomFarm.get(RandomGeneratorFarm.Aspect.DATE),post, user);
                result.add(post);
            }
        }
        return result;
    }

    /** @brief Creates a set of posts for a user..
     *  @param[in] group The group which we want to create the posts.
     *  @return The set of posts.*/ 
    public Vector<Post> createPosts( RandomGeneratorFarm randomFarm, Group group) {
        Vector<Post> result = new Vector<Post>();
        int numPosts = generateNumOfPost(randomFarm.get(RandomGeneratorFarm.Aspect.NUM_POST), group);
        for( int i = 0; i < numPosts; ++i ) {
             // Obtain the membership information about the creator of the post.
            int memberIdx = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_POST_CREATOR).nextInt(group.getNumMemberAdded());
            GroupMemberShip memberShip = group.getMemberShips()[memberIdx];
            // Generate the post info.
            PostInfo postInfo = generatePostInfo(randomFarm.get(RandomGeneratorFarm.Aspect.TAG), randomFarm.get(RandomGeneratorFarm.Aspect.DATE), group, memberShip);
            if( postInfo != null ) {
                // Create the content of the post from its tags.
                String content;
                if( memberShip.isLargePoster() ) {
                    if( randomFarm.get(RandomGeneratorFarm.Aspect.LARGE_TEXT).nextDouble() > (1.0f-largePostRatio) ) {
                        content = tagTextDic.getRandomLargeText(randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE), postInfo.tags, minLargeSizeOfPost, maxLargeSizeOfPost);
                    } else {
                        content = tagTextDic.getRandomText(randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE), randomFarm.get(RandomGeneratorFarm.Aspect.REDUCED_TEXT),postInfo.tags, minSizeOfPost, maxSizeOfPost);
                    }
                } else {
                    content = tagTextDic.getRandomText(randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE), randomFarm.get(RandomGeneratorFarm.Aspect.REDUCED_TEXT),postInfo.tags, minSizeOfPost, maxSizeOfPost);
                }
                Post post = new Post( ScalableGenerator.postId,
                  content,
                  postInfo.date,
                  memberShip.getUserId(),
                  group.getForumWallId(),
                  -1,
                  postInfo.tags,
                  ipAddressDic.getIP(randomFarm.get(RandomGeneratorFarm.Aspect.IP), randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP), randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP_FOR_TRAVELER),memberShip.getIP(), memberShip.isFrequentChange(), postInfo.date),
                  userAgentDic.getUserAgentName(randomFarm.get(RandomGeneratorFarm.Aspect.USER_AGENT), memberShip.isHaveSmartPhone(), memberShip.getAgentIdx()),
                  browserDic.getPostBrowserId(randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_BROWSER),randomFarm.get(RandomGeneratorFarm.Aspect.BROWSER), memberShip.getBrowserIdx()));
                ScalableGenerator.postId++;

                // Create the post likes
                setLikes(randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LIKE), randomFarm.get(RandomGeneratorFarm.Aspect.DATE), post, group);
                result.add(post);
            } 
        }
        return result;
    }

    /** @brief Returs the tag and creation date information of a post.
     *  @param[in] user The user that creates the post.
     *  @return The post info struct containing the information. null if it was not possible to generate.*/
    protected abstract PostInfo generatePostInfo( Random randomTag, Random randomDate, ReducedUserProfile user );

    /** @brief Returs the tag and creation date information of a post.
     *  @param[in] group The group where the post belongs.
     *  @param[in] membership The membership information of the user that creates the post.
     *  @return The post info struct containing the information. null if it was not possible to generate.*/
    protected abstract PostInfo generatePostInfo( Random randomTag, Random randomDate, Group group, GroupMemberShip membership );

    /** @brief Gets the number of posts a user will create.
     *  @param[in] user The user which we want to know the number of posts.
     *  @return The number of posts of the user.*/
    protected abstract int generateNumOfPost(Random randomNumPost, ReducedUserProfile user);

    /** @brief Gets the number of posts that will be created in a group.
     *  @param[in] group The group which we want to know the number of posts.
     *  @return The number of posts in the group.*/
    protected abstract int generateNumOfPost(Random randomNumPost, Group group);
}
