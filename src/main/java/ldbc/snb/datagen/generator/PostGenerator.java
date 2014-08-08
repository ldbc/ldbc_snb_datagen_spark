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

package ldbc.snb.datagen.generator;

import ldbc.snb.datagen.dictionary.BrowserDictionary;
import ldbc.snb.datagen.dictionary.IPAddressDictionary;
import ldbc.snb.datagen.dictionary.TagTextDictionary;
import ldbc.snb.datagen.dictionary.UserAgentDictionary;
import ldbc.snb.datagen.objects.*;
import ldbc.snb.datagen.util.RandomGeneratorFarm;
import ldbc.snb.datagen.vocabulary.SN;

import java.util.Random;
import java.util.TreeSet;
import java.util.Vector;


abstract public class PostGenerator {

    protected class PostInfo {
        public TreeSet<Integer> tags;
        public long date;

        public PostInfo() {
            this.tags = new TreeSet<Integer>();
        }
    }

    private static final String SEPARATOR = "  ";

    private TagTextDictionary tagTextDic;
    /**
     * < @brief The TagTextDictionary used to obtain the texts posts/comments.*
     */
    private UserAgentDictionary userAgentDic;
    /**
     * < @brief The user agent dictionary used to obtain the user agents.
     */
    private IPAddressDictionary ipAddressDic;
    /**
     * < @brief The IP dictionary used to obtain the ips from countries.
     */
    private BrowserDictionary browserDic;
    /**
     * < @brief The Browser dictioanry used to obtain the browsers used by the users.
     */

    /* A set of random number generator for different purposes.*/

    private int minSizeOfPost;
    /**
     * < @brief The minimum size of a post.
     */
    private int maxSizeOfPost;
    /**
     * < @brief The maximum size of a post.
     */
    private int reduceTextSize;
    /**
     * < @brief The size of small sized posts.
     */
    private int minLargeSizeOfPost;
    /**
     * < @brief The minimum size of large posts.
     */
    private int maxLargeSizeOfPost;
    /**
     * < @brief The maximum size of large posts.
     */
    private int maxNumberOfLikes;
    /**
     * < @brief The maximum number of likes a post can have.
     */

    private double reducedTextRatio;
    /**
     * < @brief The ratio of reduced texts.
     */
    private double largePostRatio;
    /**
     * < @brief The ratio of large posts.
     */
    private boolean generateText;
    /**
     * < @brief To generate text for post and comment.
     */
    private long deltaTime;
    /**
     * < @brief Delta time.
     */
    private PowerDistGenerator likesGenerator;
    protected DateGenerator dateGen;

    public PostGenerator(DateGenerator dateGen,
                         TagTextDictionary tagTextDic,
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
                         boolean generateText,
                         long deltaTime
    ) {
        this.dateGen = dateGen;
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
        this.generateText = generateText;
        this.deltaTime = deltaTime;
        this.likesGenerator = new PowerDistGenerator(1, maxNumberOfLikes, 0.07);
    }

    /**
     * @brief Initializes the post generator.
     */
    public void initialize() {
    }

    /**
     * @brief Assigns a set of likes to a post created by a user.
     * @param[in] user The user that created the post.
     */
    private void setLikes(Random randomNumLikes, Random randomDate, Message message, ReducedUserProfile user) {
        int numFriends = user.getNumFriends();
        int numLikes = likesGenerator.getValue(randomNumLikes);
        numLikes = numLikes >= numFriends ? numFriends : numLikes;
        Like[] likes = new Like[numLikes];
        Friend[] friendList = user.getFriendList();
        int startIndex = 0;
        if (numLikes < numFriends) {
            startIndex = randomNumLikes.nextInt(numFriends - numLikes);
        }
        for (int i = 0; i < numLikes; i++) {
            likes[i] = null;
            long minDate = message.getCreationDate() > friendList[startIndex + i].getCreatedTime() ? message.getCreationDate() : friendList[startIndex + i].getCreatedTime();
            long date = Math.max(dateGen.randomSevenDays(randomDate), deltaTime) + minDate;
            if (date <= dateGen.getEndDateTime()) {
                likes[i] = new Like();
                likes[i].user = friendList[startIndex + i].getFriendAcc();
                likes[i].messageId = message.getMessageId();
                likes[i].date = date;
                likes[i].type = 0;
            }
        }
        message.setLikes(likes);
    }

    /**
     * @brief Assigns a set of likes to a post created by a user.
     * @param[in] forum The forum where the post was created.
     */
    private void setLikes(Random randomNumLikes, Random randomDate, Message message, Forum forum) {
        int numMembers = forum.getNumMemberAdded();
        int numLikes = likesGenerator.getValue(randomNumLikes);
        numLikes = numLikes >= numMembers ? numMembers : numLikes;
        Like[] likes = new Like[numLikes];
        ForumMembership groupMembers[] = forum.getMemberShips();
        int startIndex = 0;
        if (numLikes < numMembers) {
            startIndex = randomNumLikes.nextInt(numMembers - numLikes);
        }
        for (int i = 0; i < numLikes; i++) {
            likes[i] = null;
            long minDate = message.getCreationDate() > groupMembers[startIndex + i].getJoinDate() ? message.getCreationDate() : groupMembers[startIndex + i].getJoinDate();
            long date = Math.max(dateGen.randomSevenDays(randomDate), deltaTime) + minDate;
            if (date <= dateGen.getEndDateTime()) {
                likes[i] = new Like();
                likes[i].user = groupMembers[startIndex + i].getUserId();
                likes[i].messageId = message.getMessageId();
                likes[i].date = date;
                likes[i].type = 0;
            }
        }
        message.setLikes(likes);
    }


    /**
     * @return The set of posts.
     * @brief Creates a set of posts for a user..
     * @param[in] user The user which we want to create the posts..
     * @param[in] extraInfo The extra information of the user.
     */
    public Vector<Post> createPosts(RandomGeneratorFarm randomFarm, ReducedUserProfile user, UserExtraInfo extraInfo, long startPostId) {
        Vector<Post> result = new Vector<Post>();
        int numPosts = generateNumOfPost(randomFarm.get(RandomGeneratorFarm.Aspect.NUM_POST), user);
        user.addNumOfPosts(numPosts); // bookkeeping for parameter generation
        for (int i = 0; i < numPosts; ++i) {
            //Generate the post info.
            PostInfo postInfo = generatePostInfo(randomFarm.get(RandomGeneratorFarm.Aspect.TAG), randomFarm.get(RandomGeneratorFarm.Aspect.DATE), user);
            if (postInfo != null) {
                user.addNumOfTagsOfPosts(postInfo.tags.size()); // bookkeeping for parameter generation
                // Create the content of the post from its tags.
                String content = "";
                int textSize;

                if (user.isLargePoster() && randomFarm.get(RandomGeneratorFarm.Aspect.LARGE_TEXT).nextDouble() > (1.0f - largePostRatio)) {
                    textSize = tagTextDic.getRandomLargeTextSize(randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE), minLargeSizeOfPost, maxLargeSizeOfPost);
                } else {
                    textSize = tagTextDic.getRandomTextSize(randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE), randomFarm.get(RandomGeneratorFarm.Aspect.REDUCED_TEXT), minSizeOfPost, maxSizeOfPost);
                }

                if (generateText) {
                    content = tagTextDic.generateText(randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE), postInfo.tags, textSize);
                    if (content.length() != textSize) {
                        System.out.println("ERROR while generating text - content size: " + content.length() + ", actual size: " + textSize);
                        System.exit(-1);
                    }
                }

                Integer languageIndex = randomFarm.get(RandomGeneratorFarm.Aspect.LANGUAGE).nextInt(extraInfo.getLanguages().size());
                Post post = new Post(SN.composeId(startPostId, postInfo.date),
                        content,
                        textSize,
                        postInfo.date,
                        user.getAccountId(),
                        user.getForumWallId(),
                        postInfo.tags,
                        ipAddressDic.getIP(randomFarm.get(RandomGeneratorFarm.Aspect.IP), randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP), randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP_FOR_TRAVELER), user.getIpAddress(), user.isFrequentChange(), postInfo.date),
                        userAgentDic.getUserAgentName(randomFarm.get(RandomGeneratorFarm.Aspect.USER_AGENT), user.isHaveSmartPhone(), user.getAgentId()),
                        browserDic.getPostBrowserId(randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_BROWSER), randomFarm.get(RandomGeneratorFarm.Aspect.BROWSER), user.getBrowserId()),
                        user.getCityId(),
                        extraInfo.getLanguages().get(languageIndex));
                startPostId++;

                // Create post likes.
                if (randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LIKE).nextDouble() <= 0.1) {
                    setLikes(randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LIKE), randomFarm.get(RandomGeneratorFarm.Aspect.DATE), post, user);
                    user.addNumOfLikesToPosts(post.getLikes().length); // bookkeeping for parameter generation
                }
                result.add(post);
            }
        }
        return result;
    }

    /**
     * @return The set of posts.
     * @brief Creates a set of posts for a user..
     * @param[in] forum The forum which we want to create the posts.
     */
    public Vector<Post> createPosts(RandomGeneratorFarm randomFarm, Forum forum, long startPostId) {
        Vector<Post> result = new Vector<Post>();
        int numPosts = generateNumOfPost(randomFarm.get(RandomGeneratorFarm.Aspect.NUM_POST), forum);
        for (int i = 0; i < numPosts; ++i) {
            // Obtain the membership information about the creator of the post.
            int memberIdx = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_POST_CREATOR).nextInt(forum.getNumMemberAdded());
            ForumMembership memberShip = forum.getMemberShips()[memberIdx];
            // Generate the post info.
            PostInfo postInfo = generatePostInfo(randomFarm.get(RandomGeneratorFarm.Aspect.TAG), randomFarm.get(RandomGeneratorFarm.Aspect.DATE), forum, memberShip);
            if (postInfo != null) {
                // Create the content of the post from its tags.
                String content = "";
                int textSize;
                if (memberShip.isLargePoster() && randomFarm.get(RandomGeneratorFarm.Aspect.LARGE_TEXT).nextDouble() > (1.0f - largePostRatio)) {
                    textSize = tagTextDic.getRandomLargeTextSize(randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE), minLargeSizeOfPost, maxLargeSizeOfPost);
                } else {
                    textSize = tagTextDic.getRandomTextSize(randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE), randomFarm.get(RandomGeneratorFarm.Aspect.REDUCED_TEXT), minSizeOfPost, maxSizeOfPost);
                }

                if (generateText) {
                    content = tagTextDic.generateText(randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE), postInfo.tags, textSize);
                    if (content.length() != textSize) {
                        System.out.println("ERROR while generating text - content size: " + content.length() + ", actual size: " + textSize);
                        System.exit(-1);
                    }
                }
                Post post = new Post(SN.composeId(startPostId, postInfo.date),
                        content,
                        textSize,
                        postInfo.date,
                        memberShip.getUserId(),
                        forum.getForumId(),
                        postInfo.tags,
                        ipAddressDic.getIP(randomFarm.get(RandomGeneratorFarm.Aspect.IP), randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP), randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP_FOR_TRAVELER), memberShip.getIP(), memberShip.isFrequentChange(), postInfo.date),
                        userAgentDic.getUserAgentName(randomFarm.get(RandomGeneratorFarm.Aspect.USER_AGENT), memberShip.isHaveSmartPhone(), memberShip.getAgentIdx()),
                        browserDic.getPostBrowserId(randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_BROWSER), randomFarm.get(RandomGeneratorFarm.Aspect.BROWSER), memberShip.getBrowserIdx()),
                        -1,
                        -1);
                startPostId++;

                // Create the post likes
                if (randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LIKE).nextDouble() <= 0.1) {
                    setLikes(randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LIKE), randomFarm.get(RandomGeneratorFarm.Aspect.DATE), post, forum);
                }
                result.add(post);
            }
        }
        return result;
    }

    /**
     * @return The post info struct containing the information. null if it was not possible to generate.
     * @brief Returs the tag and creation date information of a post.
     * @param[in] user The user that creates the post.
     */
    protected abstract PostInfo generatePostInfo(Random randomTag, Random randomDate, ReducedUserProfile user);

    /**
     * @return The post info struct containing the information. null if it was not possible to generate.
     * @brief Returs the tag and creation date information of a post.
     * @param[in] forum The forum where the post belongs.
     * @param[in] membership The membership information of the user that creates the post.
     */
    protected abstract PostInfo generatePostInfo(Random randomTag, Random randomDate, Forum forum, ForumMembership membership);

    /**
     * @return The number of posts of the user.
     * @brief Gets the number of posts a user will create.
     * @param[in] user The user which we want to know the number of posts.
     */
    protected abstract int generateNumOfPost(Random randomNumPost, ReducedUserProfile user);

    /**
     * @return The number of posts in the forum.
     * @brief Gets the number of posts that will be created in a forum.
     * @param[in] forum The forum which we want to know the number of posts.
     */
    protected abstract int generateNumOfPost(Random randomNumPost, Forum forum);
}
