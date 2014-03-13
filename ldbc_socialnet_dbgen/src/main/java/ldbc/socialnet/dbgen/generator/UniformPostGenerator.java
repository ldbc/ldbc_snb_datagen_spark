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


public class UniformPostGenerator extends PostGenerator {

    private DateGenerator dateGen;              /**< @brief the date generator.**/
    private int maxNumPostPerMonth;             /**< @brief The maximum number of posts per user per month.*/
    private int maxNumGroupPostPerMonth;        /**< @brief The maximum number of posts per group per month.*/
    private int maxNumFriends;                  /**< @brief The maximum number of friends.*/
    private int maxNumMembers;                  /**< @brief The maximum number of members of a group.*/
    private Random randomTags;
    private Random randomUserNumPost;           
    private Random randomGroupNumPost;

	public UniformPostGenerator( TagTextDictionary tagTextDic, 
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
                          long seedTextSize,
                          DateGenerator dateGen,
                          int maxNumPostPerMonth,
                          int maxNumFriends,
                          int maxNumGroupPostPerMonth,
                          int maxNumMembers,
                          long seedTags,
                          long seedUserNumPost,
                          long seedGroupNumPost
                          ) {
        super(tagTextDic, userAgentDic, ipAddressDic, browserDic, minSizeOfPost, maxSizeOfPost, reducedTextRatio, minLargeSizeOfPost,
              maxLargeSizeOfPost, largePostRatio, maxNumberOfLikes, seed, seedTextSize);
        this.dateGen = dateGen;
        this.maxNumPostPerMonth = maxNumPostPerMonth;
        this.maxNumFriends = maxNumFriends;
        this.maxNumGroupPostPerMonth = maxNumGroupPostPerMonth;
        this.maxNumMembers = maxNumMembers;
        this.randomUserNumPost = new Random(seedUserNumPost);
        this.randomGroupNumPost = new Random(seedGroupNumPost);
        this.randomTags = new Random(seedTags);
	}

    @Override
    protected PostInfo generatePostInfo( ReducedUserProfile user ) {
        PostInfo postInfo = new PostInfo();
        postInfo.tags = new TreeSet<Integer>();
        Iterator<Integer> it = user.getSetOfTags().iterator();
        while (it.hasNext()) {
            Integer value = it.next();
            if (postInfo.tags.isEmpty()) {
                postInfo.tags.add(value);
            } else {
                if (randomTags.nextDouble() < 0.2) {
                    postInfo.tags.add(value);
                }
            }
        }
        postInfo.date = dateGen.randomPostCreatedDate(randomUserNumPost,user.getCreationDate());
        return postInfo;
    }

    @Override
    protected PostInfo generatePostInfo( Group group, GroupMemberShip membership ) {
        PostInfo postInfo = new PostInfo();
        for (int i = 0; i < group.getTags().length; i++) {
            postInfo.tags.add(group.getTags()[i]);
        }
        postInfo.date = dateGen.randomPostCreatedDate(randomGroupNumPost,membership.getJoinDate());
        return postInfo;
    }

    @Override
    protected int generateNumOfPost(ReducedUserProfile user) {
        int numOfmonths = (int) dateGen.numberOfMonths(user);
        int numberPost;
        if (numOfmonths == 0) {
            numberPost = randomUserNumPost.nextInt(maxNumPostPerMonth);
        } else {
            numberPost = randomUserNumPost.nextInt(maxNumPostPerMonth * numOfmonths);
        }
        numberPost = (numberPost * user.getNumFriendsAdded()) / maxNumFriends;
        return numberPost;
    }

    @Override
    protected int generateNumOfPost(Group group) {
      int numOfmonths = (int) dateGen.numberOfMonths(group.getCreatedDate());
      int numberPost;
      if (numOfmonths == 0) {
        numberPost = randomGroupNumPost.nextInt(maxNumGroupPostPerMonth);
      } else {
        numberPost = randomGroupNumPost.nextInt(maxNumGroupPostPerMonth * numOfmonths);
      }
      numberPost = (numberPost * group.getNumMemberAdded()) / maxNumMembers;
      return numberPost;
    }
}
