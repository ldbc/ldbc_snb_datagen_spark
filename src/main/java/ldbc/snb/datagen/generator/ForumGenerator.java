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

import ldbc.snb.datagen.dictionary.PlaceDictionary;
import ldbc.snb.datagen.dictionary.TagDictionary;
import ldbc.snb.datagen.objects.*;
import ldbc.snb.datagen.util.RandomGeneratorFarm;
import ldbc.snb.datagen.vocabulary.SN;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.TreeSet;


public class ForumGenerator {
    DateGenerator dateGenerator;
    PlaceDictionary locationDic;
    TagDictionary tagDic;
    long deltaTime;

    public ForumGenerator(DateGenerator dateGenerator, PlaceDictionary locationDic,
                          TagDictionary tagDic, long deltaTime) {
        this.dateGenerator = dateGenerator;
        this.locationDic = locationDic;
        this.tagDic = tagDic;
        this.deltaTime = deltaTime;
    }

    public Forum createForum(RandomGeneratorFarm randomFarm, long forumId, ReducedUserProfile user) {
        long date = dateGenerator.randomDate(randomFarm.get(RandomGeneratorFarm.Aspect.DATE), user.getCreationDate() + deltaTime);
        if (date > dateGenerator.getEndDateTime()) return null;
        Forum forum = new Forum();

        forum.setForumId(SN.composeId(forumId, date));
        forum.setModeratorId(user.getAccountId());
        forum.setCreatedDate(date);

        //Use the user location for forum locationIdx
        forum.setLocationIdx(user.getCountryId());

        TreeSet<Integer> tagSet = user.getInterests();
        Iterator<Integer> iter = tagSet.iterator();
        int idx = randomFarm.get(RandomGeneratorFarm.Aspect.GROUP_INTEREST).nextInt(tagSet.size());
        for (int i = 0; i < idx; i++) {
            iter.next();
        }

        int interestIdx = iter.next().intValue();

        //Set tags of this forum
        Integer tags[] = new Integer[1];
        tags[0] = interestIdx;

        //Set name of forum
        forum.setForumName("Forum for " + tagDic.getName(interestIdx).replace("\"", "\\\"") + " in " + locationDic.getPlaceName(forum.getLocationIdx()));

        forum.setTags(tags);

        return forum;
    }

    public Forum createAlbum(RandomGeneratorFarm randomFarm, long groupId, ReducedUserProfile user, UserExtraInfo extraInfo, int numAlbum, double memberProb) {
        Forum forum = createForum(randomFarm, groupId, user);
        if (forum == null) return null;
        ArrayList<Integer> countries = locationDic.getCountries();
        int randomCountry = randomFarm.get(RandomGeneratorFarm.Aspect.COUNTRY).nextInt(countries.size());
        forum.setLocationIdx(countries.get(randomCountry));
        forum.setForumName("Album " + numAlbum + " of " + extraInfo.getFirstName() + " " + extraInfo.getLastName());
        Friend[] friends = user.getFriendList();
        forum.initAllMemberships(user.getNumFriends());
        for (int i = 0; i < user.getNumFriends(); i++) {
            double randMemberProb = randomFarm.get(RandomGeneratorFarm.Aspect.ALBUM_MEMBERSHIP).nextDouble();
            if (randMemberProb < memberProb) {
                ForumMembership memberShip = createForumMember(randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX), friends[i].getFriendAcc(),
                        forum.getCreatedDate(), friends[i]);
                if (memberShip != null) {
                    memberShip.setForumId(forum.getForumId());
                    forum.addMember(memberShip);
                }
            }
        }
        return forum;
    }

    public ForumMembership createForumMember(Random random, long userId, long forumCreatedDate, Friend friend) {
        long date = dateGenerator.randomDate(random, Math.max(forumCreatedDate, friend.getCreatedTime() + deltaTime));
        if (date > dateGenerator.getEndDateTime()) return null;
        ForumMembership memberShip = new ForumMembership();
        memberShip.setUserId(userId);
        memberShip.setJoinDate(date);
        memberShip.setIP(friend.getSourceIp());
        memberShip.setBrowserIdx(friend.getBrowserIdx());
        memberShip.setAgentIdx(friend.getAgentIdx());
        memberShip.setFrequentChange(friend.isFrequentChange());
        memberShip.setHaveSmartPhone(friend.isHaveSmartPhone());
        memberShip.setLargePoster(friend.isLargePoster());
        return memberShip;
    }

    public ForumMembership createForumMember(Random random, long userId, long forumCreatedDate, ReducedUserProfile user) {

        long date = dateGenerator.randomDate(random, Math.max(forumCreatedDate, user.getCreationDate() + deltaTime));
        if (date > dateGenerator.getEndDateTime()) return null;
        ForumMembership memberShip = new ForumMembership();
        memberShip.setUserId(userId);
        memberShip.setJoinDate(date);
        memberShip.setIP(user.getIpAddress());
        memberShip.setBrowserIdx(user.getBrowserId());
        memberShip.setAgentIdx(user.getAgentId());
        memberShip.setFrequentChange(user.isFrequentChange());
        memberShip.setHaveSmartPhone(user.isHaveSmartPhone());
        memberShip.setLargePoster(user.isLargePoster());
        return memberShip;
    }
}
