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
import java.util.Iterator;
import java.util.Random;
import java.util.Vector;
import java.util.Arrays;

import ldbc.socialnet.dbgen.generator.DateGenerator;
import ldbc.socialnet.dbgen.generator.ScalableGenerator;
import ldbc.socialnet.dbgen.dictionary.TagTextDictionary;
import ldbc.socialnet.dbgen.dictionary.FlashmobTagDictionary;
import ldbc.socialnet.dbgen.dictionary.UserAgentDictionary;
import ldbc.socialnet.dbgen.dictionary.IPAddressDictionary;
import ldbc.socialnet.dbgen.dictionary.BrowserDictionary;
import ldbc.socialnet.dbgen.dictionary.TagMatrix;
import ldbc.socialnet.dbgen.objects.Comment;
import ldbc.socialnet.dbgen.objects.Friend;
import ldbc.socialnet.dbgen.objects.Group;
import ldbc.socialnet.dbgen.objects.GroupMemberShip;
import ldbc.socialnet.dbgen.objects.Post;
import ldbc.socialnet.dbgen.objects.ReducedUserProfile;
import ldbc.socialnet.dbgen.objects.FlashmobTag;
import ldbc.socialnet.dbgen.util.Distribution;



public class FlashmobPostGenerator extends PostGenerator {

    private static final String SEPARATOR = "  ";

    private DateGenerator dateGen;                          /**< @brief the date generator.*/
    private FlashmobTagDictionary flashmobTagDictionary;    /**< @brief The flashmobTagDictionary used to get the tags of the posts.*/
    private TagMatrix             tagMatrix;
    private Distribution dateDistribution;
    private long hoursToMillis;
    private long flashmobSpan;


    /** This fields are used in order to reduce the number of computations needed and hence improve the performance. **/
    private FlashmobTag[] userFlashmobTags = null;    
    private FlashmobTag[] groupFlashmobTags = null;
//    private int postPerLevelScaleFactor = 0;
    private int         maxNumFlashmobPostPerMonth;
    private int         maxNumGroupFlashmobPostPerMonth;
    private int         maxNumFriends;
    private int         maxNumMembers;
    private int         maxNumTagPerFlashmobPost;

    public FlashmobPostGenerator( DateGenerator dateGen,
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
            boolean exportText,
            long deltaTime,
            FlashmobTagDictionary flashmobTagDictionary,
            TagMatrix tagMatrix,
            int maxNumFlashmobPostPerMonth,
            int maxNumGroupFlashmobPostPerMonth,
            int maxNumFriends,
            int maxNumMembers,
            int maxNumTagPerFlashmobPost,
            String flashmobDistFile
            ) {
        super(dateGen,tagTextDic, userAgentDic, ipAddressDic, browserDic, minSizeOfPost, maxSizeOfPost, reducedTextRatio, minLargeSizeOfPost,
                maxLargeSizeOfPost, largePostRatio, maxNumberOfLikes,exportText,deltaTime);
        this.dateGen = dateGen;
        this.flashmobTagDictionary = flashmobTagDictionary;
        this.tagMatrix = tagMatrix;
//        this.postPerLevelScaleFactor = postPerLevelScaleFactor;
        this.dateDistribution = new Distribution(flashmobDistFile);
        this.hoursToMillis = 60*60*1000;
        this.flashmobSpan = 72 * hoursToMillis;
        this.maxNumFlashmobPostPerMonth = maxNumFlashmobPostPerMonth;
        this.maxNumGroupFlashmobPostPerMonth = maxNumGroupFlashmobPostPerMonth;
        this.maxNumFriends = maxNumFriends;
        this.maxNumMembers = maxNumMembers;
    }

    public void initialize() {
        super.initialize();
        dateDistribution.initialize();
    }

    /** @brief Selects a random tag from a given index.
     *  @param[in] tags The array of sorted tags to select from.
     *  @param[in] index The first tag to consider.
     *  @return The index of a random tag.*/
    private int selectRandomTag( Random randomFlashmobTag, FlashmobTag[] tags, int index ) {
        int upperBound = tags.length - 1;
        int lowerBound = index;
        double prob = randomFlashmobTag.nextDouble() * (tags[upperBound].prob - tags[lowerBound].prob) + tags[lowerBound].prob;
        //System.out.println(tags[upperBound].prob+" "+tags[lowerBound].prob+" "+prob);
        int midPoint = (upperBound + lowerBound)  / 2;
        while (upperBound > (lowerBound+1)){
            if (tags[midPoint].prob > prob ){
                upperBound = midPoint;
            } else{
                lowerBound = midPoint; 
            }
            midPoint = (upperBound + lowerBound)  / 2;
        }
        return midPoint;
    }

    /** @brief Selects the earliest flashmob tag index from a given date.
     *  @return The index to the earliest flashmob tag.*/
    private int searchEarliest( FlashmobTag[] tags, GroupMemberShip membership ) {
        long fromDate = membership.getJoinDate();
        int lowerBound = 0;
        int upperBound = tags.length - 1;
        int midPoint = (upperBound + lowerBound)  / 2;
        while (upperBound > (lowerBound+1)){
            if (tags[midPoint].date > fromDate ){
                upperBound = midPoint;
            } else{
                lowerBound = midPoint; 
            }
            midPoint = (upperBound + lowerBound)  / 2;
        }
        if( tags[midPoint].date < fromDate ) return -1;
        return midPoint;
    }

    @Override
        protected PostInfo generatePostInfo( Random randomTag, Random randomDate, ReducedUserProfile user ) {
            PostInfo postInfo = new PostInfo();
            int index = selectRandomTag( randomTag, userFlashmobTags, 0 );
            FlashmobTag flashmobTag = userFlashmobTags[index];
            if( flashmobTag.date < user.getCreationDate() )  return null;
            postInfo.tags.add(flashmobTag.tag);
            postInfo.tags.addAll(tagMatrix.getSetofTags(randomTag,randomTag,flashmobTag.tag, maxNumTagPerFlashmobPost - 1));
            double prob = dateDistribution.nextDouble(randomDate);
            postInfo.date = flashmobTag.date - flashmobSpan/2 + (long)( prob * flashmobSpan); 
            return postInfo;
        }

    @Override
        protected PostInfo generatePostInfo( Random randomTag, Random randomDate, Group group, GroupMemberShip membership ) {
            PostInfo postInfo = new PostInfo();
            int index = searchEarliest(groupFlashmobTags,membership);
            if( index < 0 ) return null;
            index = selectRandomTag( randomTag, groupFlashmobTags,index);
            FlashmobTag flashmobTag =  groupFlashmobTags[index];
            postInfo.tags.add(flashmobTag.tag);
            postInfo.tags.addAll(tagMatrix.getSetofTags(randomTag,randomTag,flashmobTag.tag, maxNumTagPerFlashmobPost - 1));
            double prob = dateDistribution.nextDouble(randomDate);
            postInfo.date = flashmobTag.date - flashmobSpan/2 + (long)(prob * flashmobSpan); 
            return postInfo;
        }

    @Override
        protected int generateNumOfPost(Random randomNumPost, ReducedUserProfile user) {
            Vector<FlashmobTag> temp = flashmobTagDictionary.generateFlashmobTags( user.getSetOfTags(), user.getCreationDate());
            userFlashmobTags = new FlashmobTag[temp.size()];
            int index = 0;
            int sumLevels = 0;
            Iterator<FlashmobTag> it = temp.iterator();
            while(it.hasNext()) {
                FlashmobTag flashmobTag = new FlashmobTag();
                it.next().copyTo(flashmobTag);
                userFlashmobTags[index] = flashmobTag; 
                sumLevels+=flashmobTag.level;
                ++index; 
            }
            Arrays.sort(userFlashmobTags);
            int size = userFlashmobTags.length;
            double currentProb = 0.0;
            for( int i = 0; i < size; ++i ) {
                userFlashmobTags[i].prob = currentProb;
                currentProb += (double)(userFlashmobTags[i].level) / (double)(sumLevels);
            }
            int numOfmonths = (int) dateGen.numberOfMonths(user);
            int numberPost;
            if (numOfmonths == 0) {
                numberPost = randomNumPost.nextInt(maxNumFlashmobPostPerMonth);
            } else {
                numberPost = randomNumPost.nextInt(maxNumFlashmobPostPerMonth * numOfmonths);
            }
            numberPost = (numberPost * user.getNumFriendsAdded()) / maxNumFriends;
            return numberPost;
        }

    @Override
        protected int generateNumOfPost(Random randomNumPost, Group group) {
            Integer[] groupTags = group.getTags();
            TreeSet<Integer> tags = new TreeSet<Integer>(); 
            for( int i = 0; i < groupTags.length; ++i ) {
                tags.add(groupTags[i]);
            }
            Vector<FlashmobTag> temp = flashmobTagDictionary.generateFlashmobTags( tags, group.getCreatedDate() );
            groupFlashmobTags = new FlashmobTag[temp.size()];
            Iterator<FlashmobTag> it = temp.iterator();
            int index = 0;
            int sumLevels = 0;
            while(it.hasNext()) {
                FlashmobTag flashmobTag = new FlashmobTag();
                it.next().copyTo(flashmobTag);
                groupFlashmobTags[index] = flashmobTag; 
                sumLevels+=flashmobTag.level;
                ++index; 
            }
            Arrays.sort(groupFlashmobTags);
            int size = groupFlashmobTags.length;
            double currentProb = 0.0;
            for( int i = 0; i < size; ++i ) {
                groupFlashmobTags[i].prob = currentProb;
                currentProb += (double)(groupFlashmobTags[i].level) / (double)(sumLevels);
            }

            int numOfmonths = (int) dateGen.numberOfMonths(group.getCreatedDate());
            int numberPost;
            if (numOfmonths == 0) {
                numberPost = randomNumPost.nextInt(maxNumGroupFlashmobPostPerMonth);
            } else {
                numberPost = randomNumPost.nextInt(maxNumGroupFlashmobPostPerMonth * numOfmonths);
            }
            numberPost = (numberPost * group.getNumMemberAdded()) / maxNumMembers;
            return numberPost;
        }
}
