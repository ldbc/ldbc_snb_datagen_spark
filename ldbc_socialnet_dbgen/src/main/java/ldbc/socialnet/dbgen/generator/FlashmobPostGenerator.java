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
import java.util.Vector;
import java.util.TreeSet;

import ldbc.socialnet.dbgen.generator.DateGenerator;
import ldbc.socialnet.dbgen.generator.ScalableGenerator;
import ldbc.socialnet.dbgen.dictionary.TagTextDictionary;
import ldbc.socialnet.dbgen.dictionary.FlashmobTagDictionary;
import ldbc.socialnet.dbgen.dictionary.UserAgentDictionary;
import ldbc.socialnet.dbgen.dictionary.IPAddressDictionary;
import ldbc.socialnet.dbgen.dictionary.BrowserDictionary;
import ldbc.socialnet.dbgen.objects.Comment;
import ldbc.socialnet.dbgen.objects.Friend;
import ldbc.socialnet.dbgen.objects.Group;
import ldbc.socialnet.dbgen.objects.GroupMemberShip;
import ldbc.socialnet.dbgen.objects.Post;
import ldbc.socialnet.dbgen.objects.ReducedUserProfile;
import ldbc.socialnet.dbgen.objects.FlashmobTag;



public class FlashmobPostGenerator extends PostGenerator {

    private static final String SEPARATOR = "  ";
    
    private DateGenerator dateGen;              /**< @brief the date generator.**/
    private FlashmobTagDictionary flashmobTagDictionary;

    private ReducedUserProfile currentUser = null;
    private Vector<FlashmobTag> userFlashmobTags = null;
    private int currentTag = 0; 
    private Group currentGroup = null;
    private Vector<FlashmobTag> groupFlashmobTags = null;
    private int currentGroupTag = 0;
    private enum Mode {
      GROUP,
      USER
    }
    private Mode currentMode = USER;

	public FlashmobPostGenerator( TagTextDictionary tagTextDic, 
                          FlashmobTagDictionary flashmobTagDictionary,
                          DateGenerator dateGen,
                          int minSizeOfPost, 
                          int maxSizeOfPost, 
                          double reducedTextRatio,
                          int minLargeSizeOfPost, 
                          int maxLargeSizeOfPost, 
                          double largePostRatio,
                          int maxNumberOfLikes,
                          long seed,
                          long seedTextSize ) {
        super(tagTextDic, minSizeOfPost, maxSizeOfPost, reducedTextRatio, minLargeSizeOfPost,
              maxLargeSizeOfPost, largePostRatio, maxNumberOfLikes, seed, seedTextSize);
        this.dateGen = dateGen;
        this.flashmobTagDictionary = flashmobTagDictionary;
	}

    protected long GeneratePostDate( long minDate, TreeSet<Integer> tags ) {
        //falta ajustar la data.
        switch(currentMode){
          case GROUP:
          return groupFlashmobTags.get(currentTag).date;
          break;
          case USER:
          return userFlashmobTags.get(currentTag).date;
          break;
        }
    }

    protected TreeSet<Integer> GenerateTags( TreeSet<Integer> tags ) {
      TreeSet<Integer> returnTags = new TreeSet<Integer>();
      switch(currentMode){
        case GROUP:
        returnTags.add(groupFlashmobTags.get(currentTag).tag);
        break;
        case USER:
        returnTags.add(userFlashmobTags.get(currentTag).tag);
        break;
      }
      return returnTags;
    }



    public  Vector<Post> createPosts(ReducedUserProfile user, int language, int maxNumberOfLikes,
            UserAgentDictionary userAgentDic, IPAddressDictionary ipAddDic,
            BrowserDictionary browserDic) {
        currentUser = user;
        userFlashmobTags = flashmobTagDictionary.getFlashmobTags( user.getSetOfTags(), user.getCreatedDate());
        Vector<Post> result = new Vector<Post>();
        currentMode = USER;
        for( currentTag = 0; currentTag < userFlashmobTags.size(); ++currentTag) {
          result.add(createPost(user, language, maxNumberOfLikes, userAgentDic, ipAddDic, browserDic));
        }
        return result;
    }


    @Override
    public  Post createPost(ReducedUserProfile user, int language, int maxNumberOfLikes,
            UserAgentDictionary userAgentDic, IPAddressDictionary ipAddDic,
            BrowserDictionary browserDic) {

      if( user != currentUser ) {
        currentUser = user;
        userFlashmobTags = flashmobTagDictionary.getFlashmobTags( user.getSetOfTags(), user.getCreatedDate());
        currentTag = 0;
        currentMode = USER;
      }
      return super.createPost(user, language, maxNumberOfLikes, userAgentDic, ipAddDic, browserDic);
    }

    public Vector<Post> createPosts(Group group, int language, int maxNumberOfLikes,
            UserAgentDictionary userAgentDic, IPAddressDictionary ipAddDic,
            BrowserDictionary browserDic) {

        currentGroup = group;
        Integer[] groupTags = group.getSetOfTags();
        TreeSet<Integer> tags = new TreeSet<Integer>(); 
        for( int i = 0; i < groupTags.length(); ++i ) {
          tags.add(groupTags[i]);
        }
        groupFlashmobTags = flashmobTagDictionary.getFlashmobTags( tags, group.getCreatedDate());
        currentMode = GROUP;
        for(currentGroupTag = 0, currentGroupTag < groupFlashmobTags.size(); ++currentGroupTag){
          result.add(createPost(group, language, maxNumberOfLikes, userAgentDic, ipAddDic, browserDic));
        } 
      return result; 
    }

    public Post createPost(Group group, int language, int maxNumberOfLikes,
            UserAgentDictionary userAgentDic, IPAddressDictionary ipAddDic,
            BrowserDictionary browserDic) {

      if( group != currentGroup ) {
        currentGroup = group;
        Integer[] groupTags = group.getSetOfTags();
        TreeSet<Integer> tags = new TreeSet<Integer>(); 
        for( int i = 0; i < groupTags.length(); ++i ) {
          tags.add(groupTags[i]);
        }
        groupFlashmobTags = flashmobTagDictionary.getFlashmobTags( tags, group.getCreatedDate());
        currentGroupTag = 0;
        currentMode = GROUP;
      }
      return super.createPost(group, language, maxNumberOfLikes, userAgentDic, ipAddDic, browserDic);
    }
}
