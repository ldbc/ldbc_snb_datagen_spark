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


public class FlashmobPostGenerator extends PostGenerator {

    private static final String SEPARATOR = "  ";
    
    private DateGenerator dateGen;              /**< @brief the date generator.**/

	public FlashmobPostGenerator( TagTextDictionary tagTextDic, 
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
	}

    protected long GeneratePostDate( long minDate, TreeSet<Integer> tags ) {
        return dateGen.randomPostCreatedDate(minDate);
    }

    protected abstract TreeSet<Integer> GenerateTags( TreeSet<Integer> tags ) {
        TreeSet<Integer> returnTags = new TreeSet<Integer>();
        Iterator<Integer> it = tags.iterator();
        while (it.hasNext()) {
            Integer value = it.next();
            if (returnTags.isEmpty()) {
                returnTags.add(value);
            } else {
                if (rand.nextDouble() < 0.2) {
                    returnTags.add(value);
                }
            }
        }
        return returnTags;
    }
}
