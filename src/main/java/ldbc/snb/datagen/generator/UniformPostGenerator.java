/* 
 Copyright (c) 2013 LDBC
 Linked Data Benchmark Council (http://www.ldbcouncil.org)
 
 This file is part of ldbc_snb_datagen.
 
 ldbc_snb_datagen is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.
 
 ldbc_snb_datagen is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with ldbc_snb_datagen.  If not, see <http://www.gnu.org/licenses/>.
 
 Copyright (C) 2011 OpenLink Software <bdsmt@openlinksw.com>
 All Rights Reserved.
 
 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation;  only Version 2 of the License dated
 June 1991.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.*/
package ldbc.snb.datagen.generator;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.objects.Forum;
import ldbc.snb.datagen.objects.ForumMembership;

import java.util.Iterator;
import java.util.Random;
import java.util.TreeSet;

/**
 * @author aprat
 */
public class UniformPostGenerator extends PostGenerator {


    public UniformPostGenerator(TextGenerator generator, CommentGenerator commentGenerator, LikeGenerator likeGenerator) {
        super(generator, commentGenerator, likeGenerator);
    }

    @Override
    protected PostInfo generatePostInfo(Random randomTag, Random randomDate, final Forum forum, final ForumMembership membership) {
        PostInfo postInfo = new PostInfo();
        postInfo.tags = new TreeSet<Integer>();
        Iterator<Integer> it = forum.tags().iterator();
        while (it.hasNext()) {
            Integer value = it.next();
            if (postInfo.tags.isEmpty()) {
                postInfo.tags.add(value);
            } else {
                if (randomTag.nextDouble() < 0.05) {
                    postInfo.tags.add(value);
                }
            }
        }
        postInfo.date = Dictionaries.dates.randomDate(randomDate, membership.creationDate() + DatagenParams.deltaTime);
        return postInfo;
    }
}
