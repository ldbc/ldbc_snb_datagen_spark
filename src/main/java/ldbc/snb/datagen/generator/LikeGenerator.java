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
import ldbc.snb.datagen.objects.Like;
import ldbc.snb.datagen.objects.Like.LikeType;
import ldbc.snb.datagen.objects.Message;
import ldbc.snb.datagen.serializer.PersonActivityExporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

/**
 * @author aprat
 */
public class LikeGenerator {
    private final PowerDistGenerator likesGenerator_;
    private Like like;


    public LikeGenerator() {
        likesGenerator_ = new PowerDistGenerator(1, DatagenParams.maxNumLike, 0.07);
        this.like = new Like();
    }

    public void generateLikes(Random random, final Forum forum, final Message message, LikeType type, PersonActivityExporter exporter) throws IOException {
        int numMembers = forum.memberships().size();
        int numLikes = likesGenerator_.getValue(random);
        numLikes = numLikes >= numMembers ? numMembers : numLikes;
        ArrayList<ForumMembership> memberships = forum.memberships();
        int startIndex = 0;
        if (numLikes < numMembers) {
            startIndex = random.nextInt(numMembers - numLikes);
        }
        for (int i = 0; i < numLikes; i++) {
            ForumMembership membership = memberships.get(startIndex + i);
            long minDate = message.creationDate() > memberships.get(startIndex + i).creationDate() ? message
                    .creationDate() : membership.creationDate();
            //long date = Math.max(Dictionaries.dates.randomSevenDays(random),DatagenParams.deltaTime) + minDate;
            long date = Dictionaries.dates.randomDate(random, minDate, Dictionaries.dates
                    .randomSevenDays(random) + minDate);
            /*if( date <= Dictionaries.dates.getEndDateTime() )*/
            {
                assert ((membership.person().creationDate() + DatagenParams.deltaTime) < date);
                like.user = membership.person().accountId();
                like.userCreationDate = membership.person().creationDate();
                like.messageId = message.messageId();
                like.date = date;
                like.type = type;
                exporter.export(like);
            }
        }
    }
}
