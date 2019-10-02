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
package ldbc.snb.datagen.generator.generators;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.messages.Comment;
import ldbc.snb.datagen.entities.dynamic.messages.Message;
import ldbc.snb.datagen.entities.dynamic.messages.Post;
import ldbc.snb.datagen.entities.dynamic.person.IP;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.dynamic.relations.Like;
import ldbc.snb.datagen.generator.generators.textgenerators.TextGenerator;
import ldbc.snb.datagen.serializer.PersonActivityExporter;
import ldbc.snb.datagen.util.PersonBehavior;
import ldbc.snb.datagen.util.RandomGeneratorFarm;
import ldbc.snb.datagen.vocabulary.SN;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.TreeSet;

public class CommentGenerator {
    private String[] shortComments_ = {"ok", "good", "great", "cool", "thx", "fine", "LOL", "roflol", "no way!", "I see", "right", "yes", "no", "duh", "thanks", "maybe"};
    private TextGenerator generator;
    private LikeGenerator likeGenerator_;

    public CommentGenerator(TextGenerator generator, LikeGenerator likeGenerator) {
        this.generator = generator;
        this.likeGenerator_ = likeGenerator;
    }

    public long createComments(RandomGeneratorFarm randomFarm, final Forum forum, final Post post, long numComments, long startId, PersonActivityExporter exporter) throws IOException {
        long nextId = startId;
        List<Message> replyCandidates = new ArrayList<>();
        replyCandidates.add(post);

        Properties prop = new Properties();
        prop.setProperty("type", "comment");
        for (int i = 0; i < numComments; ++i) {
            int replyIndex = randomFarm.get(RandomGeneratorFarm.Aspect.REPLY_TO).nextInt(replyCandidates.size());
            Message replyTo = replyCandidates.get(replyIndex);
            List<ForumMembership> validMemberships = new ArrayList<>();
            for (ForumMembership fM : forum.memberships()) {
                if (fM.creationDate() + DatagenParams.deltaTime <= replyTo.creationDate()) {
                    validMemberships.add(fM);
                }
            }
            if (validMemberships.size() == 0) {
                return nextId;
            }
            ForumMembership member = validMemberships.get(randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX)
                                                                    .nextInt(validMemberships.size()));
            TreeSet<Integer> tags = new TreeSet<>();
            String content = "";


            boolean isShort = false;
            if (randomFarm.get(RandomGeneratorFarm.Aspect.REDUCED_TEXT).nextDouble() > 0.6666) {

                List<Integer> currentTags = new ArrayList<>();
                Iterator<Integer> it = replyTo.tags().iterator();
                while (it.hasNext()) {
                    Integer tag = it.next();
                    if (randomFarm.get(RandomGeneratorFarm.Aspect.TAG).nextDouble() > 0.5) {
                        tags.add(tag);
                    }
                    currentTags.add(tag);
                }

                for (int j = 0; j < (int) Math.ceil(replyTo.tags().size() / 2.0); ++j) {
                    int randomTag = currentTags.get(randomFarm.get(RandomGeneratorFarm.Aspect.TAG)
                                                              .nextInt(currentTags.size()));
                    tags.add(Dictionaries.tagMatrix
                                     .getRandomRelated(randomFarm.get(RandomGeneratorFarm.Aspect.TOPIC), randomTag));
                }
                content = this.generator.generateText(member.person(), tags, prop);
            } else {
                isShort = true;
                int index = randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE).nextInt(shortComments_.length);
                content = shortComments_[index];
            }

            long baseDate = Math.max(replyTo.creationDate(), member.creationDate()) + DatagenParams.deltaTime;
            long creationDate = Dictionaries.dates.powerlawCommDateDay(randomFarm.get(RandomGeneratorFarm.Aspect
                                                                                              .DATE), baseDate);
            int country = member.person().countryId();
            IP ip = member.person().ipAddress();
            Random random = randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP_FOR_TRAVELER);
            if (PersonBehavior.changeUsualCountry(random, creationDate)) {
                random = randomFarm.get(RandomGeneratorFarm.Aspect.COUNTRY);
                country = Dictionaries.places.getRandomCountryUniform(random);
                random = randomFarm.get(RandomGeneratorFarm.Aspect.IP);
                ip = Dictionaries.ips.getIP(random, country);
            }

            Comment comment = new Comment(SN.formId(SN.composeId(nextId++, creationDate)),
                                          creationDate,
                                          member.person(),
                                          forum.id(),
                                          content,
                                          tags,
                                          country,
                                          ip,
                                          Dictionaries.browsers.getPostBrowserId(randomFarm
                                                                                         .get(RandomGeneratorFarm.Aspect.DIFF_BROWSER), randomFarm
                                                                                         .get(RandomGeneratorFarm.Aspect.BROWSER), member
                                                                                         .person().browserId()),
                                          post.messageId(),
                                          replyTo.messageId());
            if (!isShort) replyCandidates.add(new Comment(comment));
            exporter.export(comment);
            if (comment.content().length() > 10 && randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LIKE)
                                                             .nextDouble() <= 0.1) {
                likeGenerator_.generateLikes(randomFarm
                                                     .get(RandomGeneratorFarm.Aspect.NUM_LIKE), forum, comment, Like.LikeType.COMMENT, exporter);
            }
        }
        replyCandidates.clear();
        return nextId;
    }

}
