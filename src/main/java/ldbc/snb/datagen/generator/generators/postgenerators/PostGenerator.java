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

package ldbc.snb.datagen.generator.generators.postgenerators;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.messages.Post;
import ldbc.snb.datagen.entities.dynamic.person.IP;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.dynamic.relations.Like;
import ldbc.snb.datagen.generator.generators.CommentGenerator;
import ldbc.snb.datagen.generator.generators.LikeGenerator;
import ldbc.snb.datagen.generator.generators.textgenerators.TextGenerator;
import ldbc.snb.datagen.serializer.PersonActivityExporter;
import ldbc.snb.datagen.util.PersonBehavior;
import ldbc.snb.datagen.util.RandomGeneratorFarm;
import ldbc.snb.datagen.vocabulary.SN;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.TreeSet;


abstract public class PostGenerator {

    private TextGenerator generator;
    private CommentGenerator commentGenerator;
    private LikeGenerator likeGenerator;
    private Post post;

    static class PostCore {

        private TreeSet<Integer> tags;
        private long creationDate;
        private long deletionDate;

        PostCore() {
            this.tags = new TreeSet<>();
        }

        public TreeSet<Integer> getTags() {
            return tags;
        }

        public void setTags(TreeSet<Integer> tags) {
            this.tags = tags;
        }

        public long getCreationDate() {
            return creationDate;
        }

        public void setCreationDate(long creationDate) {
            this.creationDate = creationDate;
        }

        public long getDeletionDate() {
            return deletionDate;
        }

        public void setDeletionDate(long deletionDate) {
            this.deletionDate = deletionDate;
        }
    }


    PostGenerator(TextGenerator generator, CommentGenerator commentGenerator, LikeGenerator likeGenerator) {
        this.generator = generator;
        this.commentGenerator = commentGenerator;
        this.likeGenerator = likeGenerator;
        this.post = new Post();
    }


    public void initialize() {
        // Intentionally left empty
    }

    public long createPosts(RandomGeneratorFarm randomFarm, final Forum forum, final List<ForumMembership> memberships,
                            long numPostsInForum, long startPostId, PersonActivityExporter exporter) throws IOException {

        long postIdCounter = startPostId;
        Properties properties = new Properties();
        properties.setProperty("type", "post");

        for (ForumMembership member : memberships) {

            // generate number of posts by this member
            double numPostsPerMember = numPostsInForum / (double) memberships.size();

            if (numPostsPerMember < 1.0) {
                double prob = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_POST).nextDouble();
                if (prob < numPostsPerMember) numPostsPerMember = 1.0;
            } else {
                numPostsPerMember = Math.ceil(numPostsPerMember);
            }

            // create each post for member
            for (int i = 0; i < (int) (numPostsPerMember); ++i) {

                // create post core
                PostCore postCore = generatePostInfo(randomFarm.get(RandomGeneratorFarm.Aspect.TAG),
                        randomFarm.get(RandomGeneratorFarm.Aspect.DATE), forum, member);

                if (postCore != null) {

                    // create content, county, ip - sometimes randomise
                    String content = this.generator.generateText(member.getPerson(), postCore.tags, properties);
                    int country = member.getPerson().getCountryId();
                    IP ip = member.getPerson().getIpAddress();
                    Random random = randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP_FOR_TRAVELER);
                    if (PersonBehavior.changeUsualCountry(random, postCore.getCreationDate())) {
                        random = randomFarm.get(RandomGeneratorFarm.Aspect.COUNTRY);
                        country = Dictionaries.places.getRandomCountryUniform(random);
                        random = randomFarm.get(RandomGeneratorFarm.Aspect.IP);
                        ip = Dictionaries.ips.getIP(random, country);
                    }

                    // create post with above information and from post info
                    post.initialize(SN.formId(SN.composeId(postIdCounter++, postCore.getCreationDate())),
                                     postCore.getCreationDate(),
                                     postCore.getDeletionDate(),
                                     member.getPerson(),
                                     forum.getId(),
                                     content,
                                     postCore.tags,
                                     country,
                                     ip,
                                     Dictionaries.browsers.getPostBrowserId(
                                             randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_BROWSER),
                                             randomFarm.get(RandomGeneratorFarm.Aspect.BROWSER),
                                             member.getPerson().getBrowserId()),
                                     forum.getLanguage());

                    exporter.export(post);

                    // generate likes
                    if (randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LIKE).nextDouble() <= 0.1) {
                        likeGenerator.generateLikes(randomFarm
                                                             .get(RandomGeneratorFarm.Aspect.NUM_LIKE), forum, post, Like.LikeType.POST, exporter);
                    }

                    // generate comments
                    int numComments = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_COMMENT).nextInt(DatagenParams.maxNumComments + 1);
                    postIdCounter = commentGenerator.createComments(randomFarm, forum, post, numComments, postIdCounter, exporter);
                }
            }
        }
        return postIdCounter;
    }

    protected abstract PostCore generatePostInfo(Random randomTag, Random randomDate, final Forum forum, final ForumMembership membership);
}
