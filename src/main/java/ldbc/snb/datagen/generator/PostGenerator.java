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
import ldbc.snb.datagen.objects.*;
import ldbc.snb.datagen.serializer.PersonActivityExporter;
import ldbc.snb.datagen.util.PersonBehavior;
import ldbc.snb.datagen.util.RandomGeneratorFarm;
import ldbc.snb.datagen.vocabulary.SN;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.util.TreeSet;


abstract public class PostGenerator {

    private TextGenerator generator_;
    private CommentGenerator commentGenerator_;
    private LikeGenerator likeGenerator_;
    private Post post_;

    static protected class PostInfo {
        public TreeSet<Integer> tags;
        public long date;

        public PostInfo() {
            this.tags = new TreeSet<Integer>();
        }
    }

	
	/* A set of random number generator for different purposes.*/

    public PostGenerator(TextGenerator generator, CommentGenerator commentGenerator, LikeGenerator likeGenerator) {
        this.generator_ = generator;
        this.commentGenerator_ = commentGenerator;
        this.likeGenerator_ = likeGenerator;
        this.post_ = new Post();
    }

    /**
     * @brief Initializes the post generator.
     */
    public void initialize() {
        // Intentionally left empty
    }


    public long createPosts(RandomGeneratorFarm randomFarm, final Forum forum, final ArrayList<ForumMembership> memberships, long numPosts, long startId, PersonActivityExporter exporter) throws IOException {
        long postId = startId;
        Properties prop = new Properties();
        prop.setProperty("type", "post");
        for (ForumMembership member : memberships) {
            double numPostsMember = numPosts / (double) memberships.size();
            if (numPostsMember < 1.0) {
                double prob = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_POST).nextDouble();
                if (prob < numPostsMember) numPostsMember = 1.0;
            } else {
                numPostsMember = Math.ceil(numPostsMember);
            }
            for (int i = 0; i < (int) (numPostsMember); ++i) {
                PostInfo postInfo = generatePostInfo(randomFarm.get(RandomGeneratorFarm.Aspect.TAG), randomFarm
                        .get(RandomGeneratorFarm.Aspect.DATE), forum, member);
                if (postInfo != null) {

                    String content = "";

                    // crear properties class para passar
                    content = this.generator_.generateText(member.person(), postInfo.tags, prop);

                    int country = member.person().countryId();
                    IP ip = member.person().ipAddress();
                    Random random = randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP_FOR_TRAVELER);
                    if (PersonBehavior.changeUsualCountry(random, postInfo.date)) {
                        random = randomFarm.get(RandomGeneratorFarm.Aspect.COUNTRY);
                        country = Dictionaries.places.getRandomCountryUniform(random);
                        random = randomFarm.get(RandomGeneratorFarm.Aspect.IP);
                        ip = Dictionaries.ips.getIP(random, country);
                    }

                    post_.initialize(SN.formId(SN.composeId(postId++, postInfo.date)),
                                     postInfo.date,
                                     member.person(),
                                     forum.id(),
                                     content,
                                     postInfo.tags,
                                     country,
                                     ip,
                                     Dictionaries.browsers.getPostBrowserId(randomFarm
                                                                                    .get(RandomGeneratorFarm.Aspect.DIFF_BROWSER), randomFarm
                                                                                    .get(RandomGeneratorFarm.Aspect.BROWSER), member
                                                                                    .person().browserId()),
                                     forum.language());
                    exporter.export(post_);

                    if (randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LIKE).nextDouble() <= 0.1) {
                        likeGenerator_.generateLikes(randomFarm
                                                             .get(RandomGeneratorFarm.Aspect.NUM_LIKE), forum, post_, Like.LikeType.POST, exporter);
                    }

                    //// generate comments
                    int numComments = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_COMMENT)
                                                .nextInt(DatagenParams.maxNumComments + 1);
                    postId = commentGenerator_.createComments(randomFarm, forum, post_, numComments, postId, exporter);
                }
            }
        }
        return postId;
    }

    protected abstract PostInfo generatePostInfo(Random randomTag, Random randomDate, final Forum forum, final ForumMembership membership);
}
