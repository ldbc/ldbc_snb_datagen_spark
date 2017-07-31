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
package ldbc.snb.datagen.serializer.snb.interactive;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.objects.*;
import ldbc.snb.datagen.serializer.HDFSWriter;
import ldbc.snb.datagen.serializer.PersonActivitySerializer;
import ldbc.snb.datagen.serializer.Turtle;
import ldbc.snb.datagen.vocabulary.*;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.text.SimpleDateFormat;


/**
 * @author aprat
 */
public class TurtlePersonActivitySerializer extends PersonActivitySerializer {
    private HDFSWriter[] writers;
    private long membershipId = 0;
    private long likeId = 0;
    private SimpleDateFormat dateTimeFormat = null;

    private enum FileNames {
        SOCIAL_NETWORK("social_network_activity");

        private final String name;

        private FileNames(String name) {
            this.name = name;
        }

        public String toString() {
            return name;
        }
    }

    @Override
    public void initialize(Configuration conf, int reducerId) throws IOException {

        dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        int numFiles = FileNames.values().length;
        writers = new HDFSWriter[numFiles];
        for (int i = 0; i < numFiles; ++i) {
            writers[i] = new HDFSWriter(conf.get("ldbc.snb.datagen.serializer.socialNetworkDir"), FileNames.values()[i]
                    .toString() + "_" + reducerId, conf.getInt("ldbc.snb.datagen.numPartitions", 1), conf
                                                .getBoolean("ldbc.snb.datagen.serializer.compressed", false), "ttl");
            writers[i].writeAllPartitions(Turtle.getNamespaces());
            writers[i].writeAllPartitions(Turtle.getStaticNamespaces());
        }
    }

    @Override
    public void close() {
        int numFiles = FileNames.values().length;
        for (int i = 0; i < numFiles; ++i) {
            writers[i].close();
        }
    }

    protected void serialize(final Forum forum) {

        StringBuffer result = new StringBuffer(12000);

        String forumPrefix = SN.getForumURI(forum.id());
        Turtle.addTriple(result, true, false, forumPrefix, RDF.type, SNVOC.Forum);

        Turtle.addTriple(result, false, false, forumPrefix, SNVOC.id,
                         Turtle.createDataTypeLiteral(Long.toString(forum.id()), XSD.Long));

        Turtle.addTriple(result, false, false, forumPrefix, SNVOC.title,
                         Turtle.createLiteral(forum.title()));
        Turtle.addTriple(result, false, true, forumPrefix, SNVOC.creationDate,
                         Turtle.createDataTypeLiteral(dateTimeFormat.format(forum.creationDate()), XSD.DateTime));

        Turtle.createTripleSPO(result, forumPrefix,
                               SNVOC.hasModerator, SN.getPersonURI(forum.moderator().accountId()));

        for (Integer tag : forum.tags()) {
            String topic = Dictionaries.tags.getName(tag);
            Turtle.createTripleSPO(result, forumPrefix, SNVOC.hasTag, SNTAG.fullPrefixed(topic));
        }
        writers[FileNames.SOCIAL_NETWORK.ordinal()].write(result.toString());
    }

    protected void serialize(final Post post) {

        StringBuffer result = new StringBuffer(2500);

        String prefix = SN.getPostURI(post.messageId());

        Turtle.addTriple(result, true, false, prefix, RDF.type, SNVOC.Post);

        Turtle.addTriple(result, false, false, prefix, SNVOC.id,
                         Turtle.createDataTypeLiteral(Long.toString(post.messageId()), XSD.Long));

        Turtle.addTriple(result, false, false, prefix, SNVOC.creationDate,
                         Turtle.createDataTypeLiteral(dateTimeFormat.format(post.creationDate()), XSD.DateTime));

        Turtle.addTriple(result, false, false, prefix, SNVOC.ipaddress,
                         Turtle.createLiteral(post.ipAddress().toString()));
        Turtle.addTriple(result, false, false, prefix, SNVOC.browser,
                         Turtle.createLiteral(Dictionaries.browsers.getName(post.browserId())));

        Turtle.addTriple(result, false, false, prefix, SNVOC.content,
                         Turtle.createLiteral(post.content()));
        Turtle.addTriple(result, false, true, prefix, SNVOC.length,
                         Turtle.createDataTypeLiteral(Integer.toString(post.content().length()), XSD.Int));

        Turtle.createTripleSPO(result, prefix, SNVOC.language,
                               Turtle.createLiteral(Dictionaries.languages.getLanguageName(post.language())));

        Turtle.createTripleSPO(result, prefix, SNVOC.locatedIn,
                               DBP.fullPrefixed(Dictionaries.places.getPlaceName(post.countryId())));

        Turtle.createTripleSPO(result, SN.getForumURI(post.forumId()), SNVOC.containerOf, prefix);
        Turtle.createTripleSPO(result, prefix, SNVOC.hasCreator, SN.getPersonURI(post.author().accountId()));

        for (Integer tag : post.tags()) {
            String topic = Dictionaries.tags.getName(tag);
            Turtle.createTripleSPO(result, prefix, SNVOC.hasTag, SNTAG.fullPrefixed(topic));
        }
        writers[FileNames.SOCIAL_NETWORK.ordinal()].write(result.toString());
    }

    protected void serialize(final Comment comment) {
        StringBuffer result = new StringBuffer(2000);

        String prefix = SN.getCommentURI(comment.messageId());

        Turtle.addTriple(result, true, false, prefix, RDF.type, SNVOC.Comment);

        Turtle.addTriple(result, false, false, prefix, SNVOC.id,
                         Turtle.createDataTypeLiteral(Long.toString(comment.messageId()), XSD.Long));

        Turtle.addTriple(result, false, false, prefix, SNVOC.creationDate,
                         Turtle.createDataTypeLiteral(dateTimeFormat.format(comment.creationDate()), XSD.DateTime));
        Turtle.addTriple(result, false, false, prefix, SNVOC.ipaddress,
                         Turtle.createLiteral(comment.ipAddress().toString()));
        Turtle.addTriple(result, false, false, prefix, SNVOC.browser,
                         Turtle.createLiteral(Dictionaries.browsers.getName(comment.browserId())));
        Turtle.addTriple(result, false, false, prefix, SNVOC.content,
                         Turtle.createLiteral(comment.content()));
        Turtle.addTriple(result, false, true, prefix, SNVOC.length,
                         Turtle.createDataTypeLiteral(Integer.toString(comment.content().length()), XSD.Int));

        String replied = (comment.replyOf() == comment.postId()) ? SN.getPostURI(comment.postId()) :
                SN.getCommentURI(comment.replyOf());
        Turtle.createTripleSPO(result, prefix, SNVOC.replyOf, replied);
        Turtle.createTripleSPO(result, prefix, SNVOC.locatedIn,
                               DBP.fullPrefixed(Dictionaries.places.getPlaceName(comment.countryId())));

        Turtle.createTripleSPO(result, prefix, SNVOC.hasCreator,
                               SN.getPersonURI(comment.author().accountId()));

        for (Integer tag : comment.tags()) {
            String topic = Dictionaries.tags.getName(tag);
            Turtle.createTripleSPO(result, prefix, SNVOC.hasTag, SNTAG.fullPrefixed(topic));
        }
        writers[FileNames.SOCIAL_NETWORK.ordinal()].write(result.toString());
    }

    protected void serialize(final Photo photo) {
        StringBuffer result = new StringBuffer(2500);

        String prefix = SN.getPostURI(photo.messageId());
        Turtle.addTriple(result, true, false, prefix, RDF.type, SNVOC.Post);

        Turtle.addTriple(result, false, false, prefix, SNVOC.id,
                         Turtle.createDataTypeLiteral(Long.toString(photo.messageId()), XSD.Long));

        Turtle.addTriple(result, false, false, prefix, SNVOC.hasImage, Turtle.createLiteral(photo.content()));
        Turtle.addTriple(result, false, false, prefix, SNVOC.ipaddress,
                         Turtle.createLiteral(photo.ipAddress().toString()));
        Turtle.addTriple(result, false, false, prefix, SNVOC.browser,
                         Turtle.createLiteral(Dictionaries.browsers.getName(photo.browserId())));
        Turtle.addTriple(result, false, true, prefix, SNVOC.creationDate,
                         Turtle.createDataTypeLiteral(dateTimeFormat.format(photo.creationDate()), XSD.DateTime));

        Turtle.createTripleSPO(result, prefix, SNVOC.hasCreator, SN.getPersonURI(photo.author().accountId()));
        Turtle.createTripleSPO(result, SN.getForumURI(photo.forumId()), SNVOC.containerOf, prefix);
        Turtle.createTripleSPO(result, prefix, SNVOC.locatedIn,
                               DBP.fullPrefixed(Dictionaries.places.getPlaceName(photo.countryId())));

        for (Integer tag : photo.tags()) {
            String topic = Dictionaries.tags.getName(tag);
            Turtle.createTripleSPO(result, prefix, SNVOC.hasTag, SNTAG.fullPrefixed(topic));
        }
        writers[FileNames.SOCIAL_NETWORK.ordinal()].write(result.toString());
    }

    protected void serialize(final ForumMembership membership) {
        String memberhipPrefix = SN.getMembershipURI(SN.formId(membershipId));
        String forumPrefix = SN.getForumURI(membership.forumId());
        StringBuffer result = new StringBuffer(19000);
        Turtle.createTripleSPO(result, forumPrefix, SNVOC.hasMember, memberhipPrefix);

        Turtle.addTriple(result, true, false, memberhipPrefix, SNVOC.hasPerson, SN
                .getPersonURI(membership.person().accountId()));
        Turtle.addTriple(result, false, true, memberhipPrefix, SNVOC.joinDate,
                         Turtle.createDataTypeLiteral(dateTimeFormat.format(membership.creationDate()), XSD.DateTime));
        membershipId++;
        writers[FileNames.SOCIAL_NETWORK.ordinal()].write(result.toString());
    }

    protected void serialize(final Like like) {
        StringBuffer result = new StringBuffer(2500);
        long id = SN.formId(likeId);
        String likePrefix = SN.getLikeURI(id);
        Turtle.createTripleSPO(result, SN.getPersonURI(like.user),
                               SNVOC.like, likePrefix);

        if (like.type == Like.LikeType.POST || like.type == Like.LikeType.PHOTO) {
            String prefix = SN.getPostURI(like.messageId);
            Turtle.addTriple(result, true, false, likePrefix, SNVOC.hasPost, prefix);
        } else {
            String prefix = SN.getCommentURI(like.messageId);
            Turtle.addTriple(result, true, false, likePrefix, SNVOC.hasComment, prefix);
        }
        Turtle.addTriple(result, false, true, likePrefix, SNVOC.creationDate,
                         Turtle.createDataTypeLiteral(dateTimeFormat.format(like.date), XSD.DateTime));
        likeId++;
        writers[FileNames.SOCIAL_NETWORK.ordinal()].write(result.toString());
    }

    public void reset() {
        likeId = 0;
        membershipId = 0;

    }

}
