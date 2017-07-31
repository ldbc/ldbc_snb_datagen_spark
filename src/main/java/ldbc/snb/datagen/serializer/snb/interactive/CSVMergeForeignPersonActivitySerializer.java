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
import ldbc.snb.datagen.serializer.HDFSCSVWriter;
import ldbc.snb.datagen.serializer.PersonActivitySerializer;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by aprat on 17/02/15.
 */
public class CSVMergeForeignPersonActivitySerializer extends PersonActivitySerializer {
    private HDFSCSVWriter[] writers;
    private ArrayList<String> arguments;
    private String empty = "";

    private enum FileNames {
        FORUM("forum"),
        FORUM_HASMEMBER_PERSON("forum_hasMember_person"),
        FORUM_HASTAG_TAG("forum_hasTag_tag"),
        PERSON_LIKES_POST("person_likes_post"),
        PERSON_LIKES_COMMENT("person_likes_comment"),
        POST("post"),
        POST_HASTAG_TAG("post_hasTag_tag"),
        COMMENT("comment"),
        COMMENT_HASTAG_TAG("comment_hasTag_tag");

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
        int numFiles = FileNames.values().length;
        writers = new HDFSCSVWriter[numFiles];
        for (int i = 0; i < numFiles; ++i) {
            writers[i] = new HDFSCSVWriter(conf.get("ldbc.snb.datagen.serializer.socialNetworkDir"), FileNames
                    .values()[i].toString() + "_" + reducerId, conf.getInt("ldbc.snb.datagen.numPartitions", 1), conf
                                                   .getBoolean("ldbc.snb.datagen.serializer.compressed", false), "|", conf
                                                   .getBoolean("ldbc.snb.datagen.serializer.endlineSeparator", false));
        }
        arguments = new ArrayList<String>();

        arguments.add("id");
        arguments.add("title");
        arguments.add("creationDate");
        arguments.add("moderator");
        writers[FileNames.FORUM.ordinal()].writeHeader(arguments);
        arguments.clear();

        arguments.add("Forum.id");
        arguments.add("Person.id");
        arguments.add("joinDate");
        writers[FileNames.FORUM_HASMEMBER_PERSON.ordinal()].writeHeader(arguments);
        arguments.clear();

        arguments.add("Forum.id");
        arguments.add("Tag.id");
        writers[FileNames.FORUM_HASTAG_TAG.ordinal()].writeHeader(arguments);
        arguments.clear();

        arguments.add("Person.id");
        arguments.add("Post.id");
        arguments.add("creationDate");
        writers[FileNames.PERSON_LIKES_POST.ordinal()].writeHeader(arguments);
        arguments.clear();

        arguments.add("Person.id");
        arguments.add("Comment.id");
        arguments.add("creationDate");
        writers[FileNames.PERSON_LIKES_COMMENT.ordinal()].writeHeader(arguments);
        arguments.clear();

        arguments.add("id");
        arguments.add("imageFile");
        arguments.add("creationDate");
        arguments.add("locationIP");
        arguments.add("browserUsed");
        arguments.add("language");
        arguments.add("content");
        arguments.add("length");
        arguments.add("creator");
        arguments.add("Forum.id");
        arguments.add("place");
        writers[FileNames.POST.ordinal()].writeHeader(arguments);
        arguments.clear();

        arguments.add("Post.id");
        arguments.add("Tag.id");
        writers[FileNames.POST_HASTAG_TAG.ordinal()].writeHeader(arguments);
        arguments.clear();

        arguments.add("id");
        arguments.add("creationDate");
        arguments.add("locationIP");
        arguments.add("browserUsed");
        arguments.add("content");
        arguments.add("length");
        arguments.add("creator");
        arguments.add("place");
        arguments.add("replyOfPost");
        arguments.add("replyOfComment");
        writers[FileNames.COMMENT.ordinal()].writeHeader(arguments);
        arguments.clear();

        arguments.add("Comment.id");
        arguments.add("Tag.id");
        writers[FileNames.COMMENT_HASTAG_TAG.ordinal()].writeHeader(arguments);
        arguments.clear();
    }

    @Override
    public void close() {
        int numFiles = FileNames.values().length;
        for (int i = 0; i < numFiles; ++i) {
            writers[i].close();
        }
    }

    protected void serialize(final Forum forum) {

        String dateString = Dictionaries.dates.formatDateTime(forum.creationDate());

        arguments.add(Long.toString(forum.id()));
        arguments.add(forum.title());
        arguments.add(dateString);
        arguments.add(Long.toString(forum.moderator().accountId()));
        writers[FileNames.FORUM.ordinal()].writeEntry(arguments);
        arguments.clear();

        for (Integer i : forum.tags()) {
            arguments.add(Long.toString(forum.id()));
            arguments.add(Integer.toString(i));
            writers[FileNames.FORUM_HASTAG_TAG.ordinal()].writeEntry(arguments);
            arguments.clear();
        }

    }

    protected void serialize(final Post post) {

        arguments.add(Long.toString(post.messageId()));
        arguments.add(empty);
        arguments.add(Dictionaries.dates.formatDateTime(post.creationDate()));
        arguments.add(post.ipAddress().toString());
        arguments.add(Dictionaries.browsers.getName(post.browserId()));
        arguments.add(Dictionaries.languages.getLanguageName(post.language()));
        arguments.add(post.content());
        arguments.add(Integer.toString(post.content().length()));
        arguments.add(Long.toString(post.author().accountId()));
        arguments.add(Long.toString(post.forumId()));
        arguments.add(Integer.toString(post.countryId()));
        writers[FileNames.POST.ordinal()].writeEntry(arguments);
        arguments.clear();

        for (Integer t : post.tags()) {
            arguments.add(Long.toString(post.messageId()));
            arguments.add(Integer.toString(t));
            writers[FileNames.POST_HASTAG_TAG.ordinal()].writeEntry(arguments);
            arguments.clear();
        }
    }

    protected void serialize(final Comment comment) {
        arguments.add(Long.toString(comment.messageId()));
        arguments.add(Dictionaries.dates.formatDateTime(comment.creationDate()));
        arguments.add(comment.ipAddress().toString());
        arguments.add(Dictionaries.browsers.getName(comment.browserId()));
        arguments.add(comment.content());
        arguments.add(Integer.toString(comment.content().length()));
        arguments.add(Long.toString(comment.author().accountId()));
        arguments.add(Integer.toString(comment.countryId()));
        if (comment.replyOf() == comment.postId()) {
            arguments.add(Long.toString(comment.postId()));
            arguments.add(empty);
        } else {
            arguments.add(empty);
            arguments.add(Long.toString(comment.replyOf()));
        }
        writers[FileNames.COMMENT.ordinal()].writeEntry(arguments);
        arguments.clear();

        for (Integer t : comment.tags()) {
            arguments.add(Long.toString(comment.messageId()));
            arguments.add(Integer.toString(t));
            writers[FileNames.COMMENT_HASTAG_TAG.ordinal()].writeEntry(arguments);
            arguments.clear();
        }
    }

    protected void serialize(final Photo photo) {

        arguments.add(Long.toString(photo.messageId()));
        arguments.add(photo.content());
        arguments.add(Dictionaries.dates.formatDateTime(photo.creationDate()));
        arguments.add(photo.ipAddress().toString());
        arguments.add(Dictionaries.browsers.getName(photo.browserId()));
        arguments.add(empty);
        arguments.add(empty);
        arguments.add(Integer.toString(0));
        arguments.add(Long.toString(photo.author().accountId()));
        arguments.add(Long.toString(photo.forumId()));
        arguments.add(Integer.toString(photo.countryId()));
        writers[FileNames.POST.ordinal()].writeEntry(arguments);
        arguments.clear();

        for (Integer t : photo.tags()) {
            arguments.add(Long.toString(photo.messageId()));
            arguments.add(Integer.toString(t));
            writers[FileNames.POST_HASTAG_TAG.ordinal()].writeEntry(arguments);
            arguments.clear();
        }
    }

    protected void serialize(final ForumMembership membership) {
        arguments.add(Long.toString(membership.forumId()));
        arguments.add(Long.toString(membership.person().accountId()));
        arguments.add(Dictionaries.dates.formatDateTime(membership.creationDate()));
        writers[FileNames.FORUM_HASMEMBER_PERSON.ordinal()].writeEntry(arguments);
        arguments.clear();
    }

    protected void serialize(final Like like) {
        arguments.add(Long.toString(like.user));
        arguments.add(Long.toString(like.messageId));
        arguments.add(Dictionaries.dates.formatDateTime(like.date));
        if (like.type == Like.LikeType.POST || like.type == Like.LikeType.PHOTO) {
            writers[FileNames.PERSON_LIKES_POST.ordinal()].writeEntry(arguments);
            arguments.clear();
        } else {
            writers[FileNames.PERSON_LIKES_COMMENT.ordinal()].writeEntry(arguments);
            arguments.clear();
        }
    }

    public void reset() {
        // Intentionally left empty
    }
}
