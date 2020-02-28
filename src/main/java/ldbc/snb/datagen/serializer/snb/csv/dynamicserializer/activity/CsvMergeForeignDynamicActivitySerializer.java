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
package ldbc.snb.datagen.serializer.snb.csv.dynamicserializer.activity;

import com.google.common.collect.ImmutableList;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.messages.Comment;
import ldbc.snb.datagen.entities.dynamic.messages.Photo;
import ldbc.snb.datagen.entities.dynamic.messages.Post;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.dynamic.relations.Like;
import ldbc.snb.datagen.hadoop.writer.HdfsCsvWriter;
import ldbc.snb.datagen.serializer.DynamicActivitySerializer;
import ldbc.snb.datagen.serializer.snb.csv.CsvSerializer;
import ldbc.snb.datagen.serializer.snb.csv.FileName;

import java.util.List;

import static ldbc.snb.datagen.serializer.snb.csv.FileName.*;

public class CsvMergeForeignDynamicActivitySerializer extends DynamicActivitySerializer<HdfsCsvWriter> implements CsvSerializer {

    @Override
    public List<FileName> getFileNames() {
        return ImmutableList.of(FORUM, FORUM_HASMEMBER_PERSON, FORUM_HASTAG_TAG, PERSON_LIKES_POST,
                PERSON_LIKES_COMMENT, POST, POST_HASTAG_TAG, COMMENT, COMMENT_HASTAG_TAG);
    }

    @Override
    public void writeFileHeaders() {
        writers.get(FORUM)                 .writeHeader(ImmutableList.of("creationDate", "deletionDate", "id", "title", "moderator"));
        writers.get(FORUM_HASTAG_TAG)      .writeHeader(ImmutableList.of("creationDate", "deletionDate", "Forum.id", "Tag.id"));
        writers.get(POST)                  .writeHeader(ImmutableList.of("creationDate", "deletionDate", "id", "imageFile", "locationIP", "browserUsed", "language", "content", "length", "creator", "Forum.id", "place"));
        writers.get(POST_HASTAG_TAG)       .writeHeader(ImmutableList.of("creationDate", "deletionDate", "Post.id", "Tag.id"));
        writers.get(COMMENT)               .writeHeader(ImmutableList.of("creationDate", "deletionDate", "id", "locationIP", "browserUsed", "content", "length", "creator", "place", "replyOfPost", "replyOfComment"));
        writers.get(COMMENT_HASTAG_TAG)    .writeHeader(ImmutableList.of("creationDate", "deletionDate", "Comment.id", "Tag.id"));
        writers.get(FORUM_HASMEMBER_PERSON).writeHeader(ImmutableList.of("creationDate", "deletionDate", "Forum.id", "Person.id"));
        writers.get(PERSON_LIKES_POST)     .writeHeader(ImmutableList.of("creationDate", "deletionDate", "Person.id", "Post.id"));
        writers.get(PERSON_LIKES_COMMENT)  .writeHeader(ImmutableList.of("creationDate", "deletionDate", "Person.id", "Comment.id"));

    }

    protected void serialize(final Forum forum) {
        //"creationDate", "deletionDate", "id", "title", "creationDate", "moderator"
        writers.get(FORUM).writeEntry(ImmutableList.of(
                Dictionaries.dates.formatDateTime(forum.getCreationDate()),
                Dictionaries.dates.formatDateTime(forum.getDeletionDate()),
                Long.toString(forum.getId()),
                forum.getTitle(),
                Long.toString(forum.getModerator().getAccountId())
        ));

        for (Integer i : forum.getTags()) {
            //"creationDate", "deletionDate", "Forum.id", "Tag.id,", "creationDate"
            writers.get(FORUM_HASTAG_TAG).writeEntry(ImmutableList.of(
                    Dictionaries.dates.formatDateTime(forum.getCreationDate()),
                    Dictionaries.dates.formatDateTime(forum.getDeletionDate()),
                    Long.toString(forum.getId()),
                    Integer.toString(i)
            ));
        }

    }

    protected void serialize(final Post post) {
        //"creationDate", "deletionDate", "id", "imageFile", "creationDate", "locationIP", "browserUsed", "language", "content", "length", "creator", "Forum.id", "place"
        writers.get(POST).writeEntry(ImmutableList.of(
            Dictionaries.dates.formatDateTime(post.getCreationDate()),
            Dictionaries.dates.formatDateTime(post.getDeletionDate()),
            Long.toString(post.getMessageId()),
            "",
            post.getIpAddress().toString(),
            Dictionaries.browsers.getName(post.getBrowserId()),
            Dictionaries.languages.getLanguageName(post.getLanguage()),
            post.getContent(),
            Integer.toString(post.getContent().length()),
            Long.toString(post.getAuthor().getAccountId()),
            Long.toString(post.getForumId()),
            Integer.toString(post.getCountryId())
        ));

        for (Integer t : post.getTags()) {
            //"creationDate", "deletionDate", "Post.id", "Tag.id", "creationDate"
            writers.get(POST_HASTAG_TAG).writeEntry(ImmutableList.of(
                Dictionaries.dates.formatDateTime(post.getCreationDate()),
                Dictionaries.dates.formatDateTime(post.getDeletionDate()),
                Long.toString(post.getMessageId()),
                Integer.toString(t)
            ));
        }
    }

    protected void serialize(final Comment comment) {
        //"creationDate", "deletionDate", "id", "creationDate", "locationIP", "browserUsed", "content", "length", "creator", "place", "replyOfPost", "replyOfComment"
        writers.get(COMMENT).writeEntry(ImmutableList.of(
            Dictionaries.dates.formatDateTime(comment.getCreationDate()),
            Dictionaries.dates.formatDateTime(comment.getDeletionDate()),
            Long.toString(comment.getMessageId()),
            comment.getIpAddress().toString(),
            Dictionaries.browsers.getName(comment.getBrowserId()),
            comment.getContent(),
            Integer.toString(comment.getContent().length()),
            Long.toString(comment.getAuthor().getAccountId()),
            Integer.toString(comment.getCountryId()),
            comment.replyOf() == comment.postId() ? Long.toString(comment.postId()) : "",
            comment.replyOf() == comment.postId() ? "" : Long.toString(comment.replyOf())
        ));

        for (Integer t : comment.getTags()) {
            //"creationDate", "deletionDate", "Comment.id", "Tag.id", "creationDate"
            writers.get(COMMENT_HASTAG_TAG).writeEntry(ImmutableList.of(
                Dictionaries.dates.formatDateTime(comment.getCreationDate()),
                Dictionaries.dates.formatDateTime(comment.getDeletionDate()),
                Long.toString(comment.getMessageId()),
                Integer.toString(t)
            ));
        }
    }

    protected void serialize(final Photo photo) {
        //"creationDate", "deletionDate", "id", "imageFile", "creationDate", "locationIP", "browserUsed", "language", "content", "length", "creator", "Forum.id", "place"
        writers.get(POST).writeEntry(ImmutableList.of(
            Dictionaries.dates.formatDateTime(photo.getCreationDate()),
            Dictionaries.dates.formatDateTime(photo.getDeletionDate()),
            Long.toString(photo.getMessageId()),
            photo.getContent(),
            photo.getIpAddress().toString(),
            Dictionaries.browsers.getName(photo.getBrowserId()),
            "",
            "",
            Integer.toString(0),
            Long.toString(photo.getAuthor().getAccountId()),
            Long.toString(photo.getForumId()),
            Integer.toString(photo.getCountryId())
        ));

        for (Integer t : photo.getTags()) {
            //"creationDate", "deletionDate", "Post.id", "Tag.id", "creationDate"
            writers.get(POST_HASTAG_TAG).writeEntry(ImmutableList.of(
                Dictionaries.dates.formatDateTime(photo.getCreationDate()),
                Dictionaries.dates.formatDateTime(photo.getDeletionDate()),
                Long.toString(photo.getMessageId()),
                Integer.toString(t)
            ));
        }
    }

    protected void serialize(final ForumMembership membership) {
        //"creationDate", "deletionDate", "Forum.id", "Person.id"
        writers.get(FORUM_HASMEMBER_PERSON).writeEntry(ImmutableList.of(
            Dictionaries.dates.formatDateTime(membership.getCreationDate()),
            Dictionaries.dates.formatDateTime(membership.getDeletionDate()),
            Long.toString(membership.getForumId()),
            Long.toString(membership.getPerson().getAccountId())
        ));
    }

    protected void serialize(final Like like) {
        //"creationDate", "deletionDate", "Person.id", "Post.id"/"Comment.id"
        List<String> arguments = ImmutableList.of(
            Dictionaries.dates.formatDateTime(like.likeCreationDate),
            Dictionaries.dates.formatDateTime(like.likeDeletionDate),
            Long.toString(like.person),
            Long.toString(like.messageId)
        );
        if (like.type == Like.LikeType.POST || like.type == Like.LikeType.PHOTO) {
            writers.get(PERSON_LIKES_POST).writeEntry(arguments);
        } else {
            writers.get(PERSON_LIKES_COMMENT).writeEntry(arguments);
        }
    }

}
