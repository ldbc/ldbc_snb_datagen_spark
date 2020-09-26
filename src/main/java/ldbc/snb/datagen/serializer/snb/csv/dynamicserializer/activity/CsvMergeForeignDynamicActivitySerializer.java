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
import ldbc.snb.datagen.DatagenMode;
import ldbc.snb.datagen.DatagenParams;
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
        List<String> dates = (DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) ?
                ImmutableList.of("creationDate", "deletionDate") :
                ImmutableList.of("creationDate");

        writers.get(FORUM)                 .writeHeader(dates, ImmutableList.of("id", "title", "moderator"));
        writers.get(FORUM_HASTAG_TAG)      .writeHeader(dates, ImmutableList.of("Forum.id", "Tag.id"));
        writers.get(FORUM_HASMEMBER_PERSON).writeHeader(dates, ImmutableList.of("Forum.id", "Person.id"));

        writers.get(POST)                  .writeHeader(dates, ImmutableList.of("id", "imageFile", "locationIP", "browserUsed", "language", "content", "length", "creator", "Forum.id", "place"));
        writers.get(POST_HASTAG_TAG)       .writeHeader(dates, ImmutableList.of("Post.id", "Tag.id"));

        writers.get(COMMENT)               .writeHeader(dates, ImmutableList.of("id", "locationIP", "browserUsed", "content", "length", "creator", "place", "replyOfPost", "replyOfComment"));
        writers.get(COMMENT_HASTAG_TAG)    .writeHeader(dates, ImmutableList.of("Comment.id", "Tag.id"));

        writers.get(PERSON_LIKES_POST)     .writeHeader(dates, ImmutableList.of("Person.id", "Post.id"));
        writers.get(PERSON_LIKES_COMMENT)  .writeHeader(dates, ImmutableList.of("Person.id", "Comment.id"));

    }

    protected void serialize(final Forum forum) {
        List<String> dates = (DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) ?
                ImmutableList.of(Dictionaries.dates.formatDateTime(forum.getCreationDate()), Dictionaries.dates.formatDateTime(forum.getDeletionDate())) :
                ImmutableList.of(Dictionaries.dates.formatDateTime(forum.getCreationDate()));
        List<String> moderatorDates = (DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) ?
                ImmutableList.of(Dictionaries.dates.formatDateTime(forum.getCreationDate()) + DatagenParams.delta, Dictionaries.dates.formatDateTime(forum.getDeletionDate())) :
                ImmutableList.of(Dictionaries.dates.formatDateTime(forum.getCreationDate()) + DatagenParams.delta);
        // TODO amend moderator dates

        // creationDate, [deletionDate,] id, title, creationDate, moderator
        writers.get(FORUM).writeEntry(dates, ImmutableList.of(
                Long.toString(forum.getId()),
                forum.getTitle(),
                Long.toString(forum.getModerator().getAccountId())
        ));

        for (Integer i : forum.getTags()) {
            // creationDate, [deletionDate,] Forum.id, Tag.id
            writers.get(FORUM_HASTAG_TAG).writeEntry(dates, ImmutableList.of(
                    Long.toString(forum.getId()),
                    Integer.toString(i)
            ));
        }

    }

    protected void serialize(final Post post) {
        List<String> dates = (DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) ?
                ImmutableList.of(Dictionaries.dates.formatDateTime(post.getCreationDate()), Dictionaries.dates.formatDateTime(post.getDeletionDate())) :
                ImmutableList.of(Dictionaries.dates.formatDateTime(post.getCreationDate()));

        // creationDate, [deletionDate,] id, imageFile, creationDate, locationIP, browserUsed, language, content, length, creator, Forum.id, place
        writers.get(POST).writeEntry(dates, ImmutableList.of(
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
            // creationDate, [deletionDate,] Post.id, Tag.id, creationDate
            writers.get(POST_HASTAG_TAG).writeEntry(dates, ImmutableList.of(
                Long.toString(post.getMessageId()),
                Integer.toString(t)
            ));
        }
    }

    protected void serialize(final Comment comment) {
        List<String> dates = (DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) ?
                ImmutableList.of(Dictionaries.dates.formatDateTime(comment.getCreationDate()), Dictionaries.dates.formatDateTime(comment.getDeletionDate())) :
                ImmutableList.of(Dictionaries.dates.formatDateTime(comment.getCreationDate()));

        // creationDate, [deletionDate,] id, creationDate, locationIP, browserUsed, content, length, creator, place, replyOfPost, replyOfComment
        writers.get(COMMENT).writeEntry(dates, ImmutableList.of(
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
            // creationDate, [deletionDate,] Comment.id, Tag.id, creationDate
            writers.get(COMMENT_HASTAG_TAG).writeEntry(dates, ImmutableList.of(
                Long.toString(comment.getMessageId()),
                Integer.toString(t)
            ));
        }
    }

    protected void serialize(final Photo photo) {
        List<String> dates = (DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) ?
                ImmutableList.of(Dictionaries.dates.formatDateTime(photo.getCreationDate()), Dictionaries.dates.formatDateTime(photo.getDeletionDate())) :
                ImmutableList.of(Dictionaries.dates.formatDateTime(photo.getCreationDate()));

        // creationDate, [deletionDate,] id, imageFile, creationDate, locationIP, browserUsed, language, content, length, creator, Forum.id, place
        writers.get(POST).writeEntry(dates, ImmutableList.of(
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
            // creationDate, [deletionDate,] Post.id, Tag.id, creationDate
            writers.get(POST_HASTAG_TAG).writeEntry(dates, ImmutableList.of(
                Long.toString(photo.getMessageId()),
                Integer.toString(t)
            ));
        }
    }

    protected void serialize(final ForumMembership membership) {
        List<String> dates = (DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) ?
                ImmutableList.of(Dictionaries.dates.formatDateTime(membership.getCreationDate()), Dictionaries.dates.formatDateTime(membership.getDeletionDate())) :
                ImmutableList.of(Dictionaries.dates.formatDateTime(membership.getCreationDate()));

        // creationDate, [deletionDate,] Forum.id, Person.id
        writers.get(FORUM_HASMEMBER_PERSON).writeEntry(dates, ImmutableList.of(
            Long.toString(membership.getForumId()),
            Long.toString(membership.getPerson().getAccountId())
        ));
    }

    protected void serialize(final Like like) {
        List<String> dates = (DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) ?
                ImmutableList.of(Dictionaries.dates.formatDateTime(like.getCreationDate()), Dictionaries.dates.formatDateTime(like.getDeletionDate())) :
                ImmutableList.of(Dictionaries.dates.formatDateTime(like.getCreationDate()));

        // creationDate, [deletionDate,] Person.id, Post.id/Comment.id
        List<String> arguments = ImmutableList.of(
            Long.toString(like.getPerson()),
            Long.toString(like.getMessageId())
        );
        if (like.getType() == Like.LikeType.POST || like.getType() == Like.LikeType.PHOTO) {
            writers.get(PERSON_LIKES_POST).writeEntry(dates, arguments);
        } else {
            writers.get(PERSON_LIKES_COMMENT).writeEntry(dates, arguments);
        }
    }

}
