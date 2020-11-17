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

/**
 * Serializer for the bulk load component.
 */
public class CsvBasicDynamicActivitySerializer extends DynamicActivitySerializer<HdfsCsvWriter> implements CsvSerializer {

    @Override
    public List<FileName> getFileNames() {
        return ImmutableList.of(FORUM, FORUM_CONTAINEROF_POST, FORUM_HASMEMBER_PERSON, FORUM_HASMODERATOR_PERSON, FORUM_HASTAG_TAG,
                PERSON_LIKES_POST, PERSON_LIKES_COMMENT, POST, POST_HASCREATOR_PERSON, POST_HASTAG_TAG, POST_ISLOCATEDIN_PLACE,
                COMMENT, COMMENT_HASCREATOR_PERSON, COMMENT_HASTAG_TAG, COMMENT_ISLOCATEDIN_PLACE, COMMENT_REPLYOF_POST,
                COMMENT_REPLYOF_COMMENT);
    }

    @Override
    public void writeFileHeaders() {
        List<String> dates = (DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) ?
                ImmutableList.of("creationDate", "deletionDate", "explicitlyDeleted") :
                ImmutableList.of("creationDate");

        writers.get(FORUM)                    .writeHeader(dates, ImmutableList.of("id", "title", "type"));
        writers.get(FORUM_HASMODERATOR_PERSON).writeHeader(dates, ImmutableList.of("Forum.id", "Person.id"));
        writers.get(FORUM_HASTAG_TAG)         .writeHeader(dates, ImmutableList.of("Forum.id", "Tag.id"));
        writers.get(FORUM_HASMEMBER_PERSON)   .writeHeader(dates, ImmutableList.of("Forum.id", "Person.id", "type"));

        List<String> postEnd = (DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) ?
                ImmutableList.of("Forum.id") :
                ImmutableList.of();
        writers.get(POST)                     .writeHeader(dates, ImmutableList.of("id", "imageFile", "locationIP", "browserUsed", "language", "content", "length"), postEnd);
        writers.get(POST_HASCREATOR_PERSON)   .writeHeader(dates, ImmutableList.of("Post.id", "Person.id"));
        writers.get(POST_ISLOCATEDIN_PLACE)   .writeHeader(dates, ImmutableList.of("Post.id", "Place.id"));
        writers.get(POST_HASTAG_TAG)          .writeHeader(dates, ImmutableList.of("Post.id", "Tag.id"));
        writers.get(FORUM_CONTAINEROF_POST)   .writeHeader(dates, ImmutableList.of("Forum.id", "Post.id"));

        writers.get(COMMENT)                  .writeHeader(dates, ImmutableList.of("id", "locationIP", "browserUsed", "content", "length"));
        writers.get(COMMENT_REPLYOF_POST)     .writeHeader(dates, ImmutableList.of("Comment.id", "ParentPost.id"));
        writers.get(COMMENT_REPLYOF_COMMENT)  .writeHeader(dates, ImmutableList.of("Comment.id", "ParentComment.id"));
        writers.get(COMMENT_HASCREATOR_PERSON).writeHeader(dates, ImmutableList.of("Comment.id", "Person.id"));
        writers.get(COMMENT_ISLOCATEDIN_PLACE).writeHeader(dates, ImmutableList.of("Comment.id", "Place.id"));
        writers.get(COMMENT_HASTAG_TAG)       .writeHeader(dates, ImmutableList.of("Comment.id", "Tag.id"));

        writers.get(PERSON_LIKES_POST)        .writeHeader(dates, ImmutableList.of("Person.id", "Post.id"));
        writers.get(PERSON_LIKES_COMMENT)     .writeHeader(dates, ImmutableList.of("Person.id", "Comment.id"));
    }

    protected void serialize(final Forum forum) {
        List<String> dates = (DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) ?
                ImmutableList.of(Dictionaries.dates.formatDateTime(forum.getCreationDate()), Dictionaries.dates.formatDateTime(forum.getDeletionDate()), forum.isExplicitlyDeleted()? "true" : "false") :
                ImmutableList.of(Dictionaries.dates.formatDateTime(forum.getCreationDate()));

        // creationDate, [deletionDate,] id, title, category
        writers.get(FORUM).writeEntry(dates, ImmutableList.of(
                Long.toString(forum.getId()),
                forum.getTitle(),
                forum.getForumType().toString()
        ));

        // (Forum)-[:hasModerator]->(Person)
        List<String> moderatorDates = (DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) ?
                ImmutableList.of(Dictionaries.dates.formatDateTime(forum.getCreationDate()), Dictionaries.dates.formatDateTime(forum.getModeratorDeletionDate()), forum.isExplicitlyDeleted()? "true" : "false") :
                ImmutableList.of(Dictionaries.dates.formatDateTime(forum.getCreationDate()));
        // to prevent dangling edges, we only serialize the hasModerator edge if the moderator exists and/or
        // we use 'raw data' serialization mode
        if (forum.getModeratorDeletionDate() >= Dictionaries.dates.getBulkLoadThreshold() ||
            DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) {
            // creationDate, [deletionDate,] Forum.id, Person.id
            writers.get(FORUM_HASMODERATOR_PERSON).writeEntry(moderatorDates, ImmutableList.of(
                    Long.toString(forum.getId()),
                    Long.toString(forum.getModerator().getAccountId())
            ));
        }

        for (Integer i : forum.getTags()) {
            // creationDate, [deletionDate,] Forum.id, Tag.id
            writers.get(FORUM_HASTAG_TAG).writeEntry(dates, ImmutableList.of(
                    Long.toString(forum.getId()),
                    Integer.toString(i)));
        }
    }

    protected void serialize(final ForumMembership membership) {
        List<String> dates = (DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) ?
                ImmutableList.of(Dictionaries.dates.formatDateTime(membership.getCreationDate()), Dictionaries.dates.formatDateTime(membership.getDeletionDate()), membership.isExplicitlyDeleted()? "true" : "false") :
                ImmutableList.of(Dictionaries.dates.formatDateTime(membership.getCreationDate()));

        // creationDate, [deletionDate,] Forum.id, Person.id, ForumType
        writers.get(FORUM_HASMEMBER_PERSON).writeEntry(dates, ImmutableList.of(
                Long.toString(membership.getForumId()),
                Long.toString(membership.getPerson().getAccountId()),
                membership.getForumType().toString()
        ));
    }

    protected void serialize(final Post post) {
        List<String> dates = (DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) ?
                ImmutableList.of(Dictionaries.dates.formatDateTime(post.getCreationDate()), Dictionaries.dates.formatDateTime(post.getDeletionDate()), post.isExplicitlyDeleted()? "true" : "false") :
                ImmutableList.of(Dictionaries.dates.formatDateTime(post.getCreationDate()));
        List<String> postEnd = (DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) ?
                ImmutableList.of(Long.toString(post.getForumId())) :
                ImmutableList.of();

        // creationDate, [deletionDate,] id, imageFile, locationIP, browserUsed, language, content, length[, Forum.id]
        writers.get(POST).writeEntry(dates, ImmutableList.of(
                Long.toString(post.getMessageId()),
                "",
                post.getIpAddress().toString(),
                Dictionaries.browsers.getName(post.getBrowserId()),
                Dictionaries.languages.getLanguageName(post.getLanguage()),
                post.getContent(),
                Integer.toString(post.getContent().length())),
                postEnd
        );

        // creationDate, [deletionDate,] Post.id, Person.id
        writers.get(POST_HASCREATOR_PERSON).writeEntry(dates, ImmutableList.of(
                Long.toString(post.getMessageId()),
                Long.toString(post.getAuthor().getAccountId())
        ));

        // creationDate, [deletionDate,] Post.id, Place.id
        writers.get(POST_ISLOCATEDIN_PLACE).writeEntry(dates, ImmutableList.of(
                Long.toString(post.getMessageId()),
                Integer.toString(post.getCountryId())
        ));

        // creationDate, [deletionDate,] Post.id, Tag.id
        for (Integer t : post.getTags()) {
            writers.get(POST_HASTAG_TAG).writeEntry(dates, ImmutableList.of(
                    Long.toString(post.getMessageId()),
                    Integer.toString(t)
            ));
        }
        // creationDate, [deletionDate,] Forum.id, Post.id
        writers.get(FORUM_CONTAINEROF_POST).writeEntry(dates, ImmutableList.of(
                Long.toString(post.getForumId()),
                Long.toString(post.getMessageId())
        ));
    }

    protected void serialize(final Comment comment) {
        List<String> dates = (DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) ?
                ImmutableList.of(Dictionaries.dates.formatDateTime(comment.getCreationDate()), Dictionaries.dates.formatDateTime(comment.getDeletionDate()), comment.isExplicitlyDeleted()? "true" : "false") :
                ImmutableList.of(Dictionaries.dates.formatDateTime(comment.getCreationDate()));

        // creationDate, [deletionDate,] id, locationIP, browserUsed, content, length
        writers.get(COMMENT).writeEntry(dates, ImmutableList.of(
                Long.toString(comment.getMessageId()),
                comment.getIpAddress().toString(),
                Dictionaries.browsers.getName(comment.getBrowserId()),
                comment.getContent(),
                Integer.toString(comment.getContent().length())
        ));

        if (comment.parentMessageId() == comment.rootPostId()) {
            // creationDate, [deletionDate,] Comment.id, Post.id
            writers.get(COMMENT_REPLYOF_POST).writeEntry(dates, ImmutableList.of(
                    Long.toString(comment.getMessageId()),
                    Long.toString(comment.rootPostId())
            ));
        } else {
            // creationDate, [deletionDate,] Comment.id, Comment.id
            writers.get(COMMENT_REPLYOF_COMMENT).writeEntry(dates, ImmutableList.of(
                    Long.toString(comment.getMessageId()),
                    Long.toString(comment.parentMessageId())
            ));
        }
        // creationDate, [deletionDate,] Comment.id, Person.id
        writers.get(COMMENT_HASCREATOR_PERSON).writeEntry(dates, ImmutableList.of(
                Long.toString(comment.getMessageId()),
                Long.toString(comment.getAuthor().getAccountId())
        ));

        // creationDate, [deletionDate,] Comment.id, Place.id
        writers.get(COMMENT_ISLOCATEDIN_PLACE).writeEntry(dates, ImmutableList.of(
                Long.toString(comment.getMessageId()),
                Integer.toString(comment.getCountryId())
        ));

        for (Integer t : comment.getTags()) {
            // creationDate, [deletionDate,] Comment.id, Tag.id
            writers.get(COMMENT_HASTAG_TAG).writeEntry(dates, ImmutableList.of(
                    Long.toString(comment.getMessageId()),
                    Integer.toString(t)
            ));
        }

    }

    protected void serialize(final Photo photo) {
        List<String> dates = (DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) ?
                ImmutableList.of(Dictionaries.dates.formatDateTime(photo.getCreationDate()), Dictionaries.dates.formatDateTime(photo.getDeletionDate()), photo.isExplicitlyDeleted()? "true" : "false") :
                ImmutableList.of(Dictionaries.dates.formatDateTime(photo.getCreationDate()));
        List<String> postEnd = (DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) ?
                ImmutableList.of(Long.toString(photo.getForumId())) :
                ImmutableList.of();

        // creationDate, [deletionDate,] id, imageFile, locationIP, browserUsed, language, content, length[, Forum.id]
        writers.get(POST).writeEntry(dates, ImmutableList.of(
                Long.toString(photo.getMessageId()),
                photo.getContent(),
                photo.getIpAddress().toString(),
                Dictionaries.browsers.getName(photo.getBrowserId()),
                "",
                "",
                Integer.toString(0)),
                postEnd
        );

        // creationDate, [deletionDate,] Post.id, Place.id
        writers.get(POST_ISLOCATEDIN_PLACE).writeEntry(dates, ImmutableList.of(
                Long.toString(photo.getMessageId()),
                Integer.toString(photo.getCountryId())
        ));

        // creationDate, [deletionDate,] Post.id, Tag.id
        writers.get(POST_HASCREATOR_PERSON).writeEntry(dates, ImmutableList.of(
                Long.toString(photo.getMessageId()),
                Long.toString(photo.getAuthor().getAccountId())
        ));

        // creationDate, [deletionDate,] Post.id, Tag.id
        for (Integer t : photo.getTags()) {
            writers.get(POST_HASTAG_TAG).writeEntry(dates, ImmutableList.of(
                    Long.toString(photo.getMessageId()),
                    Integer.toString(t)
            ));
        }

        // creationDate, [deletionDate,] Forum.id, Post.id
        writers.get(FORUM_CONTAINEROF_POST).writeEntry(dates, ImmutableList.of(
                Long.toString(photo.getForumId()),
                Long.toString(photo.getMessageId())
        ));
    }

    protected void serialize(final Like like) {
        List<String> dates = (DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) ?
                ImmutableList.of(Dictionaries.dates.formatDateTime(like.getCreationDate()), Dictionaries.dates.formatDateTime(like.getDeletionDate()), like.isExplicitlyDeleted()? "true" : "false") :
                ImmutableList.of(Dictionaries.dates.formatDateTime(like.getCreationDate()));

        // creationDate, [deletionDate,] Person.id, Post.id/Comment.id
        List<String> arguments = ImmutableList.of(
                Long.toString(like.getPerson()),
                Long.toString(like.getMessageId()));

        if (like.getType() == Like.LikeType.POST || like.getType() == Like.LikeType.PHOTO) {
            writers.get(PERSON_LIKES_POST).writeEntry(dates, arguments);
        } else {
            writers.get(PERSON_LIKES_COMMENT).writeEntry(dates, arguments);
        }
    }

}
