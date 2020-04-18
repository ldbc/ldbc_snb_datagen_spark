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
        if (DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) {
            writers.get(FORUM).writeHeader(ImmutableList.of("creationDate", "id", "title", "type", "deletionDate"));
            writers.get(FORUM_HASMODERATOR_PERSON).writeHeader(ImmutableList.of("creationDate", "Forum.id", "Person.id", "deletionDate"));
            writers.get(FORUM_HASTAG_TAG).writeHeader(ImmutableList.of("creationDate", "Forum.id", "Tag.id", "deletionDate"));
            writers.get(FORUM_HASMEMBER_PERSON).writeHeader(ImmutableList.of("creationDate", "Forum.id", "Person.id", "type", "deletionDate"));

            writers.get(POST).writeHeader(ImmutableList.of("creationDate", "id", "imageFile", "locationIP", "browserUsed", "language", "content", "length", "deletionDate"));
            writers.get(POST_HASCREATOR_PERSON).writeHeader(ImmutableList.of("creationDate", "Post.id", "Person.id", "deletionDate"));
            writers.get(POST_ISLOCATEDIN_PLACE).writeHeader(ImmutableList.of("creationDate", "Post.id", "Place.id", "deletionDate"));
            writers.get(POST_HASTAG_TAG).writeHeader(ImmutableList.of("creationDate", "Post.id", "Tag.id", "deletionDate"));
            writers.get(FORUM_CONTAINEROF_POST).writeHeader(ImmutableList.of("creationDate", "Forum.id", "Post.id", "deletionDate"));

            writers.get(COMMENT).writeHeader(ImmutableList.of("creationDate", "id", "locationIP", "browserUsed", "content", "length", "deletionDate"));
            writers.get(COMMENT_REPLYOF_POST).writeHeader(ImmutableList.of("creationDate", "Comment.id", "Post.id", "deletionDate"));
            writers.get(COMMENT_REPLYOF_COMMENT).writeHeader(ImmutableList.of("creationDate", "Comment.id", "Comment.id", "deletionDate"));
            writers.get(COMMENT_HASCREATOR_PERSON).writeHeader(ImmutableList.of("creationDate", "Comment.id", "Person.id", "deletionDate"));
            writers.get(COMMENT_ISLOCATEDIN_PLACE).writeHeader(ImmutableList.of("creationDate", "Comment.id", "Place.id", "deletionDate"));
            writers.get(COMMENT_HASTAG_TAG).writeHeader(ImmutableList.of("creationDate", "Comment.id", "Tag.id", "deletionDate"));

            writers.get(PERSON_LIKES_POST).writeHeader(ImmutableList.of("creationDate", "Person.id", "Post.id", "deletionDate"));
            writers.get(PERSON_LIKES_COMMENT).writeHeader(ImmutableList.of("creationDate", "Person.id", "Comment.id", "deletionDate"));

        } else {
            writers.get(FORUM).writeHeader(ImmutableList.of("creationDate", "id", "title", "type"));
            writers.get(FORUM_HASMODERATOR_PERSON).writeHeader(ImmutableList.of("creationDate", "Forum.id", "Person.id"));
            writers.get(FORUM_HASTAG_TAG).writeHeader(ImmutableList.of("creationDate", "Forum.id", "Tag.id"));
            writers.get(FORUM_HASMEMBER_PERSON).writeHeader(ImmutableList.of("creationDate", "Forum.id", "Person.id", "type"));

            writers.get(POST).writeHeader(ImmutableList.of("creationDate", "id", "imageFile", "locationIP", "browserUsed", "language", "content", "length"));
            writers.get(POST_HASCREATOR_PERSON).writeHeader(ImmutableList.of("creationDate", "Post.id", "Person.id"));
            writers.get(POST_ISLOCATEDIN_PLACE).writeHeader(ImmutableList.of("creationDate", "Post.id", "Place.id"));
            writers.get(POST_HASTAG_TAG).writeHeader(ImmutableList.of("creationDate", "Post.id", "Tag.id"));
            writers.get(FORUM_CONTAINEROF_POST).writeHeader(ImmutableList.of("creationDate", "Forum.id", "Post.id"));

            writers.get(COMMENT).writeHeader(ImmutableList.of("creationDate", "id", "locationIP", "browserUsed", "content", "length"));
            writers.get(COMMENT_REPLYOF_POST).writeHeader(ImmutableList.of("creationDate", "Comment.id", "Post.id"));
            writers.get(COMMENT_REPLYOF_COMMENT).writeHeader(ImmutableList.of("creationDate", "Comment.id", "Comment.id"));
            writers.get(COMMENT_HASCREATOR_PERSON).writeHeader(ImmutableList.of("creationDate", "Comment.id", "Person.id"));
            writers.get(COMMENT_ISLOCATEDIN_PLACE).writeHeader(ImmutableList.of("creationDate", "Comment.id", "Place.id"));
            writers.get(COMMENT_HASTAG_TAG).writeHeader(ImmutableList.of("creationDate", "Comment.id", "Tag.id"));

            writers.get(PERSON_LIKES_POST).writeHeader(ImmutableList.of("creationDate", "Person.id", "Post.id"));
            writers.get(PERSON_LIKES_COMMENT).writeHeader(ImmutableList.of("creationDate", "Person.id", "Comment.id"));
        }
    }

    protected void serialize(final Forum forum) {
        String forumCreationDate = Dictionaries.dates.formatDateTime(forum.getCreationDate());
        String forumDeletionDate = Dictionaries.dates.formatDateTime(forum.getDeletionDate());
        if (DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) {
            //"creationDate", "id", "title", "deletionDate"
            writers.get(FORUM).writeEntry(ImmutableList.of(
                    forumCreationDate,
                    Long.toString(forum.getId()),
                    forum.getTitle(),
                    forum.getForumType().toString(),
                    forumDeletionDate
            ));

            //"creationDate", "id", "title", "deletionDate"
            writers.get(FORUM).writeEntry(ImmutableList.of(
                    forumCreationDate,
                    Long.toString(forum.getId()),
                    forum.getTitle(),
                    forum.getForumType().toString(),
                    forumDeletionDate
            ));

            //"creationDate",  "Forum.id", "Person.id", "deletionDate"
            writers.get(FORUM_HASMODERATOR_PERSON).writeEntry(ImmutableList.of(
                    Long.toString(forum.getModerator().getCreationDate() + DatagenParams.delta),
                    Long.toString(forum.getId()),
                    Long.toString(forum.getModerator().getAccountId()),
                    Long.toString(forum.getModerator().getDeletionDate())
                    ));

            for (Integer i : forum.getTags()) {
                //"creationDate",  "Forum.id", "Tag.id", "deletionDate"
                writers.get(FORUM_HASTAG_TAG).writeEntry(ImmutableList.of(
                        forumCreationDate,
                        Long.toString(forum.getId()),
                        Integer.toString(i),
                        forumDeletionDate));
            }
        } else {
            //"creationDate", "id", "title"
            writers.get(FORUM).writeEntry(ImmutableList.of(
                    forumCreationDate,
                    Long.toString(forum.getId()),
                    forum.getTitle(),
                    forum.getForumType().toString()
            ));

            //"creationDate",  "Forum.id", "Person.id"
            writers.get(FORUM_HASMODERATOR_PERSON).writeEntry(ImmutableList.of(
                    forumCreationDate,
                    Long.toString(forum.getId()),
                    Long.toString(forum.getModerator().getAccountId())
            ));

            for (Integer i : forum.getTags()) {
                //"creationDate",  "Forum.id", "Tag.id",
                writers.get(FORUM_HASTAG_TAG).writeEntry(ImmutableList.of(
                        forumCreationDate,
                        Long.toString(forum.getId()),
                        Integer.toString(i)));
            }
        }
    }

    protected void serialize(final ForumMembership membership) {

        if (DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) {
            //"creationDate",  "Forum.id", "Person.id",
            writers.get(FORUM_HASMEMBER_PERSON).writeEntry(ImmutableList.of(
                    Dictionaries.dates.formatDateTime(membership.getCreationDate()),
                    Long.toString(membership.getForumId()),
                    Long.toString(membership.getPerson().getAccountId()),
                    membership.getForumType().toString()));
        } else {
            //"creationDate",  "Forum.id", "Person.id", "deletionDate"
            writers.get(FORUM_HASMEMBER_PERSON).writeEntry(ImmutableList.of(
                    Dictionaries.dates.formatDateTime(membership.getCreationDate()),
                    Long.toString(membership.getForumId()),
                    Long.toString(membership.getPerson().getAccountId()),
                    membership.getForumType().toString(),
                    Dictionaries.dates.formatDateTime(membership.getDeletionDate())));
        }
    }

    protected void serialize(final Post post) {
        String creationDate = Dictionaries.dates.formatDateTime(post.getCreationDate());
        String deletionDate = Dictionaries.dates.formatDateTime(post.getDeletionDate());

        if (DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) {
            //"creationDate",  "id", "imageFile", "locationIP", "browserUsed", "language", "content", "length","deletionDate"
            writers.get(POST).writeEntry(ImmutableList.of(
                    creationDate,
                    Long.toString(post.getMessageId()),
                    "",
                    post.getIpAddress().toString(),
                    Dictionaries.browsers.getName(post.getBrowserId()),
                    Dictionaries.languages.getLanguageName(post.getLanguage()),
                    post.getContent(),
                    Integer.toString(post.getContent().length()),
                    deletionDate
            ));

            //"creationDate",  "Post.id", "Person.id","deletionDate"
            writers.get(POST_HASCREATOR_PERSON).writeEntry(ImmutableList.of(
                    creationDate,
                    Long.toString(post.getMessageId()),
                    Long.toString(post.getAuthor().getAccountId()),
                    deletionDate
            ));

            //"creationDate",  "Post.id", "Place.id","deletionDate"
            writers.get(POST_ISLOCATEDIN_PLACE).writeEntry(ImmutableList.of(
                    creationDate,
                    Long.toString(post.getMessageId()),
                    Integer.toString(post.getCountryId()),
                    deletionDate
            ));

            //"creationDate",  "Post.id", "Tag.id","deletionDate"
            for (Integer t : post.getTags()) {
                writers.get(POST_HASTAG_TAG).writeEntry(ImmutableList.of(
                        creationDate,
                        Long.toString(post.getMessageId()),
                        Integer.toString(t),
                        deletionDate
                ));
            }
            //"creationDate",  "Forum.id", "Post.id","deletionDate"
            writers.get(FORUM_CONTAINEROF_POST).writeEntry(ImmutableList.of(
                    creationDate,
                    Long.toString(post.getForumId()),
                    Long.toString(post.getMessageId()),
                    deletionDate
            ));
        } else {
            //"creationDate",  "id", "imageFile", "locationIP", "browserUsed", "language", "content", "length"

            writers.get(POST).writeEntry(ImmutableList.of(
                    creationDate,
                    Long.toString(post.getMessageId()),
                    "",
                    post.getIpAddress().toString(),
                    Dictionaries.browsers.getName(post.getBrowserId()),
                    Dictionaries.languages.getLanguageName(post.getLanguage()),
                    post.getContent(),
                    Integer.toString(post.getContent().length())
            ));

            //"creationDate",  "Post.id", "Person.id"
            writers.get(POST_HASCREATOR_PERSON).writeEntry(ImmutableList.of(
                    creationDate,
                    Long.toString(post.getMessageId()),
                    Long.toString(post.getAuthor().getAccountId())
            ));

            //"creationDate",  "Post.id", "Place.id"
            writers.get(POST_ISLOCATEDIN_PLACE).writeEntry(ImmutableList.of(
                    creationDate,
                    Long.toString(post.getMessageId()),
                    Integer.toString(post.getCountryId())
            ));

            //"creationDate",  "Post.id", "Tag.id"
            for (Integer t : post.getTags()) {
                writers.get(POST_HASTAG_TAG).writeEntry(ImmutableList.of(
                        creationDate,
                        Long.toString(post.getMessageId()),
                        Integer.toString(t)
                ));
            }
            //"creationDate",  "Forum.id", "Post.id"
            writers.get(FORUM_CONTAINEROF_POST).writeEntry(ImmutableList.of(
                    creationDate,
                    Long.toString(post.getForumId()),
                    Long.toString(post.getMessageId())
            ));
        }

    }

    protected void serialize(final Comment comment) {
        String creationDate = Dictionaries.dates.formatDateTime(comment.getCreationDate());
        String deletionDate = Dictionaries.dates.formatDateTime(comment.getDeletionDate());

        if (DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) {
            //"creationDate",  "id", "locationIP", "browserUsed", "content", "length", "deletionDate"
            writers.get(COMMENT).writeEntry(ImmutableList.of(
                    creationDate,
                    Long.toString(comment.getMessageId()),
                    comment.getIpAddress().toString(),
                    Dictionaries.browsers.getName(comment.getBrowserId()),
                    comment.getContent(),
                    Integer.toString(comment.getContent().length()),
                    deletionDate
            ));

            if (comment.replyOf() == comment.postId()) {
                //"creationDate",  "Comment.id", "Post.id", "deletionDate"
                writers.get(COMMENT_REPLYOF_POST).writeEntry(ImmutableList.of(
                        creationDate,
                        Long.toString(comment.getMessageId()),
                        Long.toString(comment.postId()),
                        deletionDate
                ));
            } else {
                //"creationDate",  "Comment.id", "Comment.id", "deletionDate"
                writers.get(COMMENT_REPLYOF_COMMENT).writeEntry(ImmutableList.of(
                        creationDate,
                        Long.toString(comment.getMessageId()),
                        Long.toString(comment.replyOf()),
                        deletionDate
                ));
            }
            //"creationDate",  "Comment.id", "Person.id", "deletionDate"
            writers.get(COMMENT_HASCREATOR_PERSON).writeEntry(ImmutableList.of(
                    creationDate,
                    Long.toString(comment.getMessageId()),
                    Long.toString(comment.getAuthor().getAccountId()),
                    deletionDate
            ));

            //"creationDate",  "Comment.id", "Place.id", "deletionDate"
            writers.get(COMMENT_ISLOCATEDIN_PLACE).writeEntry(ImmutableList.of(
                    creationDate,
                    Long.toString(comment.getMessageId()),
                    Integer.toString(comment.getCountryId()),
                    deletionDate
            ));

            for (Integer t : comment.getTags()) {
                //"creationDate",  "Comment.id", "Tag.id", "deletionDate"
                writers.get(COMMENT_HASTAG_TAG).writeEntry(ImmutableList.of(
                        creationDate,
                        Long.toString(comment.getMessageId()),
                        Integer.toString(t),
                        deletionDate
                ));
            }
        } else {
            //"creationDate",  "id", "locationIP", "browserUsed", "content", "length"
            writers.get(COMMENT).writeEntry(ImmutableList.of(
                    creationDate,
                    Long.toString(comment.getMessageId()),
                    comment.getIpAddress().toString(),
                    Dictionaries.browsers.getName(comment.getBrowserId()),
                    comment.getContent(),
                    Integer.toString(comment.getContent().length())
            ));

            if (comment.replyOf() == comment.postId()) {
                //"creationDate",  "Comment.id", "Post.id"
                writers.get(COMMENT_REPLYOF_POST).writeEntry(ImmutableList.of(
                        creationDate,
                        Long.toString(comment.getMessageId()),
                        Long.toString(comment.postId())
                ));
            } else {
                //"creationDate",  "Comment.id", "Comment.id"
                writers.get(COMMENT_REPLYOF_COMMENT).writeEntry(ImmutableList.of(
                        creationDate,
                        Long.toString(comment.getMessageId()),
                        Long.toString(comment.replyOf())
                ));
            }
            //"creationDate",  "Comment.id", "Person.id"
            writers.get(COMMENT_HASCREATOR_PERSON).writeEntry(ImmutableList.of(
                    creationDate,
                    Long.toString(comment.getMessageId()),
                    Long.toString(comment.getAuthor().getAccountId())
            ));

            //"creationDate",  "Comment.id", "Place.id"
            writers.get(COMMENT_ISLOCATEDIN_PLACE).writeEntry(ImmutableList.of(
                    creationDate,
                    Long.toString(comment.getMessageId()),
                    Integer.toString(comment.getCountryId())
            ));

            for (Integer t : comment.getTags()) {
                //"creationDate",  "Comment.id", "Tag.id"
                writers.get(COMMENT_HASTAG_TAG).writeEntry(ImmutableList.of(
                        creationDate,
                        Long.toString(comment.getMessageId()),
                        Integer.toString(t)
                ));
            }
        }
    }

    protected void serialize(final Photo photo) {
        String creationDate = Dictionaries.dates.formatDateTime(photo.getCreationDate());
        String deletionDate = Dictionaries.dates.formatDateTime(photo.getDeletionDate());

        if (DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) {
            //"creationDate",  "id", "imageFile", "locationIP", "browserUsed", "language", "content", "length","deletionDate"
            writers.get(POST).writeEntry(ImmutableList.of(
                    creationDate,
                    Long.toString(photo.getMessageId()),
                    photo.getContent(),
                    photo.getIpAddress().toString(),
                    Dictionaries.browsers.getName(photo.getBrowserId()),
                    "",
                    "",
                    Integer.toString(0),
                    deletionDate
            ));

            //"creationDate",  "Post.id", "Place.id","deletionDate"
            writers.get(POST_ISLOCATEDIN_PLACE).writeEntry(ImmutableList.of(
                    creationDate,
                    Long.toString(photo.getMessageId()),
                    Integer.toString(photo.getCountryId()),
                    deletionDate
            ));

            //"creationDate",  "Post.id", "Tag.id","deletionDate"
            writers.get(POST_HASCREATOR_PERSON).writeEntry(ImmutableList.of(
                    creationDate,
                    Long.toString(photo.getMessageId()),
                    Long.toString(photo.getAuthor().getAccountId()),
                    deletionDate
            ));

            //"creationDate",  "Post.id", "Tag.id","deletionDate"
            for (Integer t : photo.getTags()) {
                writers.get(POST_HASTAG_TAG).writeEntry(ImmutableList.of(
                        creationDate,
                        Long.toString(photo.getMessageId()),
                        Integer.toString(t),
                        deletionDate
                ));
            }

            //"creationDate",  "Forum.id", "Post.id","deletionDate"
            writers.get(FORUM_CONTAINEROF_POST).writeEntry(ImmutableList.of(
                    creationDate,
                    Long.toString(photo.getForumId()),
                    Long.toString(photo.getMessageId()),
                    deletionDate
            ));
        } else {
            //"creationDate",  "id", "imageFile", "locationIP", "browserUsed", "language", "content", "length"
            writers.get(POST).writeEntry(ImmutableList.of(
                    creationDate,
                    Long.toString(photo.getMessageId()),
                    photo.getContent(),
                    photo.getIpAddress().toString(),
                    Dictionaries.browsers.getName(photo.getBrowserId()),
                    "",
                    "",
                    Integer.toString(0)
            ));

            //"creationDate",  "Post.id", "Place.id",
            writers.get(POST_ISLOCATEDIN_PLACE).writeEntry(ImmutableList.of(
                    creationDate,
                    Long.toString(photo.getMessageId()),
                    Integer.toString(photo.getCountryId())
            ));

            //"creationDate",  "Post.id", "Tag.id"
            writers.get(POST_HASCREATOR_PERSON).writeEntry(ImmutableList.of(
                    creationDate,
                    Long.toString(photo.getMessageId()),
                    Long.toString(photo.getAuthor().getAccountId())
            ));

            //"creationDate",  "Post.id", "Tag.id"
            for (Integer t : photo.getTags()) {
                writers.get(POST_HASTAG_TAG).writeEntry(ImmutableList.of(
                        creationDate,
                        Long.toString(photo.getMessageId()),
                        Integer.toString(t)
                ));
            }

            //"creationDate",  "Forum.id", "Post.id"
            writers.get(FORUM_CONTAINEROF_POST).writeEntry(ImmutableList.of(
                    creationDate,
                    Long.toString(photo.getForumId()),
                    Long.toString(photo.getMessageId())
            ));
        }
    }

    protected void serialize(final Like like) {
        if (DatagenParams.getDatagenMode() == DatagenMode.RAW_DATA) {
            //"creationDate",  "Person.id", "Post.id"/"Comment.id","deletionDate"
            List<String> arguments = ImmutableList.of(
                    Dictionaries.dates.formatDateTime(like.getLikeCreationDate()),
                    Long.toString(like.getPerson()),
                    Long.toString(like.getMessageId()),
                    Dictionaries.dates.formatDateTime(like.getLikeDeletionDate()));
            if (like.getType() == Like.LikeType.POST || like.getType() == Like.LikeType.PHOTO) {
                writers.get(PERSON_LIKES_POST).writeEntry(arguments);
            } else {
                writers.get(PERSON_LIKES_COMMENT).writeEntry(arguments);
            }
        }  else {
            //"creationDate",  "Person.id", "Post.id"/"Comment.id"
            List<String> arguments = ImmutableList.of(
                    Dictionaries.dates.formatDateTime(like.getLikeCreationDate()),
                    Long.toString(like.getPerson()),
                    Long.toString(like.getMessageId())
            );
            if (like.getType() == Like.LikeType.POST || like.getType() == Like.LikeType.PHOTO) {
                writers.get(PERSON_LIKES_POST).writeEntry(arguments);
            } else {
                writers.get(PERSON_LIKES_COMMENT).writeEntry(arguments);
            }
        }
    }

}
