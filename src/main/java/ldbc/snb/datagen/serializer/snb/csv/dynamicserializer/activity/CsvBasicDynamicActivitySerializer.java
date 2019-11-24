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

        writers.get(FORUM).writeHeader(ImmutableList.of( "creationDate","id", "title")); //
        writers.get(FORUM_HASMODERATOR_PERSON).writeHeader(ImmutableList.of("joinDate","Forum.id","Person.id")); //
        writers.get(FORUM_HASTAG_TAG).writeHeader(ImmutableList.of("creationDate","Forum.id","Tag.id")); //

        writers.get(FORUM_HASMEMBER_PERSON).writeHeader(ImmutableList.of("joinDate","Forum.id","Person.id"));//

        writers.get(POST).writeHeader(ImmutableList.of("creationDate","id","imageFile","locationIP","browserUsed","language","content","length")); //
        writers.get(POST_HASCREATOR_PERSON).writeHeader(ImmutableList.of("creationDate","Post.id","Person.id"));
        writers.get(POST_ISLOCATEDIN_PLACE).writeHeader(ImmutableList.of("creationDate","Post.id","Place.id"));
        writers.get(POST_HASTAG_TAG).writeHeader(ImmutableList.of("creationDate","Post.id","Tag.id"));
        writers.get(FORUM_CONTAINEROF_POST).writeHeader(ImmutableList.of("creationDate","Forum.id","Post.id"));

        writers.get(COMMENT).writeHeader(ImmutableList.of("creationDate","id","locationIP","browserUsed","content","length"));
        writers.get(COMMENT_REPLYOF_POST).writeHeader(ImmutableList.of("creationDate","Comment.id","Post.id"));
        writers.get(COMMENT_REPLYOF_COMMENT).writeHeader(ImmutableList.of("creationDate","Comment.id","Comment.id"));
        writers.get(COMMENT_HASCREATOR_PERSON).writeHeader(ImmutableList.of("creationDate","Comment.id","Person.id"));
        writers.get(COMMENT_ISLOCATEDIN_PLACE).writeHeader(ImmutableList.of("creationDate","Comment.id","Place.id"));
        writers.get(COMMENT_HASTAG_TAG).writeHeader(ImmutableList.of("creationDate","Comment.id","Tag.id"));



        writers.get(PERSON_LIKES_POST).writeHeader(ImmutableList.of("creationDate","Person.id","Post.id"));
        writers.get(PERSON_LIKES_COMMENT).writeHeader(ImmutableList.of("creationDate","Person.id","Comment.id"));
    }

    protected void serialize(final Forum forum) {
        String dateString = Dictionaries.dates.formatDateTime(forum.creationDate());

        //"id", "title", "creationDate"
        writers.get(FORUM).writeEntry(ImmutableList.of(
                dateString,
                Long.toString(forum.id()),
                forum.title()

        ));
        //"Forum.id","Person.id","joinDate"
        writers.get(FORUM_HASMODERATOR_PERSON).writeEntry(ImmutableList.of(
                dateString,
                Long.toString(forum.id()),
                Long.toString(forum.moderator().accountId())
        ));

        for (Integer i : forum.tags()) {
            //"Forum.id","Tag.id","creationDate"
            writers.get(FORUM_HASTAG_TAG).writeEntry(ImmutableList.of(
                    dateString,
                    Long.toString(forum.id()),
                    Integer.toString(i)));
        }
    }

    protected void serialize(final ForumMembership membership) {
        //"Forum.id","Person.id","joinDate"
        writers.get(FORUM_HASMEMBER_PERSON).writeEntry(ImmutableList.of(
                Dictionaries.dates.formatDateTime(membership.creationDate()),
                Long.toString(membership.forumId()),
                Long.toString(membership.person().accountId())
        ));
    }


    protected void serialize(final Post post) {
        String datestring = Dictionaries.dates.formatDateTime(post.creationDate());

        //"id","imageFile","creationDate","locationIP","browserUsed","language","content","length"
        writers.get(POST).writeEntry(ImmutableList.of(
                datestring,
                Long.toString(post.messageId()),
                "",
                post.ipAddress().toString(),
                Dictionaries.browsers.getName(post.browserId()),
                Dictionaries.languages.getLanguageName(post.language()),
                post.content(),
                Integer.toString(post.content().length())
        ));

        //"Post.id","Person.id","creationDate"
        writers.get(POST_HASCREATOR_PERSON).writeEntry(ImmutableList.of(
                datestring,
                Long.toString(post.messageId()),
                Long.toString(post.author().accountId())
        ));

        //"Post.id","Place.id","creationDate"
        writers.get(POST_ISLOCATEDIN_PLACE).writeEntry(ImmutableList.of(
                datestring,
                Long.toString(post.messageId()),
                Integer.toString(post.countryId())
        ));

        //"Post.id","Tag.id","creationDate"
        for (Integer t : post.tags()) {
            writers.get(POST_HASTAG_TAG).writeEntry(ImmutableList.of(
                    datestring,
                    Long.toString(post.messageId()),
                    Integer.toString(t)
            ));
        }
        //"Forum.id","Post.id","creationDate"
        writers.get(FORUM_CONTAINEROF_POST).writeEntry(ImmutableList.of(
                datestring,
                Long.toString(post.forumId()),
                Long.toString(post.messageId())
        ));

    }

    protected void serialize(final Comment comment) {
        String dateString = Dictionaries.dates.formatDateTime(comment.creationDate());

        //"id","creationDate","locationIP","browserUsed","content","length"
        writers.get(COMMENT).writeEntry(ImmutableList.of(
                dateString,
                Long.toString(comment.messageId()),
                comment.ipAddress().toString(),
                Dictionaries.browsers.getName(comment.browserId()),
                comment.content(),
                Integer.toString(comment.content().length())
        ));

        if (comment.replyOf() == comment.postId()) {
            //"Comment.id","Post.id","creationDate"
            writers.get(COMMENT_REPLYOF_POST).writeEntry(ImmutableList.of(
                    dateString,
                    Long.toString(comment.messageId()),
                    Long.toString(comment.postId())
            ));
        } else {
            //"Comment.id","Comment.id","creationDate"
            writers.get(COMMENT_REPLYOF_COMMENT).writeEntry(ImmutableList.of(
                    dateString,
                    Long.toString(comment.messageId()),
                    Long.toString(comment.replyOf())
            ));
        }
        //"Comment.id","Person.id","creationDate"
        writers.get(COMMENT_HASCREATOR_PERSON).writeEntry(ImmutableList.of(
                dateString,
                Long.toString(comment.messageId()),
                Long.toString(comment.author().accountId())
        ));

        //"Comment.id","Place.id","creationDate"
        writers.get(COMMENT_ISLOCATEDIN_PLACE).writeEntry(ImmutableList.of(
                dateString,
                Long.toString(comment.messageId()),
                Integer.toString(comment.countryId())
        ));

        for (Integer t : comment.tags()) {
            //"Comment.id","Tag.id","creationDate"
            writers.get(COMMENT_HASTAG_TAG).writeEntry(ImmutableList.of(
                    dateString,
                    Long.toString(comment.messageId()),
                    Integer.toString(t)
            ));
        }
    }

    protected void serialize(final Photo photo) {
        String dateString = Dictionaries.dates.formatDateTime(photo.creationDate());
        //"id","imageFile","creationDate","locationIP","browserUsed","language","content","length"
        writers.get(POST).writeEntry(ImmutableList.of(
                dateString,
                Long.toString(photo.messageId()),
                photo.content(),
                photo.ipAddress().toString(),
                Dictionaries.browsers.getName(photo.browserId()),
                "",
                "",
                Integer.toString(0)
        ));

        //"Post.id","Place.id","creationDate"
        writers.get(POST_ISLOCATEDIN_PLACE).writeEntry(ImmutableList.of(
                dateString,
                Long.toString(photo.messageId()),
                Integer.toString(photo.countryId())
        ));

        //"Post.id","Tag.id","creationDate"
        writers.get(POST_HASCREATOR_PERSON).writeEntry(ImmutableList.of(
                dateString,
                Long.toString(photo.messageId()),
                Long.toString(photo.author().accountId())
        ));

        //"Post.id","Tag.id","creationDate"
        for (Integer t : photo.tags()) {
            writers.get(POST_HASTAG_TAG).writeEntry(ImmutableList.of(
                    dateString,
                    Long.toString(photo.messageId()),
                    Integer.toString(t)
            ));
        }

        //"Forum.id","Post.id","creationDate"
        writers.get(FORUM_CONTAINEROF_POST).writeEntry(ImmutableList.of(
                dateString,
                Long.toString(photo.forumId()),
                Long.toString(photo.messageId())
        ));
    }



    protected void serialize(final Like like) {
        if (like.type == Like.LikeType.POST || like.type == Like.LikeType.PHOTO) {
            //"Person.id","Post.id","creationDate"
            writers.get(PERSON_LIKES_POST).writeEntry(ImmutableList.of(
                    Dictionaries.dates.formatDateTime(like.date),
                    Long.toString(like.user),
                    Long.toString(like.messageId)
            ));
        } else {
            //"Person.id","Comment.id","creationDate"
            writers.get(PERSON_LIKES_COMMENT).writeEntry(ImmutableList.of(
                    Dictionaries.dates.formatDateTime(like.date),
                    Long.toString(like.user),
                    Long.toString(like.messageId)
            ));
        }
    }

}
