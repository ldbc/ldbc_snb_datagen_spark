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
package ldbc.snb.datagen.serializer.snb.csv.mergeforeign;

import com.google.common.collect.ImmutableList;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.hadoop.writer.HDFSCSVWriter;
import ldbc.snb.datagen.entities.dynamic.messages.Comment;
import ldbc.snb.datagen.entities.dynamic.messages.Photo;
import ldbc.snb.datagen.entities.dynamic.messages.Post;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.dynamic.relations.Like;
import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.serializer.DynamicActivitySerializer;
import static ldbc.snb.datagen.serializer.snb.csv.FileName.*;

import ldbc.snb.datagen.serializer.snb.csv.FileName;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by aprat on 17/02/15.
 */
public class CSVMergeForeignDynamicActivitySerializer extends DynamicActivitySerializer {

    private ArrayList<String> arguments;

    @Override
    public List<FileName> getFileNames() {
        return Arrays.asList(FORUM, FORUM_HASMEMBER_PERSON, FORUM_HASTAG_TAG, PERSON_LIKES_POST,
                PERSON_LIKES_COMMENT, POST, POST_HASTAG_TAG, COMMENT, COMMENT_HASTAG_TAG);
    }

    @Override
    public void writeFileHeaders() {
        writers.get(FORUM).writeHeader(ImmutableList.of("id", "title", "creationDate", "moderator"));
        writers.get(FORUM_HASMEMBER_PERSON).writeHeader(ImmutableList.of("Forum.id", "Person.id", "joinDate"));
        writers.get(FORUM_HASTAG_TAG).writeHeader(ImmutableList.of("Forum.id", "Tag.id"));
        writers.get(PERSON_LIKES_POST).writeHeader(ImmutableList.of("Person.id", "Post.id", "creationDate"));
        writers.get(PERSON_LIKES_COMMENT).writeHeader(ImmutableList.of("Person.id", "Comment.id", "creationDate"));
        writers.get(POST).writeHeader(ImmutableList.of("id", "imageFile", "creationDate", "locationIP", "browserUsed", "language", "content", "length", "creator", "Forum.id", "place"));
        writers.get(POST_HASTAG_TAG).writeHeader(ImmutableList.of("Post.id", "Tag.id"));
        writers.get(COMMENT).writeHeader(ImmutableList.of("id", "creationDate", "locationIP", "browserUsed", "content", "length", "creator", "place", "replyOfPost", "replyOfComment"));
        writers.get(COMMENT_HASTAG_TAG).writeHeader(ImmutableList.of("Comment.id", "Tag.id"));
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
        arguments.add("");
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
            arguments.add("");
        } else {
            arguments.add("");
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
        arguments.add("");
        arguments.add("");
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
