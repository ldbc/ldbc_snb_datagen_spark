/*
* To change this license header, choose License Headers in Project Properties.
* To change this template file, choose Tools | Templates
* and open the template in the editor.
*/
package ldbc.snb.datagen.serializer.snb.interactive;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.objects.*;
import ldbc.snb.datagen.serializer.HDFSCSVWriter;
import ldbc.snb.datagen.serializer.PersonActivitySerializer;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;

/**
 *
 * @author aprat
 */
public class CSVPersonActivitySerializer extends PersonActivitySerializer {
	private HDFSCSVWriter [] writers;
	private ArrayList<String> arguments;
	private String empty="";
	
	private enum FileNames {
		FORUM ("forum"),
		FORUM_CONTAINEROF_POST ("forum_containerOf_post"),
		FORUM_HASMEMBER_PERSON ("forum_hasMember_person"),
		FORUM_HASMODERATOR_PERSON ("forum_hasModerator_person"),
		FORUM_HASTAG_TAG ("forum_hasTag_tag"),
		PERSON_LIKES_POST ("person_likes_post"),
		PERSON_LIKES_COMMENT ("person_likes_comment"),
		POST("post"),
		POST_HASCREATOR_PERSON("post_hasCreator_person"),
		POST_HASTAG_TAG("post_hasTag_tag"),
		POST_ISLOCATEDIN_PLACE("post_isLocatedIn_place"),
		COMMENT("comment"),
		COMMENT_HASCREATOR_PERSON("comment_hasCreator_person"),
		COMMENT_HASTAG_TAG("comment_hasTag_tag"),
		COMMENT_ISLOCATEDIN_PLACE("comment_isLocatedIn_place"),
		COMMENT_REPLYOF_POST("comment_replyOf_post"),
		COMMENT_REPLYOF_COMMENT("comment_replyOf_comment");
		
		private final String name;
		
		private FileNames( String name ) {
			this.name = name;
		}
		public String toString() {
			return name;
		}
	}
	
	public CSVPersonActivitySerializer() {
	}
	
	@Override
	public void initialize(Configuration conf, int reducerId) {
		int numFiles = FileNames.values().length;
		writers = new HDFSCSVWriter[numFiles];
		for( int i = 0; i < numFiles; ++i) {
			writers[i] = new HDFSCSVWriter(conf.get("ldbc.snb.datagen.serializer.socialNetworkDir"),FileNames.values()[i].toString()+"_"+reducerId,conf.getInt("ldbc.snb.datagen.numPartitions",1),conf.getBoolean("ldbc.snb.datagen.serializer.compressed",false),"|");
		}
		arguments = new ArrayList<String>();

        arguments.add("id");
        arguments.add("title");
        arguments.add("creationDate");
        writers[FileNames.FORUM.ordinal()].writeEntry(arguments);
        arguments.clear();

        arguments.add("Forum.id");
        arguments.add("Post.id");
        writers[FileNames.FORUM_CONTAINEROF_POST.ordinal()].writeEntry(arguments);
        arguments.clear();

        arguments.add("Forum.id");
        arguments.add("Person.id");
        arguments.add("joinDate");
        writers[FileNames.FORUM_HASMEMBER_PERSON.ordinal()].writeEntry(arguments);
        arguments.clear();

        arguments.add("Forum.id");
        arguments.add("Person.id");
        writers[FileNames.FORUM_HASMODERATOR_PERSON.ordinal()].writeEntry(arguments);
        arguments.clear();

        arguments.add("Forum.id");
        arguments.add("Tag.id");
        writers[FileNames.FORUM_HASTAG_TAG.ordinal()].writeEntry(arguments);
        arguments.clear();

        arguments.add("Person.id");
        arguments.add("Post.id");
        arguments.add("creationDate");
        writers[FileNames.PERSON_LIKES_POST.ordinal()].writeEntry(arguments);
        arguments.clear();

        arguments.add("Person.id");
        arguments.add("Comment.id");
        arguments.add("creationDate");
        writers[FileNames.PERSON_LIKES_COMMENT.ordinal()].writeEntry(arguments);
        arguments.clear();

        arguments.add("id");
        arguments.add("imageFile");
        arguments.add("creationDate");
        arguments.add("locationIP");
        arguments.add("browserUsed");
        arguments.add("language");
        arguments.add("content");
        arguments.add("length");
        writers[FileNames.POST.ordinal()].writeEntry(arguments);
        arguments.clear();

        arguments.add("Post.id");
        arguments.add("Person.id");
        writers[FileNames.POST_HASCREATOR_PERSON.ordinal()].writeEntry(arguments);
        arguments.clear();

        arguments.add("Post.id");
        arguments.add("Tag.id");
        writers[FileNames.POST_HASTAG_TAG.ordinal()].writeEntry(arguments);
        arguments.clear();

        arguments.add("Post.id");
        arguments.add("Place.id");
        writers[FileNames.POST_ISLOCATEDIN_PLACE.ordinal()].writeEntry(arguments);
        arguments.clear();

        arguments.add("id");
        arguments.add("creationDate");
        arguments.add("locationIP");
        arguments.add("browserUsed");
        arguments.add("content");
        arguments.add("length");
        writers[FileNames.COMMENT.ordinal()].writeEntry(arguments);
        arguments.clear();

        arguments.add("id");
        arguments.add("creationDate");
        arguments.add("locationIP");
        arguments.add("browserUsed");
        arguments.add("content");
        arguments.add("length");
        writers[FileNames.COMMENT.ordinal()].writeEntry(arguments);
        arguments.clear();

        arguments.add("Comment.id");
        arguments.add("Person.id");
        writers[FileNames.COMMENT_HASCREATOR_PERSON.ordinal()].writeEntry(arguments);
        arguments.clear();

        arguments.add("Comment.id");
        arguments.add("Tag.id");
        writers[FileNames.COMMENT_HASTAG_TAG.ordinal()].writeEntry(arguments);
        arguments.clear();

        arguments.add("Comment.id");
        arguments.add("Place.id");
        writers[FileNames.COMMENT_ISLOCATEDIN_PLACE.ordinal()].writeEntry(arguments);
        arguments.clear();

        arguments.add("Comment.id");
        arguments.add("Post.id");
        writers[FileNames.COMMENT_REPLYOF_POST.ordinal()].writeEntry(arguments);
        arguments.clear();

        arguments.add("Comment.id");
        arguments.add("Comment.id");
        writers[FileNames.COMMENT_REPLYOF_COMMENT.ordinal()].writeEntry(arguments);
        arguments.clear();

	}
	
	@Override
	public void close() {
		int numFiles = FileNames.values().length;
		for(int i = 0; i < numFiles; ++i) {
			writers[i].close();
		}
	}
	
	protected void serialize( Forum forum ) {
		
		String dateString = Dictionaries.dates.formatDateDetail(forum.creationDate());
		
		arguments.add(Long.toString(forum.id()));
		arguments.add(forum.title());
		arguments.add(dateString);
		writers[FileNames.FORUM.ordinal()].writeEntry(arguments);
		arguments.clear();
		
		arguments.add(Long.toString(forum.id()));
		arguments.add(Long.toString(forum.moderator().accountId()));
		writers[FileNames.FORUM_HASMODERATOR_PERSON.ordinal()].writeEntry(arguments);
		arguments.clear();
		
		for( Integer i : forum.tags()) {
			arguments.add(Long.toString(forum.id()));
			arguments.add(Integer.toString(i));
			writers[FileNames.FORUM_HASTAG_TAG.ordinal()].writeEntry(arguments);
			arguments.clear();
		}
		
	}
	
	protected void serialize( Post post ) {
		
		arguments.add(Long.toString(post.messageId()));
		arguments.add(empty);
		arguments.add(Dictionaries.dates.formatDateDetail(post.creationDate()));
		arguments.add(post.ipAddress().toString());
		arguments.add(Dictionaries.browsers.getName(post.browserId()));
		arguments.add(Dictionaries.languages.getLanguageName(post.language()));
		arguments.add(post.content());
		arguments.add(Integer.toString(post.content().length()));
		writers[FileNames.POST.ordinal()].writeEntry(arguments);
		arguments.clear();
		
		arguments.add(Long.toString(post.messageId()));
		arguments.add(Integer.toString(post.countryId()));
		writers[FileNames.POST_ISLOCATEDIN_PLACE.ordinal()].writeEntry(arguments);
		arguments.clear();
		
		arguments.add(Long.toString(post.messageId()));
		arguments.add(Long.toString(post.author().accountId()));
		writers[FileNames.POST_HASCREATOR_PERSON.ordinal()].writeEntry(arguments);
		arguments.clear();
		
		
		arguments.add(Long.toString(post.forumId()));
		arguments.add(Long.toString(post.messageId()));
		writers[FileNames.FORUM_CONTAINEROF_POST.ordinal()].writeEntry(arguments);
		arguments.clear();
		
		for( Integer t : post.tags() ) {
			arguments.add(Long.toString(post.messageId()));
			arguments.add(Integer.toString(t));
			writers[FileNames.POST_HASTAG_TAG.ordinal()].writeEntry(arguments);
			arguments.clear();
		}
	}
	
	protected void serialize( Comment comment ) {
		arguments.add(Long.toString(comment.messageId()));
		arguments.add(Dictionaries.dates.formatDateDetail(comment.creationDate()));
		arguments.add(comment.ipAddress().toString());
		arguments.add(Dictionaries.browsers.getName(comment.browserId()));
		arguments.add(comment.content());
		arguments.add(Integer.toString(comment.content().length()));
		writers[FileNames.COMMENT.ordinal()].writeEntry(arguments);
		arguments.clear();
		
		if (comment.replyOf() == comment.postId()) {
			arguments.add(Long.toString(comment.messageId()));
			arguments.add(Long.toString(comment.postId()));
			writers[FileNames.COMMENT_REPLYOF_POST.ordinal()].writeEntry(arguments);
			arguments.clear();
		} else {
			arguments.add(Long.toString(comment.messageId()));
			arguments.add(Long.toString(comment.replyOf()));
			writers[FileNames.COMMENT_REPLYOF_COMMENT.ordinal()].writeEntry(arguments);
			arguments.clear();
		}
		arguments.add(Long.toString(comment.messageId()));
		arguments.add(Integer.toString(comment.countryId()));
		writers[FileNames.COMMENT_ISLOCATEDIN_PLACE.ordinal()].writeEntry(arguments);
		arguments.clear();
		
		arguments.add(Long.toString(comment.messageId()));
		arguments.add(Long.toString(comment.author().accountId()));
		writers[FileNames.COMMENT_HASCREATOR_PERSON.ordinal()].writeEntry(arguments);
		arguments.clear();
		
		for( Integer t : comment.tags() ) {
			arguments.add(Long.toString(comment.messageId()));
			arguments.add(Integer.toString(t));
			writers[FileNames.COMMENT_HASTAG_TAG.ordinal()].writeEntry(arguments);
			arguments.clear();
		}
	}
	
	protected void serialize( Photo photo ) {
		
		arguments.add(Long.toString(photo.messageId()));
		arguments.add(photo.content());
		arguments.add(Dictionaries.dates.formatDateDetail(photo.creationDate()));
		arguments.add(photo.ipAddress().toString());
		arguments.add(Dictionaries.browsers.getName(photo.browserId()));
		arguments.add(empty);
		arguments.add(empty);
		arguments.add(Integer.toString(0));
		writers[FileNames.POST.ordinal()].writeEntry(arguments);
		arguments.clear();
		
		
		arguments.add(Long.toString(photo.messageId()));
		arguments.add(Integer.toString(photo.countryId()));
		writers[FileNames.POST_ISLOCATEDIN_PLACE.ordinal()].writeEntry(arguments);
		arguments.clear();
		
		arguments.add(Long.toString(photo.messageId()));
		arguments.add(Long.toString(photo.author().accountId()));
		writers[FileNames.POST_HASCREATOR_PERSON.ordinal()].writeEntry(arguments);
		arguments.clear();
		
		
		arguments.add(Long.toString(photo.forumId()));
		arguments.add(Long.toString(photo.messageId()));
		writers[FileNames.FORUM_CONTAINEROF_POST.ordinal()].writeEntry(arguments);
		arguments.clear();
		
		for( Integer t : photo.tags() ) {
			arguments.add(Long.toString(photo.messageId()));
			arguments.add(Integer.toString(t));
			writers[FileNames.POST_HASTAG_TAG.ordinal()].writeEntry(arguments);
			arguments.clear();
		}
	}
	
	protected void serialize( ForumMembership membership ) {
		arguments.add(Long.toString(membership.forumId()));
		arguments.add(Long.toString(membership.person().accountId()));
		arguments.add(Dictionaries.dates.formatDateDetail(membership.creationDate()));
		writers[FileNames.FORUM_HASMEMBER_PERSON.ordinal()].writeEntry(arguments);
		arguments.clear();
	}
	
	protected void serialize( Like like ) {
		arguments.add(Long.toString(like.user));
		arguments.add(Long.toString(like.messageId));
		arguments.add(Dictionaries.dates.formatDateDetail(like.date));
		if( like.type == Like.LikeType.POST || like.type == Like.LikeType.PHOTO ) {
			writers[FileNames.PERSON_LIKES_POST.ordinal()].writeEntry(arguments);
			arguments.clear();
		} else {
			writers[FileNames.PERSON_LIKES_COMMENT.ordinal()].writeEntry(arguments);
			arguments.clear();
		}
	}
	
}
