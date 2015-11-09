/*
* To change this license header, choose License Headers in Project Properties.
* To change this template file, choose Tools | Templates
* and open the template in the editor.
*/
package ldbc.snb.datagen.serializer.snb.interactive;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.objects.*;

import ldbc.snb.datagen.serializer.HDFSWriter;
import ldbc.snb.datagen.serializer.PersonActivitySerializer;
import ldbc.snb.datagen.serializer.Turtle;
import ldbc.snb.datagen.vocabulary.*;
import org.apache.hadoop.conf.Configuration;



/**
 *
 * @author aprat
 */
public class TurtlePersonActivitySerializer extends PersonActivitySerializer {
	private HDFSWriter[] writers;
	
	private long membershipId = 0;
	private long likeId       = 0;

	private enum FileNames {
		SOCIAL_NETWORK ("social_network_activity");

		private final String name;

		private FileNames( String name ) {
			this.name = name;
		}
		public String toString() {
			return name;
		}
	}

	public TurtlePersonActivitySerializer() {
	}
	
	@Override
	public void initialize(Configuration conf, int reducerId) {

		int numFiles = FileNames.values().length;
		writers = new HDFSWriter[numFiles];
		for( int i = 0; i < numFiles; ++i) {
			writers[i] = new HDFSWriter(conf.get("ldbc.snb.datagen.serializer.socialNetworkDir"), FileNames.values()[i].toString()+"_"+reducerId,conf.getInt("ldbc.snb.datagen.numPartitions",1),conf.getBoolean("ldbc.snb.datagen.serializer.compressed",false),"ttl");
			writers[i].writeAllPartitions(Turtle.getNamespaces());
			writers[i].writeAllPartitions(Turtle.getStaticNamespaces());
		}
	}
	
	@Override
	public void close() {
		int numFiles = FileNames.values().length;
		for(int i = 0; i < numFiles; ++i) {
			writers[i].close();
		}
	}
	
	protected void serialize( Forum forum ) {

		StringBuffer result = new StringBuffer(12000);

		String forumPrefix = SN.getForumURI(forum.id());
		Turtle.AddTriple(result, true, false, forumPrefix, RDF.type, SNVOC.Forum);

		Turtle.AddTriple(result, false, false, forumPrefix, SNVOC.id,
				Turtle.createLiteral(Long.toString(forum.id())));

		Turtle.AddTriple(result, false, false, forumPrefix, SNVOC.title,
				Turtle.createLiteral(forum.title()));
		Turtle.AddTriple(result, false, true, forumPrefix, SNVOC.creationDate,
				Turtle.createDataTypeLiteral(Dictionaries.dates.formatDateDetail(forum.creationDate()), XSD.DateTime));

		Turtle.createTripleSPO(result, forumPrefix,
				SNVOC.hasModerator, SN.getPersonURI(forum.moderator().accountId()));

		for(Integer tag : forum.tags()) {
			String topic = Dictionaries.tags.getName(tag);
			Turtle.createTripleSPO(result, forumPrefix, SNVOC.hasTag, DBP.fullPrefixed(topic));
		}
		writers[FileNames.SOCIAL_NETWORK.ordinal()].write(result.toString());
	}
	
	protected void serialize( Post post ) {

		StringBuffer result = new StringBuffer(2500);

		String prefix = SN.getPostURI(post.messageId());

		Turtle.AddTriple(result, true, false, prefix, RDF.type, SNVOC.Post);

		Turtle.AddTriple(result, false, false, prefix, SNVOC.id,
				Turtle.createLiteral(Long.toString(post.messageId())));

		Turtle.AddTriple(result, false, false, prefix, SNVOC.creationDate,
				Turtle.createDataTypeLiteral(Dictionaries.dates.formatDateDetail(post.creationDate()), XSD.DateTime));

		Turtle.AddTriple(result, false, false, prefix, SNVOC.ipaddress,
					Turtle.createLiteral(post.ipAddress().toString()));
		Turtle.AddTriple(result, false, false, prefix, SNVOC.browser,
				Turtle.createLiteral(Dictionaries.browsers.getName(post.browserId())));

		Turtle.AddTriple(result, false, false, prefix, SNVOC.content,
					Turtle.createLiteral(post.content()));
		Turtle.AddTriple(result, false, true, prefix, SNVOC.length,
				Turtle.createLiteral(Integer.toString(post.content().length())));

		Turtle.createTripleSPO(result, prefix, SNVOC.language,
				Turtle.createLiteral(Dictionaries.languages.getLanguageName(post.language())));

		Turtle.createTripleSPO(result, prefix, SNVOC.locatedIn,
				DBP.fullPrefixed(Dictionaries.places.getPlaceName(post.countryId())));

		Turtle.createTripleSPO(result, SN.getForumURI(post.forumId()), SNVOC.containerOf, prefix);
		Turtle.createTripleSPO(result, prefix, SNVOC.hasCreator, SN.getPersonURI(post.author().accountId()));

		for( Integer tag : post.tags()) {
			String topic = Dictionaries.tags.getName(tag);
			Turtle.createTripleSPO(result, prefix, SNVOC.hasTag, DBP.fullPrefixed(topic));
		}
		writers[FileNames.SOCIAL_NETWORK.ordinal()].write(result.toString());
	}
	
	protected void serialize( Comment comment ) {
		StringBuffer result = new StringBuffer(2000);

		String prefix = SN.getCommentURI(comment.messageId());

		Turtle.AddTriple(result, true, false, prefix, RDF.type, SNVOC.Comment);

		Turtle.AddTriple(result, false, false, prefix, SNVOC.id,
				Turtle.createLiteral(Long.toString(comment.messageId())));

		Turtle.AddTriple(result, false, false, prefix, SNVOC.creationDate,
				Turtle.createDataTypeLiteral(Dictionaries.dates.formatDateDetail(comment.creationDate()), XSD.DateTime));
		Turtle.AddTriple(result, false, false, prefix, SNVOC.ipaddress,
				Turtle.createLiteral(comment.ipAddress().toString()));
		Turtle.AddTriple(result, false, false, prefix, SNVOC.browser,
				Turtle.createLiteral(Dictionaries.browsers.getName(comment.browserId())));
		Turtle.AddTriple(result, false, false, prefix, SNVOC.content,
				Turtle.createLiteral(comment.content()));
		Turtle.AddTriple(result, false, true, prefix, SNVOC.length,
				Turtle.createLiteral(Integer.toString(comment.content().length())));

		String replied = (comment.replyOf() == comment.postId()) ? SN.getPostURI(comment.postId()) :
				SN.getCommentURI(comment.replyOf());
		Turtle.createTripleSPO(result, prefix, SNVOC.replyOf, replied);
		Turtle.createTripleSPO(result, prefix, SNVOC.locatedIn,
				DBP.fullPrefixed(Dictionaries.places.getPlaceName(comment.countryId())));

		Turtle.createTripleSPO(result, prefix, SNVOC.hasCreator,
				SN.getPersonURI(comment.author().accountId()));

		for( Integer tag : comment.tags()) {
			String topic = Dictionaries.tags.getName(tag);
			Turtle.createTripleSPO(result, prefix, SNVOC.hasTag, DBP.fullPrefixed(topic));
		}
		writers[FileNames.SOCIAL_NETWORK.ordinal()].write(result.toString());
	}
	
	protected void serialize( Photo photo ) {
		StringBuffer result = new StringBuffer(2500);

		String prefix = SN.getPostURI(photo.messageId());
		Turtle.AddTriple(result, true, false, prefix, RDF.type, SNVOC.Post);

		Turtle.AddTriple(result, false, false, prefix, SNVOC.id,
				Turtle.createLiteral(Long.toString(photo.messageId())));

		Turtle.AddTriple(result, false, false, prefix, SNVOC.hasImage, Turtle.createLiteral(photo.content()));
		Turtle.AddTriple(result, false, false, prefix, SNVOC.ipaddress,
				Turtle.createLiteral(photo.ipAddress().toString()));
		Turtle.AddTriple(result, false, false, prefix, SNVOC.browser,
				Turtle.createLiteral(Dictionaries.browsers.getName(photo.browserId())));
		Turtle.AddTriple(result, false, true, prefix, SNVOC.creationDate,
				Turtle.createDataTypeLiteral(Dictionaries.dates.formatDateDetail(photo.creationDate()), XSD.DateTime));

		Turtle.createTripleSPO(result, prefix, SNVOC.hasCreator, SN.getPersonURI(photo.author().accountId()));
		Turtle.createTripleSPO(result, SN.getForumURI(photo.forumId()), SNVOC.containerOf, prefix);
		Turtle.createTripleSPO(result, prefix, SNVOC.locatedIn,
				DBP.fullPrefixed(Dictionaries.places.getPlaceName(photo.countryId())));

		for( Integer tag: photo.tags()) {
			String topic = Dictionaries.tags.getName(tag);
			Turtle.createTripleSPO(result, prefix, SNVOC.hasTag, DBP.fullPrefixed(topic));
		}
		writers[FileNames.SOCIAL_NETWORK.ordinal()].write(result.toString());
	}
	
	protected void serialize( ForumMembership membership ) {
		String memberhipPrefix = SN.getMembershipURI(SN.formId(membershipId));
		String forumPrefix = SN.getForumURI(membership.forumId());
		StringBuffer result = new StringBuffer(19000);
		Turtle.createTripleSPO(result, forumPrefix, SNVOC.hasMember, memberhipPrefix);

		Turtle.AddTriple(result, true, false, memberhipPrefix, SNVOC.hasPerson, SN.getPersonURI(membership.person().accountId()));
		Turtle.AddTriple(result, false, true, memberhipPrefix, SNVOC.joinDate,
				Turtle.createDataTypeLiteral(Dictionaries.dates.formatDateDetail(membership.creationDate()), XSD.DateTime));
		membershipId++;
		writers[FileNames.SOCIAL_NETWORK.ordinal()].write(result.toString());
	}
	
	protected void serialize( Like like ) {
		StringBuffer result = new StringBuffer(2500);
		long id = SN.formId(likeId);
		String likePrefix = SN.getLikeURI(id);
		Turtle.createTripleSPO(result, SN.getPersonURI(like.user),
				SNVOC.like, likePrefix);

		if( like.type == Like.LikeType.POST || like.type == Like.LikeType.PHOTO ) {
			String prefix = SN.getPostURI(like.messageId);
			Turtle.AddTriple(result, true, false, likePrefix, SNVOC.hasPost, prefix);
		} else {
			String prefix = SN.getCommentURI(like.messageId);
			Turtle.AddTriple(result, true, false, likePrefix, SNVOC.hasComment, prefix);
		}
		Turtle.AddTriple(result, false, true, likePrefix, SNVOC.creationDate,
				Turtle.createDataTypeLiteral(Dictionaries.dates.formatDateDetail(like.date), XSD.DateTime));
		likeId++;
		writers[FileNames.SOCIAL_NETWORK.ordinal()].write(result.toString());
	}

	public void reset() {
		likeId = 0;
		membershipId = 0;

	}
	
}
