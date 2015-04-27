/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ldbc.snb.datagen.generator;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.objects.Forum;
import ldbc.snb.datagen.objects.ForumMembership;
import ldbc.snb.datagen.objects.Like;
import ldbc.snb.datagen.objects.Like.LikeType;
import ldbc.snb.datagen.objects.Message;

import java.util.ArrayList;
import java.util.Random;

/**
 *
 * @author aprat
 */
public class LikeGenerator {
	private double maxNumberOfLikes;
	private final PowerDistGenerator likesGenerator_;

	

	public LikeGenerator() {
		likesGenerator_ = new PowerDistGenerator(1,DatagenParams.maxNumLike,0.07);
	}

	public ArrayList<Like> generateLikes( Random random, Forum forum, Message message, LikeType type ) {
		ArrayList<Like> likes = new ArrayList<Like>();
		int numMembers = forum.memberships().size();
		int numLikes = likesGenerator_.getValue(random);
		numLikes = numLikes >= numMembers ?  numMembers : numLikes;
		ArrayList<ForumMembership> memberships = forum.memberships();
		int startIndex = 0;
		if( numLikes < numMembers ) {
			startIndex = random.nextInt(numMembers - numLikes);
		}
		for (int i = 0; i < numLikes; i++) {
			ForumMembership membership = memberships.get(startIndex+i);
			long minDate = message.creationDate() > memberships.get(startIndex+i).creationDate() ? message.creationDate() : membership.creationDate();
			long date = Math.max(Dictionaries.dates.randomSevenDays(random),DatagenParams.deltaTime) + minDate;
			if( date <= Dictionaries.dates.getEndDateTime() ) {
				Like like = new Like();
				like.user = membership.person().accountId();
				like.userCreationDate = membership.person().creationDate();
				like.messageId = message.messageId();
				like.date = date;
				like.type = type;
				likes.add(like);
			}
		}
		return likes;
	}
}
