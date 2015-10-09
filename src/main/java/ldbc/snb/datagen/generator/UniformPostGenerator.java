/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ldbc.snb.datagen.generator;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.dictionary.TextGenerator;
import ldbc.snb.datagen.objects.Forum;
import ldbc.snb.datagen.objects.ForumMembership;

import java.util.Iterator;
import java.util.Random;
import java.util.TreeSet;

/**
 *
 * @author aprat
 */
public class UniformPostGenerator extends PostGenerator {


	public UniformPostGenerator(TextGenerator generator) {
		super(generator);
	}
	
    protected PostInfo generatePostInfo( Random randomTag, Random randomDate, Forum forum, ForumMembership membership ) {
	    PostInfo postInfo = new PostInfo();
	    postInfo.tags = new TreeSet<Integer>();
	    Iterator<Integer> it = forum.tags().iterator();
	    while (it.hasNext()) {
		    Integer value = it.next();
		    if (postInfo.tags.isEmpty()) {
			    postInfo.tags.add(value);
		    } else {
			    if (randomTag.nextDouble() < 0.05) {
				    postInfo.tags.add(value);
			    }
		    }
	    }
	    postInfo.date = Dictionaries.dates.randomDate(randomDate,membership.creationDate()+DatagenParams.deltaTime);
	    if( postInfo.date > Dictionaries.dates.getEndDateTime() ) return null;
	    return postInfo;
    }
}
