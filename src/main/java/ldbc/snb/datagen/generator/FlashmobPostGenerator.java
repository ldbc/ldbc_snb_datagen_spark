/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ldbc.snb.datagen.generator;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.dictionary.TextGenerator;
import ldbc.snb.datagen.objects.FlashmobTag;
import ldbc.snb.datagen.objects.Forum;
import ldbc.snb.datagen.objects.ForumMembership;
import ldbc.snb.datagen.util.Distribution;

import java.util.*;

import static ldbc.snb.datagen.generator.DatagenParams.maxNumTagPerFlashmobPost;

/**
 *
 * @author aprat
 */
public class FlashmobPostGenerator extends PostGenerator{
	private Distribution dateDistribution_;
	private FlashmobTag[] forumFlashmobTags = null;
	private long hoursToMillis_;
	private long flashmobSpan_;
	private long currentForum = -1;
	public FlashmobPostGenerator(TextGenerator generator) {
		super(generator);
		dateDistribution_ = new Distribution(DatagenParams.flashmobDistFile);
		hoursToMillis_ = 60*60*1000;
		flashmobSpan_ = 72 * hoursToMillis_;
		dateDistribution_.initialize();
	}
	
	/** @brief Selects a random tag from a given index.
     *  @param[in] tags The array of sorted tags to select from.
     *  @param[in] index The first tag to consider.
     *  @return The index of a random tag.*/
    private int selectRandomTag( Random randomFlashmobTag, FlashmobTag[] tags, int index ) {
        int upperBound = tags.length - 1;
        int lowerBound = index;
        double prob = randomFlashmobTag.nextDouble() * (tags[upperBound].prob - tags[lowerBound].prob) + tags[lowerBound].prob;
        int midPoint = (upperBound + lowerBound)  / 2;
        while (upperBound > (lowerBound+1)){
            if (tags[midPoint].prob > prob ){
                upperBound = midPoint;
            } else{
                lowerBound = midPoint;
            }
            midPoint = (upperBound + lowerBound)  / 2;
        }
        return midPoint;
    }

    /** @brief Selects the earliest flashmob tag index from a given date.
     *  @return The index to the earliest flashmob tag.*/
    private int searchEarliest( FlashmobTag[] tags, ForumMembership membership ) {
        long fromDate = membership.creationDate();
        int lowerBound = 0;
        int upperBound = tags.length - 1;
        int midPoint = (upperBound + lowerBound)  / 2;
        while (upperBound > (lowerBound+1)){
            if (tags[midPoint].date > fromDate ){
                upperBound = midPoint;
            } else{
                lowerBound = midPoint;
            }
            midPoint = (upperBound + lowerBound)  / 2;
        }
        if( tags[midPoint].date < fromDate ) return -1;
        return midPoint;
    }

    private void populateForumFlashmobTags( Random randomNumPost, Forum forum ) {
	    
	    ArrayList<Integer> forumTags = forum.tags();
	    TreeSet<Integer> tags = new TreeSet<Integer>();
	    for( Integer tag : tags ) {
		    tags.add(tag);
	    }
	    ArrayList<FlashmobTag> temp = Dictionaries.flashmobs.generateFlashmobTags( randomNumPost, tags, forum.creationDate() );
	    forumFlashmobTags = new FlashmobTag[temp.size()];
	    Iterator<FlashmobTag> it = temp.iterator();
	    int index = 0;
	    int sumLevels = 0;
	    while(it.hasNext()) {
		    FlashmobTag flashmobTag = new FlashmobTag();
		    it.next().copyTo(flashmobTag);
		    forumFlashmobTags[index] = flashmobTag;
		    sumLevels+=flashmobTag.level;
		    ++index;
	    }
	    Arrays.sort(forumFlashmobTags);
	    int size = forumFlashmobTags.length;
	    double currentProb = 0.0;
	    for( int i = 0; i < size; ++i ) {
		    forumFlashmobTags[i].prob = currentProb;
		    currentProb += (double)(forumFlashmobTags[i].level) / (double)(sumLevels);
	    }
    }

    protected PostGenerator.PostInfo generatePostInfo( Random randomTag, Random randomDate, Forum forum, ForumMembership membership ) {
	    if( currentForum != forum.id() ) {
		    populateForumFlashmobTags(randomTag,forum);
		    currentForum = forum.id();
	    }
	    if( forumFlashmobTags.length < 1 ) return null;
	    PostInfo postInfo = new PostInfo();
	    int index = searchEarliest(forumFlashmobTags,membership);
	    if( index < 0 ) return null;
	    index = selectRandomTag( randomTag, forumFlashmobTags,index);
	    FlashmobTag flashmobTag =  forumFlashmobTags[index];
	    postInfo.tags.add(flashmobTag.tag);
	    Set<Integer> extraTags = Dictionaries.tagMatrix.getSetofTags(randomTag,randomTag,flashmobTag.tag, maxNumTagPerFlashmobPost - 1);
	    Iterator<Integer> it = extraTags.iterator();
	    while (it.hasNext()) {
		    Integer value = it.next();
		    if(randomTag.nextDouble() < 0.05) {
			    postInfo.tags.add(value);
		    }
	    }
	    double prob = dateDistribution_.nextDouble(randomDate);
	    postInfo.date = flashmobTag.date - flashmobSpan_/2 + (long)(prob * flashmobSpan_);
	    if( postInfo.date > Dictionaries.dates.getEndDateTime() ) return null;
	    return postInfo;
    }
}
