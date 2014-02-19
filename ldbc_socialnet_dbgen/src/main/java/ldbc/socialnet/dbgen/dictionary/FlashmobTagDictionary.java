/*
 * Copyright (c) 2013 LDBC
 * Linked Data Benchmark Council (http://ldbc.eu)
 *
 * This file is part of ldbc_socialnet_dbgen.
 *
 * ldbc_socialnet_dbgen is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ldbc_socialnet_dbgen is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ldbc_socialnet_dbgen.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2011 OpenLink Software <bdsmt@openlinksw.com>
 * All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation;  only Version 2 of the License dated
 * June 1991.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package ldbc.socialnet.dbgen.dictionary;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Random;
import java.util.Vector;
import java.util.TreeSet;
import java.util.Arrays;
import java.util.Iterator;
import ldbc.socialnet.dbgen.dictionary.TagDictionary;
import ldbc.socialnet.dbgen.generator.DateGenerator;
import ldbc.socialnet.dbgen.generator.PowerDistGenerator;
import ldbc.socialnet.dbgen.objects.FlashmobTag;

public class FlashmobTagDictionary {

    private static final String SEPARATOR = "\t";
    
    DateGenerator dateGen;
	PowerDistGenerator levelGenerator;
	Random random;
	TagDictionary tagDictionary;
	HashMap<Integer,Vector<FlashmobTag> > flashmobTags;
	FlashmobTag[] flashmobTagCumDist;
	double flashmobTagsPerMonth;
	double probInterestFlashmobTag;
	double maxRandomFlashmobTagsUserMonth;
	int maxUserPostsPerFlashmobTag;

	public FlashmobTagDictionary( TagDictionary tagDictionary, 
								  DateGenerator dateGen,
								  int flashmobTagsPerMonth,
								  double probInterestFlashmobTag,
								  int maxRandomFlashmobTagsUserMonth,
								  double flashmobTagMinLevel,
								  double flashmobTagMaxLevel,
								  double flashmobTagDistExp,
								  long seed ) {

		this.tagDictionary = tagDictionary;	    
		this.dateGen = dateGen;
		this.levelGenerator  = new PowerDistGenerator(flashmobTagMinLevel, flashmobTagMaxLevel, flashmobTagDistExp, seed); 
		this.random = new Random(seed);
		this.flashmobTags = new HashMap<Integer,Vector<FlashmobTag> >();
		this.flashmobTagsPerMonth = flashmobTagsPerMonth;
		this.probInterestFlashmobTag = probInterestFlashmobTag;
		this.maxUserPostsPerFlashmobTag = maxUserPostsPerFlashmobTag;
	}

    public void initialize() {
    	int numFlashmobTags = (int)(flashmobTagsPerMonth * dateGen.numberOfMonths(dateGen.getStartDateTime()));
    	Integer[] tags = tagDictionary.getRandomTags(numFlashmobTags);
    	flashmobTagCumDist = new FlashmobTag[numFlashmobTags];
    	double sumLevels = 0;
    	for( int i = 0; i < numFlashmobTags; ++i ){
    		Vector<FlashmobTag> instances = flashmobTags.get(tags[i]);
    		if( instances == null ) {
    			instances = new Vector<FlashmobTag>();	
    			flashmobTags.put(tags[i],instances);
    		}
   			FlashmobTag flashmobTag = new FlashmobTag();
   			flashmobTag.date = dateGen.randomDateInMillis(dateGen.getStartDateTime(), dateGen.getCurrentDateTime());
   			flashmobTag.level = levelGenerator.getValue();
   			sumLevels += flashmobTag.level;
   			flashmobTag.tag = tags[i];
    		instances.add(flashmobTag);
    		flashmobTagCumDist[i] = flashmobTag;
    	}
    	Arrays.sort(flashmobTagCumDist);
    	int size = flashmobTagCumDist.length;
    	double currentProb = 0.0;
    	for( int i = 0; i < numFlashmobTags; ++i ) {
    		flashmobTagCumDist[i].prob = currentProb;
    		currentProb += flashmobTagCumDist[i].level / sumLevels;
    	}
    }

    private int searchEarliestIndex( long fromDate ) {
            int lowerBound = 0;
            int upperBound = flashmobTagCumDist.length;
            int midPoint = (upperBound + lowerBound)  / 2;
            while (upperBound > (lowerBound+1)){
                if (flashmobTagCumDist[midPoint].date > fromDate ){
                    upperBound = midPoint;
                } else{
                    lowerBound = midPoint; 
                }
                midPoint = (upperBound + lowerBound)  / 2;
            }
            return midPoint;

    }

    private FlashmobTag searchRandomFlashmobTag( int index ) {
            double randomDis = random.nextDouble()*(1.0 - flashmobTagCumDist[index].prob) + flashmobTagCumDist[index].prob; 
            int lowerBound = index;
            int upperBound = flashmobTagCumDist.length;
            int midPoint = (upperBound + lowerBound)  / 2;
            while (upperBound > (lowerBound+1)){
                if (flashmobTagCumDist[midPoint].prob > randomDis ){
                    upperBound = midPoint;
                } else{
                    lowerBound = midPoint; 
                }
                midPoint = (upperBound + lowerBound)  / 2;
            }
            return flashmobTagCumDist[midPoint];
    }

	public Vector<FlashmobTag> getFlashmobTags( TreeSet<Integer> interests, long fromDate ) {
		Vector<FlashmobTag> result = new Vector<FlashmobTag>();
		Iterator<Integer> it = interests.iterator();
		while(it.hasNext()) {
			Integer tag = it.next();
			Vector<FlashmobTag> instances = flashmobTags.get(tag);
			if( instances != null ) {
				Iterator<FlashmobTag> it2 = instances.iterator();
				while( it2.hasNext()){
					FlashmobTag instance = it2.next();
					if( instance.date >= fromDate ) {
						if(random.nextDouble() > probInterestFlashmobTag){
							int numInstances = random.nextInt((int)(instance.level)) + 1;
							for( int k = 0; k < numInstances; ++k ) {
								result.add(instance);
							}
						}
					} 
				}
			}
		}
		int numberOfMonths = (int)(dateGen.numberOfMonths(fromDate));	
		int randomTagsPerMonth = random.nextInt((int)(maxRandomFlashmobTagsUserMonth) + 1);
		int numberOfRandomTags = numberOfMonths * randomTagsPerMonth; 
		int earliestIndex = searchEarliestIndex(fromDate);
		for( int i = 0; i < numberOfRandomTags; ++i ) {
				FlashmobTag flashmobTag = searchRandomFlashmobTag(earliestIndex);
				int numInstances = random.nextInt((int)(flashmobTag.level)) + 1;
				for( int k = 0; k < numInstances; ++k ) {
					result.add(flashmobTag); 
				}
		}
		return result;
	}
}
