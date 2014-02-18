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
import ldbc.socialnet.dbgen.dictionary.TagTextDictionary;
import ldbc.socialnet.dbgen.generator.DateGenerator;

public class FlashmobTagDictionary {

    private static final String SEPARATOR = "\t";
    
    DateGenerator dateGen;
	Random rand; 
	TagTextDictionary tagTextDictionary;
	HashMap<Integer,Vector<long> > flashmobTags;
	double flashmobTagsPerMonth;
	double probInterestFlashmobTag;
	double maxRandomFlashmobTagsUserMonth;

	public FlashmobTagDictionary( TagTextDictionary tagTextDictionary, 
								  DateGenerator dateGen,
								  int flashmobTagsPerMonth,
								  double probInterestFlashmobTag,
								  int maxRandomFlashmobTagsUserMonth,
								  long seed ) {

		this.tagTextDictionary = tagTextDictionary;	    
		this.dateGen = dateGen;
		rand  = new Random(seed); 
		this.flashmobtags = new HashMap<Integer,Vector<Integer> >();
		this.flashmobTagsPerMonth = flashmobTagsPerMonth;
		this.probInterestFlashmobTag = probInterestFlashmobTag;
	}
	
	public String getName(int id) {
	    return tagNames.get(id);
	}
	
	public String getDescription(int id) {
        return tagDescription.get(id);
    }
	
	public Integer getTagClass(int id) {
        return tagClass.get(id);
    }
	
	public String getClassName(int id) {
        return className.get(id);
    }
	
	public String getClassLabel(int id) {
        return classLabel.get(id);
    }
	
	public Integer getClassParent(int id) {
	    if (!classHierarchy.containsKey(id)) {
	        return -1;
	    }
        return classHierarchy.get(id);
    }

    public void initialize() {
    	int numFlashmobTags = flashmobTagsPerMonth * dateGen.numberOfMonths(dateGen.getStartDateTime());
    	Integer[] tags = tagTextDictionary.getRandomTags(numFlashmobTags);
    	for( int i = 0; i < numFlashmobTags; ++i ){
    		Vector<long> dates = flashmobTags.get(tags[i]);
    		if( dates == null ) {
    			dates = new Vector<long>();	
    			dates.add(dateGen.randomDateInMillis(dateGen.getStartDateTime(), dateGen.getCurrentDateTime()));
    			flashmobTags.add(tags[i],dates);
    		} else {
    			dates.add(dateGen.randomDateInMillis(dateGen.getStartDateTime(), dateGen.getCurrentDateTime()));
    		}
    	}
    }

	public Integer[] GetFlashmobTags( TreeSet<Integer> tags, long fromDate ) {
		// Per cada tag a integer set, 
			// comprobar si es flashmob i en cas afirmatiu, calcular si cal affegirlo.
		// Calcular el nombre de random tags.
		
	}
}
