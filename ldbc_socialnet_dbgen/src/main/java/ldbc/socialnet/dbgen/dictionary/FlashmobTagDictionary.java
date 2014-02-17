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

public class FlashmobTagDictionary {
	
    private static final String SEPARATOR = "\t";
    
	Random 	rnd; 
	TagTextDictionary tagTextDictionary;
	Vector<Integer> flashMobTags;
	int numFlashmobTags;
	double probRandomFlashmobTag;

	public FlashmobTagDictionary( TagTextDictionary tagTextDictionary, 
								  int numFlashmobTags,
								  double probRandomFlashmobTag ) {
		this.tagTextDictionary = tagTextDictionary;	    
		rnd  = new Random(seed); 
		this.flashMobTags = new TreeSet<Integer>();
		this.numFlashmobTags = numFlashmobTags;
		this.probRandomFlashmobTag = probRandomFlashmobTag;
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
    	Integer[] tags = tagTextDictionary.getRandomTags(numFlashmobTags);
    	for( int i = 0; i < numFlashmobTags; ++i ){
    		flashMobTags.add(tags[i]);
    	}
    }

	public Integer GetFlashmobTag( TreeSet<Integer> tags ) {
		
	}
}
