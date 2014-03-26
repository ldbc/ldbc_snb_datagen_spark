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
import java.util.Set;
import java.util.Vector;


public class TagDictionary {
	
    private static final String SEPARATOR = "\t";
    
    int numCelebrity;
    double tagCountryCorrProb;
    
    String dicFileName;
    String dicTopic;
    String tagClassFile;
    String tagHierarchyFile;
    
	Vector<Vector<Integer>> tagsByCountry;
	Vector<Vector<Double>> tagCummulativeDist; 
	
	HashMap<Integer, String>  className;
	HashMap<Integer, String>  classLabel;
	HashMap<Integer, Integer> classHierarchy;
	HashMap<Integer, Integer> tagClass;
	HashMap<Integer, String>  tagNames;
	HashMap<Integer, String>  tagDescription;  // a.k.a foaf:Names
	
	public TagDictionary(String dicTopic, String _dicFileName, String tagClassFile, String tagHierarchyFile, 
	        int numLocations, double tagCountryCorrProb) {
	    
		this.dicFileName = _dicFileName;
		this.dicTopic = dicTopic;
		this.tagClassFile = tagClassFile;
		this.tagHierarchyFile = tagHierarchyFile;
		this.tagCountryCorrProb = tagCountryCorrProb;
		
		tagCummulativeDist = new Vector<Vector<Double>>(numLocations);
		tagsByCountry = new Vector<Vector<Integer>>(numLocations);
		tagNames = new HashMap<Integer, String>();
		tagClass = new HashMap<Integer, Integer>();
		tagDescription = new HashMap<Integer, String>();
		className = new HashMap<Integer, String>();
	    classLabel = new HashMap<Integer, String>();
	    classHierarchy = new HashMap<Integer, Integer>();
		
		for (int i =  0; i < numLocations; i++){
			tagCummulativeDist.add(new Vector<Double>());
			tagsByCountry.add(new Vector<Integer>());
		}
		
		numCelebrity = 0;
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
		try {
		    BufferedReader dictionary = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(tagClassFile), "UTF-8"));
            
		    String line;
		    while ((line = dictionary.readLine()) != null){
                String data[] = line.split(SEPARATOR);
                Integer classId = Integer.valueOf(data[0]);
                className.put(classId, data[1]);
                classLabel.put(classId, data[2]);
            }
            
            dictionary.close();
		    dictionary = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(tagHierarchyFile), "UTF-8"));
            while ((line = dictionary.readLine()) != null){
                String infos[] = line.split(SEPARATOR);
                Integer classId = Integer.valueOf(infos[0]);
                Integer parentId = Integer.valueOf(infos[1]);
                classHierarchy.put(classId, parentId);
            }
		    
            dictionary.close();
		    dictionary = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(dicTopic), "UTF-8"));
		    while ((line = dictionary.readLine()) != null){
		        String infos[] = line.split(SEPARATOR);
		        int tagId = Integer.valueOf(infos[0]);
		        Integer classId = Integer.valueOf(infos[1]);
		        tagClass.put(tagId, classId);
		        tagNames.put(tagId, infos[2]);
		        tagDescription.put(tagId, infos[3]);
		    }
		    
		    dictionary.close();
			dictionary = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(dicFileName), "UTF-8"));
			while ((line = dictionary.readLine()) != null){
				String infos[] = line.split(" ");
				int countryId = Integer.parseInt(infos[0]);
				int tagId = Integer.parseInt(infos[1]);
				double cummulative = Double.parseDouble(infos[2]);
				
				tagCummulativeDist.get(countryId).add(cummulative);
				tagsByCountry.get(countryId).add(tagId);
				if (tagId + 1 > numCelebrity) {
				    numCelebrity = tagId + 1;
				}
			}
			
			dictionary.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public int getaTagByCountry(Random randomTagOtherCountry, Random randomTagCountryProb, int _countryId){
		int countryId; 
		countryId = _countryId; 
			
		if (tagsByCountry.get(countryId).size() == 0 || randomTagOtherCountry.nextDouble() > tagCountryCorrProb) {
			do {
				countryId = randomTagOtherCountry.nextInt(tagsByCountry.size());
			} while (tagsByCountry.get(countryId).size() == 0);
		}
		
		double randomDis = randomTagCountryProb.nextDouble();
		int lowerBound = 0;
		int upperBound = tagsByCountry.get(countryId).size();
		int curIdx = (upperBound + lowerBound)  / 2;
		
		while (upperBound > (lowerBound+1)) {
			if (tagCummulativeDist.get(countryId).get(curIdx) > randomDis ){
				upperBound = curIdx;
			} else {
				lowerBound = curIdx; 
			}
			curIdx = (upperBound + lowerBound)  / 2;
		}
		
		return tagsByCountry.get(countryId).get(curIdx); 
	} 

	public int getNumCelebrity() {
		return numCelebrity;
	}

	public Integer[] getRandomTags( Random random, int num ) {
		Integer[] result = new Integer[num];
		for( int i = 0; i < num; ) {
			int randomCountry = random.nextInt(tagsByCountry.size());
			Vector<Integer> tags = tagsByCountry.get(randomCountry);
			if( tags.size() > 0 ){ 
				result[i] = tags.get(random.nextInt(tags.size()));
				++i;
			}
		}
		return result;
	}

    public Set<Integer> getTags() {
        return tagNames.keySet();
    }
}
