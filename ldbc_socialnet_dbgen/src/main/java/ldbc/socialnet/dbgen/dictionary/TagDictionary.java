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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Random;
import java.util.Vector;

import ldbc.socialnet.dbgen.objects.Tag;


public class TagDictionary {
	
    private final String SEPARATOR = "\t";
    
    BufferedReader dictionary; 
	String dicFileName;
	String dicTopic;
	String tagClassFile;
	String tagHierarchyFile;
	Vector<Vector<Tag>> vecTagByCountry;
	Vector<Vector<Double>> vecTagCumDist; 
	Vector<Vector<Integer>> vecTagId;
	
	int 	numCelebrity = 0 ; 
	
	HashMap<Integer, String> className;
	HashMap<Integer, String> classLabel;
	HashMap<Integer, Integer> classHierarchy;
	HashMap<Integer, Integer> tagClass;
	HashMap<Integer, String> tagNames;
	HashMap<Integer, String> tagDescription;  // a.k.a foaf:Names
	Random 	rnd; 
	Random 	rnd2;

	int totalReferences; 		//Total number of references to tags
	
	double tagCountryCorrProb; 		// The probability to select a tag from its country
									// May be 0.5
	
	

	public TagDictionary(String dicTopic, String _dicFileName, String tagClassFile, String tagHierarchyFile, 
	        int _numLocations, long seed, double _tagCountryCorrProb){
		this.dicFileName = _dicFileName;
		this.dicTopic = dicTopic;
		this.tagClassFile = tagClassFile;
		this.tagHierarchyFile = tagHierarchyFile;
		vecTagCumDist = new Vector<Vector<Double>>(_numLocations);
		vecTagId = new Vector<Vector<Integer>>(_numLocations);
		tagNames = new HashMap<Integer, String>();
		tagClass = new HashMap<Integer, Integer>();
		tagDescription = new HashMap<Integer, String>();
		className = new HashMap<Integer, String>();
	    classLabel = new HashMap<Integer, String>();
	    classHierarchy = new HashMap<Integer, Integer>();
		
		for (int i =  0; i < _numLocations; i++){
			vecTagCumDist.add(new Vector<Double>());
			vecTagId.add(new Vector<Integer>());
		}
		
		tagCountryCorrProb = _tagCountryCorrProb;
		
		rnd = new Random(seed); 
		rnd2 = new Random(seed);
	}
	
	public HashMap<Integer, String> getTagsNamesMapping() {
	    return tagNames;
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


	public void extractTags(){
		String line;
		int countryId; 
		double cumm; 
		int tagId = 0; 
		try {
		    
		    dictionary = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(tagClassFile), "UTF-8"));
            while ((line = dictionary.readLine()) != null){
                String infos[] = line.split(SEPARATOR);
                Integer classId = Integer.valueOf(infos[0]);
                className.put(classId, infos[1]);
                classLabel.put(classId, infos[2]);
            }
		    
		    dictionary = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(tagHierarchyFile), "UTF-8"));
            while ((line = dictionary.readLine()) != null){
                String infos[] = line.split(SEPARATOR);
                Integer classId = Integer.valueOf(infos[0]);
                Integer parentId = Integer.valueOf(infos[1]);
                classHierarchy.put(classId, parentId);
            }
		    
		    dictionary = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(dicTopic), "UTF-8"));
		    while ((line = dictionary.readLine()) != null){
		        String infos[] = line.split(SEPARATOR);
		        tagId = Integer.valueOf(infos[0]);
		        Integer classId = Integer.valueOf(infos[1]);
		        tagClass.put(tagId, classId);
		        tagNames.put(tagId, infos[2]);
		        tagDescription.put(tagId, infos[3]);
		    }
		    dictionary.close();
			dictionary = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(dicFileName), "UTF-8"));
			while ((line = dictionary.readLine()) != null){
				String infos[] = line.split(" ");
				countryId = Integer.parseInt(infos[0]);
				tagId = Integer.parseInt(infos[1]);
				cumm = Double.parseDouble(infos[2]);
				
				vecTagCumDist.get(countryId).add(cumm);
				vecTagId.get(countryId).add(tagId);
				if (tagId + 1 > numCelebrity) {
				    numCelebrity = tagId + 1;
				}
			}
			
			dictionary.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	public int getaTagByCountry(int _countryId){
		int countryId; 
		countryId = _countryId; 
		
		if (vecTagId.get(countryId).size() == 0 || rnd.nextDouble() > tagCountryCorrProb){
			//Randomly get from other country.
			do {
				countryId = rnd.nextInt(vecTagId.size());
			} while (vecTagId.get(countryId).size() == 0);
		}
		
		// Doing binary search for finding the tag
		double randomDis = rnd2.nextDouble(); 
		int lowerBound = 0;
		int upperBound = vecTagId.get(countryId).size(); 
		
		int curIdx = (upperBound + lowerBound)  / 2;
		
		while (upperBound > (lowerBound+1)){
			if (vecTagCumDist.get(countryId).get(curIdx) > randomDis ){
				upperBound = curIdx;
			}
			else{
				lowerBound = curIdx; 
			}
			curIdx = (upperBound + lowerBound)  / 2;
		}
		
		return vecTagId.get(countryId).get(curIdx); 
	} 

	public int getNumCelebrity() {
		return numCelebrity;
	}

	public void setNumCelebrity(int numCelebrity) {
		this.numCelebrity = numCelebrity;
	}
}
