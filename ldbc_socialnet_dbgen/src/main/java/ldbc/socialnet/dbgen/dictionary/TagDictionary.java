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
	
    BufferedReader dictionary; 
	String dicFileName;
	String dicTopic;
	Vector<Vector<Tag>> vecTagByCountry;
	Vector<Vector<Double>> vecTagCumDist; 
	Vector<Vector<Integer>> vecTagId;
	
	int 	numCelebrity = 0 ; 
	
	HashMap<String, Integer> locationNames;
	HashMap<Integer, String> tagNames;
	Random 	rnd; 
	Random 	rnd2;

	int totalReferences; 		//Total number of references to tags
	
	double tagCountryCorrProb; 		// The probability to select a tag from its country
									// May be 0.5
	
	

	public TagDictionary(String dicTopic, String _dicFileName, int _numLocations, long seed, double _tagCountryCorrProb){
		this.dicFileName = _dicFileName;
		this.dicTopic = dicTopic;
		vecTagCumDist = new Vector<Vector<Double>>(_numLocations);
		vecTagId = new Vector<Vector<Integer>>(_numLocations);
		tagNames = new HashMap<Integer, String>();
		
		for (int i =  0; i < _numLocations; i++){
			vecTagCumDist.add(new Vector<Double>());
			vecTagId.add(new Vector<Integer>());
		}
		
		tagCountryCorrProb = _tagCountryCorrProb;
		
		rnd = new Random(seed); 
		rnd2 = new Random(seed);
		
	}
	
	public TagDictionary(String _dicFileName, HashMap<String, Integer> _locationNames){
		this.dicFileName = _dicFileName; 
		this.locationNames = _locationNames;
				
		//init tagByCountry. Each country has several celebrities
		vecTagByCountry = new Vector<Vector<Tag>>(locationNames.size());
		for (int i =  0; i < locationNames.size(); i++){
			vecTagByCountry.add(new Vector<Tag>());
		}
	}
	
	public HashMap<Integer, String> getTagsNamesMapping() {
	    return tagNames;
	}


	public void extractTags(){
		String line;
		int countryId; 
		double cumm; 
		int tagId = 0; 
		try {
		    dictionary = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(dicTopic), "UTF-8"));
		    while ((line = dictionary.readLine()) != null){
		        String infos[] = line.split(" ");
		        tagId = Integer.parseInt(infos[0]);
		        tagNames.put(tagId, infos[1]);
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
			// TODO Auto-generated catch block
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
		
		//System.out.println("Select country is " + countryId + " Total tags number: " + vecTagId.get(countryId).size()); 
		
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



	//This function is run only one time.
	public void initCummulativeTagCountry(String celebrityFileName, String clbByCountryFileName){
		
		String line;
		String tagText;
		String city; 
		String country;
		int countryId; 
		int refNo; 
		int tagId = 0; 
		try {
		    dictionary = new BufferedReader(new InputStreamReader(new FileInputStream(dicFileName), "UTF-8"));
			
			while ((line = dictionary.readLine()) != null){
				String infos[] = line.split(" ");
				tagText = infos[2];
				city = infos[1];
				country=infos[0];
				if (!locationNames.containsKey(country)){
					System.out.println("The country " + country +" is not in the list"); 
				}
				else{
					countryId = locationNames.get(country); 
					refNo = Integer.parseInt(infos[3]); 
					Tag tag = new Tag(tagId, countryId, tagText, refNo);
					vecTagByCountry.get(countryId).add(tag);
					tagId++;
				}
				//add(Math.round(cumdistribution*(float)numberOfUsers));
			}
			
			dictionary.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		try {
			FileOutputStream 	outputCelebrityDic;
			outputCelebrityDic = new FileOutputStream(celebrityFileName);
			FileOutputStream 	outputCelebrityByCountryDic;
			outputCelebrityByCountryDic = new FileOutputStream(clbByCountryFileName);
			OutputStreamWriter writerCel; 
			writerCel = new OutputStreamWriter(outputCelebrityDic);
			OutputStreamWriter writerCelByCountry; 
			writerCelByCountry = new OutputStreamWriter(outputCelebrityByCountryDic);
			
			//Compute the cummulative distribution
			for (int i = 0; i < locationNames.size(); i++){
				int sumRef = 0; 	//total number of reference per country
				double cummulativeDist;
				int curRef = 0; 
				//Get total number of references
				for (int j = 0; j < vecTagByCountry.get(i).size(); j++){
					sumRef +=  vecTagByCountry.get(i).get(j).getNoRef();
				}
				 
				if (sumRef != 0){
					// Get the cummulative distribution value for each tag
					for (int j = 0; j < vecTagByCountry.get(i).size(); j++){
						curRef+=  vecTagByCountry.get(i).get(j).getNoRef();
						cummulativeDist =  (double) curRef/sumRef;
						Tag tag = vecTagByCountry.get(i).get(j); 
						writerCel.write(tag.getText() + " "+ tag.getId() + "\n");
						writerCelByCountry.write(tag.getLocationId() + " " + tag.getId() + " " + cummulativeDist +"\n");
					}
				}
			}
			
			writerCel.close();
			writerCelByCountry.close();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
