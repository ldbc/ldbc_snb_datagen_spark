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
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;

import umontreal.iro.lecuyer.probdist.GeometricDist;

public class NamesDictionary {
	
    /**
     * Geometric probability used in 
     */
    private static final double GEOMETRIC_RATIO = 0.2;
    
	//RandomAccessFile dictionary;
    BufferedReader surnameDictionary; 
    BufferedReader givennameDictionary; 
	
	//String dicFileName;
	String surdicFileName;
	String givendicFileName;

	HashMap<String, Integer> locationNames;
	
	Vector<Vector<String>> surNamesByLocations;
	Vector<Vector<Vector<String>>> givenNamesByLocationsMale;    // Year / Location / Names		
	Vector<Vector<Vector<String>>> givenNamesByLocationsFemale;
	
	GeometricDist geoDist;
	Random 		rand;
	Random		randUniform;
	
	// Store the statistic for testdriver
	int[][] countBySurNames; 		
	int[][] countByGivenNames;
	
	final int topN = 30; 
	
	public NamesDictionary(String _surdicFileName, String _givendicFileName, 
						HashMap<String, Integer> _locationNames, long seedRandom){
		this.locationNames = _locationNames; 
		//this.dicFileName = _dicFileName;
		this.surdicFileName = _surdicFileName; 
		this.givendicFileName = _givendicFileName; 
		this.rand = new Random(seedRandom);
		this.randUniform = new Random(seedRandom);
		geoDist = new GeometricDist(GEOMETRIC_RATIO);
	}
	public void init(){
		try {
			//dictionary = new RandomAccessFile(dicFileName, "r");
			surnameDictionary  = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(surdicFileName), "UTF-8"));
			givennameDictionary  = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(givendicFileName), "UTF-8"));
			
			//System.out.println("Extracting names into a dictionary ");
			
			surNamesByLocations = new Vector<Vector<String>>(locationNames.size());

			//assume that there is only 2 periods of birthyears
			int birthYearPeriod = 2; 
			givenNamesByLocationsMale = new Vector<Vector<Vector<String>>>(birthYearPeriod);
			givenNamesByLocationsFemale = new Vector<Vector<Vector<String>>>(birthYearPeriod);
			for (int i = 0; i < birthYearPeriod; i++){
				givenNamesByLocationsMale.add(new Vector<Vector<String>>(locationNames.size()));
				givenNamesByLocationsFemale.add(new Vector<Vector<String>>(locationNames.size()));
				for (int j = 0; j < locationNames.size(); j++){
					givenNamesByLocationsMale.lastElement().add(new Vector<String>());
					givenNamesByLocationsFemale.lastElement().add(new Vector<String>());
				}
			}
			
			for (int i = 0; i < locationNames.size(); i++){
				surNamesByLocations.add(new Vector<String>());
			}
			
			extractSurNames();
			
			surnameDictionary.close();
			
			extractGivenNames(); 
			
			givennameDictionary.close();
			
			//System.out.println("Sort popular names in Germany");
			//getFrequency(89);
			//getFrequency(69);
			//System.exit(-1);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void extractSurNames(){
		System.out.println("Extract surnames by locations ...");
		String line; 
		String locationName; 
		String surName; 
		String lastLocationName = "";
		int curLocationId = -1; 
		int totalSurNames = 0;
		try {
			while ((line = surnameDictionary.readLine()) != null){
			    String infos[] = line.split(",");
			    locationName = infos[1];
				
				if (locationName.compareTo(lastLocationName) != 0){ 	// New location

					if (locationNames.containsKey(locationName)){		// Check whether it exists
						curLocationId = locationNames.get(locationName);
						surName = infos[2].trim();
						surNamesByLocations.get(curLocationId).add(surName);
						totalSurNames++;
					}
				}
				else{
				    surName = infos[2].trim();
					surNamesByLocations.get(curLocationId).add(surName);
					totalSurNames++;
				}

			}
			
			System.out.println("Done ... " + totalSurNames + " surnames were extracted ");
			
			// For statictic of the testdriver
			//countBySurNames = new int[locationNames.size()][totalNumNames];
			//countByGivenNames = new int[locationNames.size()][totalNumNames];
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void extractGivenNames(){
		System.out.println("Extract given by locations ...");
		String line; 
		String locationName; 
		String givenName; 
		String lastLocationName = "";
		int curLocationId = -1; 
		int totalGivenNames = 0;
		int gender; 
		int birthYearPeriod;
		try {
			while ((line = givennameDictionary.readLine()) != null){
				String infos[] = line.split("  ");
				locationName = infos[0];
				gender = Integer.parseInt(infos[2]);
				birthYearPeriod = Integer.parseInt(infos[3]);
				
				if (locationName.compareTo(lastLocationName) != 0){ 	// New location

					if (locationNames.containsKey(locationName)){		// Check whether it exists
						curLocationId = locationNames.get(locationName);
						givenName = infos[1].trim();
						if (gender == 0)
							givenNamesByLocationsMale.get(birthYearPeriod).get(curLocationId).add(givenName);
						else
							givenNamesByLocationsFemale.get(birthYearPeriod).get(curLocationId).add(givenName);
						
						totalGivenNames++;
					}
				}
				else{
					givenName = infos[1].trim();
					if (gender == 0)
						givenNamesByLocationsMale.get(birthYearPeriod).get(curLocationId).add(givenName);
					else
						givenNamesByLocationsFemale.get(birthYearPeriod).get(curLocationId).add(givenName);

					totalGivenNames++;
				}

			}
			
			System.out.println("Done ... " + totalGivenNames + " given names were extracted ");
			
			// For statictic of the testdriver
			//countBySurNames = new int[locationNames.size()][totalNumNames];
			//countByGivenNames = new int[locationNames.size()][totalNumNames];
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	/*
	 * If the number of names is smaller than the computed rank
	 * uniformly get a name from all names
	 * Else, from 0 to (limitRank - 1) will be distributed according to 
	 * geometric distribution, out of this scope will be distribution
	 */

	public int getGeoDistRandomIdx(int numOfNames){
		int limitRank = topN; 
		int nameIdx = -1; 
		double prob = rand.nextDouble();
		
		int rank = geoDist.inverseFInt(prob);
		
		if (rank < limitRank) 
			if (numOfNames > rank)
				nameIdx = rank; 
			else
				nameIdx = randUniform.nextInt(numOfNames);
		else
			if (numOfNames > rank)
				nameIdx = limitRank + randUniform.nextInt(numOfNames - limitRank);
			else
				nameIdx = randUniform.nextInt(numOfNames);
		
		return nameIdx;
	}
	public String getRandomSurName(int locationId){
		
		String surName = ""; 
		int surNameIdx = getGeoDistRandomIdx(surNamesByLocations.get(locationId).size());
		surName = surNamesByLocations.get(locationId).get(surNameIdx);
		
		// For statistic of the test driver 
		//countBySurNames[locationId][randomSurNameIdx]++;
		
		return surName;
	}
	public String getRandomGivenName(int locationId, boolean isMale, int birthYear){
		String givenName = "";
		int givenNameIdx; 
		int period = -1;
		if (birthYear < 1985) 
			period = 0;
		else 
			period = 1;
		
		// Note that, only vector of names for the first period
		// contains list of names not in topN
		if (isMale){
			//givenNameIdx = getGeoDistRandomIdx(givenNamesByLocationsMale.get(period).get(locationId).size());
			givenNameIdx = getGeoDistRandomIdx(givenNamesByLocationsMale.get(0).get(locationId).size());
			if (givenNameIdx >= topN){
				givenName = givenNamesByLocationsMale.get(0).get(locationId).get(givenNameIdx);
			}
			else{
				givenName = givenNamesByLocationsMale.get(period).get(locationId).get(givenNameIdx);				
			}
		}
		else{
			givenNameIdx = getGeoDistRandomIdx(givenNamesByLocationsFemale.get(0).get(locationId).size());
			if (givenNameIdx >= topN){
				givenName = givenNamesByLocationsFemale.get(0).get(locationId).get(givenNameIdx);
			}
			else{
				givenName = givenNamesByLocationsFemale.get(period).get(locationId).get(givenNameIdx);
			}
		}
		
		// For statistic of the test driver
		//countByGivenNames[locationId][randomGivenNameIdx]++;
		
		return givenName;
	}

	public Vector<Vector<String>> getSurNamesByLocations() {
		return surNamesByLocations;
	}
	public void setSurNamesByLocations(Vector<Vector<String>> surNamesByLocations) {
		this.surNamesByLocations = surNamesByLocations;
	}

	public int[][] getCountBySurNames() {
		return countBySurNames;
	}
	public void setCountBySurNames(int[][] countBySurNames) {
		this.countBySurNames = countBySurNames;
	}
	public int[][] getCountByGivenNames() {
		return countByGivenNames;
	}
	public void setCountByGivenNames(int[][] countByGivenNames) {
		this.countByGivenNames = countByGivenNames;
	}

	
	/*
	 * The remaining part is for getting frequency of a name and do sorting
	 * according to names' frequencies
	 * DO NOT NEED NOW 
	 */
	
	public void getFrequency(int index){
		// Sort
		
		Vector<String> names = surNamesByLocations.get(index);
		 
		Vector<NameFreq> nameFrequency = new Vector<NameFreq>();
		
		Collections.sort(names);
		String preName = "";
		int count =0; 
		int totalCount = 0; 
		
		for (int i=0; i < names.size(); i++){
			
			if (names.get(i).compareTo(preName) != 0){
				//System.out.println(" " + preName + " :  " + count);
				nameFrequency.add(new NameFreq(preName, count));
				preName = names.get(i); 
				count = 0;
			}
			count++;
			totalCount++;
		}
		
		NameFreq[] sortNameFreq = new NameFreq[nameFrequency.size()];
		for (int i = 0; i <  nameFrequency.size(); i ++){
			sortNameFreq[i] = new NameFreq(nameFrequency.get(i).name, nameFrequency.get(i).freq);
		}
		
		System.out.println("Number of names " + sortNameFreq.length);
		Arrays.sort(sortNameFreq);
		for (int i = 0; i < sortNameFreq.length; i ++){
			sortNameFreq[i].printPercent(totalCount);
		}
		
	}
	class NameFreq implements Comparable{
		String name;
		int freq; 
		public NameFreq(String _name, int _freq){
			this.name = _name; 
			this.freq = _freq; 
		}
		public int compareTo(Object obj)
		{
			NameFreq tmp = (NameFreq)obj;
			if(this.freq < tmp.freq)
			{	
				return -1;
			}
			else if(this.freq > tmp.freq)
			{
				return 1;
			}
			return 0;
		}
		public void print(){
			System.out.println(name + "  " + freq);
		}
		public void printPercent(int total){
			System.out.println(name + "  " + (double)100*freq/total);
		}
	}
}

