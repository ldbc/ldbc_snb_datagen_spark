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

import umontreal.iro.lecuyer.probdist.GeometricDist;

public class NamesDictionary {
	
    /**
     * Geometric probability used
     */
    private static final double GEOMETRIC_RATIO = 0.2;
    
    private static final int topN = 30;
	
	String surnameFile;
	String givennameFile;

	LocationDictionary locationDic;
	
	HashMap<Integer, Vector<String>> surNamesByLocations;
	Vector<HashMap<Integer, Vector<String>>> givenNamesByLocationsMale;    // Year / Location / Names		
	Vector<HashMap<Integer, Vector<String>>> givenNamesByLocationsFemale;
	
	Random rand;
	Random randUniform;
	GeometricDist geoDist;
	
	public NamesDictionary(String surnameFile, String givennameFile, 
						LocationDictionary locationDic, long seed) {
		this.locationDic = locationDic;
		this.surnameFile = surnameFile; 
		this.givennameFile = givennameFile; 
		
		rand        = new Random(seed);
		randUniform = new Random(seed);
		geoDist = new GeometricDist(GEOMETRIC_RATIO);
	}
	
	public void init() {
	    surNamesByLocations = new HashMap<Integer, Vector<String>>();
	    for (Integer id : locationDic.getCountries()) {
	        surNamesByLocations.put(id, new Vector<String>());
	    }

	    //assume that there is only 2 periods of birthyears
	    int birthYearPeriod = 2; 
	    givenNamesByLocationsMale = new Vector<HashMap<Integer, Vector<String>>>(birthYearPeriod);
	    givenNamesByLocationsFemale = new Vector<HashMap<Integer, Vector<String>>>(birthYearPeriod);
	    for (int i = 0; i < birthYearPeriod; i++){
	        givenNamesByLocationsMale.add(new HashMap<Integer, Vector<String>>());
	        givenNamesByLocationsFemale.add(new HashMap<Integer, Vector<String>>());
	        for (Integer id : locationDic.getCountries()) {
	            givenNamesByLocationsMale.lastElement().put(id, new Vector<String>());
	            givenNamesByLocationsFemale.lastElement().put(id, new Vector<String>());
	        }
	    }

	    extractSurNames();
	    extractGivenNames();
	}
	
	public void extractSurNames() {
		try {
		    BufferedReader surnameDictionary  = new BufferedReader(
		            new InputStreamReader(getClass( ).getResourceAsStream(surnameFile), "UTF-8"));
		    
		    String line;
		    int curLocationId = -1; 
	        int totalSurNames = 0;
		    String lastLocationName = "";
			while ((line = surnameDictionary.readLine()) != null) {
			    String infos[] = line.split(",");
			    String locationName = infos[1];
				if (locationName.compareTo(lastLocationName) != 0) { 	// New location
					if (locationDic.getCountryId(locationName) != LocationDictionary.INVALID_LOCATION) { // Check whether it exists
						curLocationId = locationDic.getCountryId(locationName);
						String surName = infos[2].trim();
						surNamesByLocations.get(curLocationId).add(surName);
						totalSurNames++;
					}
				} else {
				    String surName = infos[2].trim();
					surNamesByLocations.get(curLocationId).add(surName);
					totalSurNames++;
				}
			}
			surnameDictionary.close();
			System.out.println("Done ... " + totalSurNames + " surnames were extracted ");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void extractGivenNames() {
		try {
		    BufferedReader givennameDictionary  = new BufferedReader(
		            new InputStreamReader(getClass( ).getResourceAsStream(givennameFile), "UTF-8"));
		    
		    String line;
		    int curLocationId = -1; 
	        int totalGivenNames = 0;
		    String lastLocationName = "";
			while ((line = givennameDictionary.readLine()) != null){
				String infos[] = line.split("  ");
				String locationName = infos[0];
				int gender = Integer.parseInt(infos[2]);
				int birthYearPeriod = Integer.parseInt(infos[3]);
				
				if (locationName.compareTo(lastLocationName) != 0) { 	// New location
					if (locationDic.getCountryId(locationName) != LocationDictionary.INVALID_LOCATION){		// Check whether it exists
						curLocationId = locationDic.getCountryId(locationName);
						String givenName = infos[1].trim();
						if (gender == 0) {
							givenNamesByLocationsMale.get(birthYearPeriod).get(curLocationId).add(givenName);
						} else {
							givenNamesByLocationsFemale.get(birthYearPeriod).get(curLocationId).add(givenName);
						}
						totalGivenNames++;
					}
				} else {
				    String givenName = infos[1].trim();
					if (gender == 0) {
						givenNamesByLocationsMale.get(birthYearPeriod).get(curLocationId).add(givenName);
					} else {
						givenNamesByLocationsFemale.get(birthYearPeriod).get(curLocationId).add(givenName);
					}
					totalGivenNames++;
				}
			}
			givennameDictionary.close();
			System.out.println("Done ... " + totalGivenNames + " given names were extracted ");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	/*
	 * If the number of names is smaller than the computed rank
	 * uniformly get a name from all names
	 * Else, from 0 to (limitRank - 1) will be distributed according to 
	 * geometric distribution, out of this scope will be distribution
	 */
	private int getGeoDistRandomIdx(int numNames){
		int nameIdx = -1; 
		double prob = rand.nextDouble();
		int rank = geoDist.inverseFInt(prob);
		
		if (rank < topN) {
			if (numNames > rank) {
				nameIdx = rank;
			} else {
				nameIdx = randUniform.nextInt(numNames);
			}
		} else {
			if (numNames > rank) {
				nameIdx = topN + randUniform.nextInt(numNames - topN);
			} else {
				nameIdx = randUniform.nextInt(numNames);
			}
		}
		return nameIdx;
	}
	
	public String getRandomSurname(int locationId) {
		int surNameIdx = getGeoDistRandomIdx(surNamesByLocations.get(locationId).size());
		return surNamesByLocations.get(locationId).get(surNameIdx);
	}
	
	public String getRandomGivenName(int locationId, boolean isMale, int birthYear){
		String name = "";
		int period = (birthYear < 1985) ? 0 : 1;
		Vector<HashMap<Integer, Vector<String>>> target = (isMale) ? givenNamesByLocationsMale : givenNamesByLocationsFemale;
		
		// Note that, only vector of names for the first period contains list of names not in topN
		int nameId = getGeoDistRandomIdx(target.get(0).get(locationId).size());
		if (nameId >= topN) {
		    name = target.get(0).get(locationId).get(nameId);
		} else {
		    name = target.get(period).get(locationId).get(nameId);
		}
		
		return name;
	}
}

