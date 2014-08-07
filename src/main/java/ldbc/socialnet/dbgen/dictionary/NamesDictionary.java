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
import java.util.ArrayList;
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
	private LocationDictionary                              locationDictionary;     /**< @brief The location dictioanry. **/
	private HashMap<Integer, ArrayList<String>>             surnamesByCountry;      /**< @brief The surnames by country. **/
	private ArrayList<HashMap<Integer, ArrayList<String>>>  maleNamesByCountry;     /**< @brief The male names by country per year. **/
    private	ArrayList<HashMap<Integer, ArrayList<String>>>  femaleNamesByCountry;   /**< @brief The female names by country per year. **/
	private GeometricDist                                   geoDist;                /**< @brief The geometric distribution. **/

    /**
     * @brief   Constructor
     * @param   locationDic The location dictionary.
     */
	public NamesDictionary( LocationDictionary locationDic ) {
		this.locationDictionary = locationDic;
		this.geoDist = new GeometricDist(GEOMETRIC_RATIO);
        this.surnamesByCountry = new HashMap<Integer, ArrayList<String>>();
        for (Integer id : locationDic.getCountries()) {
            surnamesByCountry.put(id, new ArrayList<String>());
        }
        int birthYearPeriod = 2;
        maleNamesByCountry = new ArrayList<HashMap<Integer, ArrayList<String>>>(birthYearPeriod);
        femaleNamesByCountry = new ArrayList<HashMap<Integer, ArrayList<String>>>(birthYearPeriod);
        for (int i = 0; i < birthYearPeriod; i++){
            maleNamesByCountry.add(new HashMap<Integer, ArrayList<String>>());
            femaleNamesByCountry.add(new HashMap<Integer, ArrayList<String>>());
            for (Integer id : locationDic.getCountries()) {
                maleNamesByCountry.get(maleNamesByCountry.size()-1).put(id, new ArrayList<String>());
                femaleNamesByCountry.get(femaleNamesByCountry.size()-1).put(id, new ArrayList<String>());
            }
        }
	}

    /**
     * @brief   Loads the dictionary.
     * @param   surnamesFileName The surnames file name.
     * @param   namesFileName The names file name.
     */
	public void load( String surnamesFileName, String namesFileName ) {
	    extractSurnames( surnamesFileName );
	    extractNames( namesFileName );
	}

    /**
     * @brief   Loads the surnames.
     * @param   fileName The surnames file name.
     */
	public void extractSurnames( String fileName ) {
		try {
		    BufferedReader surnameDictionary  = new BufferedReader(
		            new InputStreamReader(getClass( ).getResourceAsStream( fileName ), "UTF-8"));
		    
		    String line;
		    int curLocationId = -1; 
	        int totalSurNames = 0;
		    String lastLocationName = "";
			while ((line = surnameDictionary.readLine()) != null) {
			    String infos[] = line.split(",");
			    String locationName = infos[1];
				if (locationName.compareTo(lastLocationName) != 0) { 	// New location
					if ( locationDictionary.getCountryId(locationName) != LocationDictionary.INVALID_LOCATION ) { // Check whether it exists
						curLocationId = locationDictionary.getCountryId(locationName);
						String surName = infos[2].trim();
						surnamesByCountry.get(curLocationId).add(surName);
						totalSurNames++;
					}
				} else {
				    String surName = infos[2].trim();
					surnamesByCountry.get(curLocationId).add(surName);
					totalSurNames++;
				}
			}
			surnameDictionary.close();
			System.out.println("Done ... " + totalSurNames + " surnames were extracted ");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

    /**
     * @brief Loads the names.
     * @param fileName  The names file names.
     */
	public void extractNames( String fileName ) {
		try {
		    BufferedReader givennameDictionary  = new BufferedReader(
		            new InputStreamReader(getClass( ).getResourceAsStream( fileName ), "UTF-8"));
		    
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
					if ( locationDictionary.getCountryId(locationName) != LocationDictionary.INVALID_LOCATION ){		// Check whether it exists
						curLocationId = locationDictionary.getCountryId(locationName);
						String givenName = infos[1].trim();
						if (gender == 0) {
							maleNamesByCountry.get(birthYearPeriod).get(curLocationId).add(givenName);
						} else {
							femaleNamesByCountry.get(birthYearPeriod).get(curLocationId).add(givenName);
						}
						totalGivenNames++;
					}
				} else {
				    String givenName = infos[1].trim();
					if (gender == 0) {
						maleNamesByCountry.get(birthYearPeriod).get(curLocationId).add(givenName);
					} else {
						femaleNamesByCountry.get(birthYearPeriod).get(curLocationId).add(givenName);
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
	 * @brief   If the number of names is smaller than the computed rank
	 *          uniformly get a name from all names
	 *          Else, from 0 to (limitRank - 1) will be distributed according to
	 *          geometric distribution, out of this scope will be distribution
	 * @param   random The random number generator.
	 * @param   numNames The  number of names to be considered in top.
	 * @return  The name identifier.
	 */
	private int getGeoDistRandomIdx(Random random, int numNames){
		int nameIdx = -1; 
		double prob = random.nextDouble();
		int rank = geoDist.inverseFInt(prob);
		
		if (rank < topN) {
			if (numNames > rank) {
				nameIdx = rank;
			} else {
				nameIdx = random.nextInt(numNames);
			}
		} else {
			if (numNames > rank) {
				nameIdx = topN + random.nextInt(numNames - topN);
			} else {
				nameIdx = random.nextInt(numNames);
			}
		}
		return nameIdx;
	}

    /**
     * @brief   Get a random surname.
     * @param   random  The random number generator.
     * @param   countryId The country id
     * @return  The surname identifier.
     */
	public String getRandomSurname(Random random,int countryId) {
		int surNameIdx = getGeoDistRandomIdx(random, surnamesByCountry.get(countryId).size());
		return surnamesByCountry.get(countryId).get(surNameIdx);
	}

    /**
     * @brief   Gets a random name.
     * @param   random The random number generator.
     * @param   countryId The country id
     * @param   isMale True if want a male name.
     * @param   birthYear The birthyear.
     * @return  The name identifier.
     */
	public String getRandomName(Random random, int countryId, boolean isMale, int birthYear){
		String name = "";
		int period = (birthYear < 1985) ? 0 : 1;
		ArrayList<HashMap<Integer, ArrayList<String>>> target = (isMale) ? maleNamesByCountry : femaleNamesByCountry;
		// Note that, only vector of names for the first period contains list of names not in topN
		int nameId = getGeoDistRandomIdx(random, target.get(0).get(countryId).size());
		if (nameId >= topN) {
		    name = target.get(0).get(countryId).get(nameId);
		} else {
		    name = target.get(period).get(countryId).get(nameId);
		}
		return name;
	}
}

