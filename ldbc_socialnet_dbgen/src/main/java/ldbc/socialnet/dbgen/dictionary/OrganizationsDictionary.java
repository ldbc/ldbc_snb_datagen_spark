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


public class OrganizationsDictionary {
    
    private static final String SEPARATOR = "  ";
    
    String dicFileName; 

	HashMap<String, Integer> organizationToLocation;
	HashMap<Integer, Vector<String>> organizationsByLocations;
	
	double probTopUniv; 
	double probUnCorrelatedOrganization;
	Random rand;
	Random randTopUniv;
	Random randUnRelatedOrganization;
	Random randUnRelatedLocation;
	LocationDictionary locationDic; 
	
	public OrganizationsDictionary(String dicFileName, LocationDictionary locationDic, 
									long seedRandom, double probUnCorrelatedOrganization, 
									long seedTopUni, double probTopUni){
	    
		this.dicFileName = dicFileName;
		this.probTopUniv = probTopUni;
		this.locationDic = locationDic;
		this.probUnCorrelatedOrganization = probUnCorrelatedOrganization;
		
		rand = new Random(seedRandom);
		randTopUniv = new Random(seedTopUni);
		randUnRelatedLocation = new Random(seedRandom);
		randUnRelatedOrganization = new Random(seedRandom);
	}
	
	public void init(){
	    organizationToLocation = new HashMap<String, Integer>();
	    organizationsByLocations = new HashMap<Integer, Vector<String>>();
	    for (Integer id : locationDic.getCountries()){
	        organizationsByLocations.put(id, new Vector<String>());
	    }
	    extractOrganizationNames();
	}
	
	public HashMap<String, Integer> GetOrganizationLocationMap() {
	    return organizationToLocation;
	}
	
	public void extractOrganizationNames() {
		try {
		    BufferedReader dicAllInstitutes = new BufferedReader(
		            new InputStreamReader(getClass( ).getResourceAsStream(dicFileName), "UTF-8"));
		    
		    String line;
		    int curLocationId = -1; 
            int totalNumOrganizations = 0;
		    String lastLocationName = "";
			while ((line = dicAllInstitutes.readLine()) != null){
				String data[] = line.split(SEPARATOR);
				String locationName = data[0];
				if (locationName.compareTo(lastLocationName) != 0) {
					if (locationDic.getCountryId(locationName) != LocationDictionary.INVALID_LOCATION) {
						lastLocationName = locationName;
						curLocationId = locationDic.getCountryId(locationName); 
						String organizationName = data[1].trim();
						organizationsByLocations.get(curLocationId).add(organizationName);
						Integer cityId = locationDic.getCityId(data[2]);
						organizationToLocation.put(organizationName, cityId);
						totalNumOrganizations++;
					}
				} else{
				    String organizationName = data[1].trim();
					organizationsByLocations.get(curLocationId).add(organizationName);
					Integer cityId = locationDic.getCityId(data[2]);
                    organizationToLocation.put(organizationName, cityId);
					totalNumOrganizations++;
				}
			}
			dicAllInstitutes.close();
			System.out.println("Done ... " + totalNumOrganizations + " organizations were extracted");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// 90% of people go to top-10 universities
	// 10% go to remaining universities
	public int getRandomOrganization(int countryId) {
	    
		int locationId = countryId;
		double prob = randUnRelatedOrganization.nextDouble();
		
		Vector<Integer> countries = locationDic.getCountries();
		if (randUnRelatedOrganization.nextDouble() <= probUnCorrelatedOrganization) {
		    locationId = countries.get(randUnRelatedLocation.nextInt(countries.size()));
		}
		
		while (organizationsByLocations.get(locationId).size() == 0) {
            locationId = countries.get(randUnRelatedLocation.nextInt(countries.size()));
        }
		
		int range = organizationsByLocations.get(locationId).size();
		if (prob > probUnCorrelatedOrganization && randTopUniv.nextDouble() < probTopUniv) {
				range = Math.min(organizationsByLocations.get(locationId).size(), 10);
		}
		
		int randomOrganizationIdx = rand.nextInt(range);
		int zOrderLocation = locationDic.getZorderID(locationId);
        int locationOrganization = (zOrderLocation << 24) | (randomOrganizationIdx << 12);
		return locationOrganization;
	}
	
	public String getOrganizationName(int locationOrganization) {
		int zOrderlocationId = locationOrganization >> 24;
		int organizationId = (locationOrganization >> 12) & 0x0FFF;
		int locationId = locationDic.getLocationIdFromZOrder(zOrderlocationId);
		
		return organizationsByLocations.get(locationId).get(organizationId);
	}
}
