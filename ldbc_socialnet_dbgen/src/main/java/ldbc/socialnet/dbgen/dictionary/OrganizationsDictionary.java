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
import java.util.HashMap;
import java.util.Random;
import java.util.Vector;



public class OrganizationsDictionary {
    BufferedReader dicAllInstitutes; 
	String dicFileName;

	HashMap<String, Integer> locationNames;
	HashMap<String, Integer> organizationToLocation;
	
	Vector<Vector<String>> organizationsByLocations;
	Random 		rand;
	
	Random		randUnRelatedOrganization;
	double		probUnCorrelatedOrganization;
	Random		randUnRelatedLocation;

	// For top institutes
	Random		randTopUniv;
	double 		probTopUniv; 
	LocationDictionary locationDic; 
	
	public OrganizationsDictionary(String _dicFileName, HashMap<String, Integer> _locationNames, 
									long seedRandom, double _probUnCorrelatedOrganization, 
									long _seedTopUni, double _probTopUni,
									LocationDictionary _locationDic){
		this.locationNames = _locationNames; 
		this.organizationToLocation = new HashMap<String, Integer>();
		this.dicFileName = _dicFileName;
		this.rand = new Random(seedRandom);
		this.randUnRelatedLocation = new Random(seedRandom);
		this.randUnRelatedOrganization = new Random(seedRandom);
		this.probUnCorrelatedOrganization = _probUnCorrelatedOrganization;
		this.randTopUniv = new Random(_seedTopUni);
		this.probTopUniv = _probTopUni;
		this.locationDic = _locationDic;
	}
	public void init(){
		try {
			dicAllInstitutes = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(dicFileName), "UTF-8"));
			
			System.out.println("Building dictionary of organizations (by locations)");
			
			organizationsByLocations = new Vector<Vector<String>>(locationNames.size());
			for (int i = 0; i < locationNames.size(); i++){
				organizationsByLocations.add(new Vector<String>());
			}
			
			extractOrganizationNames();
			
			dicAllInstitutes.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public HashMap<String, Integer> GetOrganizationLocationMap() {
	    return organizationToLocation;
	}
	
	public void extractOrganizationNames(){
		//System.out.println("Extract organizations by location ...");
		String line; 
		String locationName; 
		String organizationName; 
		String lastLocationName = "";
		int curLocationId = -1; 
		int totalNumOrganizations = 0;
		try {
			while ((line = dicAllInstitutes.readLine()) != null){
				//System.out.println("Line --> " + line);
				//System.out.println("[0]: " + line.split(" ")[0]);
				String infos[] = line.split("  ");
				locationName = infos[0];
				//System.out.println("Line in names = " + line); 
				if (locationName.compareTo(lastLocationName) != 0){ 	// New location
					if (locationNames.containsKey(locationName)){		// Check whether it exists
						lastLocationName = locationName;
						curLocationId = locationNames.get(locationName); 
						organizationName = infos[1].trim();
						organizationsByLocations.get(curLocationId).add(organizationName);
						organizationToLocation.put(organizationName, curLocationId);
						totalNumOrganizations++;
					}
				}
				else{
					organizationName = infos[1].trim();
					organizationsByLocations.get(curLocationId).add(organizationName);
					organizationToLocation.put(organizationName, curLocationId);
					totalNumOrganizations++;
				}

			}
			
			System.out.println("Done ... " + totalNumOrganizations + " organizations were extracted");
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	// 90% of people go to top-10 universities
	// 10% go to remaining universities
	public int getRandomOrganization(int _locationId){
		int bitmask = 0x00FF; 
		int locationOrganization; 
		int randomOrganizationIdx;
		int locationId = _locationId;
		
		// User may not study at a university in her location
		if (randUnRelatedOrganization.nextDouble() > probUnCorrelatedOrganization){
			while (organizationsByLocations.get(locationId).size() == 0){
				locationId = randUnRelatedLocation.nextInt(locationNames.size());
			}
			
			//Select a university in top 10
			if (randTopUniv.nextDouble() < probTopUniv){
				randomOrganizationIdx = rand.nextInt(
							Math.min(organizationsByLocations.get(locationId).size(), 10));
			}
			//Select a random organization
			else{
				randomOrganizationIdx = rand.nextInt(organizationsByLocations.get(locationId).size());
			}
			
			int zOrderLocation = locationDic.getZorderID(locationId);
			
			locationOrganization = (zOrderLocation << 24) | (randomOrganizationIdx << 12);
			
			return locationOrganization;
		}
		else{		// Randomly select one institute out of the location
			int uncorrelateLocationIdx = randUnRelatedLocation.nextInt(locationNames.size());
			while (organizationsByLocations.get(uncorrelateLocationIdx).size() == 0){
				uncorrelateLocationIdx = randUnRelatedLocation.nextInt(locationNames.size());
			}
			
			
			randomOrganizationIdx = rand.nextInt(organizationsByLocations.get(uncorrelateLocationIdx).size());
			
			int zOrderLocation = locationDic.getZorderID(uncorrelateLocationIdx);
			
			locationOrganization = (zOrderLocation << 24) | (randomOrganizationIdx << 12);
			
			return locationOrganization;
		}
	}
	public String getOrganizationName(int locationOrganization){
		String organization; 
		int zOrderlocationId = locationOrganization >> 24;
		int organizationId = (locationOrganization >> 12) & 0x0FFF;
		int locationId = locationDic.getLocationIdFromZOrder(zOrderlocationId);
		
		organization = organizationsByLocations.get(locationId).get(organizationId);
				
		return organization;
	}
	
}
