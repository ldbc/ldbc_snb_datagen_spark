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

public class CompanyDictionary {
    BufferedReader dictionary; 
	String dicFileName;

	HashMap<String, Integer> locationNames;
	
	Vector<Vector<String>> companiesByLocations;
	HashMap<String, Integer> companyCountry; //company->locationId(country)
	Random 		rand; 
	Random		randUnRelatedCompany;
	double		probUnCorrelatedCompany;
	Random		randUnRelatedLocation;
	
	public CompanyDictionary(String _dicFileName, HashMap<String, Integer> _locationNames, 
							long seedRandom, double _probUnCorrelatedCompany){
		this.locationNames = _locationNames; 
		this.dicFileName = _dicFileName;
		this.rand = new Random(seedRandom);
		this.randUnRelatedCompany = new Random(seedRandom);
		this.randUnRelatedLocation = new Random(seedRandom);
		
		this.probUnCorrelatedCompany = _probUnCorrelatedCompany;
	}
	public void init(){
		try {
		    companyCountry = new HashMap<String, Integer>();
			dictionary = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(dicFileName), "UTF-8"));
			
			System.out.println("Building dictionary of companies (by locations)");
			
			companiesByLocations = new Vector<Vector<String>>(locationNames.size());
			for (int i = 0; i < locationNames.size(); i++){
				companiesByLocations.add(new Vector<String>());
			}
			
			extractCompanyNames();
			
			dictionary.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void extractCompanyNames(){
		//System.out.println("Extract companies by location ...");
		String line; 
		String locationName; 
		String companyName; 
		String lastLocationName = "";
		int curLocationId = -1; 
		int totalNumCompanies = 0;
		try {
			while ((line = dictionary.readLine()) != null){
				String infos[] = line.split("  ");
				locationName = infos[0];
				if (locationName.compareTo(lastLocationName) != 0){ 	// New location
					if (locationNames.containsKey(locationName)){		// Check whether it exists 
						lastLocationName = locationName;
						curLocationId = locationNames.get(locationName); 
						companyName = infos[1].trim();
						companiesByLocations.get(curLocationId).add(companyName);
						companyCountry.put(companyName, curLocationId);
						totalNumCompanies++;
					}
				}
				else{
					companyName = infos[1].trim();
					companiesByLocations.get(curLocationId).add(companyName);
					companyCountry.put(companyName, curLocationId);
					totalNumCompanies++;
				}

			}
			
			System.out.println("Done ... " + totalNumCompanies + " companies were extracted");
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	// Check whether there is any location having no company
	public void checkCompleteness(){
		for (int i = 0; i  < locationNames.size(); i++){
			if (companiesByLocations.get(i).size() == 0){
				System.out.println("Location " + i + " has no company!");
			}
		}
		
		System.exit(-1);
	}
	
	public HashMap<String, Integer> getCompanyCountryMap() {
	    return companyCountry;
	}
	
	// Get a random company from that location
	// if that location does not have any company, go to another location
	public String getRandomCompany(int _locationId){
		String company = ""; 
		int randomCompanyIdx;
		int locationId = _locationId;
		
		if (randUnRelatedCompany.nextDouble() > probUnCorrelatedCompany){
			while (companiesByLocations.get(locationId).size() == 0){
				locationId = randUnRelatedLocation.nextInt(locationNames.size());
			}
		
			randomCompanyIdx = rand.nextInt(companiesByLocations.get(locationId).size()); 
			company = companiesByLocations.get(locationId).get(randomCompanyIdx);
			return company;
		}
		else{		// Randomly select one company out of the location
			int uncorrelateLocationIdx = randUnRelatedLocation.nextInt(locationNames.size());
			while (companiesByLocations.get(uncorrelateLocationIdx).size() == 0){
				uncorrelateLocationIdx = randUnRelatedLocation.nextInt(locationNames.size());
			}
			
			
			randomCompanyIdx = rand.nextInt(companiesByLocations.get(uncorrelateLocationIdx).size()); 
			company = companiesByLocations.get(uncorrelateLocationIdx).get(randomCompanyIdx);
			
			return company;
		}
			
	}
}
