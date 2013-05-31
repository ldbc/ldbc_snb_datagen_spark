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

import ldbc.socialnet.dbgen.objects.PopularPlace;


public class PopularPlacesDictionary {

    BufferedReader dicPopularPlace; 
	String dicFileName;

	HashMap<String, Integer> locationNames;
	
	Vector<Vector<PopularPlace>> popularPlacesByLocations;		//Popular places in each country
	Random 		randPopularPlaceId;
	
	int numLocations; 

	public PopularPlacesDictionary(String _dicFileName, HashMap<String, Integer> _locationNames, 
			long seedRandom){
		
		this.dicFileName = _dicFileName; 
		this.locationNames = _locationNames; 
		this.randPopularPlaceId = new Random(seedRandom);
	}
	public void init(){
		try {
			dicPopularPlace = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(dicFileName), "UTF-8"));
			
			System.out.println("Building dictionary of popular places (by countries)");
			
			numLocations = locationNames.size(); 
			
			popularPlacesByLocations = new Vector<Vector<PopularPlace>>(numLocations);
			for (int i = 0; i < locationNames.size(); i++){
				popularPlacesByLocations.add(new Vector<PopularPlace>());
			}
			
			//removePopularPlacesDuplication(); // Run only one time
			
			extractPopularPlaces();
			
			//checkCompleteness();
			
			dicPopularPlace.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void extractPopularPlaces(){
		//System.out.println("Extract organizations by location ...");
		String line; 
		String locationName; 
		String popularPlaceName; 
		String lastLocationName = "";
		int curLocationId = -1; 
		int totalNumPopularPlaces = 0;
		
		String label; 
		double latt; 
		double longt; 
		try {
			while ((line = dicPopularPlace.readLine()) != null){
					
				String infos[] = line.split("  ");	//country  Name  Label  Lat  Long
				locationName = infos[0];
				//System.out.println("Line in names = " + line); 
				if (locationName.compareTo(lastLocationName) != 0){ 	// New location
					if (locationNames.containsKey(locationName)){		// Check whether it exists
						lastLocationName = locationName;
						curLocationId = locationNames.get(locationName); 
						popularPlaceName = infos[1];
						label = infos[2];
						latt = Double.parseDouble(infos[3]);
						longt = Double.parseDouble(infos[4]);
						popularPlacesByLocations.get(curLocationId).add(new PopularPlace(label, latt, longt));
						
						totalNumPopularPlaces++;
					}
						
				}
				else{
					popularPlaceName = infos[1];
					label = infos[2];
					latt = Double.parseDouble(infos[3]);
					longt = Double.parseDouble(infos[4]);
					popularPlacesByLocations.get(curLocationId).add(new PopularPlace(label, latt, longt));
					totalNumPopularPlaces++;
				}

			}
			
			System.out.println("Done ... " + totalNumPopularPlaces + " popular places were extracted");
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	// Check whether there is any location having no institute
	public void checkCompleteness(){
		for (int i = 0; i  < locationNames.size(); i++){
			if (popularPlacesByLocations.get(i).size() == 0){
				System.out.println("Location " + i + " has no popular place!");
			}
		}
		
		System.exit(-1);
	}
	
	public short getPopularPlace(int locationidx){
		if (popularPlacesByLocations.get(locationidx).size() == 0) return -1;
		
		return (short) randPopularPlaceId.nextInt(popularPlacesByLocations.get(locationidx).size());
	}
	
	
	public int getPopularPlaceNoCheck(int locationIdx){
		return randPopularPlaceId.nextInt(popularPlacesByLocations.get(locationIdx).size()); 
	}
	
	public PopularPlace getPopularPlace(int locationIdx, int placeId){
		return popularPlacesByLocations.get(locationIdx).get(placeId);
	}
	public int getNumPopularPlaces(int locationIdx){
		return popularPlacesByLocations.get(locationIdx).size();
	}
	
	public int getNumLocations() {
		return numLocations;
	}
	public void setNumLocations(int numLocations) {
		this.numLocations = numLocations;
	}
	
	public void removePopularPlacesDuplication(){
		//System.out.println("Extract organizations by location ...");
		String dicPopularPlacesOriginal = "/export/scratch1/duc/work/virtuosoServer/virtuosoOPS/var/lib/virtuoso/db/popularPlacesByCountry.txt.original";
		String dicPopularPlaces = "/export/scratch1/duc/work/virtuosoServer/virtuosoOPS/var/lib/virtuoso/db/popularPlacesByCountry.txt";

		String line; 
		String locationName; 
		String popularPlaceName; 
		String lastLocationName = "";
		int curLocationId = -1; 
		int totalNumPopularPlaces = 0;
		String lastAddedPopularName = "";
		
		
		try {
			FileOutputStream 	dicPopularPlaceFile;
			dicPopularPlaceFile = new FileOutputStream(dicPopularPlaces);
			OutputStreamWriter writer; 
			writer = new OutputStreamWriter(dicPopularPlaceFile);
			
			BufferedReader dicPopularPlace = new BufferedReader(new InputStreamReader(new FileInputStream(dicPopularPlacesOriginal), "UTF-8"));

			while ((line = dicPopularPlace.readLine()) != null){
					
				String infos[] = line.split("  ");	//country  Name  Label  Lat  Long
				locationName = infos[0];
				//System.out.println("Line in names = " + line); 
				if (locationName.compareTo(lastLocationName) != 0){ 	// New location
					if (locationNames.containsKey(locationName)){		// Check whether it exists
						lastLocationName = locationName;
						curLocationId = locationNames.get(locationName); 
						popularPlaceName = infos[1].trim();
						if (popularPlaceName.compareTo(lastAddedPopularName) != 0){
							writer.write(line + "\n");
							lastAddedPopularName = popularPlaceName;
							totalNumPopularPlaces++;
						}
					}
						
				}
				else{
					popularPlaceName = infos[1].trim();
					if (popularPlaceName.compareTo(lastAddedPopularName) != 0){
						writer.write(line + "\n");
						lastAddedPopularName = popularPlaceName;
						totalNumPopularPlaces++;
					}
					
				}

			}
			
			writer.close();
			
			System.out.println("Done ... " + totalNumPopularPlaces + " organizations were extracted");
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
