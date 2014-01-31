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

import ldbc.socialnet.dbgen.objects.PopularPlace;


public class PopularPlacesDictionary {
    
    String dicFileName;

    LocationDictionary locationDic;
	HashMap<Integer, Vector<PopularPlace>> popularPlacesByLocations;
	
	Random rand;

	public PopularPlacesDictionary(String dicFileName, LocationDictionary locationDic, 
			long seedRandom){
		
		this.dicFileName = dicFileName; 
		this.locationDic = locationDic; 
		
		rand = new Random(seedRandom);
	}
	
	public void init(){

	    popularPlacesByLocations = new HashMap<Integer, Vector<PopularPlace>>();
	    for (Integer id : locationDic.getCountries()) {
	        popularPlacesByLocations.put(id, new Vector<PopularPlace>());
	    }

	    extractPopularPlaces();
	}
	
	public void extractPopularPlaces(){
		String line; 
		String locationName;
		String lastLocationName = "";
		int curLocationId = -1; 
		int totalNumPopularPlaces = 0;
		
		String label;
		try {
		    BufferedReader dicPopularPlace = new BufferedReader(
		            new InputStreamReader(getClass().getResourceAsStream(dicFileName), "UTF-8"));
		    
			while ((line = dicPopularPlace.readLine()) != null) {
			    double latt;
		        double longt;
				String infos[] = line.split("  ");
				locationName = infos[0];
				if (locationName.compareTo(lastLocationName) != 0) {
					if (locationDic.getCountryId(locationName) != LocationDictionary.INVALID_LOCATION) {
						lastLocationName = locationName;
						curLocationId = locationDic.getCountryId(locationName); 
						label = infos[2];
						latt = Double.parseDouble(infos[3]);
						longt = Double.parseDouble(infos[4]);
						popularPlacesByLocations.get(curLocationId).add(new PopularPlace(label, latt, longt));
						totalNumPopularPlaces++;
					}
				} else {
					label = infos[2];
					latt = Double.parseDouble(infos[3]);
					longt = Double.parseDouble(infos[4]);
					popularPlacesByLocations.get(curLocationId).add(new PopularPlace(label, latt, longt));
					totalNumPopularPlaces++;
				}
			}
			dicPopularPlace.close();
			System.out.println("Done ... " + totalNumPopularPlaces + " popular places were extracted");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public short getPopularPlace(int locationidx) {
		if (popularPlacesByLocations.get(locationidx).size() == 0) {
		    return -1;
		}
		
		return (short) rand.nextInt(popularPlacesByLocations.get(locationidx).size());
	}
	
	public PopularPlace getPopularPlace(int locationIdx, int placeId){
		return popularPlacesByLocations.get(locationIdx).get(placeId);
	}
}
