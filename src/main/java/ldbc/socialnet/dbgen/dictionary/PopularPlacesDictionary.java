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
import java.util.ArrayList;

import ldbc.socialnet.dbgen.objects.PopularPlace;


public class PopularPlacesDictionary {
    
    private LocationDictionary                          locationDictionary;         /**< @brief The location dictionary. **/
	private HashMap<Integer, ArrayList<PopularPlace>>   popularPlacesByCountry;   /**< @brief The popular places by country .**/

    /**
     * @brief   Constructor
     * @param   locationDic The location dictionary.
     */
	public PopularPlacesDictionary(LocationDictionary locationDic){
		this.locationDictionary = locationDic;
        this.popularPlacesByCountry = new HashMap<Integer, ArrayList<PopularPlace>>();
        for (Integer id : locationDictionary.getCountries()) {
            this.popularPlacesByCountry.put(id, new ArrayList<PopularPlace>());
        }
	}

    /**
     * @brief   Loads a popular places file.
     * @param   fileName The popular places file name.
     */
	public void load( String fileName ){
		String line; 
		String locationName;
		String lastLocationName = "";
		int curLocationId = -1; 
		int totalNumPopularPlaces = 0;
		
		String label;
		try {
		    BufferedReader dicPopularPlace = new BufferedReader(
		            new InputStreamReader(getClass().getResourceAsStream(fileName), "UTF-8"));
		    
			while ((line = dicPopularPlace.readLine()) != null) {
			    double latt;
		        double longt;
				String infos[] = line.split("  ");
				locationName = infos[0];
				if (locationName.compareTo(lastLocationName) != 0) {
					if (locationDictionary.getCountryId(locationName) != LocationDictionary.INVALID_LOCATION) {
						lastLocationName = locationName;
						curLocationId = locationDictionary.getCountryId(locationName);
						label = infos[2];
						latt = Double.parseDouble(infos[3]);
						longt = Double.parseDouble(infos[4]);
						popularPlacesByCountry.get(curLocationId).add(new PopularPlace(label, latt, longt));
						totalNumPopularPlaces++;
					}
				} else {
					label = infos[2];
					latt = Double.parseDouble(infos[3]);
					longt = Double.parseDouble(infos[4]);
					popularPlacesByCountry.get(curLocationId).add(new PopularPlace(label, latt, longt));
					totalNumPopularPlaces++;
				}
			}
			dicPopularPlace.close();
			System.out.println("Done ... " + totalNumPopularPlaces + " popular places were extracted");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

    /**
     * @brief   Gets the popular places of a country.
     * @param   random The random number generator.
     * @param   countryId the locationid
     * @return  The popular place identifier.
     */
	public short getPopularPlace(Random random, int countryId) {
		if (popularPlacesByCountry.get(countryId).size() == 0) {
		    return -1;
		}
		return (short) random.nextInt(popularPlacesByCountry.get(countryId).size());
	}

    /**
     * @brief   Gets a popular place.
     * @param   countryId   the id of the country.
     * @param   placeId The popular place id.
     * @return  The popular place.
     */
	public PopularPlace getPopularPlace(int countryId, int placeId){
		return popularPlacesByCountry.get(countryId).get(placeId);
	}
}
