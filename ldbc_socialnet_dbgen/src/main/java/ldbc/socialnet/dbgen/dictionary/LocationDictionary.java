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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;
import java.util.Vector;

import ldbc.socialnet.dbgen.dictionary.NamesDictionary.NameFreq;
import ldbc.socialnet.dbgen.objects.Location;
import ldbc.socialnet.dbgen.util.ZOrder;


public class LocationDictionary {

	int numberOfUsers; 
	Vector<Integer> vecLocationDistribution;	// Store the number of people in each location

	Vector<Location> vecCountry;             // Countries
	Vector<Location> vecCities;                // Cities
	Vector<Location> vecContinents;            // Continents
	Location earth;
	HashMap<Integer, Integer> cityCountry; // cityId -> countryId
	HashMap<Integer, Integer> countryContinent; //countryId -> continentId
	Vector<Vector<Integer>> vecCountryCities; // countryId -> cityId
	Vector<Vector<Integer>> vecContinentCountries; // continentIds -> countyId
	
	BufferedReader dictionary; 
	String dicCountryFile;
	String dicCityFile;
	HashMap<String, Integer> countryNameMapping; 	//Mapping from a Country location name to a id
	HashMap<String, Integer> citiesNameMapping;    //Mapping from a city location name to a id 
	HashMap<String, Integer> continentsNameMapping;    //Mapping from a continent location name to a id 
	
	boolean isCummulativeDist = false; 	// Store the vecLocationDistribution according to cumulative values
	int countNumOfSameLocation = 0; 
	int curLocationIdx = 0;
	
	LocationZorder[] sortLocation;
	
	Random rand;
	
	public LocationDictionary(int _numberOfUsers, long seed, String dicCountryFile, String dicCityFile){
        this.numberOfUsers = _numberOfUsers; 
        this.dicCountryFile = dicCountryFile;
        this.dicCityFile = dicCityFile;
        
        rand = new Random(seed);
    }
	
	public HashMap<String, Integer> getLocationNameMapping() {
		return countryNameMapping;
	}
	
	public void setLocationNameMapping(HashMap<String, Integer> locationNameMapping) {
		this.countryNameMapping = locationNameMapping;
	}
	
	public Vector<Integer> getVecLocationDistribution() {
		return vecLocationDistribution;
	}
	public void setVecLocationDistribution(Vector<Integer> vecLocationDistribution) {
		this.vecLocationDistribution = vecLocationDistribution;
	}

	public Vector<Location> getVecLocations() {
		return vecCountry;
	}

	public void setVecLocations(Vector<Location> vecLocations) {
		this.vecCountry = vecLocations;
	}

	public String getLocationName(int locationIdx){
	    if (locationIdx < 0 || locationIdx >= (vecCountry.size() + vecCities.size() + vecContinents.size() + 1))
        {
            System.out.println("Invalid locationId");
            return "";
        } else if (locationIdx < vecCountry.size()) {
            return vecCountry.get(locationIdx).getName();
        } else if (locationIdx < vecCountry.size() + vecCities.size()) {
            return vecCities.get(locationIdx - vecCountry.size()).getName();
        } else if (locationIdx < vecCountry.size() + vecCities.size() + vecContinents.size()){
            return vecContinents.get(locationIdx - vecCities.size() - vecCountry.size()).getName();
        } else {
            return earth.getName();
        }
	}
	public String getType(int locationIdx){
        if (locationIdx < 0 || locationIdx >= (vecCountry.size() + vecCities.size() + vecContinents.size() + 1))
        {
            System.out.println("Invalid locationId");
            return "";
        } else if (locationIdx < vecCountry.size()) {
            return vecCountry.get(locationIdx).getType();
        } else if (locationIdx < vecCountry.size() + vecCities.size()) {
            return vecCities.get(locationIdx - vecCountry.size()).getType();
        } else if (locationIdx < vecCountry.size() + vecCities.size() + vecContinents.size()){
            return vecContinents.get(locationIdx - vecCities.size() - vecCountry.size()).getType();
        } else {
            return earth.getType();
        }
    }
	public double getLatt(int locationIdx){
	    if (locationIdx < 0 || locationIdx >= (vecCountry.size() + vecCities.size() + vecContinents.size() + 1))
        {
            System.out.println("Invalid locationId");
            return 0;
        } else if (locationIdx < vecCountry.size()) {
            return vecCountry.get(locationIdx).getLatt();
        } else if (locationIdx < vecCountry.size() + vecCities.size()) {
            return vecCities.get(locationIdx - vecCountry.size()).getLatt();
        } else if (locationIdx < vecCountry.size() + vecCities.size() + vecContinents.size()){
            return vecContinents.get(locationIdx - vecCities.size() - vecCountry.size()).getLatt();
        } else {
            return earth.getLatt();
        }
	}
	public double getLongt(int locationIdx){
	    if (locationIdx < 0 || locationIdx >= (vecCountry.size() + vecCities.size() + vecContinents.size() + 1)) {
            System.out.println("Invalid locationId");
            return 0;
        } else if (locationIdx < vecCountry.size()) {
            return vecCountry.get(locationIdx).getLongt();
        } else if (locationIdx < vecCountry.size() + vecCities.size()) {
            return vecCities.get(locationIdx - vecCountry.size()).getLongt();
        } else if (locationIdx < vecCountry.size() + vecCities.size() + vecContinents.size()){
            return vecContinents.get(locationIdx - vecCities.size() - vecCountry.size()).getLongt();
        } else {
            return earth.getLongt();
        }
	}
	
	public String getCountryName(int countryId) {
	    if (countryId < 0 || countryId >= vecCountry.size())
        {
            System.out.println("Invalid countryId");
            return "";
        }
        
        return vecCities.get(countryId).getName();
	}
	
	public String getCityName(int cityId) {
	    if (cityId < vecCountry.size() || cityId >= (vecCountry.size() + vecCities.size()))
        {
            System.out.println("Invalid cityId");
            return "";
        }
        
        return vecCities.get(cityId - vecCountry.size()).getName();
	}
	
	public int getCityId(String cityName) {
        if (!citiesNameMapping.containsKey(cityName))
        {
            System.out.println("Invalid cityId");
            return -1;
        }
        
        return citiesNameMapping.get(cityName) + vecCountry.size();
    }
	
	public int belongsTo(int locationId) {
	    if (locationId < 0 || locationId >= (vecCountry.size() + vecCities.size())) {
	        return -1;
	    } else if (locationId < vecCountry.size() && countryContinent.containsKey(locationId)) {
	        return vecContinents.get(countryContinent.get(locationId)).getId();
	    } else if (locationId < vecCountry.size() + vecCities.size() && cityCountry.containsKey(locationId-vecCountry.size())) {
	        return vecCountry.get(cityCountry.get(locationId-vecCountry.size())).getId();
	    }
	    return earth.getId();
	}
	
	public int getRandomCity(int countryId) {
        if (countryId < 0 || countryId >= vecCountry.size())
        {
            System.out.println("Invalid countryId");
            return -1;
        }
        if (vecCountryCities.get(countryId).size() == 0)
        {
            System.out.println("Country with no known cities");
            return -1;
        }
        
        int randomNumber = rand.nextInt(vecCountryCities.get(countryId).size());
        return vecCountryCities.get(countryId).get(randomNumber) + vecCountry.size();
	}

	public int getContinent(int countryId) {
	    if (countryId < 0 || countryId >= vecCountry.size() || !countryContinent.containsKey(countryId))
	    {
	        System.err.println("Invalid countryId");
	        return -1;
	    }
	    
	    
	    return countryContinent.get(countryId) + vecCountry.size() + vecCities.size();
    }
	
	public String getContinentName(int continentId)
    {
        if (continentId < (vecCountry.size() + vecCities.size()) 
                || continentId >= vecCountry.size() + vecCities.size() + vecContinents.size())
        {
            System.err.println("Invalid continentId");
            return "";
        }
        
        return vecContinents.get(continentId - vecCountry.size() - vecCities.size()).getName();
    }
	
	public void init(){
		try {
			vecLocationDistribution = new Vector<Integer>();
			vecCountry = new Vector<Location>();
			vecCities = new Vector<Location>();
		    vecContinents = new Vector<Location>();
			countryNameMapping = new HashMap<String, Integer>();
			citiesNameMapping = new HashMap<String, Integer>();
			continentsNameMapping = new HashMap<String, Integer>();
			vecCountryCities = new Vector<Vector<Integer>>();
		    vecContinentCountries = new Vector<Vector<Integer>>();
		    countryContinent = new HashMap<Integer, Integer>();
		    cityCountry = new HashMap<Integer, Integer>();
			
		    dictionary = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(dicCountryFile), "UTF-8"));

			extractLocationsCummulative();
			
			orderByZ();
			
			dictionary.close();
			
			dictionary = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(dicCityFile), "UTF-8"));
            
            extractCities();
            
            dictionary.close();
            
            for (int i = 0; i < vecContinents.size(); i++) {
                vecContinents.get(i).setId(vecCountry.size() + vecCities.size() + i);
            }
            
            earth = new Location();
            earth.setId(vecCountry.size() + vecCities.size() + vecContinents.size());
            earth.setName("Earth");
            earth.setLatt(0);
            earth.setLongt(0);
            earth.setPopulation(7000000000L);
            earth.setType("AREA");
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void extractCities()
    {
        try {
            String line;
            while ((line = dictionary.readLine()) != null){
                String infos[] = line.split("  ");
                if (countryNameMapping.containsKey(infos[0])) {
                    Integer countryId = countryNameMapping.get(infos[0]);
                    if (!citiesNameMapping.containsKey(infos[2])) {
                        Location location = new Location(); 
                        location.setId(vecCountry.size() + vecCities.size());
                        location.setName(infos[2]);
                        location.setLatt(vecCountry.get(countryId).getLatt());
                        location.setLongt(vecCountry.get(countryId).getLongt());
                        location.setPopulation(-1);
                        location.setType(Location.CITY);

                        citiesNameMapping.put(infos[2], vecCities.size());
                        vecCities.add(location);
                        
                        Integer cityId = citiesNameMapping.get(infos[2]);
                        vecCountryCities.get(countryId).add(cityId);
                        cityCountry.put(cityId, countryId);
                    }
                } else {
                    //System.err.println("Unknown country " + infos[0] + " for city " + infos[2]);
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
	
	public void extractLocationsCummulative(){
		String locationName; 
		float cumdistribution;	//cummulative distribution value
		String line; 
		
		isCummulativeDist = true; 
		
		try {
			while ((line = dictionary.readLine()) != null){
				String infos[] = line.split(" ");
				locationName = infos[1];

				countryNameMapping.put(locationName,vecCountry.size());	
				
				Location location = new Location(); 
				location.setId(vecCountry.size());
				location.setName(locationName);
				location.setLatt(Double.parseDouble(infos[2]));
				location.setLongt(Double.parseDouble(infos[3]));
				location.setPopulation(Integer.parseInt(infos[4]));
				location.setType(Location.COUNTRY);
				
				vecCountry.add(location);
				vecCountryCities.add(new Vector<Integer>());
				
				cumdistribution = Float.parseFloat(infos[5]);
				vecLocationDistribution.add(Math.round(cumdistribution*(float)numberOfUsers));

				if (!continentsNameMapping.containsKey(infos[0])) {
				    
				    vecContinentCountries.add(new Vector<Integer>());
				    Location continent = new Location(); 
				    continent.setId(0); //Defined later
				    continent.setName(infos[0]);
				    continent.setLatt(Double.parseDouble(infos[2]));
				    continent.setLongt(Double.parseDouble(infos[3]));
				    continent.setPopulation(0);
				    continent.setType(Location.CONTINENT);

				    continentsNameMapping.put(infos[0], vecContinents.size());
				    vecContinents.add(continent);
				}

				Integer continentId = continentsNameMapping.get(infos[0]);
				Integer countryId = countryNameMapping.get(infos[1]);
				countryContinent.put(countryId, continentId);
				vecContinentCountries.get(continentId).add(countryId);
				vecContinents.get(continentId).setPopulation(vecContinents.get(continentId).getPopulation() + vecCountry.get(countryId).getPopulation());
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		System.out.println("Done ... " + vecLocationDistribution.size() + " locations were extracted");
		//Recalculate the number of people for each locations
	}
	public void extractLocations(){
		String locationName; 
		float cumdistribution;	//cummulative distribution value
		String line; 
		int total = 0;
		int lasttotal = 0; 
		
		isCummulativeDist = false; 
		
		try {
			while ((line = dictionary.readLine()) != null){
				String infos[] = line.split(" ");
				locationName = infos[1];
				cumdistribution = Integer.parseInt(infos[4]);
				
				Location location = new Location(); 
				location.setId(vecCountry.size());
				location.setName(locationName);
				location.setLatt(Double.parseDouble(infos[1]));
				location.setLongt(Double.parseDouble(infos[2]));
				location.setPopulation(Integer.parseInt(infos[3]));
				location.setType("COUNTRY");
				
				vecCountry.add(location);
				vecCountryCities.add(new Vector<Integer>());
				
				total = Math.round(cumdistribution*(float)numberOfUsers);
				vecLocationDistribution.add(total - lasttotal);
				lasttotal = total;
				
				if (!continentsNameMapping.containsKey(infos[0])) {
				    vecContinentCountries.add(new Vector<Integer>());
                    Location continent = new Location(); 
                    continent.setId(0); //Defined later
                    continent.setName(infos[0]);
                    continent.setLatt(Double.parseDouble(infos[2]));
                    continent.setLongt(Double.parseDouble(infos[3]));
                    continent.setPopulation(0);
                    continent.setType("CONTINENT");

                    continentsNameMapping.put(infos[0], vecContinents.size());
                    vecContinents.add(continent);
                }

                Integer continentId = continentsNameMapping.get(infos[0]);
                Integer countryId = countryNameMapping.get(infos[1]);
                countryContinent.put(countryId, continentId);
                vecContinentCountries.get(continentId).add(countryId);
                vecContinents.get(continentId).setPopulation(vecContinents.get(continentId).getPopulation() + vecCountry.get(countryId).getPopulation());
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		System.out.println(vecLocationDistribution.size() + " locations were extracted");
		//Recalculate the number of people for each locations
	}
	
	public int getLocation(int userIdx){
		if (isCummulativeDist){
			if (userIdx < vecLocationDistribution.get(curLocationIdx))	return curLocationIdx;
			else
			{
				curLocationIdx++;
				return curLocationIdx;
			}
					
		}
		else{
			if (countNumOfSameLocation < vecLocationDistribution.get(curLocationIdx)){
				countNumOfSameLocation++;
				return curLocationIdx;
			}
			else{
				countNumOfSameLocation = 0;
				curLocationIdx++;
				return curLocationIdx;
			}
		}
	}
	
	public void orderByZ(){
		sortLocation = new LocationZorder[vecCountry.size()];
		ZOrder zorder = new ZOrder(8);
		
		for (int i = 0; i < vecCountry.size(); i++){
			Location loc = vecCountry.get(i);
			int zvalue = zorder.getZValue(((int)Math.round(loc.getLongt()) + 180)/2, ((int)Math.round(loc.getLatt()) + 180)/2);
			sortLocation[i] = new LocationZorder(loc.getId(),zvalue);
		}
		
		Arrays.sort(sortLocation);
		
		System.out.println("Sorted location according to their z-value ");
		
		for (int i = 0; i < sortLocation.length; i ++){
			//sortLocation[i].print();
			//System.out.println(sortLocation[i].id + "   " + vecLocations.get(sortLocation[i].id).getName() + "   "  + sortLocation[i].zvalue);
		    vecCountry.get(sortLocation[i].id).setzId(i);
		}
	}
	
	public int getZorderID(int _locationId){
		return vecCountry.get(_locationId).getzId();
	}
	public int getLocationIdFromZOrder(int _zOrderId){
		return sortLocation[_zOrderId].id;
	}
	
	class LocationZorder implements Comparable{
		int id;
		int zvalue; 
		public LocationZorder(int _id, int _zvalue){
			this.id = _id; 
			this.zvalue = _zvalue; 
		}
		public int compareTo(Object obj)
		{
			LocationZorder tmp = (LocationZorder)obj;
			if(this.zvalue < tmp.zvalue)
			{	
				return -1;
			}
			else if(this.zvalue > tmp.zvalue)
			{
				return 1;
			}
			return 0;
		}
		public void print(){
			System.out.println(id + "  " + zvalue);
		}
	}
}
