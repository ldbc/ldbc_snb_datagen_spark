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
import java.util.*;

import ldbc.socialnet.dbgen.objects.Location;
import ldbc.socialnet.dbgen.util.ZOrder;

/**
 * This class reads the files containing the country data and city data used in the ldbc socialnet generation and 
 * provides access methods to get such data.
 * Most of the users has the prerequisite of requiring a valid location id.
 */
public class LocationDictionary {

    public static final int INVALID_LOCATION = -1;
    private static final String SEPARATOR = " ";
    private static final String SEPARATOR_CITY = " ";
    
	int numUsers;
	int curLocationIdx;
	
	LocationZorder[] sortLocation;
	Vector<Integer> locationDistribution;
    
	String cityFile;
	String countryFile;    

	Vector<Integer> countries;
    HashMap<Integer, Location> locations;
    HashMap<Integer, Integer>  isPartOf;
	HashMap<Integer, Vector<Integer>> citiesFromCountry;
	
	HashMap<String, Integer> cityNames;
	HashMap<String, Integer> countryNames;
	
	/**
	 * Private class used to sort countries by their z-order value.
	 */
    private class LocationZorder implements Comparable<LocationZorder> {
        
        public int id;
        public Integer zvalue; 
        
        public LocationZorder(int id, int zvalue) {
            this.id = id; 
            this.zvalue = zvalue; 
        }
        
        public int compareTo(LocationZorder obj) {
            return zvalue.compareTo(obj.zvalue);
        }
    }
	
    /**
     * Creator.
     * 
     * @param numUsers: The total number of users.
     * @param seed: The random selector seed.
     * @param countryFile: The country and continent data file.
     * @param cityFile: The city data file.
     */
	public LocationDictionary(int numUsers, String countryFile, String cityFile){
        this.numUsers = numUsers; 
        this.countryFile = countryFile;
        this.cityFile = cityFile;
    }

    public Set<Integer> getLocations() {
        return locations.keySet();
    }
	
	/**
	 * Gets a list of the country ids.
	 */
	public Vector<Integer> getCountries() {
		return new Vector<Integer>(countries);
	}

	/**
	 * Given a location id returns the name of said location.
	 */
	public String getLocationName(int locationId) {
	    return locations.get(locationId).getName();
	}
	
	/**
     * Given a location id returns the population of said location.
     */
	public Long getPopulation(int locationId) {
	    return locations.get(locationId).getPopulation();
    }
	
	/**
     * Given a location id returns the Type ({@link ldbc.socialnet.dbgen.objects.Location#CITY} |
     * {@link ldbc.socialnet.dbgen.objects.Location#COUNTRY} | {@link ldbc.socialnet.dbgen.objects.Location#CONTINENT} |
     * {@link ldbc.socialnet.dbgen.objects.Location#AREA}) of said location.
     */
	public String getType(int locationId) {
	    return locations.get(locationId).getType();
    }
	
	/**
     * Given a location id returns the latitude of said location.
     */
	public double getLatt(int locationId) {
        return locations.get(locationId).getLatt();
	}
	
	/**
     * Given a location id returns the longitude of said location.
     */
	public double getLongt(int locationId) {
        return locations.get(locationId).getLongt();
	}
	
	/**
     * Given a city name returns the id of the city or {{@link #INVALID_LOCATION} if it does not exist.
     */
	public int getCityId(String cityName) { 
	    if (!cityNames.containsKey(cityName)) {
            return INVALID_LOCATION;
        }
        return cityNames.get(cityName);
    }
	
	/**
     * Given a country name returns the id of the country or {{@link #INVALID_LOCATION} if it does not exist.
     */
	public int getCountryId(String countryName) {
	    if (!countryNames.containsKey(countryName)) {
	        return INVALID_LOCATION;
	    }
        return countryNames.get(countryName);
    }
	
	/**
     * Given a location id returns the id of the location which the input is part of or 
     * {{@link #INVALID_LOCATION} if it does not exist any.
     */
	public int belongsTo(int locationId) {
	    if (!isPartOf.containsKey(locationId)) {
	        return INVALID_LOCATION;
	    }
	    return isPartOf.get(locationId);
	}
	
	/**
     * Given a country id returns an id of one of its cities.
     */
	public int getRandomCity(Random random, int countryId) {
	    if (!citiesFromCountry.containsKey(countryId)) {
            System.err.println("Invalid countryId");
            return INVALID_LOCATION;
        }
	    
        if (citiesFromCountry.get(countryId).size() == 0) {
            Location location = locations.get(countryId);
            String countryName = location.getName(); 
            System.err.println("Country with no known cities: "+countryName);
            return INVALID_LOCATION;
        }
        
        int randomNumber = random.nextInt(citiesFromCountry.get(countryId).size());
        return citiesFromCountry.get(countryId).get(randomNumber);
	}
	
	/**
	 * Initializes the dictionary.
	 */
	public void init() {
	    curLocationIdx = 0;
	    locationDistribution = new Vector<Integer>();
	    countryNames = new HashMap<String, Integer>();
	    cityNames = new HashMap<String, Integer>();
	    locations = new HashMap<Integer, Location>();
	    isPartOf = new HashMap<Integer, Integer>();
	    countries = new Vector<Integer>();
	    citiesFromCountry = new HashMap<Integer, Vector<Integer>>();

	    readCountries();
	    orderByZ();
	    readCities();
	    readContinents();
	}
	
	/**
	 * Reads the city data from the file.
	 * It only stores the cities which belonging to a known country.
	 */
	private void readCities() {
        try {
            BufferedReader dictionary = new BufferedReader(
                    new InputStreamReader(getClass( ).getResourceAsStream(cityFile), "UTF-8"));
            
            int cities = 0;
            String line;
            while ((line = dictionary.readLine()) != null){
                String data[] = line.split(SEPARATOR_CITY);
//                System.err.println(data[0]);
                if (countryNames.containsKey(data[0])) {
                    Integer countryId = countryNames.get(data[0]);
                    if (!cityNames.containsKey(data[1])) {
                        Location location = new Location(); 
                        location.setId(locations.size());
                        location.setName(data[1]);
                        location.setLatt(locations.get(countryId).getLatt());
                        location.setLongt(locations.get(countryId).getLongt());
                        location.setPopulation(-1);
                        location.setType(Location.CITY);

                        locations.put(location.getId(), location);
                        isPartOf.put(location.getId(), countryId);
                        citiesFromCountry.get(countryId).add(location.getId());
                        
                        cityNames.put(data[1], location.getId());
                        
                        cities++;
                    }
                }
            }
            dictionary.close();
            System.out.println("Done ... " + cities + " cities were extracted");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

	/**
     * Reads the countries data from the file.
     */
	private void readCountries() {
	    try {
	        BufferedReader dictionary = new BufferedReader(
	                new InputStreamReader(getClass( ).getResourceAsStream(countryFile), "UTF-8"));

	        String line;
	        while ((line = dictionary.readLine()) != null){
	            String data[] = line.split(SEPARATOR);
	            String locationName = data[1];

	            Location location = new Location(); 
	            location.setId(locations.size());
	            location.setName(locationName);
	            location.setLatt(Double.parseDouble(data[2]));
	            location.setLongt(Double.parseDouble(data[3]));
	            location.setPopulation(Integer.parseInt(data[4]));
	            location.setType(Location.COUNTRY);

	            locations.put(location.getId(), location);
	            countryNames.put(locationName, location.getId());    
	            float cummulativeDistribution = Float.parseFloat(data[5]);
	            locationDistribution.add(Math.round(cummulativeDistribution * (float)numUsers));
                countries.add(location.getId());
	            
	            citiesFromCountry.put(location.getId(), new Vector<Integer>());
	        }
	        dictionary.close();
	        System.out.println("Done ... " + countries.size() + " countries were extracted");
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
	}

	/**
     * Reads the continent data from the file and links a country to a continent.
     */
	private void readContinents() {
        HashMap<String, Integer> treatedContinents = new HashMap<String, Integer>();
        try {
            BufferedReader dictionary = new BufferedReader(
                    new InputStreamReader(getClass( ).getResourceAsStream(countryFile), "UTF-8"));
            
            String line;
            while ((line = dictionary.readLine()) != null){
                String data[] = line.split(SEPARATOR);
                String locationName = data[1];

                int countryId = countryNames.get(locationName);

                if (!treatedContinents.containsKey(data[0])) {
                    
                    Location continent = new Location(); 
                    continent.setId(locations.size());
                    continent.setName(data[0]);
                    continent.setLatt(Double.parseDouble(data[2]));
                    continent.setLongt(Double.parseDouble(data[3]));
                    continent.setPopulation(0);
                    continent.setType(Location.CONTINENT);

                    locations.put(continent.getId(), continent);
                    treatedContinents.put(data[0], continent.getId());
                }
                Integer continentId = treatedContinents.get(data[0]);
                long population = locations.get(continentId).getPopulation() + locations.get(countryId).getPopulation();
                locations.get(continentId).setPopulation(population);
                isPartOf.put(countryId, continentId);
            }
            dictionary.close();
            System.out.println("Done ... " + treatedContinents.size() + " continents were extracted");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
	
	/**
	 * Gets a country id based on population.
	 * This method is assumed to be called in an ascending order of user ID.
	 */
	public int getLocation(int userId) {
	    if (userId >= locationDistribution.get(curLocationIdx)) {
	        curLocationIdx++;
	    }
	    return countries.get(curLocationIdx);
	}


    public void advanceToUser(int user) {
        curLocationIdx=0;
        for(int i = 0; i < user-1; ++i){
            getLocation(i);
        } 
    }
	
	/**
	 * Sorts countries by its z-order value.
	 */
	private void orderByZ() {
	    ZOrder zorder = new ZOrder(8);
		sortLocation = new LocationZorder[countries.size()];
		
		for (int i = 0; i < countries.size(); i++) {
			Location loc = locations.get(countries.get(i));
			int zvalue = zorder.getZValue(((int)Math.round(loc.getLongt()) + 180)/2, ((int)Math.round(loc.getLatt()) + 180)/2);
			sortLocation[i] = new LocationZorder(loc.getId(), zvalue);
		}
		
		Arrays.sort(sortLocation);
		System.out.println("Sorted countries according to their z-value");
		
		for (int i = 0; i < sortLocation.length; i++) {
		    locations.get(sortLocation[i].id).setzId(i);
		}
	}
	
	/**
	 * Gets the z-order ID from the given country.
	 */
	public int getZorderID(int locationId) {
		return locations.get(locationId).getzId();
	}
	
	/**
     * Gets country id from the given z-order ID.
     */
	public int getLocationIdFromZOrder(int zOrderId) {
		return sortLocation[zOrderId].id;
	}
}
