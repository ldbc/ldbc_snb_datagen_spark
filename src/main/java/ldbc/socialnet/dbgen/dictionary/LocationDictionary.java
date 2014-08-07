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
    
	private int                 numUsers;
	private int                 curLocationIdx;
	private LocationZorder[]    sortedLocation;
	private ArrayList<Integer>  locationDistribution;
    private ArrayList<Float>   cumulativeDistribution;

	private     ArrayList<Integer>                      countries;          /**< @brief The set of countries. **/
    private     HashMap<Integer, Location>              locations;          /**< @brief The locations by id. **/
    private     HashMap<Integer, Integer>               isPartOf;           /**< @brief The location hierarchy. **/
	private     HashMap<Integer, ArrayList<Integer>>    citiesByCountry;    /**< @brief The cities by country. **/
	private     HashMap<String, Integer>                cityNames;          /**< @brief The city names. **/
    private 	HashMap<String, Integer>                countryNames;       /**< @brief The country names. **/
	
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
     * @brief   Creator.
     * @param   numUsers: The total number of users.
     */
	public LocationDictionary( int numUsers ){
        this.numUsers = numUsers;
        this.curLocationIdx = 0;
        this.locationDistribution = new ArrayList<Integer>();
        this.cumulativeDistribution = new ArrayList<Float>();
        this.countryNames = new HashMap<String, Integer>();
        this.cityNames = new HashMap<String, Integer>();
        this.locations = new HashMap<Integer, Location>();
        this.isPartOf = new HashMap<Integer, Integer>();
        this.countries = new ArrayList<Integer>();
        this.citiesByCountry = new HashMap<Integer, ArrayList<Integer>>();
    }

    /**
     * @brief   Gets the set of locations.
     * @return  The set of locations.
     */
    public Set<Integer> getLocations() {
        return locations.keySet();
    }
	
	/**
	 * @brief Gets a list of the country ids.
     * @return The set of countries
	 */
	public ArrayList<Integer> getCountries() {
		return new ArrayList<Integer>(countries);
	}

	/**
	 * @brief   Given a location id returns the name of said location.
     * @param   locationId Gets the name of a location.
     * @return  The name of the location.
	 */
	public String getLocationName(int locationId) {
	    return locations.get(locationId).getName();
	}
	
	/**
     * @brief   Given a location id returns the population of said location.
     * @param   locationId The location identifier.
     * @return  Population of that location.
     */
	public Long getPopulation(int locationId) {
	    return locations.get(locationId).getPopulation();
    }

    /**
     * @brief   Gets the type of a location.
     * @param   locationId The location identifier.
     * @return  The type of the location.
     */
	public String getType(int locationId) {
	    return locations.get(locationId).getType();
    }

    /**
     * @brief   Gets the lattitude of a location.
     * @param   locationId The location identifier.
     * @return  The lattitude of the location.
     */
	public double getLatt(int locationId) {
        return locations.get(locationId).getLatt();
	}

    /**
     * @brief   Gets the longitude of a location.
     * @param   locationId The location identifier.
     * @return  The longitude of the location.
     */
	public double getLongt(int locationId) {
        return locations.get(locationId).getLongt();
	}

    /**
     * @brief   Gets The identifier of a city.
     * @param   cityName The name of the city.
     * @return  The identifier of the city.
     */
	public int getCityId(String cityName) {
	    if (!cityNames.containsKey(cityName)) {
            return INVALID_LOCATION;
        }
        return cityNames.get(cityName);
    }

    /**
     * @brief   Gets the identifier of a country.
     * @param   countryName The name of the country.
     * @return  The identifier ot the country.
     */
	public int getCountryId(String countryName) {
	    if (!countryNames.containsKey(countryName)) {
	        return INVALID_LOCATION;
	    }
        return countryNames.get(countryName);
    }

    /**
     * @brief   Gets the parent of a location in the location hierarchy.
     * @param   locationId The location identifier.
     * @return  The parent location identifier.
     */
	public int belongsTo(int locationId) {
	    if (!isPartOf.containsKey(locationId)) {
	        return INVALID_LOCATION;
	    }
	    return isPartOf.get(locationId);
	}

    /**
     * @brief   Gets a random city from a country.
     * @param   random The random  number generator.
     * @param   countryId The country Identifier.
     * @return  The city identifier.
     */
	public int getRandomCity(Random random, int countryId) {
	    if (!citiesByCountry.containsKey(countryId)) {
            System.err.println("Invalid countryId");
            return INVALID_LOCATION;
        }
        if (citiesByCountry.get(countryId).size() == 0) {
            Location location = locations.get(countryId);
            String countryName = location.getName(); 
            System.err.println("Country with no known cities: "+countryName);
            return INVALID_LOCATION;
        }
        
        int randomNumber = random.nextInt(citiesByCountry.get(countryId).size());
        return citiesByCountry.get(countryId).get(randomNumber);
	}

    /**
     * @brief   Loads the dictionary files.
     * @param   citiesFileName The cities file name.
     * @param countriesFileName The countries file name.
     */
	public void load( String citiesFileName, String countriesFileName ) {

	    readCountries( countriesFileName );
	    orderByZ();
	    readCities( citiesFileName );
	    readContinents( countriesFileName );
	}

    /**
     * @brief   Reads a cities file name.
     * @param   fileName The cities file name to read.
     */
	private void readCities( String fileName ) {
        try {
            BufferedReader dictionary = new BufferedReader(
                    new InputStreamReader(getClass( ).getResourceAsStream(fileName), "UTF-8"));
            
            int cities = 0;
            String line;
            while ((line = dictionary.readLine()) != null){
                String data[] = line.split(SEPARATOR_CITY);
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
                        citiesByCountry.get(countryId).add(location.getId());
                        
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
     * @brief   Reads a countries file.
     * @param   fileName The countries file name.
     */
	private void readCountries( String fileName ) {
	    try {
	        BufferedReader dictionary = new BufferedReader(
	                new InputStreamReader(getClass( ).getResourceAsStream( fileName ), "UTF-8"));

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
	            float dist = Float.parseFloat(data[5]);
                cumulativeDistribution.add(dist);
	            locationDistribution.add(Math.round(dist * (float)numUsers));
                countries.add(location.getId());
	            
	            citiesByCountry.put(location.getId(), new ArrayList<Integer>());
	        }
	        dictionary.close();
	        System.out.println("Done ... " + countries.size() + " countries were extracted");
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
	}

    /**
     * @brief Reads a continents file name.
     * @param fileName The continents file name.
     */
	private void readContinents( String fileName ) {
        HashMap<String, Integer> treatedContinents = new HashMap<String, Integer>();
        try {
            BufferedReader dictionary = new BufferedReader(
                    new InputStreamReader(getClass( ).getResourceAsStream(fileName), "UTF-8"));
            
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
     * @brief   Gets a country for a user.
     * @param   random The random number generator.
     * @return  The country for the user.
     */
	public int getCountryForUser( Random random) {
        float prob = random.nextFloat();
        int minIdx = 0;
        int maxIdx = cumulativeDistribution.size();
        // Binary search
        while ((maxIdx - minIdx) > 1) {
            int middlePoint = minIdx + (maxIdx - minIdx) / 2;
            if (prob > cumulativeDistribution.get(middlePoint)) {
                minIdx = middlePoint;
            } else {
                maxIdx = middlePoint;
            }
        }
        return maxIdx;
	    /*if (userId >= locationDistribution.get(curLocationIdx)) {
	        curLocationIdx++;
	    }
	    return countries.get(curLocationIdx);
	    */
	}

    public void advanceToUser(int user) {
/*        curLocationIdx=0;
        for(int i = 0; i < user-1; ++i){
            getCountryForUser(i);
        }
        */
    }

    /**
     * @brief Sorts locations by Z order.
     */
	private void orderByZ() {
	    ZOrder zorder = new ZOrder(8);
		sortedLocation = new LocationZorder[countries.size()];
		
		for (int i = 0; i < countries.size(); i++) {
			Location loc = locations.get(countries.get(i));
			int zvalue = zorder.getZValue(((int)Math.round(loc.getLongt()) + 180)/2, ((int)Math.round(loc.getLatt()) + 180)/2);
			sortedLocation[i] = new LocationZorder(loc.getId(), zvalue);
		}
		
		Arrays.sort(sortedLocation);
		System.out.println("Sorted countries according to their z-value");
		
		for (int i = 0; i < sortedLocation.length; i++) {
		    locations.get(sortedLocation[i].id).setzId(i);
		}
	}

    /**
     * @brief   Gets the z order of a location.
     * @param   locationId The location identifier.
     * @return  The z order of the location.
     */
	public int getZorderID(int locationId) {
		return locations.get(locationId).getzId();
	}

    /**
     * @brief   Gets the location identifier from a z order.
     * @param   zOrderId the z order.
     * @return  The location identifier.
     */
	public int getLocationIdFromZOrder(int zOrderId) {
		return sortedLocation[zOrderId].id;
	}

    /**
     * @brief   Gets a location from its identifier.
     * @param   id The location identifier.
     * @return  The location whose identifier is id.
     */
    public Location getLocation( int id ) {
        return locations.get(id);
    }
}
