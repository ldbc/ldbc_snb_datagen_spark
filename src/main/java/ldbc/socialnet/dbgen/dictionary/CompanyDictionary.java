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

import ldbc.socialnet.dbgen.util.RandomGeneratorFarm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeMap;

/**
 * This class reads the file containing the names and countries for the companies used in the ldbc socialnet generation and 
 * provides access methods to get such data.
 */
public class CompanyDictionary {
    
    private static final String SEPARATOR = "  ";
    private TreeMap<Long, String> companyName;                /**< @brief A map containing the name of each company.**/
	private TreeMap<Long, Integer>              companyCountry;             /**< @brief A map containing the location of each company. **/
	private TreeMap<Integer, ArrayList<Long>>   companiesByCountry;         /**< @brief A map containing the companies of each country. **/
	private LocationDictionary                  locationDictionary;         /**< @brief The location dictionary.**/
    private double                              probUnCorrelatedCompany;    /**< @brief The probability of working in a uncorrelated company.**/
	
    /**
     * @brief   Constructor.
     * @param   probUnCorrelatedCompany: Probability of selecting a country unrelated company.
     */
	public CompanyDictionary(LocationDictionary locationDictionary,
	         double probUnCorrelatedCompany) {
	    
		this.locationDictionary = locationDictionary;
		this.probUnCorrelatedCompany = probUnCorrelatedCompany;
        this.companyName = new TreeMap<Long,String>();
        this.companyCountry = new TreeMap<Long, Integer>();
        this.companiesByCountry = new TreeMap<Integer, ArrayList<Long>>();
        for (Integer id : locationDictionary.getCountries()){
            this.companiesByCountry.put(id, new ArrayList<Long>());
        }
	}

	/**
     * @brief   Loads the company dictionary file.
     * @param   fileName The file to load.
     */
	public void load( String fileName ) {
	    try {
            BufferedReader dictionary = new BufferedReader(
                    new InputStreamReader(getClass( ).getResourceAsStream(fileName), "UTF-8"));
            String line;  
            int currentId = -1;
            long totalNumCompanies = 0;
            while ((line = dictionary.readLine()) != null) {
                String data[] = line.split(SEPARATOR);
                String locationName = data[0];
                String companyName  = data[1].trim();
                if (locationDictionary.getCountryId(locationName) != LocationDictionary.INVALID_LOCATION) {
                    currentId  = locationDictionary.getCountryId(locationName);
                    companiesByCountry.get(currentId).add(totalNumCompanies);
                    companyCountry.put(totalNumCompanies, currentId);
                    this.companyName.put(totalNumCompanies, companyName);
                    totalNumCompanies++;
                }
            }
            dictionary.close();
            System.out.println("Done ... " + totalNumCompanies + " companies were extracted");
            
        } catch (IOException e) {
            e.printStackTrace();
        }
	}

	/**
	 * @brief   Gets the company country id.
     * @param   company The company identifier.
     * @return  The id of the country.
	 */
	public int getCountry(Long company) {
	    return companyCountry.get(company);
	}
	
	/**
	 * @brief   Gets a random company of the input country. In case the given country doesn't any company
	 *          a random one will be selected.
     * @param   randomFarm The random farm used to get random numbers.
	 * @param   countryId: A country id.
     * @return  The id of the company.
	 */
	public long getRandomCompany(RandomGeneratorFarm randomFarm, int countryId) {
		int locId = countryId;
		ArrayList<Integer> countries = locationDictionary.getCountries();
		if (randomFarm.get(RandomGeneratorFarm.Aspect.UNCORRELATED_COMPANY).nextDouble() <= probUnCorrelatedCompany) {
		    locId = countries.get(randomFarm.get(RandomGeneratorFarm.Aspect.UNCORRELATED_COMPANY_LOCATION).nextInt(countries.size()));
		}
		// In case the country doesn't have any company select another country.
		while (companiesByCountry.get(locId).size() == 0){
		    locId = countries.get(randomFarm.get(RandomGeneratorFarm.Aspect.UNCORRELATED_COMPANY_LOCATION).nextInt(countries.size()));
        }
		int randomCompanyIdx = randomFarm.get(RandomGeneratorFarm.Aspect.COMPANY).nextInt(companiesByCountry.get(locId).size());
		return companiesByCountry.get(locId).get(randomCompanyIdx);
	}


    /**
     * @brief   Gets the set of of companies in the dictionary.
     * @return  The set of companies.
     */
    public Set<Long> getCompanies() {
        return companyCountry.keySet();
    }

    /**
     * @brief   Gets the number of companies in the dictionary.
     * @return  The number of companies.
     */
    public int getNumCompanies() {
        return companyName.size();
    }

    /**
     * @brief   Gets the name of a company.
     * @param   id The company's id.
     * @return  The name of the company.
     */
    public String getCompanyName( long id ) {
        return companyName.get(id);
    }
}
