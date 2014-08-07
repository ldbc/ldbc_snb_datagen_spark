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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Vector;


public class LanguageDictionary {

    private static final String SEPARATOR = "  "; 
    private static final String ISO_ENGLISH_CODE = "en"; 
    
    private ArrayList<String>                       languages;                      /**< @brief The array of languages. **/
    private HashMap<Integer, ArrayList<Integer>>    officalLanguagesByCountry;      /**< @brief The official languages by country. **/
    private HashMap<Integer, ArrayList<Integer>>    languagesByCountry;             /**< @brief The languages by country. **/
    private LocationDictionary                      locationDictionary;             /**< @brief The location dictionary. **/
	private double                                  probEnglish;                    /**< @brief The probability to speak english. **/
	private double                                  probSecondLang;                 /**< @brief The probability of speaking a second language. **/

    /**
     * @brief   Constructor
     * @param   locationDic The location dictionary.
     * @param   probEnglish The probability of speaking english.
     * @param   probSecondLang The probability of speaking a second language.
     */
	public LanguageDictionary( LocationDictionary locationDic,
	        double probEnglish, double probSecondLang){
        this.locationDictionary = locationDic;
        this.probEnglish = probEnglish;
        this.probSecondLang = probSecondLang;
        this.languages = new ArrayList<String>();
        this.officalLanguagesByCountry = new HashMap<Integer, ArrayList<Integer>>();
        this.languagesByCountry = new HashMap<Integer, ArrayList<Integer>>();
    }

    /**
     * @brief   Loads a dictionary file.
     * @param   fileName  The name of the dictionary file.
     */
	public void load( String fileName ){
		try {
		    for (Integer id : locationDictionary.getCountries()) {
		        officalLanguagesByCountry.put(id, new ArrayList<Integer>());
		        languagesByCountry.put(id, new ArrayList<Integer>());
		    }
		    BufferedReader dictionary = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(fileName), "UTF-8"));
		    String line;
		    while ((line = dictionary.readLine()) != null) {
		        String data[] = line.split(SEPARATOR);
		        if (locationDictionary.getCountryId(data[0]) != LocationDictionary.INVALID_LOCATION) {
		            for (int i = 1; i < data.length; i++) {
		                Integer countryId = locationDictionary.getCountryId(data[0]);
		                String languageData[] = data[i].split(" ");
		                Integer id = languages.indexOf(languageData[0]);
		                if (id == -1) {
		                    id = languages.size();
		                    languages.add(languageData[0]);
		                }
		                if (languageData.length == 3) {
		                    officalLanguagesByCountry.get(countryId).add(id);
		                }
		                languagesByCountry.get(countryId).add(id);
		            }
		        }
		    }
		    dictionary.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

    /**
     * @brief   Gets the name of the language.
     * @param   languageId The language identifier.
     * @return  The name of the language.
     */
	public String getLanguageName(int languageId) {
        if (languageId < 0 || languageId >= languages.size()) {
            System.err.println("Trying to acces the invalid language with id="+languageId);
            return "";
        }
        return languages.get(languageId);
    }

    /**
     * @breif   Gets a set of random languages from a country.
     * @param   random Random number generator.
     * @param   country The country to retrieve the languages from.
     * @return  The set of randomly choosen languages.
     */
	public ArrayList<Integer> getLanguages(Random random, int country) {
	    ArrayList<Integer> langSet = new ArrayList<Integer>();
	    if (officalLanguagesByCountry.get(country).size() != 0) {
	        int id = random.nextInt(officalLanguagesByCountry.get(country).size());
	        langSet.add(officalLanguagesByCountry.get(country).get(id));
	    } else {
	        int id = random.nextInt(languagesByCountry.get(country).size());
            langSet.add(languagesByCountry.get(country).get(id));
	    }
	    if (random.nextDouble() < probSecondLang) {
	        int id = random.nextInt(languagesByCountry.get(country).size());
	        if (langSet.indexOf(languagesByCountry.get(country).get(id)) == -1) {
	            langSet.add(languagesByCountry.get(country).get(id));
	        }
	    }
	    return langSet;
	}

    /**
     * @brief   Gets a random language.
     * @param   random
     * @return  The language.
     */
	public int getInternationlLanguage(Random random) {
	    Integer languageId = -1;
        if (random.nextDouble() < probEnglish) {
            languageId = languages.indexOf(ISO_ENGLISH_CODE);
        }
        return languageId;
	}
}
