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


public class LanguageDictionary {

    private static final String SEPARATOR = "  "; 
    private static final String ISO_ENGLISH_CODE = "en"; 
    
    Vector<String> languages;
    HashMap<Integer, Vector<Integer>> officalLanguagesFromCountries;
    HashMap<Integer, Vector<Integer>> languagesFromCountries;
	
    LocationDictionary locationDic;
	String dicFile;
	double probEnglish;
	double probSecondLang;
	
	Random rand;
	
	public LanguageDictionary(String dicFile,  LocationDictionary locationDic, 
	        double probEnglish, double probSecondLang, long seed){
        this.dicFile = dicFile; 
        this.locationDic = locationDic;
        this.probEnglish = probEnglish;
        this.probSecondLang = probSecondLang;
        
        rand = new Random(seed);
    }
	
	public void init(){
		try {
		    languages = new Vector<String>();
		    officalLanguagesFromCountries = new HashMap<Integer, Vector<Integer>>();
		    languagesFromCountries = new HashMap<Integer, Vector<Integer>>();
		    for (Integer id : locationDic.getCountries()) {
		        officalLanguagesFromCountries.put(id, new Vector<Integer>());
		        languagesFromCountries.put(id, new Vector<Integer>());
		    }

		    BufferedReader dictionary = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(dicFile), "UTF-8"));

		    String line;
		    while ((line = dictionary.readLine()) != null) {
		        String data[] = line.split(SEPARATOR);
		        if (locationDic.getCountryId(data[0]) != LocationDictionary.INVALID_LOCATION) {
		            for (int i = 1; i < data.length; i++) {
		                Integer countryId = locationDic.getCountryId(data[0]);
		                String languageData[] = data[i].split(" ");
		                Integer id = languages.indexOf(languageData[0]);
		                if (id == -1) {
		                    id = languages.size();
		                    languages.add(languageData[0]);
		                }
		                if (languageData.length == 3) {
		                    officalLanguagesFromCountries.get(countryId).add(id);
		                }
		                languagesFromCountries.get(countryId).add(id);
		            }
		        }
		    }
		    dictionary.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public String getLanguagesName(int languageId) {
        if (languageId < 0 || languageId >= languages.size()) {
            System.err.println("Trying to acces the invalid language with id="+languageId);
            return "";
        }
        return languages.get(languageId);
    }
	
	public Vector<Integer> getLanguages(int locationId) {
	    Vector<Integer> langSet = new Vector<Integer>();
	    if (officalLanguagesFromCountries.get(locationId).size() != 0) {
	        int id = rand.nextInt(officalLanguagesFromCountries.get(locationId).size());
	        langSet.add(officalLanguagesFromCountries.get(locationId).get(id));
	    } else {
	        int id = rand.nextInt(languagesFromCountries.get(locationId).size());
            langSet.add(languagesFromCountries.get(locationId).get(id));
	    }

	    if (rand.nextDouble() < probSecondLang) {
	        int id = rand.nextInt(languagesFromCountries.get(locationId).size());
	        if (langSet.indexOf(languagesFromCountries.get(locationId).get(id)) == -1) {
	            langSet.add(languagesFromCountries.get(locationId).get(id));
	        }
	    }
	    
	    return langSet;
	}
	
	public Integer getInternationlLanguage() {
	    Integer languageId = -1;
        if (rand.nextDouble() < probEnglish) {
            languageId = languages.indexOf(ISO_ENGLISH_CODE);
        }
        return languageId;
	}
}
