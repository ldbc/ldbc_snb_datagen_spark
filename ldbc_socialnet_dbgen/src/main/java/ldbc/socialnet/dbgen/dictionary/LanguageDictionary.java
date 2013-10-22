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
import java.util.TreeSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Vector;

import ldbc.socialnet.dbgen.dictionary.NamesDictionary.NameFreq;
import ldbc.socialnet.dbgen.objects.Location;
import ldbc.socialnet.dbgen.util.ZOrder;


public class LanguageDictionary {

    Vector<String> languages;
    Vector<Vector<Integer>> officalLanguagesFromCountries;
    Vector<Vector<Integer>> languagesFromCountries;
	
    HashMap<String, Integer> countryNames;
	BufferedReader dictionary; 
	String dicFile;
	double probEnglish;
	double probSecondLang;
	
	Random rand;
	
	public LanguageDictionary(String dicFile,  HashMap<String, Integer> countryNames, 
	        double probEnglish, double probSecondLang, long seed){
        this.dicFile = dicFile; 
        this.countryNames = countryNames;
        this.probEnglish = probEnglish;
        this.probSecondLang = probSecondLang;
        
        rand = new Random(seed);
    }
	
	public void init(){
		try {
		    languages = new Vector<String>();
		    officalLanguagesFromCountries = new Vector<Vector<Integer>>();
		    languagesFromCountries = new Vector<Vector<Integer>>();
		    for (int i = 0; i < countryNames.size(); i++) {
		        officalLanguagesFromCountries.add(new Vector<Integer>());
		        languagesFromCountries.add(new Vector<Integer>());
		    }
			
		    dictionary = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(dicFile), "UTF-8"));

			extractLanguages();
			
			dictionary.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void extractLanguages()
    {
        try {
            String line;
            while ((line = dictionary.readLine()) != null) {
                String splitted[] = line.split("  ");
                if (countryNames.containsKey(splitted[0])) {
                    for (int i = 1; i < splitted.length; i++) {
                        Integer countryId = countryNames.get(splitted[0]);
                        String languageData[] = splitted[i].split(" ");
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
            System.out.println("Extracted " + languages.size() +  " languages");
        } catch (IOException e) {
            // TODO Auto-generated catch block
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
	    double prob = rand.nextDouble();
	    if (prob < probSecondLang) {
	        int id = rand.nextInt(languagesFromCountries.get(locationId).size());
	        if (langSet.indexOf(languagesFromCountries.get(locationId).get(id)) == -1) {
	            langSet.add(languagesFromCountries.get(locationId).get(id));
	        }
	    }
	    
	    return langSet;
	}
	public Integer getInternationlLanguage() {
	    Integer languageId = -1;
	    double prob = rand.nextDouble();
        if (prob < probEnglish) {
            languageId = languages.indexOf("en");
        }
        return languageId;
	}
}
