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
import java.util.Random;
import java.util.Vector;

/**
 * This class reads the file containing the names and distributions for the browsers used in the ldbc socialnet generation and 
 * provides access methods to get such data.
 */
public class BrowserDictionary {
	
    private static final String SEPARATOR = "  ";
    
    Vector<String>      vBrowser;
	Vector<Double>  	vBrowserCummulative; 

	String fileName;
	
	double probAnotherBrowser;
	/**
	 * Creator.
	 * 
	 * @param fileName: The file which contains the browser data.
	 * @param probAnotherBrowser: Probability of the user using another browser.
	 */
	public BrowserDictionary(String fileName, double probAnotherBrowser){
		this.fileName = fileName;
		this.probAnotherBrowser = probAnotherBrowser;
	}
	
	/**
	 * Initializes the dictionary extracting the data from the file.
	 */
	public void init(){
		try {
		    BufferedReader dictionary = new BufferedReader(
		            new InputStreamReader(getClass( ).getResourceAsStream(fileName), "UTF-8"));
			vBrowser = new Vector<String>();
			vBrowserCummulative = new Vector<Double>();
			
			String line; 
			double cummulativeDist = 0.0;

			while ((line = dictionary.readLine()) != null){
			    String data[] = line.split(SEPARATOR);
			    String browser = data[0];
			    cummulativeDist += Double.parseDouble(data[1]);
			    vBrowser.add(browser);
			    vBrowserCummulative.add(cummulativeDist);
			}			
			dictionary.close();
			System.out.println("Done ... " + vBrowser.size() + " browsers were extracted");
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Gets the browser name.
	 */
	public String getName(byte id) {
        return vBrowser.get(id);
    }
	
	/**
	 * Gets a random browser id.
	 */
	public byte getRandomBrowserId(Random random) {
	    double prob = random.nextDouble();
		int minIdx = 0;
		int maxIdx = (prob < vBrowserCummulative.get(minIdx)) ? minIdx : vBrowserCummulative.size() - 1;
		while ((maxIdx - minIdx) > 1) {
			
		    int middlePoint = minIdx + (maxIdx - minIdx) / 2;
			if (prob > vBrowserCummulative.get(middlePoint)) {
				minIdx = middlePoint;
			} else {
				maxIdx = middlePoint;
			}
		}
		return (byte)maxIdx;
	}

	/**
	 * Gets the post browser. There is a chance of being different from the user preferred browser
	 * @param userBrowserId: The user preferred browser.
	 */
	public byte getPostBrowserId(Random random, byte userBrowserId){
		double prob = random.nextDouble();
		return (prob < probAnotherBrowser) ? getRandomBrowserId(random) : userBrowserId;
	}
}
