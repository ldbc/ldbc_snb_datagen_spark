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
 * This class reads the file containing the email domain and its popularity used in the ldbc socialnet generation and 
 * provides access methods to get such data.
 */
public class EmailDictionary {
    
    private static final String SEPARATOR = " "; 
    Vector<String> emails;
	Vector<Double> topEmailCummulative;
	String fileName;
	/**
     * Constructor.
     * 
     * @param fileName: The file with the email data.
     * @param seed: Seed for the random selector.
     */
	public EmailDictionary(String fileName) {
		this.fileName = fileName;
	}
	
	/**
	 * Initializes the dictionary with the file data.
	 */
	public void init(){
		try {
		    BufferedReader emailDictionary = new BufferedReader(
		            new InputStreamReader(getClass( ).getResourceAsStream(fileName), "UTF-8"));
		    
		    emails = new Vector<String>();
		    topEmailCummulative = new Vector<Double>();

		    String line;
			double cummulativeDist = 0.0;
			while ((line = emailDictionary.readLine()) != null){
			    String data[] = line.split(SEPARATOR);
                emails.add(data[0]);
                if (data.length == 2) {
			        cummulativeDist += Double.parseDouble(data[1]);
			        topEmailCummulative.add(cummulativeDist);
			    }
			}
			emailDictionary.close();
			System.out.println("Done ... " + emails.size() + " email domains were extracted");
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Gets a random email domain based on its popularity.
	 */
	public String getRandomEmail(Random random){
		int minIdx = 0;
		int maxIdx = topEmailCummulative.size() - 1;
		double prob = random.nextDouble();
		if (prob > topEmailCummulative.get(maxIdx)){
		    int Idx = random.nextInt(emails.size() - topEmailCummulative.size()) + topEmailCummulative.size();
		    return emails.get(Idx);
		} else if (prob < topEmailCummulative.get(minIdx)){
		    return emails.get(minIdx);
		}
		
		while ((maxIdx - minIdx) > 1){
		    int middlePoint = minIdx + (maxIdx - minIdx) / 2;
			if (prob > topEmailCummulative.get(middlePoint)){
				minIdx =  middlePoint;
			} else {
				maxIdx =  middlePoint;
			}
		}
		return emails.get(maxIdx);
	}	
}
