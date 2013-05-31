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
package ldbc.socialnet.dbgen.generator;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Vector;

public class ArticlesLocationDictionary {
	
	RandomAccessFile dictionary; 
	String dicFileName;
	HashMap<String, Integer>	locationNameMapping; 	//Mapping from a location name to a id
	int totalNumberOfArticles; 
	
	public ArticlesLocationDictionary(String dicFileName, HashMap<String, Integer> _locationNameMapping){
		this.dicFileName = dicFileName;
		this.locationNameMapping = _locationNameMapping; 
	}
	
	Vector<Vector<String>>		locationArticles; 			// 

	public HashMap<String, Integer> getLocationNameMapping() {
		return locationNameMapping;
	}
	public void setLocationNameMapping(HashMap<String, Integer> locationNameMapping) {
		this.locationNameMapping = locationNameMapping;
	}

	public void init(){
		try {
			dictionary = new RandomAccessFile(dicFileName, "r");
			
			locationArticles = new Vector<Vector<String>>(locationNameMapping.size()); 
			for (int i = 0; i < locationArticles.capacity(); i ++){
				locationArticles.add(new Vector<String>());
			}
			
			System.out.println("Extracting articles with correlated locations ");
			extractArticles();
			
			dictionary.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void extractArticles(){
		String location; 

		String line;
		String content; 
		
		try {
			while ((line = dictionary.readLine()) != null){
				location = line.split(" ")[0];
				content = line.substring(location.length() + 1);
				content = content.trim();
				// Add to the vector of articles
				if (locationNameMapping.containsKey(location)){
					locationArticles.get(locationNameMapping.get(location)).add(content); 
					totalNumberOfArticles++;
				}
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

		System.out.println(totalNumberOfArticles + " articles have been collected");
	}

}
