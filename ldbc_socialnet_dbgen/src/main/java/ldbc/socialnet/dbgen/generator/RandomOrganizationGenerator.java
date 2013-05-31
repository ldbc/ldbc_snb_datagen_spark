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
import java.util.Random;
import java.util.Vector;

public class RandomOrganizationGenerator {
	String dicFileName; 
	Vector<String> vecOrganizations; 		// vector for storing all the organizations URI
	RandomAccessFile dictionary; 
	Random rand; 
	private int organizationIdx; 
	
	public int getOrganizationIdx() {
		return organizationIdx;
	}

	public void setOrganizationIdx(int organizationIdx) {
		this.organizationIdx = organizationIdx;
	}

	public RandomOrganizationGenerator(String fileName){
		rand = new Random(); 
		dicFileName = fileName; 
		init();
	}
	
	public RandomOrganizationGenerator(String fileName, long seed){
		dicFileName = fileName;
		rand = new Random(seed);
		init();
	}
	
	public void init(){
		try {
			dictionary = new RandomAccessFile(dicFileName, "r");
			vecOrganizations = new Vector<String>();
			
			System.out.println("Extracting organizations into a dictionary ");
			extractOrganizations();
			
			dictionary.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void extractOrganizations(){
		String organization; 
		try {
			while ((organization = dictionary.readLine()) != null){
				vecOrganizations.add(organization);
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println(vecOrganizations.size() + " organizations were extracted");
		
	} 
	
	public String getRandomOrganization(){
		organizationIdx = rand.nextInt(vecOrganizations.size()-1);
		
		return vecOrganizations.elementAt(organizationIdx) ;
	}
	
	public String getCorrelatedOrganization(double probability, int correlatedIdx){

		double randProb = rand.nextDouble();
		if (randProb < probability){
			organizationIdx = correlatedIdx;
			return vecOrganizations.elementAt(organizationIdx) ;
		}
		
		organizationIdx = rand.nextInt(vecOrganizations.size()-1);
		return vecOrganizations.elementAt(organizationIdx) ;
	}	

}
