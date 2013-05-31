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
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Random;
import java.util.Vector;

import ldbc.socialnet.dbgen.objects.Location;


// Dictionary of emails contains five top email domains which their popularities and list of 460 free emails

public class EmailDictionary {
	Vector<Double>  vTopEmailCummulative; 
	double 			randomFreeEmailCummulative;
	
	Vector<String> 	vEmail;
	Random 			randEmail; 
	Random 			randIdx; 
	int 			numTopEmail = 5; 
	
	int 			totalNumEmail = 0;
	BufferedReader emailDictionary; 
	String 				emailDicFileName; 
	public EmailDictionary(String _emailDicFileName, long seedEmail){
		randEmail = new Random(seedEmail);
		randIdx = new Random(seedEmail);
		emailDicFileName = _emailDicFileName;
	}
	
	public void init(){
		try {
			emailDictionary = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(emailDicFileName), "UTF-8"));
			vTopEmailCummulative = new Vector<Double>();
			vEmail = new Vector<String>();
			
			emailExtract();
			
			emailDictionary.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	
	public void emailExtract(){
		String emailDomain; 
		double cumdistribution = 0.0;	//cummulative distribution value
		String line; 
		int i = 0; 
		

		try {
			while ((line = emailDictionary.readLine()) != null){
				if (i < numTopEmail){
					String infos[] = line.split(" ");
					emailDomain = infos[0];
					cumdistribution = cumdistribution + Double.parseDouble(infos[1]);
					vEmail.add(emailDomain);
					vTopEmailCummulative.add(cumdistribution);
					i++;
				}
				else 
					vEmail.add(line);
				
				totalNumEmail++;
			}
			
			System.out.println("Done ... " + vEmail.size() + " email domains were extracted");
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public String getRandomEmail(){
		double prob = randEmail.nextDouble();
		
		int idx = 0;
		int minIdx = 0;
		int maxIdx = numTopEmail - 1;
		if (prob > vTopEmailCummulative.get(maxIdx)){
			//Randomly select one email from non-top email
			idx = randIdx.nextInt(totalNumEmail - numTopEmail) + numTopEmail;
			return vEmail.get(idx);
		}
		if (prob < vTopEmailCummulative.get(minIdx)){
			return vEmail.get(minIdx);
		}
		
		while ((maxIdx - minIdx) > 1){
			
			if (prob > vTopEmailCummulative.get(minIdx + (maxIdx - minIdx)/2)){
				minIdx =  minIdx + (maxIdx - minIdx)/2;
			}
			else{
				maxIdx =  minIdx + (maxIdx - minIdx)/2;
			}
		}
		
		return vEmail.get(maxIdx);
	}	
}
