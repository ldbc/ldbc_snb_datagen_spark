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
import java.util.Random;
import java.util.Vector;

public class BrowserDictionary {
	
	Vector<Double>  	vBrowserCummulative; 
	Vector<String> 		vBrowser;

	Random 				randBrowsers; 
	BufferedReader 	browserDictionary; 
	String 				browserDicFileName;
	
	int 				totalNumBrowsers;
	
	double 				probAnotherBrowser; 	// Probability that a user uses another browser
	Random				randDifBrowser;			// whether user change to another browser or not
	
	public BrowserDictionary(String _browserDicFileName, long seedBrowser,double _probAnotherBrowser){
		randBrowsers = new Random(seedBrowser);
		randDifBrowser = new Random(seedBrowser);
		browserDicFileName = _browserDicFileName;
		probAnotherBrowser = _probAnotherBrowser;
	}
	
	public void init(){
		try {
			browserDictionary = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(browserDicFileName), "UTF-8"));
			vBrowser = new Vector<String>();
			vBrowserCummulative = new Vector<Double>();
			
			browsersExtract();
			
			browserDictionary.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	
	public void browsersExtract(){
		String browser; 
		double cumdistribution = 0.0;	//cummulative distribution value
		String line; 
		int i = 0; 
		totalNumBrowsers = 0;

		try {
			while ((line = browserDictionary.readLine()) != null){
				String infos[] = line.split("  ");
				browser = infos[0];
				cumdistribution = cumdistribution + Double.parseDouble(infos[1]);
				vBrowser.add(browser);
				//System.out.println(cumdistribution);
				vBrowserCummulative.add(cumdistribution);
				i++;
				
				totalNumBrowsers++;
			}
			
			System.out.println("Done ... " + vBrowser.size() + " browsers were extracted");
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public String getRandomBrowser(){
		double prob = randBrowsers.nextDouble();

		int minIdx = 0;
		int maxIdx = totalNumBrowsers - 1;

		if (prob < vBrowserCummulative.get(minIdx)){
			return vBrowser.get(minIdx);
		}
		
		while ((maxIdx - minIdx) > 1){
			
			if (prob > vBrowserCummulative.get(minIdx + (maxIdx - minIdx)/2)){
				minIdx =  minIdx + (maxIdx - minIdx)/2;
			}
			else{
				maxIdx =  minIdx + (maxIdx - minIdx)/2;
			}
		}
		
		return vBrowser.get(maxIdx);
	}
	
	public String getBrowserName(byte browserId){
		return vBrowser.get(browserId);
	}
	public byte getRandomBrowserId(){
		double prob = randBrowsers.nextDouble();
		int minIdx = 0;
		int maxIdx = totalNumBrowsers - 1;

		if (prob < vBrowserCummulative.get(minIdx)){
			return (byte)minIdx;
		}
		
		while ((maxIdx - minIdx) > 1){
			
			if (prob > vBrowserCummulative.get(minIdx + (maxIdx - minIdx)/2)){
				minIdx =  minIdx + (maxIdx - minIdx)/2;
			}
			else{
				maxIdx =  minIdx + (maxIdx - minIdx)/2;
			}
		}
		
		return (byte)maxIdx;
	}

	public byte getPostBrowserId(byte userBrowserId){
		double prob = randDifBrowser.nextDouble();
		if (prob < probAnotherBrowser){
			return getRandomBrowserId();
		}
		else{
			return userBrowserId;
		}
	}
	public byte getCommentBrowserId(byte userBrowserId){
		double prob = randDifBrowser.nextDouble();
		if (prob < probAnotherBrowser){
			return getRandomBrowserId();
		}
		else{
			return userBrowserId;
		}
	}	
	
	public String getBrowserForAUser(String originalBrowser){
		double prob = randDifBrowser.nextDouble();
		if (prob < probAnotherBrowser){
			return getRandomBrowser();
		}
		else{
			return originalBrowser;
		}
	}
	
	public Vector<String> getvBrowser() {
		return vBrowser;
	}

	public void setvBrowser(Vector<String> vBrowser) {
		this.vBrowser = vBrowser;
	}

}
