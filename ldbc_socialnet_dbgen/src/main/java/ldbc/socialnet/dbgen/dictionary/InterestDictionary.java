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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Vector;

import ldbc.socialnet.dbgen.util.ZOrder;


public class InterestDictionary {

	Vector<Vector<Double>> vLocationInterest; 
	HashMap<String, Integer> 	interestNames; 		// Store the name of singers
	HashMap<Integer, String> 	interestdsNamesMapping;
	Vector<MusicGenres> vMusicGenres; 	// Set of genres for each singer

	BufferedReader 	dictionary; 
	BufferedReader 	interestFile;
	BufferedReader 	singerGenresFile;
	String 				distributionFileName;
	String				interestFileName; 
	String				singerGenresFileName; 
	
	Random 				randInterests; 	// For selecting random interests from the singernames
	int 				numOfIntersts;
	ZOrder				zOrder;
	int 				numOfGenres; 
	
	public InterestDictionary(String _distributionFileName){
		this.distributionFileName = _distributionFileName; 
	}
	
	public InterestDictionary(String _distributionFileName, String _interestFileName, long randomInterestSeed,
							String _singerGenresFileName){
		this.distributionFileName = _distributionFileName;
		this.interestFileName = _interestFileName; 
		randInterests = new Random(randomInterestSeed);
		this.singerGenresFileName = _singerGenresFileName;
		zOrder = new ZOrder(32);
	}
	public void init(){
		try {
			
			interestNames = new HashMap<String, Integer>();
			interestdsNamesMapping = new HashMap<Integer, String>();
			dictionary = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(distributionFileName), "UTF-8"));
			interestFile = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(interestFileName), "UTF-8"));
			vLocationInterest = new Vector<Vector<Double>>();
			
			//System.out.println("Extracting locations into a dictionary ");
			
			extractInterests();
			
			extractInterestCummulative();
			
			getSingerGenres();
			
			System.out.println("Done ... " + interestNames.size() + " interests were extracted ");
			
			dictionary.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}
	
	public void extractInterests(){
		try {
			String line; 
			int idx  = -1; 
			while ((line = interestFile.readLine()) != null){
				idx++;
				interestNames.put(line.trim().toLowerCase(),idx);
				interestdsNamesMapping.put(idx,line.trim());
			}
			
			numOfIntersts = interestNames.size();
			interestFile.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void extractInterestCummulative(){
		double cumdistribution;	//cumulative distribution value
		String line; 
		
		try {
			while ((line = dictionary.readLine()) != null){
				Vector<Double> vInterestDist = new Vector<Double>(interestNames.size());
				for (int i = 0; i < numOfIntersts; i++){
					line = dictionary.readLine();	// This line is for location name
					cumdistribution = Double.parseDouble(line.trim());
					vInterestDist.add(cumdistribution);
				}
				vLocationInterest.add(vInterestDist);
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void getSingerGenres(){
		vMusicGenres = new Vector<MusicGenres>(interestNames.size());
		System.out.println("Number of singer names: " + interestNames.size());
		
		try {
			singerGenresFile = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(singerGenresFileName), "UTF-8"));
			String line; 
			int idx  = -1; 
			while ((line = singerGenresFile.readLine()) != null){
				idx++;
				String extract[] = line.split(",");
				numOfGenres = extract.length-1;
				byte genres[] = new byte[numOfGenres];
				for (int i = 1; i < extract.length; i++){
					genres[i-1] = Byte.parseByte(extract[i]);
				}
				
				MusicGenres musicgenre = new MusicGenres(genres);
				
				vMusicGenres.add(musicgenre);

			}
			
			singerGenresFile.close();
			System.out.println("Size of vMusicGenres: " + vMusicGenres.size());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	

	public HashSet<Integer> getInterests(int locationIdx, int _noInterests){
		HashSet<Integer> setOfInterests = new HashSet<Integer>(); 
		
		// Randomly select noInterests from vectorGenresSingle.get(selectedGenreIdx)
		int singerIdx; 
		for (int i=0; i < _noInterests; i++){
			double randDis = randInterests.nextDouble();
			singerIdx = getInterestIdxFromLocation(randDis, locationIdx);
			if (!setOfInterests.contains(singerIdx)) setOfInterests.add(singerIdx);
		}
		
		return setOfInterests;
	} 
	public MusicGenres getGenresFromInterests(HashSet<Integer> hsInterest){
		
		int noOfSingers = hsInterest.size();
		MusicGenres tmpGenres[] = new MusicGenres[noOfSingers];
		Iterator<Integer> iter = hsInterest.iterator(); 
		int idx = -1;
		while (iter.hasNext()){
			idx++;
			int singIdx = iter.next();
			tmpGenres[idx] = vMusicGenres.get(singIdx);
		}
		// Merge all the vector
		int noOfGenres = tmpGenres[0].getGenres().length;
		MusicGenres musicGenre = new MusicGenres(noOfGenres);
		for (int i = 0; i < noOfSingers; i++){
			for (int j=0;j<noOfGenres;j++){
				if (tmpGenres[i].getValueOfGenre(j) > musicGenre.getValueOfGenre(j))
					musicGenre.setValueForGenre(j, tmpGenres[i].getValueOfGenre(j));
			}
		}
		
		return musicGenre;
	}
	public int getZValue(HashSet<Integer> hsInterest){
		return zOrder.getZValue(getGenresFromInterests(hsInterest), numOfGenres);
	}
	public int getInterestIdxFromLocation(double randomDis, int locationidx){
		Vector<Double> vInterestDis = vLocationInterest.get(locationidx);
		
		int lowerBound = 0;
		int upperBound = numOfIntersts - 1; 
		
		int curIdx = (upperBound + lowerBound)  / 2;
		
		while (upperBound > (lowerBound+1)){
			if (vInterestDis.get(curIdx) > randomDis ){
				upperBound = curIdx;
			}
			else{
				lowerBound = curIdx; 
			}
			curIdx = (upperBound + lowerBound)  / 2;
		}
		
		return curIdx; 
	}

	// We use a higher similarity score for two users' interests
	// SIM(A,B) = |A intersects B| / (min(|A|,|B|))
	public double getInterestSimilarityScore(HashSet<Integer> interestSet1,HashSet<Integer> interestSet2){
		int numSameInterests = 0; 
		Iterator<Integer> it = interestSet1.iterator();
		while (it.hasNext()){
			if (interestSet2.contains(it.next()))	numSameInterests++;
		}
		
		return (double)(numSameInterests)/ Math.min(interestSet1.size(),interestSet2.size());
	}
	
	public HashMap<String, Integer> getInterestNames() {
		return interestNames;
	}
	public void setInterestNames(HashMap<String, Integer> interestNames) {
		this.interestNames = interestNames;
	}
    public HashMap<Integer, String> getInterestdsNamesMapping() {
        return interestdsNamesMapping;
    }
}
