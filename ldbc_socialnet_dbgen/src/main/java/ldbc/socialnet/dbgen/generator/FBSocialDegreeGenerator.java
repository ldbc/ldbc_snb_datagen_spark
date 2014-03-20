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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;

/**
 * This class generates Facebook-like social degree distribution
 * 
 * A. Preparation
 * For a specific social network size (total number of users) 
 	1) compute the mean value of social degree 
	2) compute the range of each bucket (100 buckets) using the data from facebookBucket100.dat 
 * B. Generate social degree for each user 
   	1) Determine the bucket (Take a random number from 0-99)
	2) Randomly select a social degree in the range of that bucket
 * 
 */

public class FBSocialDegreeGenerator {
	int 				mean = 0; 
	Random 				randomPercentile;
	Random 				randomDegree[];
	String				fbDataFile; 	/* File for original facebook data*/
	Bucket[] 			buckets;
	static final int	FB_MEAN = 190;
	static final int	BUCKET_NUM = 100;
	double 				avgPathLength; 
	int[]				counter;  		/* Count the number of users at specific social degree */
	int[]				percenttileIDCounter;	/* Current user Id in a certain percentile */
	int					percentileIdx; 			/* Store the Idx of the current percentile */
	
	
	public FBSocialDegreeGenerator(int networkSize, String fbDataFile, long seed, double avgPathLength){
		this.avgPathLength = avgPathLength;
		//mean = (int) Math.round(Math.pow(networkSize, 1.00/avgPathLength));
		//Do not use the simple mean computation from network size, but use the formula
		// considering the upward trend over the degree of separation (path length)
		// mean = N ^ (0.512 - 0.028 * log(N))
		mean = (int) Math.round(Math.pow(networkSize, (0.512 - 0.028 * Math.log10(networkSize))));
		
		System.out.println("Mean = " + mean);
		randomPercentile = new Random(seed);
		percenttileIDCounter = new int[BUCKET_NUM];
		this.percentileIdx = -1; 
		
		randomDegree = new Random[BUCKET_NUM];
		for (int i = 0; i < BUCKET_NUM; i++){
			randomDegree[i] = new Random(seed);
			percenttileIDCounter[i] = 0;
		}
		this.fbDataFile = fbDataFile;
		this.buckets = new Bucket[BUCKET_NUM];

	}
	
	public void loadFBBuckets(){
		try{
		    BufferedReader fbDataReader = new BufferedReader(
		    		new InputStreamReader(getClass( ).getResourceAsStream(fbDataFile), "UTF-8"));
		    String line;
		    int idx = 0;
		    int percentile = 0;
		    while ((line = fbDataReader.readLine()) != null){
		    	percentile++;
		    	String data[] = line.split(" ");
		    	buckets[idx] = new Bucket(Double.parseDouble(data[0]), Double.parseDouble(data[1]), percentile);
		    	idx++;
		    }
		    fbDataReader.close();	
		} catch(IOException e){
			e.printStackTrace();
		}
	} 
	public void rebuildBucketRange(){
		double newMin, newMax;
		int	minRange, maxRange; 
		for (int i = 0; i< BUCKET_NUM; i++){
			newMin =  buckets[i].getMin() * mean / FB_MEAN;
			newMax =  buckets[i].getMax() * mean / FB_MEAN;
					
			buckets[i].setMin(newMin);
			buckets[i].setMax(newMax);
			
			//set the range
			minRange = (int) Math.floor(buckets[i].getMin() + 1);
			maxRange = (int) Math.floor(buckets[i].getMax());
			if (maxRange < minRange) maxRange = minRange; 
			
			buckets[i].setMinRange(minRange);
			buckets[i].setMaxRange(maxRange);
		}
		
		//Init counter
		int maxCounterIdx = buckets[BUCKET_NUM-1].getMaxRange();
		counter = new int[maxCounterIdx + 1];
		for (int i = 0; i < (maxCounterIdx + 1); i++){
			counter[i] = 0;
		}
		
	}
	public short getSocialDegree(){
		int degree; 
		int idx = randomPercentile.nextInt(100);
		this.percentileIdx = idx; 

		degree = randomDegree[idx].nextInt(buckets[idx].getMaxRange() - buckets[idx].getMinRange() + 1) + buckets[idx].getMinRange();
		
		counter[degree]++;
		
		return (short)degree;
	}
	
	//Assign new ID to user based on the percentile that he/she belongs to
	public int getIDByPercentile(){
		int id = percenttileIDCounter[percentileIdx] * 100 + percentileIdx;
		percenttileIDCounter[percentileIdx]++;
		return id;
	}
	
	public void printNewBucketRange(String filename){
		try{
			BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
			for (int i = 0; i < BUCKET_NUM; i++){
				writer.write(i + ":   " + buckets[i].getMinRange() + " --> " + buckets[i].getMaxRange() + "\n");
			}
			writer.close();
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	public void printSocialDegree(String filename, int numUser){
		int maxDegree = buckets[BUCKET_NUM-1].getMaxRange();
		try{
			BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
			for (int i = 1; i < (maxDegree + 1); i++){
				writer.write(i + "   " + ((double)counter[i]*100/numUser) + "	" + counter[i] + "\n");
			}
			writer.close();
		}catch(IOException e){
			e.printStackTrace();
		}
	}

	public class Bucket{
		double min; 
		double max;
		int minRange; 
		int maxRange; 
		int percentile; 
		public Bucket(double min, double max, int percentile){
			this.min = min; 
			this.max = max; 
			this.percentile = percentile; 
		}
		public double getMin() {
			return min;
		}
		public void setMin(double min) {
			this.min = min;
		}
		public double getMax() {
			return max;
		}
		public void setMax(double max) {
			this.max = max;
		}
		public int getPercentile() {
			return percentile;
		}
		public void setPercentile(int percentile) {
			this.percentile = percentile;
		}
		public int getMinRange() {
			return minRange;
		}
		public void setMinRange(int minRange) {
			this.minRange = minRange;
		}
		public int getMaxRange() {
			return maxRange;
		}
		public void setMaxRange(int maxRange) {
			this.maxRange = maxRange;
		}
		
	}
}
