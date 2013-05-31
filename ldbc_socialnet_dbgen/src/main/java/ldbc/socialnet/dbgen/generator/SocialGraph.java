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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import ldbc.socialnet.dbgen.objects.FriendShip;

import umontreal.iro.lecuyer.functions.SqrtMathFunction;

public class SocialGraph {
	private long vertexNo;
	private double connectProb; 
	private Random rand;
	//private RandomAccessFile outputFile;
	private FileWriter fstream;
	private BufferedWriter outFile; 
	
	/*
	 * Temporarily store the social graph in an dynamic array (Must be changed)
	 */
	ArrayList edges[]; 
	
	public SocialGraph(long vertexNo, int averageNoOfFriends, long seed){
		this(vertexNo, ((double)averageNoOfFriends/(double)vertexNo), seed);
	}
	public SocialGraph(long vertexNo, double prob, long seed){
		this.vertexNo = vertexNo;
		this.connectProb = prob;
		rand = new Random(seed);
		
			try {
				fstream = new FileWriter("outSocialGraph.txt");
				outFile   = new BufferedWriter(fstream);
				//outFile.write("Hello Java");
				
				//outFile.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
	}
	public void generateRandomGraph(){			//According to GraphZER
		int E;
		E = 0;
		
		Double pickedNumber; 
		long maxE = (vertexNo * (vertexNo-1)) / 2;

		// Initiate edges for each vertex
		edges = new ArrayList[(int)vertexNo];
		for (int j = 0; j < (int)vertexNo; j++){
			edges[j] = new ArrayList();
		}

		// Generate edges
		long i = -1;
		long k; 			// Number of skipped edges
		double lnP = Math.log(1 - connectProb);
		double lnDelta; 
		
		int curVertexIdx; 
		curVertexIdx = -1;
		int checkIdx = 0; 
		int friendIdx = -1; 
	
		while (i < maxE){
			pickedNumber = rand.nextDouble();
			
			lnDelta = Math.log(pickedNumber);
			k = Math.max (0, Math.round(Math.ceil(lnDelta/lnP) - 1)); 
			i = i + k + 1;
			
			if (i > maxE) break;
			//checkIdx = (int)Math.floor(Math.sqrt(2*i));
			checkIdx = (int)((Math.sqrt(8*i-7) + 1)/2);
			
			friendIdx = (int)(i*2 - checkIdx*(checkIdx-1) - 2)/2;

			// Add to the list of edges
			edges[checkIdx].add(friendIdx);
			edges[friendIdx].add(checkIdx);
			
			// Write to a file
			/*
			if (curVertexIdx == checkIdx){
				try {
					outFile.append(" " + friendIdx);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else{
				curVertexIdx = checkIdx; 
				try {
					outFile.newLine();
					outFile.append(curVertexIdx + " : " + friendIdx);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			*/
			writeToFile();
			
			E++;
		} 
		
		System.out.println("Number of edges: " + E );
		
		try {
			outFile.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/*
	 *	Get the list of friends of a user
	 *	In case it is stored in a file,
	 *	store the offset for each list in the file, and then
	 *	retrieve the list according to the offset	
	 * */
	public ArrayList getFriends(int userIdx){
		
		return edges[userIdx];
	}
	
	// Write to file from the memory-based edges
	public void writeToFile(){
		Iterator it;
		try {
			for (int i = 0; i < vertexNo; i ++){
				it = edges[i].iterator(); 
				while (it.hasNext()){
					outFile.append(it.next() + " ");
				}
				outFile.newLine();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
