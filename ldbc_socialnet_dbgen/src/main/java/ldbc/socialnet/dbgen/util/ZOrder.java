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
package ldbc.socialnet.dbgen.util;

import ldbc.socialnet.dbgen.dictionary.MusicGenres;

/**
 * 
 * @author Minh-Duc Pham
 *
 */

public class ZOrder {

	/**
	 * @param args
	 */
	public int MAX_BIT_NO = 8;
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ZOrder zorder = new ZOrder(8); 
		zorder.getZValue(2, 0);
	}
			
	public ZOrder(int maxNumBit){
		this.MAX_BIT_NO = maxNumBit;
	} 
	
	public int getZValue(int x, int y){
		String sX = Integer.toBinaryString(x);
		
		
		int numberToAddX = MAX_BIT_NO - sX.length();
		for (int i = 0; i < numberToAddX; i++){
			sX = "0" + sX;
		}
		
		String sY = Integer.toBinaryString(y);
		
		int numberToAddY = MAX_BIT_NO - sY.length();
		for (int i = 0; i < numberToAddY; i++){
			sY = "0" + sY;
		}		
		
		
		//System.out.println(sX);
		//System.out.println(sY); 
		
		String sZ = ""; 
		for (int i = 0; i < sX.length(); i++){
			sZ = sZ + sX.substring(i, i+1) + "" + sY.substring(i, i+1);
		}
		
		//System.out.println(sZ);
		//System.out.println("The z-value is: " + Integer.parseInt(sZ, 2));
		
		return Integer.parseInt(sZ, 2);
		
	}
	
	public int getZValue(MusicGenres musicgenre, int numOfGenres){
		int NO_BIT_PER_GENRE = 4; 
		String sGenres[] = new String[numOfGenres];
		byte genres[] = musicgenre.getGenres();
		
		for (int i = 0; i < numOfGenres; i ++){
			String value = Integer.toBinaryString(genres[i]);
			int numberToAdd = NO_BIT_PER_GENRE - value.length();
			for (int j = 0; j < numberToAdd; j++){
				value = "0" + value;
			}
			sGenres[i] = value; 
		}
		
		String sZ = ""; 
		for (int i = 0; i < NO_BIT_PER_GENRE; i++){
			for (int j = 0; j < numOfGenres; j++){
				sZ = sZ + sGenres[j].substring(i, i+1);
			}
		}
		
		return Integer.parseInt(sZ,2);
		//return Integer.parseInt(sZ);
		
	}
}
