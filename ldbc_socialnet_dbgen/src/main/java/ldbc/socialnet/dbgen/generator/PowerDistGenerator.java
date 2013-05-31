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

import java.util.Random;

import umontreal.iro.lecuyer.probdist.PowerDist;

import java.util.Arrays;

public class PowerDistGenerator {
	private PowerDist powerDist; 
	private Random rand; 
	double a; 
	double b; 
	public PowerDistGenerator(double a, double b, double alpha, long seed){
		this.a = a;
		this.b = b;
		//powerDist = new PowerDist(alpha);
		powerDist = new PowerDist(a, b, alpha);
		rand = new Random(seed);
	}
	public int getValue(){
		double randVal = powerDist.inverseF(rand.nextDouble());
		//return (int)(a + (b - a) * randVal);
		return (int)randVal;
	}
	
	public double getDouble(){
		return powerDist.inverseF(rand.nextDouble());
	}

	public static void main(String args[]){
		PowerDistGenerator pdg = new PowerDistGenerator(5.0, 50.0, 0.8
				, 80808080);
		int[] arr = new int[400];
		for (int i = 0; i < 400; i ++){
			//System.out.println(pdg.getValue() + " ");
			 arr[i] = pdg.getValue();
		}
		Arrays.sort(arr);
		System.out.println(Arrays.toString(arr));
		
		int j = 0;
		int lastvalue = -1;
		for (int i = 0; i < 400; i ++){
			if (lastvalue != arr[i]){
				System.out.println(lastvalue + " :   " + j);
				lastvalue = arr[i];
				j = 0; 
			}
			j ++;
			
		}
	}
}
