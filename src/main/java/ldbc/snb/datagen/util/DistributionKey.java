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
package ldbc.snb.datagen.util;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;



public class DistributionKey {

    static class Pair{
        private double l;
        private String r;
        public Pair(double l, String r){
            this.l = l;
            this.r = r;
        }
        public double getL(){ return l; }
        public String getR(){ return r; }
        public void setL(double l){ this.l = l; }
        public void setR(String r){ this.r = r; }

    }


    ArrayList<Pair> distribution;
    String distributionFile;

    public DistributionKey(String distributionFile) {
        this.distributionFile = distributionFile;
    }

    public void initialize() {
        try {
            BufferedReader distributionBuffer = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(distributionFile), "UTF-8"));
            ArrayList<Double> temp = new ArrayList<Double>();
            String line;
            distribution = new ArrayList<Pair>();

            while ((line = distributionBuffer.readLine()) != null) {
            	String[] parts = line.split(" ");
            	String key = parts[0];
            	Double valor = Double.valueOf(parts[1]); 
            	Pair p = new Pair(valor,key);               
            	distribution.add(p);
            }
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private int binarySearch(double prob) {
        int upperBound = distribution.size() - 1;
        int lowerBound = 0;
        int midPoint = (upperBound + lowerBound) / 2;
        while (upperBound > (lowerBound + 1)) {
            if (distribution.get(midPoint).getL() > prob) {
                upperBound = midPoint;
            } else {
                lowerBound = midPoint;
            }
            midPoint = (upperBound + lowerBound) / 2;
        }
        return midPoint;
    }

    public String nextDouble(Random random) {
        return distribution.get(binarySearch(random.nextDouble())).getR() ;
    }
}
