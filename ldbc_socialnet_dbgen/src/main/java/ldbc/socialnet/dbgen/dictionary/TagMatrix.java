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
import java.io.InputStreamReader;
import java.util.TreeSet;
import java.util.Vector;
import java.util.Random;

public class TagMatrix {
    
    private static final String SEPARATOR = " ";
    
    String dicFileName;
    
    Vector<Vector<Double>> vecCumulative;
    Vector<Vector<Integer>> vecTopicID;
    
    Random  rnd;
    Random  rnd2;
    
    public TagMatrix(String dicFileName, int numCelebrities, long seed){
        
        this.dicFileName = dicFileName;
        
        vecCumulative = new Vector<Vector<Double>>(numCelebrities);
        vecTopicID    = new Vector<Vector<Integer>>(numCelebrities);
        for (int i =  0; i < numCelebrities; i++){
            vecCumulative.add(new Vector<Double>());
            vecTopicID.add(new Vector<Integer>());
        }
        
        rnd  = new Random(seed);
        rnd2 = new Random(seed);
    }
    
    public void initMatrix() {
        try {
            BufferedReader dictionary = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(dicFileName), "UTF-8"));
            String line;
            while ((line = dictionary.readLine()) != null) {
                String data[] = line.split(SEPARATOR);
                int celebrityId = Integer.parseInt(data[0]);
                int topicId = Integer.parseInt(data[1]);
                double cumuluative = Double.parseDouble(data[2]);
                
                vecCumulative.get(celebrityId).add(cumuluative);
                vecTopicID.get(celebrityId).add(topicId);
            }
            dictionary.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    // Combine the main tag and related tags
    public TreeSet<Integer> getSetofTags(int celebrityId, int numTags){
        TreeSet<Integer> resultTags = new TreeSet<Integer>();
        resultTags.add(celebrityId);
        
        while (resultTags.size() < numTags) {
            int tagId; 
            tagId = celebrityId; 
            
            while (vecTopicID.get(tagId).size() == 0) {
                tagId = rnd.nextInt(vecTopicID.size());
            }

            // Doing binary search for finding the tag
            double randomDis = rnd2.nextDouble(); 
            int lowerBound = 0;
            int upperBound = vecTopicID.get(tagId).size();
            int midPoint = (upperBound + lowerBound)  / 2;

            while (upperBound > (lowerBound+1)){
                if (vecCumulative.get(tagId).get(midPoint) > randomDis ){
                    upperBound = midPoint;
                } else{
                    lowerBound = midPoint; 
                }
                midPoint = (upperBound + lowerBound)  / 2;
            }
            
            resultTags.add(vecTopicID.get(tagId).get(midPoint));
        }
        
        return resultTags;    
    }
}
