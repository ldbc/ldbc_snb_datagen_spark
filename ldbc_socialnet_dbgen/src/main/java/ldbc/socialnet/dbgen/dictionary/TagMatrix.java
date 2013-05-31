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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Vector;
import java.util.Random;

public class TagMatrix {
    
    BufferedReader dictionary;
    String dicFileName;
    
    Vector < Vector<Double> > vecCumulative;
    Vector < Vector <Integer>> vecTopicID;
    
    Random  rnd;
    Random  rnd2;
    
    public TagMatrix(String _dicFileName, int _numCelebrities, long seed){
        this.dicFileName = _dicFileName;
        vecCumulative = new Vector<Vector<Double>>(_numCelebrities);
        vecTopicID = new Vector<Vector<Integer>>(_numCelebrities);
        
        for (int i =  0; i < _numCelebrities; i++){
            vecCumulative.add(new Vector<Double>());
            vecTopicID.add(new Vector<Integer>());
        }
        
        rnd = new Random(seed);
        rnd2 = new Random(seed);
    }
    
    public void initMatrix() {
        try {
            dictionary = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(dicFileName), "UTF-8"));
            String line;
            while ((line = dictionary.readLine()) != null){
                String infos[] = line.split(" ");
                int celebrityId = Integer.parseInt(infos[0]);
                int topicId = Integer.parseInt(infos[1]);
                double cumuluative = Double.parseDouble(infos[2]);
                
                vecCumulative.get(celebrityId).add(cumuluative);
                vecTopicID.get(celebrityId).add(topicId);
            }
            dictionary.close();
            	
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    // Combine the main tag and related tags
    
    public HashSet<Integer> getSetofTags(int _celebrityId, int numTags){
        HashSet<Integer> resultTags = new HashSet<Integer>();
        resultTags.add(_celebrityId);
        
        while (resultTags.size() < numTags) {
            int tagId; 
            tagId = _celebrityId; 

            if (vecTopicID.get(tagId).size() == 0){
                //Randomly get from other country.
                do {
                    tagId = rnd.nextInt(vecTopicID.size());
                } while (vecTopicID.get(tagId).size() == 0);
            }


            // Doing binary search for finding the tag
            double randomDis = rnd2.nextDouble(); 
            int lowerBound = 0;
            int upperBound = vecTopicID.get(tagId).size(); 

            int curIdx = (upperBound + lowerBound)  / 2;

            while (upperBound > (lowerBound+1)){
                if (vecCumulative.get(tagId).get(curIdx) > randomDis ){
                    upperBound = curIdx;
                }
                else{
                    lowerBound = curIdx; 
                }
                curIdx = (upperBound + lowerBound)  / 2;
            }
            
            resultTags.add(vecTopicID.get(tagId).get(curIdx));
        }
        
        return resultTags;    
    }
}
