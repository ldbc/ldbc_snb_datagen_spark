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
import java.util.*;

public class TagMatrix {
    
    private static final String SEPARATOR = " ";
    
    String dicFileName;
    
    ArrayList<ArrayList<Double>> vecCumulative;
    ArrayList<ArrayList<Integer>> vecTopicID;
    TreeMap<Integer,ArrayList<Integer>>  auxMatrix;
    ArrayList<Integer> tagList;
    
    public TagMatrix(String dicFileName, int numCelebrities){
        this.dicFileName = dicFileName;
        vecCumulative = new ArrayList<ArrayList<Double>>(numCelebrities);
        vecTopicID    = new ArrayList<ArrayList<Integer>>(numCelebrities);
        for (int i =  0; i < numCelebrities; i++){
            vecCumulative.add(new ArrayList<Double>());
            vecTopicID.add(new ArrayList<Integer>());
        }
        auxMatrix = new TreeMap<Integer,ArrayList<Integer>>();
        tagList = new ArrayList<Integer>();
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
                Insert(celebrityId,topicId);
            }
            dictionary.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void Insert(Integer tag1, Integer tag2) {
        ArrayList<Integer> vect = auxMatrix.get(tag1);
        if( vect == null ) {
           vect = new ArrayList<Integer> ();
           auxMatrix.put(tag1,vect);
           tagList.add(tag1);
        }
        vect.add(tag2);

        vect = auxMatrix.get(tag2);
        if( vect == null ) {
            vect = new ArrayList<Integer> ();
            auxMatrix.put(tag2,vect);
            tagList.add(tag1);
        }
        vect.add(tag1);
    }

    public Integer getRandomRelated( Random randomTopic, Random randomTag, int tag) {
        ArrayList<Integer> vect = auxMatrix.get(tag);
        if( vect != null ) {
            int index = randomTag.nextInt(vect.size());
            return vect.get(index);
        } else {
            return tagList.get(randomTag.nextInt(tagList.size()));
        }
    }
    
    // Combine the main tag and related tags
    public TreeSet<Integer> getSetofTags(Random randomTopic, Random randomTag, int celebrityId, int numTags){
        TreeSet<Integer> resultTags = new TreeSet<Integer>();
        resultTags.add(celebrityId);
        while (resultTags.size() < numTags) {
            int tagId; 
            tagId = celebrityId; 
            
            while (vecTopicID.get(tagId).size() == 0) {
                tagId = randomTopic.nextInt(vecTopicID.size());
            }

            // Doing binary search for finding the tag
            double randomDis = randomTag.nextDouble();
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
