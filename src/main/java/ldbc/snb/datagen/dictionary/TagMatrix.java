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
package ldbc.snb.datagen.dictionary;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

public class TagMatrix {

    private static final String SEPARATOR = " ";

    private ArrayList<ArrayList<Integer>> relatedTags;
    /**
     * < @brief An array of related tags per tag.
     */
    private ArrayList<ArrayList<Double>> cumulative;
    /**
     * < @brief The cumulative distribution to pick a tag.
     */
    private TreeMap<Integer, ArrayList<Integer>> auxMatrix;
    /**
     * < @brief Left because it works, but I think this is reduntant.
     */
    private ArrayList<Integer> tagList;

    /**
     * < @brief The list of tags.
     */

    public TagMatrix(int numPopularTags) {
        cumulative = new ArrayList<ArrayList<Double>>(numPopularTags);
        relatedTags = new ArrayList<ArrayList<Integer>>(numPopularTags);
        for (int i = 0; i < numPopularTags; i++) {
            cumulative.add(new ArrayList<Double>());
            relatedTags.add(new ArrayList<Integer>());
        }
        auxMatrix = new TreeMap<Integer, ArrayList<Integer>>();
        tagList = new ArrayList<Integer>();
    }

    /**
     * @param tagMatrixFileName The tag matrix file name.
     * @brief Loads the tag matrix from a file.
     */
    public void load(String tagMatrixFileName) {
        try {
            BufferedReader dictionary = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(tagMatrixFileName), "UTF-8"));
            String line;
            while ((line = dictionary.readLine()) != null) {
                String data[] = line.split(SEPARATOR);
                int celebrityId = Integer.parseInt(data[0]);
                int topicId = Integer.parseInt(data[1]);
                double cumuluative = Double.parseDouble(data[2]);
                cumulative.get(celebrityId).add(cumuluative);
                relatedTags.get(celebrityId).add(topicId);
                Insert(celebrityId, topicId);
            }
            dictionary.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @param tag1 The first tag id.
     * @param tag2 The second tag id.
     * @brief Inserts a tag matrix position into the dictionary.
     */
    private void Insert(Integer tag1, Integer tag2) {
        ArrayList<Integer> vect = auxMatrix.get(tag1);
        if (vect == null) {
            vect = new ArrayList<Integer>();
            auxMatrix.put(tag1, vect);
            tagList.add(tag1);
        }
        vect.add(tag2);

        vect = auxMatrix.get(tag2);
        if (vect == null) {
            vect = new ArrayList<Integer>();
            auxMatrix.put(tag2, vect);
            tagList.add(tag1);
        }
        vect.add(tag1);
    }

    /**
     * @param randomTag The random tag number generator.
     * @param tag       The tag identifier.
     * @return The related tag identifier.
     * @brief Gets a random related tag.
     */
    public Integer getRandomRelated(Random randomTag, int tag) {
        ArrayList<Integer> vect = auxMatrix.get(tag);
        if (vect != null) {
            int index = randomTag.nextInt(vect.size());
            return vect.get(index);
        } else {
            return tagList.get(randomTag.nextInt(tagList.size()));
        }
    }

    /**
     * @param randomTopic  The random number generator used to select aditional popular tags
     * @param randomTag    The random number generator used to select related tags.
     * @param popularTagId The popular tag identifier.
     * @param numTags      The number of related tags to retrieve.
     * @return The set of related tags.
     * @brief Get a set of related tags.
     */
    public TreeSet<Integer> getSetofTags(Random randomTopic, Random randomTag, int popularTagId, int numTags) {
        TreeSet<Integer> resultTags = new TreeSet<Integer>();
        resultTags.add(popularTagId);
        while (resultTags.size() < numTags) {
            int tagId;
            tagId = popularTagId;

            while (relatedTags.get(tagId).size() == 0) {
                tagId = randomTopic.nextInt(relatedTags.size());
            }

            // Doing binary search for finding the tag
            double randomDis = randomTag.nextDouble();
            int lowerBound = 0;
            int upperBound = relatedTags.get(tagId).size();
            int midPoint = (upperBound + lowerBound) / 2;

            while (upperBound > (lowerBound + 1)) {
                if (cumulative.get(tagId).get(midPoint) > randomDis) {
                    upperBound = midPoint;
                } else {
                    lowerBound = midPoint;
                }
                midPoint = (upperBound + lowerBound) / 2;
            }
            resultTags.add(relatedTags.get(tagId).get(midPoint));
        }
        return resultTags;
    }
}
