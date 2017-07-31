/* 
 Copyright (c) 2013 LDBC
 Linked Data Benchmark Council (http://www.ldbcouncil.org)
 
 This file is part of ldbc_snb_datagen.
 
 ldbc_snb_datagen is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.
 
 ldbc_snb_datagen is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with ldbc_snb_datagen.  If not, see <http://www.gnu.org/licenses/>.
 
 Copyright (C) 2011 OpenLink Software <bdsmt@openlinksw.com>
 All Rights Reserved.
 
 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation;  only Version 2 of the License dated
 June 1991.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.*/
package ldbc.snb.datagen.generator;

import ldbc.snb.datagen.objects.Knows;
import ldbc.snb.datagen.objects.Person;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.roaringbitmap.RoaringBitmap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by aprat on 12/07/16.
 */
public class BTERKnowsGenerator implements KnowsGenerator {

    private int graphSize = 0;
    private Random random;
    private Configuration conf;
    private long[] expectedDegree;
    private double[] p;
    private HashMap<Long, RoaringBitmap> openCommunities = new HashMap<Long, RoaringBitmap>();
    private ArrayList<RoaringBitmap> closedCommunities = new ArrayList<RoaringBitmap>();
    private RoaringBitmap smallDegreeNodes = new RoaringBitmap();
    private RoaringBitmap[] adjacencyMatrix;

    public int binarySearch(ArrayList<Pair<Long, Double>> array, Long degree) {
        int min = 0;
        int max = array.size();
        while (min <= max) {
            int midPoint = (max - min) / 2 + min;
            if (midPoint >= array.size()) return array.size() - 1;
            if (midPoint < 0) return 0;
            if (array.get(midPoint).getKey() > degree) {
                max = midPoint - 1;
            } else if (array.get(midPoint).getKey() < degree) {
                min = midPoint + 1;
            } else {
                return midPoint;
            }
        }
        return max;
    }

    private void generateCommunities(RoaringBitmap block) {
        Iterator<Integer> iter = block.iterator();
        while (iter.hasNext()) {
            int node = iter.next();
            RoaringBitmap community = openCommunities.get(expectedDegree[node] + 1);
            if (community != null) {
                community.add(node);
                if (community.getCardinality() >= (expectedDegree[node] + 1)) {
                    openCommunities.remove(expectedDegree[node] + 1);
                    closedCommunities.add(community);
                }
            } else {
                community = new RoaringBitmap();
                community.add(node);
                openCommunities.put(expectedDegree[node] + 1, community);
            }
        }
    }

    private void generateEdgesInCommunity(RoaringBitmap community) {
        Iterator<Integer> iter = community.iterator();
        while (iter.hasNext()) {
            int nodeA = iter.next();
            Iterator<Integer> iter2 = community.iterator();
            while (iter2.hasNext()) {
                int nodeB = iter2.next();
                if (nodeA < nodeB) {
                    double prob = random.nextDouble();
                    if (prob < p[community.getCardinality() - 1]) {
                        adjacencyMatrix[nodeA].add(nodeB);
                        adjacencyMatrix[nodeB].add(nodeA);
                    }
                }
            }
        }
    }

    private void generateRemainingEdges() {
        LinkedList<Integer> stubs = new LinkedList<Integer>();
        for (int i = 0; i < graphSize; ++i) {
            long difference = expectedDegree[i] - adjacencyMatrix[i].getCardinality();
            if (difference > 0) {
                for (int j = 0; j < difference; ++j) {
                    stubs.add(i);
                }
            }
        }
        Collections.shuffle(stubs, random);
        while (!stubs.isEmpty()) {
            int node1 = stubs.get(0);
            stubs.remove(0);
            if (!stubs.isEmpty()) {
                int node2 = stubs.get(0);
                stubs.remove(0);
                if (node1 != node2) {
                    adjacencyMatrix[node1].add(node2);
                    adjacencyMatrix[node2].add(node1);
                }
            }
        }
    }

    @Override
    public void generateKnows(ArrayList<Person> persons, int seed, ArrayList<Float> percentages, int step_index) {

        graphSize = persons.size();
        expectedDegree = new long[graphSize];
        adjacencyMatrix = new RoaringBitmap[graphSize];
        p = new double[graphSize];
        for (int i = 0; i < graphSize; ++i) {
            adjacencyMatrix[i] = new RoaringBitmap();
        }
        random = new Random();
        random.setSeed(seed);
        openCommunities.clear();
        closedCommunities.clear();
        smallDegreeNodes.clear();
        int maxExpectedDegree = 0;
        for (int i = 0; i < graphSize; ++i) {
            adjacencyMatrix[i].clear();
            expectedDegree[i] = Knows.targetEdges(persons.get(i), percentages, step_index);
            maxExpectedDegree = maxExpectedDegree < expectedDegree[i] ? (int) expectedDegree[i] : maxExpectedDegree;
        }
        p = new double[maxExpectedDegree + 1];

        /** Initializing the array of triangles **/
        ArrayList<Pair<Long, Double>> ccDistribution = new ArrayList<Pair<Long, Double>>();
        try {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(getClass()
                                                  .getResourceAsStream(conf.get("ldbc.snb.datagen.generator.BTERKnowsGenerator.ccDistribution")), "UTF-8"));
            String line;
            while ((line = reader.readLine()) != null) {
                String data[] = line.split(" ");
                ccDistribution.add(new Pair<Long, Double>(Long.parseLong(data[0]), Double.parseDouble(data[1])));
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        p[0] = 0.0;
        p[1] = 0.0;
        for (int i = 2; i < maxExpectedDegree + 1; ++i) {
            int degree = i;
            int pos = binarySearch(ccDistribution, (long) degree);
            if (ccDistribution.get(pos).getKey() == degree || pos == (ccDistribution.size() - 1)) {
                p[degree] = ccDistribution.get(pos).getValue();
            } else if (pos < ccDistribution.size() - 1) {
                long minDegree = ccDistribution.get(pos).getKey();
                long maxDegree = ccDistribution.get(pos + 1).getKey();
                double ratio = (degree - minDegree) / (maxDegree - minDegree);
                double minCC = ccDistribution.get(pos).getValue();
                double maxCC = ccDistribution.get(pos + 1).getValue();
                double cc_current = ratio * (maxCC - minCC) + minCC;
                p[degree] = Math.pow(cc_current, 1 / 3.0);
            }
        }

        RoaringBitmap block = new RoaringBitmap();
        for (int i = 0; i < graphSize; ++i) {
            if (expectedDegree[i] > 1) {
                block.add(i);
            } else {
                smallDegreeNodes.add(i);
            }
        }
        generateCommunities(block);

        TreeMap<Long, RoaringBitmap> sortedMap = new TreeMap<Long, RoaringBitmap>(openCommunities);
        RoaringBitmap currentCommunity = null;
        long currentCommunitySize = 0;
        for (Map.Entry<Long, RoaringBitmap> community : sortedMap.entrySet()) {
            RoaringBitmap nextCommunity = community.getValue();
            if (currentCommunity == null) {
                currentCommunity = nextCommunity;
                currentCommunitySize = community.getKey();
            } else {
                while (currentCommunity.getCardinality() <= currentCommunitySize && nextCommunity
                        .getCardinality() > 0) {
                    int nextNode = nextCommunity.select(0);
                    currentCommunity.add(nextNode);
                    nextCommunity.remove(nextNode);
                }
                if (currentCommunity.getCardinality() >= currentCommunitySize) {
                    closedCommunities.add(currentCommunity);
                    currentCommunity = null;
                    currentCommunitySize = 0;
                    if (nextCommunity.getCardinality() > 0) {
                        currentCommunity = nextCommunity;
                        currentCommunitySize = community.getKey();
                    }
                }
            }
        }
        openCommunities.clear();

        for (RoaringBitmap community : closedCommunities) {
            generateEdgesInCommunity(community);
        }

        generateRemainingEdges();

        for (int i = 0; i < graphSize; ++i) {
            Iterator<Integer> it = adjacencyMatrix[i].iterator();
            while (it.hasNext()) {
                int next = it.next();
                Knows.createKnow(random, persons.get(i), persons.get(next));
            }
        }
    }

    @Override
    public void initialize(Configuration conf) {
        this.conf = conf;
    }
}
