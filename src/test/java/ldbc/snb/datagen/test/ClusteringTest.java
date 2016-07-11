package ldbc.snb.datagen.test;

import javafx.util.Pair;
import ldbc.snb.datagen.generator.distribution.EmpiricalDistribution;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

import java.io.*;
import java.util.*;

/**
 * Created by aprat on 6/07/16.
 */
public class ClusteringTest {

    //private int graphSize = 317080;
    //private int graphSize = 5242;
    private int graphSize = 20;
    private int communitySize = 10;
    private int groupingFactor = 2;
    private RoaringBitmap [] adjacencyMatrix  = new RoaringBitmap[graphSize];
    private RoaringBitmap nodesWithStubs = new RoaringBitmap();
    private Set<Pair<Integer,Integer>> blackList = new HashSet<Pair<Integer,Integer>>();
    long [] degrees;
    long [] triangles;
    long [] currentTriangles;

    public int BinarySearch(ArrayList<Pair<Long,Double>> array, Long degree) {

        int min = 0;
        int max = array.size();
        while(min <= max) {
            int midPoint = (max - min) / 2 + min;
            if(array.get(midPoint).getKey() > degree ) {
                max = midPoint - 1;
            } else if(array.get(midPoint).getKey() < degree) {
                min = midPoint + 1;
            } else {
                return midPoint;
            }
        }
        return max;
    }

    public RoaringBitmap extractBlock(int low, int high) {
        RoaringBitmap block = new RoaringBitmap();
        for(int i = low; i < high; ++i) {
            if(degrees[i] > adjacencyMatrix[i].getCardinality() || triangles[i] > currentTriangles[i]) {
                block.add(i);
            }
        }
        return block;
    }

    public void generateEdges(RoaringBitmap block) {
        Random random = new Random();
        for(int i = 0; i < block.getCardinality(); ++i) {
            Integer nodeA = block.select(i);
            for(int j = i+1; j < block.getCardinality() && j < 10000 && degrees[nodeA] - adjacencyMatrix[nodeA].getCardinality() > 0 ;j++) {
                Integer nodeB = block.select(j);
                if( degrees[nodeB]-adjacencyMatrix[nodeB].getCardinality() > 0 && !adjacencyMatrix[nodeA].contains(nodeB)) {
                    double randProb = random.nextDouble();
                    double prob = Math.pow(0.95, j - i);
                    if ((randProb < prob) || (randProb < 0.2)) {
                        adjacencyMatrix[nodeA].add(nodeB);
                        adjacencyMatrix[nodeB].add(nodeA);
                    }
                }
            }
        }
    }

    public int attemptTriangle(int nodeA, int nodeB, int nodeC) {

        if (currentTriangles[nodeA] + 2 > triangles[nodeA]) return 0;
        if (currentTriangles[nodeB] + 2 > triangles[nodeB]) return 0;
        if (currentTriangles[nodeC] + 2 > triangles[nodeC]) return 0;

        HashMap<Integer,Integer> newTriangles = new HashMap<Integer,Integer>();
        newTriangles.put(nodeA,2);
        newTriangles.put(nodeB,2);
        newTriangles.put(nodeC,2);
        HashMap<Integer,Integer> newEdges = new HashMap<Integer,Integer>();
        newEdges.put(nodeA,0);
        newEdges.put(nodeB,0);
        newEdges.put(nodeC,0);
        ArrayList<Pair<Integer,Integer>> edges = new ArrayList<Pair<Integer,Integer>>();
        edges.add(new Pair<Integer,Integer>(nodeA,nodeB));
        edges.add(new Pair<Integer,Integer>(nodeA,nodeC));
        edges.add(new Pair<Integer,Integer>(nodeB,nodeC));
        int numTriangles = 0;
        for(Pair<Integer,Integer> p : edges) {
            int node1 = p.getKey();
            int node2 = p.getValue();
            if (!adjacencyMatrix[node1].contains(node2)) {
                Iterator<Integer> iter = RoaringBitmap.and(adjacencyMatrix[node1], adjacencyMatrix[node2]).iterator();
                while (iter.hasNext()) {
                    int node = iter.next();
                    if(!newTriangles.containsKey(node)) {
                        newTriangles.put(node,0);
                    }

                    if(currentTriangles[node]+2 > triangles[node]) {
                        blackList.add(new Pair<Integer,Integer>(node1,node2));
                        return 0;
                    }
                    int currentAdded = newTriangles.get(node)+2;
                    if (currentTriangles[node] + currentAdded > triangles[node]) return 0;
                    newTriangles.put(node,currentAdded);

                    currentAdded = newTriangles.get(node1)+2;
                    if (currentTriangles[node1] + currentAdded > triangles[node1]) return 0;

                    currentAdded = newTriangles.get(node2)+2;
                    if (currentTriangles[node2] + currentAdded > triangles[node2]) return 0;
                }
                int currentAdded1  = newEdges.get(node1)+1;
                int currentAdded2  = newEdges.get(node2)+1;
                if ((adjacencyMatrix[node1].getCardinality() + currentAdded1) > degrees[node1] ||
                        adjacencyMatrix[node2].getCardinality() + currentAdded2 > degrees[node2])
                    return 0;
                newEdges.put(node1,currentAdded1);
                newEdges.put(node2,currentAdded2);
            }
        }
        adjacencyMatrix[nodeA].add(nodeB);
        adjacencyMatrix[nodeB].add(nodeA);
        adjacencyMatrix[nodeB].add(nodeC);
        adjacencyMatrix[nodeC].add(nodeB);
        adjacencyMatrix[nodeA].add(nodeC);
        adjacencyMatrix[nodeC].add(nodeA);
        for(Map.Entry<Integer,Integer> p : newTriangles.entrySet()) {
            currentTriangles[p.getKey()]+=p.getValue();
            numTriangles+=p.getValue();
        }
        if(adjacencyMatrix[nodeA].getCardinality() >= degrees[nodeA] || currentTriangles[nodeA] >= triangles[nodeA]) nodesWithStubs.remove(nodeA);
        if(adjacencyMatrix[nodeB].getCardinality() >= degrees[nodeB] || currentTriangles[nodeB] >= triangles[nodeB]) nodesWithStubs.remove(nodeB);
        if(adjacencyMatrix[nodeC].getCardinality() >= degrees[nodeC] || currentTriangles[nodeC] >= triangles[nodeC]) nodesWithStubs.remove(nodeC);
        return numTriangles;
    }

    public int generateTriangles(RoaringBitmap block) {
        int closedTriangles = 0;
        Random random = new Random();
        long min = block.select(0);
        long max = block.select(block.getCardinality()-1);
        for(int i = 0; i < block.getCardinality(); ++i) {
            Integer nodeA = block.select(i);
            RoaringBitmap adjacencyListA = adjacencyMatrix[nodeA];
            RoaringBitmap cAdjacencyListA = RoaringBitmap.flip(adjacencyListA,min,max+1);
            cAdjacencyListA.and(block);
            cAdjacencyListA.and(nodesWithStubs);
            cAdjacencyListA.remove(nodeA);
            while(cAdjacencyListA.getCardinality() > 2 && nodesWithStubs.contains(nodeA)) {
                int indexB = random.nextInt(cAdjacencyListA.getCardinality());
                int nodeB = cAdjacencyListA.select(indexB);
                if((nodeA < nodeB) && nodesWithStubs.contains(nodeB) && !blackList.contains(new Pair<Integer,Integer>(nodeA,nodeB))) {
                    RoaringBitmap adjacencyListB = adjacencyMatrix[nodeB];
                    RoaringBitmap cAdjacencyListB = RoaringBitmap.flip(adjacencyListB, min, max);
                    cAdjacencyListB.and(block);
                    cAdjacencyListB.and(nodesWithStubs);
                    cAdjacencyListB.remove(nodeA);
                    cAdjacencyListB.remove(nodeB);
                    cAdjacencyListB.andNot(adjacencyListA);
                    while (cAdjacencyListB.getCardinality() > 1 && nodesWithStubs.contains(nodeB) && nodesWithStubs.contains(nodeA)) {
                        int indexC = random.nextInt(cAdjacencyListB.getCardinality());
                        int nodeC = cAdjacencyListB.select(indexC);
                        if ((nodeB < nodeC) && nodesWithStubs.contains(nodeC) && !blackList.contains(new Pair<Integer,Integer>(nodeA,nodeC)) &&
                                !blackList.contains(new Pair<Integer,Integer>(nodeB,nodeC))) {
                            closedTriangles+=attemptTriangle(nodeA,nodeB,nodeC);
                        }
                        cAdjacencyListB.remove(nodeC);
                        cAdjacencyListA.remove(nodeC);
                    }
                }
                cAdjacencyListA.remove(nodeB);
            }
        }
        return closedTriangles;
    }

    @Test
    public void clusteringTest() {

        degrees = new long[graphSize];
        triangles = new long[graphSize];
        currentTriangles = new long[graphSize];
        adjacencyMatrix  = new RoaringBitmap[graphSize];
        for(int i = 0; i < graphSize; ++i) {
            adjacencyMatrix[i] = new RoaringBitmap();
            currentTriangles[i] = 0;
        }

        EmpiricalDistribution degreeGen = new EmpiricalDistribution();
        Configuration conf = new Configuration();
        conf.set("ldbc.snb.datagen.generator.distribution.EmpiricalDistribution.fileName","/distributions/relcollab.dat");
        conf.set("ldbc.snb.datagen.generator.numPersons",Integer.toString(graphSize));
        degreeGen.initialize(conf);


        /** Initializing degree sequence **/
        int numEdges = 0;
        for(int i = 0; i < graphSize; ++i) {
            degrees[i] = degreeGen.nextDegree();
            numEdges+=degrees[i];
        }
        numEdges/=2;
        System.out.println("Expected number of edges "+numEdges);

        /** Initializing the array of triangles **/
        ArrayList<Pair<Long,Double>> cc = new ArrayList<Pair<Long,Double>>();
        try {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(getClass().getResourceAsStream("/distributions/relcollab_cc.dat"), "UTF-8"));
            String line;
            while ((line = reader.readLine()) != null) {
                String data[] = line.split(" ");
                cc.add(new Pair<Long, Double>(Long.parseLong(data[0]), Double.parseDouble(data[1])));
            }
            reader.close();
        } catch( IOException e) {
           e.printStackTrace();
        }

        for(int i = 0; i < graphSize; ++i) {
            long degree = degrees[i];
            int pos = BinarySearch(cc,degree);
            if(cc.get(pos).getKey() == degree || pos == (cc.size() - 1)) {
                triangles[i] = (long)(Math.ceil(cc.get(pos).getValue() * degree * (degree - 1)));
            } else if( pos < cc.size() - 1 ){
                long minDegree = cc.get(pos).getKey();
                long maxDegree = cc.get(pos+1).getKey();
                double ratio = (degree - minDegree) / (maxDegree - minDegree);
                double minCC = cc.get(pos).getValue();
                double maxCC = cc.get(pos+1).getValue();
                double cc_current = ratio * (maxCC - minCC ) + minCC;
                triangles[i] = (long)(Math.ceil(cc_current * degree * (degree-1)));
            }
        }

        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("./test_graph.csv")));
        }catch(IOException e) {
            e.printStackTrace();
        }


        int numDegreeZero = 0;
        for(int i = 0; i < graphSize; ++i ) {
            if (degrees[i] > 1 && triangles[i] > 0) nodesWithStubs.add(i);
            if (degrees[i] == 0) numDegreeZero++;
            System.out.println(degrees[i]+" "+triangles[i]);
        }
        System.out.println("Number of nodes with degree zero: "+numDegreeZero);

        long totalTimeTriangles = 0;
        int iter = 0;
        int currentBlockSize = (int)(communitySize*Math.pow(groupingFactor,iter));
        while(true) {
            System.out.println("Generating Triangles for block size: "+currentBlockSize);
            long start = System.currentTimeMillis();
            int closedTriangles = 0;
            int numBlocks = graphSize / currentBlockSize + 1;
            for(int i = 0; i < numBlocks-1; ++i) {
                int low = i*currentBlockSize;
                int high = Math.min((i+1)*currentBlockSize,graphSize);
                RoaringBitmap block = extractBlock(low,high);
                closedTriangles += generateTriangles(block);
            }
            long end = System.currentTimeMillis();
            System.out.println("Time to generate triangles "+(end-start));
            System.out.println("Number of closed triangles this iteration "+closedTriangles);
            totalTimeTriangles+=(end-start);
            if(currentBlockSize >= graphSize) break;
            iter++;
            currentBlockSize = Math.min((int)(communitySize*Math.pow(groupingFactor,iter)),graphSize);
        }
        System.out.println("Total time to generate triangles: "+totalTimeTriangles);

        iter = 0;
        currentBlockSize = (int)(communitySize*Math.pow(groupingFactor,iter));
        while(true) {
            System.out.println("Generating Edges for block size: "+currentBlockSize);
            long start = System.currentTimeMillis();
            int numBlocks = graphSize / currentBlockSize + 1;
            for(int i = 0; i < numBlocks-1; ++i) {
                int low = i*currentBlockSize;
                int high = Math.min((i+1)*currentBlockSize,graphSize);
                RoaringBitmap block = extractBlock(low,high);
                generateEdges(block);
            }
            long end = System.currentTimeMillis();
            System.out.println("Time to generate edges "+(end-start));
            if(currentBlockSize >= graphSize) break;
            iter++;
            currentBlockSize = Math.min((int)(communitySize*Math.pow(groupingFactor,iter)),graphSize);
        }

        try {
            int belowDegree = 0;
            int belowTriangles = 0;
            int aboveDegree = 0;
            int aboveTriangles = 0;
            for (int i = 0; i < graphSize; ++i) {
                if( adjacencyMatrix[i].getCardinality() > degrees[i] ) {
                    //System.out.println("Node "+i+" has a degree of "+adjacencyMatrix[i].getCardinality()+" while it should have "+degrees[i]);
                    aboveDegree++;
                }

                if( currentTriangles[i] > triangles[i] ) {
                    //System.out.println("Node "+i+" has "+currentTriangles[i]+" while it should have "+triangles[i]+" and degree "+degrees[i]);
                    aboveTriangles++;
                }

                if( triangles[i] > currentTriangles[i] ) {
                    //System.out.println("Node "+i+" has "+currentTriangles[i]+" triangles while it should have "+triangles[i]+" triangles");
                    belowTriangles++;
                }

                if( degrees[i] > adjacencyMatrix[i].getCardinality() ) {
                    //System.out.println("Node "+i+" has degree "+adjacencyMatrix[i].getCardinality()+" while it should have "+degrees[i]+" degree");
                    belowDegree++;
                }
                Iterator<Integer> it = adjacencyMatrix[i].iterator();
                while (it.hasNext()) {
                    int next = it.next();
                    if(i < next)
                        writer.write(i + " " + next+"\n");
                }
            }
            System.out.println("Number of nodes aboveDegree "+aboveDegree);
            System.out.println("Number of nodes aboveTriangles "+aboveTriangles);
            System.out.println("Number of nodes without sufficient degree "+belowDegree);
            System.out.println("Number of nodes without sufficient triangles "+belowTriangles);
            writer.flush();
            writer.close();
        }catch(IOException e) {
            e.printStackTrace();
        }
    }
}
