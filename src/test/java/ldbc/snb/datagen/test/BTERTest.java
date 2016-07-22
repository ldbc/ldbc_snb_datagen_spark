package ldbc.snb.datagen.test;

import javafx.util.Pair;
import ldbc.snb.datagen.generator.distribution.EmpiricalDistribution;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

import java.io.*;
import java.util.*;

/**
 * Created by aprat on 11/07/16.
 */
public class BTERTest {

    private static int graphSize = 317080;
    //private static int graphSize = 10000;
    private static int blockSize = 10000;
    //private static long [] remainingTriangles = new long[graphSize];
    private static double [] cc = new double[graphSize];
    private static long [] expectedDegree = new long[graphSize];
    private static HashMap<Long,RoaringBitmap> openCommunities = new HashMap<Long,RoaringBitmap>();
    private static ArrayList<RoaringBitmap>  closedCommunities = new ArrayList<RoaringBitmap>();
    private static RoaringBitmap smallDegreeNodes = new RoaringBitmap();
    private RoaringBitmap [] adjacencyMatrix  = new RoaringBitmap[graphSize];

    public int BinarySearch(ArrayList<Pair<Long,Double>> array, Long degree) {

        int min = 0;
        int max = array.size();
        while(min <= max) {
            int midPoint = (max - min) / 2 + min;
            if(midPoint >= array.size()) return array.size()-1;
            if(midPoint < 0) return 0;
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
            if(expectedDegree[i] > 1 ) {
                block.add(i);
            } else {
                smallDegreeNodes.add(i);
            }
        }
        return block;
    }

    void generateCommunities(RoaringBitmap block) {
        Iterator<Integer> iter = block.iterator();
        while(iter.hasNext()) {
            int node = iter.next();
            RoaringBitmap community = openCommunities.get(expectedDegree[node]+1);
            if(community != null) {
                community.add(node);
                if(community.getCardinality() >= (expectedDegree[node]+1)) {
                    openCommunities.remove(expectedDegree[node]+1);
                    closedCommunities.add(community);
                }
            } else {
                community = new RoaringBitmap();
                community.add(node);
                openCommunities.put(expectedDegree[node]+1,community);
            }
        }
    }

    void generateEdgesInCommunity(RoaringBitmap community) {
        Random random = new Random();
        random.setSeed(System.currentTimeMillis());
        Iterator<Integer> iter = community.iterator();
        while(iter.hasNext()) {
            int nodeA = iter.next();
            Iterator<Integer> iter2 = community.iterator();
            while(iter2.hasNext()) {
                int nodeB = iter2.next();
                if(nodeA != nodeB) {
                    double p = random.nextDouble();
                    if(p < cc[community.getCardinality()-1]) {
                        adjacencyMatrix[nodeA].add(nodeB);
                        adjacencyMatrix[nodeB].add(nodeA);
                    }
                }
            }
        }
    }

    void generateRemainingEdges() {
        Random random = new Random();
        random.setSeed(System.currentTimeMillis());
        LinkedList<Integer> stubs = new LinkedList<Integer>();
        for(int i = 0; i < graphSize; ++i) {
            long difference = expectedDegree[i]-adjacencyMatrix[i].getCardinality();
            if( difference > 0) {
               for(int j = 0; j < difference; ++j)  {
                   stubs.add(i);
               }
            }
        }
        Collections.shuffle(stubs);
        while(!stubs.isEmpty()) {
            int node1 = stubs.get(0);
            stubs.remove(0);
            if(!stubs.isEmpty()) {
                int node2 = stubs.get(0);
                stubs.remove(0);
                if(node1 != node2) {
                    adjacencyMatrix[node1].add(node2);
                    adjacencyMatrix[node2].add(node1);
                }
            }
        }
    }

    @Test
    public void bterTest() {

        for(int i = 0; i < graphSize; ++i) {
            adjacencyMatrix[i] = new RoaringBitmap();
            cc[i] = 0.0;
            expectedDegree[i] = 0;
        }

        EmpiricalDistribution degreeGen = new EmpiricalDistribution();
        Configuration conf = new Configuration();
        conf.set("ldbc.snb.datagen.generator.distribution.EmpiricalDistribution.fileName","/distributions/dblp.dat");
        conf.set("ldbc.snb.datagen.generator.numPersons",Integer.toString(graphSize));
        degreeGen.initialize(conf);

        /** Initializing degree sequence **/
        int numEdges = 0;
        for(int i = 0; i < graphSize; ++i) {
            expectedDegree[i] = degreeGen.nextDegree();
            numEdges+=expectedDegree[i];
        }
        numEdges/=2;
        System.out.println("Expected number of edges "+numEdges);

        /** Initializing the array of triangles **/
        ArrayList<Pair<Long,Double>> ccDistribution = new ArrayList<Pair<Long,Double>>();
        try {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(getClass().getResourceAsStream("/distributions/dblp_cc.dat"), "UTF-8"));
            String line;
            while ((line = reader.readLine()) != null) {
                String data[] = line.split(" ");
                ccDistribution.add(new Pair<Long, Double>(Long.parseLong(data[0]), Double.parseDouble(data[1])));
            }
            reader.close();
        } catch( IOException e) {
            e.printStackTrace();
        }

        cc[0] = 0.0;
        cc[1] = 0.0;
        for(int i = 2; i < graphSize; ++i) {
            int degree = i;
            int pos = BinarySearch(ccDistribution,(long)degree);
            if(ccDistribution.get(pos).getKey() == degree || pos == (ccDistribution.size() - 1)) {
                cc[degree] = ccDistribution.get(pos).getValue();
            } else if( pos < ccDistribution.size() - 1 ){
                long minDegree = ccDistribution.get(pos).getKey();
                long maxDegree = ccDistribution.get(pos+1).getKey();
                double ratio = (degree - minDegree) / (maxDegree - minDegree);
                double minCC = ccDistribution.get(pos).getValue();
                double maxCC = ccDistribution.get(pos+1).getValue();
                double cc_current = ratio * (maxCC - minCC ) + minCC;
                cc[degree] = cc_current;
            }
        }

        // GENERATING EDGES INSIDE COMMUNITIES
        int numBlocks = graphSize / blockSize + 1;
        for(int i = 0; i < numBlocks-1; ++i ) {
            System.out.println("Generating communities for block "+i);
            int low = i*blockSize;
            int high = Math.min((i+1)*blockSize,graphSize);
            RoaringBitmap block = extractBlock(low,high);
            generateCommunities(block);
        }

        for(HashMap.Entry<Long,RoaringBitmap> community : openCommunities.entrySet()) {
           closedCommunities.add(community.getValue());
        }
        openCommunities.clear();

        System.out.println("Generating edges inside communities");
        for(RoaringBitmap community : closedCommunities) {
            System.out.println("Size: "+community.getCardinality()+" nodes: ");
            for(int i = 0; i < community.getCardinality(); ++i) {
                int node = community.select(i);
                System.out.println(node+" "+expectedDegree[node]+" "+cc[(int)expectedDegree[node]]);
            }
            System.out.println("\n");
            generateEdgesInCommunity(community);
        }

        System.out.println("Generating edges outside communities");
        // GENERATING REMIANING EDGES
        generateRemainingEdges();

        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("./test_graph.csv")));
        }catch(IOException e) {
            e.printStackTrace();
        }
        try {
            int belowDegree = 0;
            //int belowTriangles = 0;
            int aboveDegree = 0;
            //int aboveTriangles = 0;
            for (int i = 0; i < graphSize; ++i) {
                if( adjacencyMatrix[i].getCardinality() > expectedDegree[i] ) {
                    //System.out.println("Node "+i+" has a degree of "+adjacencyMatrix[i].getCardinality()+" while it should have "+degrees[i]);
                    aboveDegree++;
                }

                if( expectedDegree[i] > adjacencyMatrix[i].getCardinality() ) {
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
            //System.out.println("Number of nodes aboveTriangles "+aboveTriangles);
            System.out.println("Number of nodes without sufficient degree "+belowDegree);
            //System.out.println("Number of nodes without sufficient triangles "+belowTriangles);
            writer.flush();
            writer.close();
        }catch(IOException e) {
            e.printStackTrace();
        }
    }
}
