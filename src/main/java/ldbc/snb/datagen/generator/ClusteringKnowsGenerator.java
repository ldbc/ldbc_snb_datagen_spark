package ldbc.snb.datagen.generator;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.generator.tools.GraphUtils;
import ldbc.snb.datagen.generator.tools.MinHash;
import ldbc.snb.datagen.generator.tools.PersonGraph;
import ldbc.snb.datagen.objects.Knows;
import ldbc.snb.datagen.objects.Person;
import ldbc.snb.datagen.util.RandomGeneratorFarm;

import java.util.*;

/**
 * Created by aprat on 11/15/14.
 */
public class ClusteringKnowsGenerator implements KnowsGenerator {

    DistanceKnowsGenerator distanceKnowsGenerator_;
    private double targetCC_ = 0.5;
    private int maxIterations_ = 100;
    private int numMinHashes_ = 10;
    private Map<Long, Integer> personPosition;

    public ClusteringKnowsGenerator() {
        this.distanceKnowsGenerator_ = new DistanceKnowsGenerator();
    }

    public class MinHashComparator implements Comparator<MinHashTuple> {
        private int function_;
        MinHashComparator(int function) {
            function_ = function;
        }
        @Override
        public int compare(MinHashTuple o1, MinHashTuple o2) {
            long a =  o1.minHashes_.get(function_);
            long b =  o1.minHashes_.get(function_);
            if( a < b ) return -1;
            if( a > b ) return 1;
            return 0;
        }
    }

    protected  class MinHashTuple {
        public long index_;
        ArrayList<Long> minHashes_;
        MinHashTuple( long index, ArrayList<Long> minHashes ){
            index_ = index;
            minHashes_ = minHashes;
        }
    }

    protected class PersonTuple implements Comparable<PersonTuple> {
        public long a_;
        public long b_;
        public double score_;

        PersonTuple(long a, long b, double score) {
            a_=Math.min(a,b);
            b_=Math.max(a,b);
            score_ = score;
        }

        @Override
        public int compareTo(PersonTuple b) {
            if( score_ < b.score_) return 1;
            if( score_ > b.score_) return -1;
            if( a_ < b.a_) return -1;
            if( a_ > b.a_) return 1;
            if( b_ < b.b_) return -1;
            if( b_ > b.b_) return 1;
            return 0;
        }
    }

    public void generateKnows( ArrayList<Person> persons, int seed, float upperBound )  {
        Random random = new Random();
        Map<Long, Integer> personPosition = new HashMap<Long, Integer>();
        for( int i = 0; i < persons.size(); ++i ) {
            personPosition.put(persons.get(i).accountId(), i);
        }
        distanceKnowsGenerator_.generateKnows(persons,seed,upperBound);
        PersonGraph bestGraph = new PersonGraph(persons);
        MinHash minHash = new MinHash(numMinHashes_, 0);
        double bestCC = GraphUtils.ClusteringCoefficient(bestGraph);
        int numIterations = 0;
        ArrayList<MinHashTuple> minHashes = new ArrayList<MinHashTuple>();
        PersonGraph currentGraph = new PersonGraph(bestGraph);
        double currentCC = bestCC;
        while(bestCC < targetCC_ && numIterations < maxIterations_){
            System.out.println("Starting Refining Iteration "+bestCC+" "+currentCC+" "+numIterations);
            minHashes.clear();
            for(Long l : currentGraph.persons()) {
                minHashes.add(new MinHashTuple(l, minHash.minHash(currentGraph.neighbors(l))));
            }
            PriorityQueue<PersonTuple> pq = new PriorityQueue<PersonTuple>();
            for(int k = 0; k < numMinHashes_; ++k ) {
                Collections.sort(minHashes, new MinHashComparator(k));
                for(int i = 0; i < minHashes.size() - 1; ++i) {
                    long personA = minHashes.get(i).index_;
                    long personB = minHashes.get(i+1).index_;
                    double score = computeScore(currentGraph,personA,personB);
                    if(score > 0.5) {
                        pq.add(new PersonTuple(personA, personB, score));
                    }
                }
            }
            random.setSeed(numIterations);
            HashSet<Long> touched = new HashSet<Long>();
            while(pq.size() > 0) {
                PersonTuple t = pq.poll();
                long personA = t.a_;
                long personB = t.b_;
                HashSet<Long> candidatesA = new HashSet<Long>(currentGraph.neighbors(personA));
                HashSet<Long> candidatesB = new HashSet<Long>(currentGraph.neighbors(personB));
                HashSet<Long> intersection = new HashSet<Long>(candidatesA);
                intersection.retainAll(candidatesB);
                candidatesA.removeAll(intersection);
                candidatesB.removeAll(intersection);
                if(candidatesA.size() > 0 && candidatesB.size() > 0) {
                    List<Long> candidatesListA = new ArrayList<Long>(candidatesA);
                    List<Long> candidatesListB = new ArrayList<Long>(candidatesB);
                    Collections.shuffle(candidatesListA,random);
                    Collections.shuffle(candidatesListB,random);
                    long kq = candidatesListA.get(random.nextInt(candidatesListA.size()));
                    long kt = candidatesListB.get(random.nextInt(candidatesListB.size()));
                    if( !touched.contains(personA) &&
                        !touched.contains(personB) &&
                        !touched.contains(kq) &&
                        !touched.contains(kt)
                            ) {
                        touched.add(personA);
                        touched.add(personB);
                        touched.add(kq);
                        touched.add(kt);
                        /*HashSet<Long> auxA = new HashSet<Long>(currentGraph.neighbors(personA));
                        auxA.retainAll(currentGraph.neighbors(kq));
                        long degreeAuxA = Math.min(currentGraph.neighbors(personA).size(), currentGraph.neighbors(kq).size());
                        double scoreAq =  0.0;
                        if(degreeAuxA > 1)
                            scoreAq = (2*auxA.size()) / (double)(degreeAuxA*(degreeAuxA-1));

                        HashSet<Long> auxB = new HashSet<Long>(currentGraph.neighbors(personB));
                        long degreeAuxB = Math.min(currentGraph.neighbors(personB).size(), currentGraph.neighbors(kt).size());
                        auxB.retainAll(currentGraph.neighbors(kt));
                        double scoreBt =  0.0;
                        if(degreeAuxB > 1)
                            scoreBt = (2*auxA.size()) / (double)(degreeAuxB*(degreeAuxB-1));
                        if( scoreBt > bestCC && scoreAq > bestCC)
                        */
                            rewire(currentGraph, t.a_, t.b_, kq, kt);
                    }
                }
            }
            currentCC = GraphUtils.ClusteringCoefficient(currentGraph);
            numIterations++;
            if(currentCC > bestCC ) {
                numIterations = 0;
                bestGraph = currentGraph;
                bestCC = currentCC;
            }
        }

        for(Person p : persons) {
            p.knows().clear();
        }
        for(Long l : bestGraph.persons()) {
            Person p = persons.get(personPosition.get(l));
            Set<Long> neighbors = bestGraph.neighbors(l);
            for( Long n: neighbors) {
                if( l < n ) {
                    Person other = persons.get(personPosition.get(n));
                    p.knows().add(new Knows(other, 0, 0));
                    other.knows().add(new Knows(p, 0, 0));
                }
            }
        }
    }

    void rewire(PersonGraph graph, long i , long j, long q, long t) {
        graph.neighbors(i).add(j);
        graph.neighbors(j).add(i);
        graph.neighbors(i).remove(q);
        graph.neighbors(q).remove(i);
        graph.neighbors(j).remove(t);
        graph.neighbors(t).remove(j);
        graph.neighbors(q).add(t);
        graph.neighbors(t).add(q);
    }

    double computeScore(PersonGraph graph, long personA, long personB){
        if(graph.neighbors(personA).contains(personB)) return 0.0;
        HashSet<Long> intersection = new HashSet<Long>(graph.neighbors(personA));
        intersection.retainAll(graph.neighbors(personB));
        if(intersection.size() == 0) return 0.0;
        int degreeA = graph.neighbors(personA).size();
        double score = 0.0;
        if(degreeA > 1)
            score += intersection.size() / (double) (degreeA * (degreeA - 1));
        int degreeB = graph.neighbors(personB).size();
        if(degreeB > 1)
            score += intersection.size() / (double) (degreeB * (degreeB - 1));
        return score;
    }

}
