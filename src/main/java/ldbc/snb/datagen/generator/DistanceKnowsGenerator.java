package ldbc.snb.datagen.generator;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.objects.Knows;
import ldbc.snb.datagen.objects.Person;
import ldbc.snb.datagen.util.RandomGeneratorFarm;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;

/**
 * Created by aprat on 11/15/14.
 */
public class DistanceKnowsGenerator implements KnowsGenerator {

    private RandomGeneratorFarm randomFarm;

    public DistanceKnowsGenerator() {
        this.randomFarm = new RandomGeneratorFarm();
    }

    public void generateKnows( ArrayList<Person> persons, int seed, ArrayList<Float> percentages, int step_index )  {
        randomFarm.resetRandomGenerators(seed);
        for( int i = 0; i < persons.size(); ++i ) {
            Person p = persons.get(i);
           for( int j = i+1; ( Knows.target_edges(p, percentages, step_index) > p.knows().size() ) && ( j < persons.size() ); ++j  ) {
                if( know(p, persons.get(j), j - i, percentages, step_index)) {
                   Knows.createKnow(randomFarm.get(RandomGeneratorFarm.Aspect.DATE), p, persons.get(j));
                }
           }
        }
    }

    public void initialize( Configuration conf ) {

    }

    boolean know( Person personA, Person personB, int dist, ArrayList<Float> percentages, int step_index ) {
        if( personA.knows().size() >= Knows.target_edges( personA, percentages, step_index) ||
            personB.knows().size() >= Knows.target_edges( personB, percentages, step_index) ) return false;
        double randProb = randomFarm.get(RandomGeneratorFarm.Aspect.UNIFORM).nextDouble();
        double prob = Math.pow(DatagenParams.baseProbCorrelated, dist);
        if ((randProb < prob) || (randProb < DatagenParams.limitProCorrelated)) {
            return true;
        }
        return false;
    }

}
