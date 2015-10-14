package ldbc.snb.datagen.generator;

import ldbc.snb.datagen.objects.Knows;
import ldbc.snb.datagen.objects.Person;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Random;


/**
 * Created by aprat on 11/15/14.
 */
public class RandomKnowsGenerator implements KnowsGenerator {

    Random rand;


    public RandomKnowsGenerator() {
        rand = new Random();
    }



    public void generateKnows( ArrayList<Person> persons, int seed, ArrayList<Float> percentages, int step_index )  {

        rand.setSeed(seed);
        ArrayList<Integer> stubs = new ArrayList<Integer>();
        int index = 0;
        for(Person p : persons ) {
            long degree = Knows.target_edges(p, percentages, step_index);
            for( int i =0; i < degree; ++i ) {
                stubs.add(index);
            }
            ++index;
        }
        Collections.shuffle(stubs,rand);
        while(!stubs.isEmpty()) {
            int first = rand.nextInt(stubs.size());
            int first_index = stubs.get(first);
            stubs.remove(first);
            if(!stubs.isEmpty()) {
                int second = rand.nextInt(stubs.size());
                int second_index = stubs.get(second);
                stubs.remove(second);
                if(first_index != second_index ) {
                    Person p1 = persons.get(first_index);
                    Person p2 = persons.get(second_index);
                    Knows.createKnow(rand, p1, p2);
                }
            }
        }
    }

    public void initialize( Configuration conf ) {

    }
}
