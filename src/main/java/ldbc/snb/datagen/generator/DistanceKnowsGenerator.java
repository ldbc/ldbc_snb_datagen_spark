package ldbc.snb.datagen.generator;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.objects.Knows;
import ldbc.snb.datagen.objects.Person;
import ldbc.snb.datagen.util.RandomGeneratorFarm;

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
           for( int j = i+1; ( target_edges(p, percentages, step_index) > p.knows().size() ) && ( j < persons.size() ); ++j  ) {
                if( know(p, persons.get(j), j - i, percentages, step_index)) {
                   createKnow(p, persons.get(j));
                }
           }
        }
    }

    boolean know( Person personA, Person personB, int dist, ArrayList<Float> percentages, int step_index ) {
        if( personA.knows().size() >= target_edges( personA, percentages, step_index) ||
            personB.knows().size() >= target_edges( personB, percentages, step_index) ) return false;
        double randProb = randomFarm.get(RandomGeneratorFarm.Aspect.UNIFORM).nextDouble();
        double prob = Math.pow(DatagenParams.baseProbCorrelated, dist);
        if ((randProb < prob) || (randProb < DatagenParams.limitProCorrelated)) {
            return true;
        }
        return false;
    }

    void createKnow( Person personA, Person personB ) {
        long  creationDate = Dictionaries.dates.randomKnowsCreationDate(
                randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                personA,
                personB);
        creationDate = creationDate - personA.creationDate() >= DatagenParams.deltaTime ? creationDate : creationDate + (DatagenParams.deltaTime - (creationDate - personA.creationDate()));
        creationDate = creationDate - personB.creationDate() >= DatagenParams.deltaTime ? creationDate : creationDate + (DatagenParams.deltaTime - (creationDate - personB.creationDate()));
        if( creationDate <= Dictionaries.dates.getEndDateTime() ) {
            float similarity = Person.personSimilarity.Similarity(personA,personB);
            personB.knows().add(new Knows(personA, creationDate, similarity));
            personA.knows().add(new Knows(personB, creationDate, similarity));
        }
    }

    long target_edges(Person person, ArrayList<Float> percentages, int step_index ) {
        int generated_edges = 0;
        for (int i = 0; i < step_index; ++i) {
            generated_edges += Math.ceil(percentages.get(i)*person.maxNumKnows());
        }
        generated_edges = Math.min(generated_edges, (int)person.maxNumKnows());
        int to_generate = Math.min( (int)person.maxNumKnows() - generated_edges, (int)Math.ceil(percentages.get(step_index)*person.maxNumKnows()));
        return  to_generate;
    }
}
