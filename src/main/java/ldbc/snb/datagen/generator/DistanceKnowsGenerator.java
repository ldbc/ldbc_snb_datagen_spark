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

    public void generateKnows( ArrayList<Person> persons, int seed, float percentage, boolean firstStep )  {
        randomFarm.resetRandomGenerators(seed);
        for( int i = 0; i < persons.size(); ++i ) {
            Person p = persons.get(i);
           for( int j = i+1; ( target_edges(p, percentage, firstStep) > p.knows().size() ) && ( j < persons.size() ); ++j  ) {
                if( know(p, persons.get(j), j - i, percentage, firstStep)) {
                   createKnow(p, persons.get(j));
                }
           }
        }
    }

    boolean know( Person personA, Person personB, int dist, float percentage, boolean firstStep ) {
        if((float)(personA.knows().size()) >= target_edges(personA,percentage, firstStep) ||
           personB.knows().size() >= target_edges(personB,percentage, firstStep) ) return false;
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
            float similarity = Person.Similarity(personA,personB);
            personB.knows().add(new Knows(personA, creationDate, similarity));
            personA.knows().add(new Knows(personB, creationDate, similarity));
        }
    }

    long target_edges(Person person, float percentage, boolean firstStep) {
        long max =  (long) (person.maxNumKnows() * percentage);
        if(max == 0 && firstStep ) {
            return person.maxNumKnows();
        }
        return  max;
    }
}
