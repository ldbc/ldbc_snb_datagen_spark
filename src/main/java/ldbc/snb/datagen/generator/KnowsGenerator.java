package ldbc.snb.datagen.generator;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.objects.Knows;
import ldbc.snb.datagen.objects.Person;
import ldbc.snb.datagen.util.RandomGeneratorFarm;

import java.util.ArrayList;

/**
 * Created by aprat on 11/15/14.
 */
public class KnowsGenerator {

    private RandomGeneratorFarm randomFarm;

    public KnowsGenerator() {
        this.randomFarm = new RandomGeneratorFarm();
    }

    public void generateKnows( ArrayList<Person> persons, int seed, float upperBound )  {
        randomFarm.resetRandomGenerators(seed);
        for( int i = 0; i < persons.size(); ++i ) {
            Person p = persons.get(i);
           for( int j = i+1; (p.maxNumKnows()*upperBound > p.knows().size()) && (j < (i + 1000)) && (j < persons.size()); ++j  ) {
                if( know(p, persons.get(j), j - i, upperBound)) {
                   createKnow(p, persons.get(j));
                }
           }
        }
    }

    boolean know( Person personA, Person personB, int dist, float upperBound ) {
        if((float)(personA.knows().size()) >= (float)(personA.maxNumKnows())*upperBound ||
           personB.knows().size() >= (float)(personB.maxNumKnows())*upperBound ) return false;
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
}
