package ldbc.snb.datagen.generator;

import ldbc.snb.datagen.objects.Knows;
import ldbc.snb.datagen.objects.Person;
import ldbc.snb.datagen.util.RandomGeneratorFarm;

import java.util.ArrayList;
import java.util.GregorianCalendar;

/**
 * Created by aprat on 11/15/14.
 */
public class KnowsGenerator {

    private RandomGeneratorFarm randomFarm;
    private DateGenerator dateGenerator;

    public KnowsGenerator() {
        this.dateGenerator = new DateGenerator( new GregorianCalendar(DatagenParams.startYear,
                                                DatagenParams.startMonth,
                                                DatagenParams.startDate),
                                                new GregorianCalendar(DatagenParams.endYear,
                                                        DatagenParams.endMonth,
                                                        DatagenParams.endDate),
                                                DatagenParams.alpha,
                                                DatagenParams.deltaTime );
        this.randomFarm = new RandomGeneratorFarm();
    }

    public void generateKnows( ArrayList<Person> persons, int seed, float upperBound )  {
        randomFarm.resetRandomGenerators(seed);
        for( int i = 0; i < persons.size(); ++i ) {
           for( int j = i+1; (j < (i + 1000)) && (j < persons.size()); ++j  ) {
                if( know(persons.get(i), persons.get(j), j - i, upperBound)) {
                   createKnow(persons.get(i), persons.get(j));
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
        long  creationDate = dateGenerator.randomKnowsCreationDate(
                randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                personA,
                personB);
        creationDate = creationDate - personA.creationDate() >= DatagenParams.deltaTime ? creationDate : creationDate + (DatagenParams.deltaTime - (creationDate - personA.creationDate()));
        creationDate = creationDate - personB.creationDate() >= DatagenParams.deltaTime ? creationDate : creationDate + (DatagenParams.deltaTime - (creationDate - personB.creationDate()));
        if( creationDate <= dateGenerator.getEndDateTime() ) {
            personB.knows().add(new Knows(personB, personA, creationDate));
            personA.knows().add(new Knows(personA, personB, creationDate));
        }
    }
}
