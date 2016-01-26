package ldbc.snb.datagen.objects.similarity;

import ldbc.snb.datagen.objects.Person;

import java.util.Set;
import java.util.TreeSet;

/**
 * Created by aprat on 22/01/16.
 */
public class InterestsSimilarity implements Person.PersonSimilarity {
    public float Similarity(Person personA, Person personB) {
        Set<Integer> union = new TreeSet<Integer>(personA.interests());
        union.addAll(personB.interests());
        union.add(personA.mainInterest());
        union.add(personB.mainInterest());
        Set<Integer> intersection = new TreeSet<Integer>(personA.interests());
        intersection.retainAll(personB.interests());
        if(personA.mainInterest() == personB.mainInterest()) intersection.add(personA.mainInterest());
        return union.size() > 0 ? intersection.size() / (float)union.size() : 0;
    }
}
