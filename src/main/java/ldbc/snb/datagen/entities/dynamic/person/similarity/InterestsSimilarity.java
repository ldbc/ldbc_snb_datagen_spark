package ldbc.snb.datagen.entities.dynamic.person.similarity;

import ldbc.snb.datagen.entities.dynamic.person.Person;

import java.util.Set;
import java.util.TreeSet;

public class InterestsSimilarity implements Person.PersonSimilarity {
    public float similarity(Person personA, Person personB) {
        Set<Integer> union = new TreeSet<>(personA.getInterests());
        union.addAll(personB.getInterests());
        union.add(personA.getMainInterest());
        union.add(personB.getMainInterest());
        Set<Integer> intersection = new TreeSet<>(personA.getInterests());
        intersection.retainAll(personB.getInterests());
        if (personA.getMainInterest() == personB.getMainInterest()) intersection.add(personA.getMainInterest());
        return union.size() > 0 ? intersection.size() / (float) union.size() : 0;
    }
}
