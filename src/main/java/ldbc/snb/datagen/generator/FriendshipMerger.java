package ldbc.snb.datagen.generator;

import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.entities.dynamic.relations.Knows;

import java.util.ArrayList;
import java.util.List;

public class FriendshipMerger {
    public int getNumRepeated() {
        return numRepeated;
    }

    private int numRepeated = 0;

    public Person apply(Iterable<Person> valueSet) {
        List<Knows> knows = new ArrayList<>();
        Person person = null;
        int index = 0;
        for (Person p : valueSet) {
            if (index == 0) {
                person = new Person(p);
            }
            knows.addAll(p.getKnows());
            index++;
        }
        person.getKnows().clear();
        Knows.FullComparator comparator = new Knows.FullComparator();
        knows.sort(comparator);
        if (knows.size() > 0) {
            long currentTo = knows.get(0).to().getAccountId();
            person.getKnows().add(knows.get(0));
            for (index = 1; index < knows.size(); ++index) {
                Knows nextKnows = knows.get(index);
                if (currentTo != knows.get(index).to().getAccountId()) {
                    person.getKnows().add(nextKnows);
                    currentTo = nextKnows.to().getAccountId();
                } else {
                    numRepeated++;
                }
            }
        }
        return person;
    }
}
