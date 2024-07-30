package ldbc.snb.datagen.entities.dynamic.person.similarity;

import ldbc.snb.datagen.generator.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.person.Person;

public class GeoDistanceSimilarity implements Person.PersonSimilarity {
    @Override
    public float similarity(Person personA, Person personB) {
        int zorderA = Dictionaries.places.getZorderID(personA.getCountryId());
        int zorderB = Dictionaries.places.getZorderID(personB.getCountryId());
        return 1.0f - (Math.abs(zorderA - zorderB) / 256.0f);
    }
}
