package ldbc.snb.datagen.objects.similarity;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.objects.Person;

/**
 * Created by aprat on 22/01/16.
 */
public class GeoDistanceSimilarity implements Person.PersonSimilarity{
    public float Similarity(Person personA, Person personB) {
        int zorderA = Dictionaries.places.getZorderID(personA.countryId());
        int zorderB = Dictionaries.places.getZorderID(personB.countryId());
        return 1.0f - (Math.abs(zorderA - zorderB) / 256.0f);
    }
}
