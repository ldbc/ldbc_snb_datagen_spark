package ldbc.snb.datagen.generator.generators.knowsgenerators;

import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.util.GeneratorConfiguration;

import java.util.List;

public interface KnowsGenerator {
    void generateKnows(List<Person> persons, int blockId, List<Float> percentages, int step_index, Person.PersonSimilarity personSimilarity);

    void initialize(GeneratorConfiguration conf);
}
