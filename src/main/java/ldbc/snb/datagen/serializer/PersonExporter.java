package ldbc.snb.datagen.serializer;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.spark.generation.entities.dynamic.person.Person;
import ldbc.snb.datagen.spark.generation.entities.dynamic.relations.Knows;
import ldbc.snb.datagen.spark.generation.entities.dynamic.relations.StudyAt;
import ldbc.snb.datagen.spark.generation.entities.dynamic.relations.WorkAt;

import java.util.List;

public class PersonExporter implements AutoCloseable {
    DynamicPersonSerializer dynamicPersonSerializer;

    public PersonExporter(DynamicPersonSerializer dynamicPersonSerializer) {
        this.dynamicPersonSerializer = dynamicPersonSerializer;
    }

    public void export(Person person) {
        dynamicPersonSerializer.serialize(person);

        long universityId = Dictionaries.universities.getUniversityFromLocation(person.getUniversityLocationId());
        if ((universityId != -1) && (person.getClassYear() != -1)) {
            StudyAt studyAt = new StudyAt();
            studyAt.year = person.getClassYear();
            studyAt.person = person.getAccountId();
            studyAt.university = universityId;
            dynamicPersonSerializer.serialize(studyAt, person);
        }

        for (long companyId : person.getCompanies().keySet()) {
            WorkAt workAt = new WorkAt();
            workAt.company = companyId;
            workAt.person = person.getAccountId();
            workAt.year = person.getCompanies().get(companyId);
            dynamicPersonSerializer.serialize(workAt, person);
        }

        List<Knows> knows = person.getKnows();

        for (Knows know : knows) {
            if (person.getAccountId() < know.to().getAccountId())
                dynamicPersonSerializer.serialize(person, know);
        }
    }

    @Override
    public void close() throws Exception {
        dynamicPersonSerializer.close();
    }
}
