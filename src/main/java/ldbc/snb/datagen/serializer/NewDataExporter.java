package ldbc.snb.datagen.serializer;

import ldbc.snb.datagen.dictionary.CompanyDictionary;
import ldbc.snb.datagen.dictionary.PlaceDictionary;
import ldbc.snb.datagen.dictionary.UniversityDictionary;
import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.objects.Friend;
import ldbc.snb.datagen.objects.Person;
import ldbc.snb.datagen.objects.StudyAt;
import ldbc.snb.datagen.objects.WorkAt;

import java.util.Iterator;

/**
 * Created by aprat on 10/15/14.
 */
public class NewDataExporter {

    private PersonSerializer personSerializer;
    private UniversityDictionary universityDictionary;
    private PlaceDictionary placeDictionary;
    private CompanyDictionary companyDictionary;

    public NewDataExporter( PersonSerializer personSerializer) {
        this.personSerializer = personSerializer;

        placeDictionary = new PlaceDictionary(DatagenParams.numPersons);
        placeDictionary.load(DatagenParams.cityDictionaryFile, DatagenParams.countryDictionaryFile);

        companyDictionary = new CompanyDictionary(placeDictionary, DatagenParams.probUnCorrelatedCompany);
        companyDictionary.load(DatagenParams.companiesDictionaryFile);

        universityDictionary = new UniversityDictionary(placeDictionary,
                DatagenParams.probUnCorrelatedOrganization,
                DatagenParams.probTopUniv,
                companyDictionary.getNumCompanies());
        universityDictionary.load(DatagenParams.universityDictionaryFile);
    }


    public void export(Person person) {

        personSerializer.serialize(person);

        long universityId = universityDictionary.getUniversityFromLocation(person.universityLocationId);
        if (universityId != -1) {
            if (person.classYear != -1) {
                StudyAt studyAt = new StudyAt();
                studyAt.year = person.classYear;
                studyAt.user = person.accountId;
                studyAt.university = universityId;
                personSerializer.serialize(studyAt);
            }
        }

        Iterator<Long> it = person.companies.keySet().iterator();
        while (it.hasNext()) {
            long companyId = it.next();
            WorkAt workAt = new WorkAt();
            workAt.company = companyId;
            workAt.user = person.accountId;
            workAt.year = person.companies.get(companyId);
            personSerializer.serialize(workAt);
        }
    }
}
