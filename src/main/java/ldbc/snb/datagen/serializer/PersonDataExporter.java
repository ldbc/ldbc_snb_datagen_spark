package ldbc.snb.datagen.serializer;

import ldbc.snb.datagen.dictionary.CompanyDictionary;
import ldbc.snb.datagen.dictionary.PlaceDictionary;
import ldbc.snb.datagen.dictionary.TagDictionary;
import ldbc.snb.datagen.dictionary.UniversityDictionary;
import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.objects.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by aprat on 10/15/14.
 */
public class PersonDataExporter {

    private PersonSerializer personSerializer;
    private UniversityDictionary universityDictionary;
    private PlaceDictionary placeDictionary;
    private CompanyDictionary companyDictionary;
    private TagDictionary tagDictionary;

    public PersonDataExporter( PersonSerializer personSerializer) {
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

        tagDictionary = new TagDictionary(  placeDictionary.getCountries().size(),
                DatagenParams.tagCountryCorrProb);
        tagDictionary.load( DatagenParams.tagsFile,
                DatagenParams.popularTagByCountryFile,
                DatagenParams.tagClassFile,
                DatagenParams.tagClassHierarchyFile);
    }


    public void export(Person person) {

        personSerializer.serialize(person);
        for( Knows k : person.knows) {
            personSerializer.serialize(k);
        }

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
