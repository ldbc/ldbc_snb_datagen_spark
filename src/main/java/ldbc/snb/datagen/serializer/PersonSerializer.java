package ldbc.snb.datagen.serializer;

import ldbc.snb.datagen.dictionary.CompanyDictionary;
import ldbc.snb.datagen.dictionary.PlaceDictionary;
import ldbc.snb.datagen.dictionary.TagDictionary;
import ldbc.snb.datagen.dictionary.UniversityDictionary;
import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.objects.*;

import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by aprat on 10/15/14.
 */
abstract public class PersonSerializer {

    protected UniversityDictionary universityDictionary_;
    protected PlaceDictionary placeDictionary_;
    protected CompanyDictionary companyDictionary_;
    protected TagDictionary tagDictionary_;

    public PersonSerializer( ) {

        placeDictionary_ = new PlaceDictionary(DatagenParams.numPersons);

        companyDictionary_ = new CompanyDictionary(placeDictionary_, DatagenParams.probUnCorrelatedCompany);

        universityDictionary_ = new UniversityDictionary(placeDictionary_,
                DatagenParams.probUnCorrelatedOrganization,
                DatagenParams.probTopUniv,
                companyDictionary_.getNumCompanies());

        tagDictionary_ = new TagDictionary(  placeDictionary_.getCountries().size(),
                DatagenParams.tagCountryCorrProb);
    }


    public void export(Person person) {

        serialize(person);
        for( Knows k : person.knows()) {
            serialize(k);
        }

        long universityId = universityDictionary_.getUniversityFromLocation(person.universityLocationId());
        if (universityId != -1) {
            if (person.classYear() != -1) {
                StudyAt studyAt = new StudyAt();
                studyAt.year = person.classYear();
                studyAt.user = person.accountId();
                studyAt.university = universityId;
                serialize(studyAt);
            }
        }

        Iterator<Long> it = person.companies().keySet().iterator();
        while (it.hasNext()) {
            long companyId = it.next();
            WorkAt workAt = new WorkAt();
            workAt.company = companyId;
            workAt.user = person.accountId();
            workAt.year = person.companies().get(companyId);
            serialize(workAt);
        }
    }

    abstract public void initialize(Configuration conf, int reducerId);

    abstract public void close();

    abstract protected void serialize(Person p);

    abstract protected void serialize(StudyAt studyAt);

    abstract protected void serialize(WorkAt workAt);

    abstract protected void serialize(Knows knows);
}
