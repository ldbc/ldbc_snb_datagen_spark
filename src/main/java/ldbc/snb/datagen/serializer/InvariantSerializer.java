package ldbc.snb.datagen.serializer;

import ldbc.snb.datagen.dictionary.CompanyDictionary;
import ldbc.snb.datagen.dictionary.PlaceDictionary;
import ldbc.snb.datagen.dictionary.TagDictionary;
import ldbc.snb.datagen.dictionary.UniversityDictionary;
import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.objects.Organization;
import ldbc.snb.datagen.objects.Place;
import ldbc.snb.datagen.objects.Tag;
import ldbc.snb.datagen.objects.TagClass;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.objects.Person;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by aprat on 12/17/14.
 */
abstract public class InvariantSerializer {

    protected TreeSet<Integer> exportedClasses_;

    public InvariantSerializer() {

        exportedClasses_ = new TreeSet<Integer>();
    }



    private void exportTagHierarchy(Tag tag) {
        int classId = tag.tagClass;
        while (classId != -1 && !exportedClasses_.contains(classId)) {
            exportedClasses_.add(classId);
            TagClass tagClass = new TagClass();
            tagClass.id = classId;
            tagClass.name = Dictionaries.tags.getClassName(classId);
            tagClass.parent = Dictionaries.tags.getClassParent(tagClass.id);
            serialize(tagClass);
            classId = tagClass.parent;
        }
    }

    public void exportPlaces() {
        Set<Integer> locations = Dictionaries.places.getPlaces();
        Iterator<Integer> it = locations.iterator();
        while(it.hasNext()) {
            Place place = Dictionaries.places.getLocation(it.next());
            serialize(place);
        }
    }

    public void exportOrganizations() {
        Set<Long> companies = Dictionaries.companies.getCompanies();
        Iterator<Long> it = companies.iterator();
        while(it.hasNext()) {
            Organization company = new Organization();
            company.id = it.next();
            company.type = Organization.OrganisationType.company;
            company.name = Dictionaries.companies.getCompanyName(company.id);
            company.location = Dictionaries.companies.getCountry(company.id);
            serialize(company);
        }

        Set<Long> universities = Dictionaries.universities.getUniversities();
        it = universities.iterator();
        while(it.hasNext()) {
            Organization university = new Organization();
            university.id = it.next();
            university.type = Organization.OrganisationType.university;
            university.name = Dictionaries.universities.getUniversityName(university.id);
            university.location = Dictionaries.universities.getUniversityCity(university.id);
	    serialize(university);
        }
    }

    public void exportTags() {
        Set<Integer>  tags = Dictionaries.tags.getTags();
        Iterator<Integer> it = tags.iterator();
        while(it.hasNext()) {
            Tag tag = new Tag();
            tag.id = it.next();
            tag.name = Dictionaries.tags.getName(tag.id);
            tag.name.replace("\"", "\\\"");
            tag.tagClass = Dictionaries.tags.getTagClass(tag.id);
            serialize(tag);
            exportTagHierarchy(tag);
        }
    }

    abstract public void initialize(Configuration conf, int reducerId);

    abstract public void close();

    abstract protected void serialize(Place place);

    abstract protected void serialize(Organization organization);

    abstract protected void serialize(TagClass tagClass);

    abstract protected void serialize(Tag tag);
}
