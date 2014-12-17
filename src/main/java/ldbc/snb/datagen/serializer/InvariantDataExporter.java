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

/**
 * Created by aprat on 12/17/14.
 */
public class InvariantDataExporter {

    private PersonSerializer personSerializer;
    private InvariantSerializer invariantSerializer;
    private UniversityDictionary universityDictionary;
    private PlaceDictionary placeDictionary;
    private CompanyDictionary companyDictionary;
    private TagDictionary tagDictionary;
    private TreeSet<Integer> exportedClasses;

    public InvariantDataExporter(InvariantSerializer invariantSerializer) {
        this.invariantSerializer = invariantSerializer;

        exportedClasses = new TreeSet<Integer>();

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



    private void exportTagHierarchy(Tag tag) {
        int classId = tag.tagClass;
        while (classId != -1 && !exportedClasses.contains(classId)) {
            exportedClasses.add(classId);
            TagClass tagClass = new TagClass();
            tagClass.id = classId;
            tagClass.name = tagDictionary.getClassName(classId);
            tagClass.parent = tagDictionary.getClassParent(tagClass.id);
            invariantSerializer.serialize(tagClass);
            classId = tagClass.parent;
        }
    }

    public void exportPlaces() {
        Set<Integer> locations = placeDictionary.getPlaces();
        Iterator<Integer> it = locations.iterator();
        while(it.hasNext()) {
            Place place = placeDictionary.getLocation(it.next());
            invariantSerializer.serialize(place);
        }
    }

    public void exportOrganizations() {
        Set<Long> companies = companyDictionary.getCompanies();
        Iterator<Long> it = companies.iterator();
        while(it.hasNext()) {
            Organization company = new Organization();
            company.id = it.next();
            company.type = Organization.OrganisationType.company;
            company.name = companyDictionary.getCompanyName(company.id);
            company.location = companyDictionary.getCountry(company.id);
            invariantSerializer.serialize(company);
        }

        Set<Long> universities = universityDictionary.getUniversities();
        it = universities.iterator();
        while(it.hasNext()) {
            Organization university = new Organization();
            university.id = it.next();
            university.type = Organization.OrganisationType.university;
            university.name = universityDictionary.getUniversityName(university.id);
            university.location = universityDictionary.getUniversityCity(university.id);
            invariantSerializer.serialize(university);
        }
    }

    public void exportTags() {
        Set<Integer>  tags = tagDictionary.getTags();
        Iterator<Integer> it = tags.iterator();
        while(it.hasNext()) {
            Tag tag = new Tag();
            tag.id = it.next();
            tag.name = tagDictionary.getName(tag.id);
            tag.name.replace("\"", "\\\"");
            tag.tagClass = tagDictionary.getTagClass(tag.id);
            invariantSerializer.serialize(tag);
            exportTagHierarchy(tag);
        }
    }
}
