package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.generator.LDBCDatagen;
import ldbc.snb.datagen.objects.Organization;
import ldbc.snb.datagen.objects.Place;
import ldbc.snb.datagen.objects.Tag;
import ldbc.snb.datagen.objects.TagClass;
import ldbc.snb.datagen.serializer.InvariantSerializer;
import org.apache.hadoop.conf.Configuration;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by aprat on 12/17/14.
 */
public class HadoopInvariantSerializer {

    private InvariantSerializer [] invariantSerializer_;
    private TreeSet<Integer> exportedClasses_;
    private int currentFile_ = 0;

    private Configuration conf_;

    public HadoopInvariantSerializer( Configuration conf ) {
        conf_ = new Configuration(conf);
        exportedClasses_ = new TreeSet<Integer>();
        LDBCDatagen.init(conf_);
    }

    public void run() throws Exception {

        try {
            invariantSerializer_ = new InvariantSerializer[DatagenParams.numThreads];
            for( int i = 0; i < DatagenParams.numThreads; ++i ) {
                invariantSerializer_[i] = (InvariantSerializer) Class.forName(conf_.get("ldbc.snb.datagen.serializer.invariantSerializer")).newInstance();
                invariantSerializer_[i].initialize(conf_, i);
            }
        } catch( Exception e ) {
            System.err.println(e.getMessage());
            e.printStackTrace();
        }

        exportPlaces();
        exportTags();
        exportOrganizations();

        for( int i = 0; i < DatagenParams.numThreads; ++i ) {
            invariantSerializer_[i].close();
        }
    }

    private int nextFile() {
        int ret = currentFile_;
        currentFile_ = (++currentFile_) % DatagenParams.numThreads;
        return ret;
    }

    private void exportTagHierarchy(Tag tag) {
        int classId = tag.tagClass;
        while (classId != -1 && !exportedClasses_.contains(classId)) {
            exportedClasses_.add(classId);
            TagClass tagClass = new TagClass();
            tagClass.id = classId;
            tagClass.name = Dictionaries.tags.getClassName(classId);
            tagClass.parent = Dictionaries.tags.getClassParent(tagClass.id);
            invariantSerializer_[nextFile()].export(tagClass);
            classId = tagClass.parent;
        }
    }

    public void exportPlaces() {
        Set<Integer> locations = Dictionaries.places.getPlaces();
        Iterator<Integer> it = locations.iterator();
        while(it.hasNext()) {
            Place place = Dictionaries.places.getLocation(it.next());
            invariantSerializer_[nextFile()].export(place);
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
            invariantSerializer_[nextFile()].export(company);
        }

        Set<Long> universities = Dictionaries.universities.getUniversities();
        it = universities.iterator();
        while(it.hasNext()) {
            Organization university = new Organization();
            university.id = it.next();
            university.type = Organization.OrganisationType.university;
            university.name = Dictionaries.universities.getUniversityName(university.id);
            university.location = Dictionaries.universities.getUniversityCity(university.id);
            invariantSerializer_[nextFile()].export(university);
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
            invariantSerializer_[nextFile()].export(tag);
            exportTagHierarchy(tag);
        }
    }

}
