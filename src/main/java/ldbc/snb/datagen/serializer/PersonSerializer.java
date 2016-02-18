package ldbc.snb.datagen.serializer;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.objects.Knows;
import ldbc.snb.datagen.objects.Person;
import ldbc.snb.datagen.objects.StudyAt;
import ldbc.snb.datagen.objects.WorkAt;
import org.apache.hadoop.conf.Configuration;

import java.util.Iterator;

/**
 * Created by aprat on 10/15/14.
 */
abstract public class PersonSerializer {

    public PersonSerializer() {
	    
    }

    public void export(final Person person) {
//        System.out.println(person.maxNumKnows());

        serialize(person);

        long universityId = Dictionaries.universities.getUniversityFromLocation(person.universityLocationId());
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

    public void export(final Person p, final Knows k ) {
        if( p.accountId() < k.to().accountId())
            serialize(p, k);
    }

    abstract public void reset();

    abstract public void initialize(Configuration conf, int reducerId);

    abstract public void close();

    abstract protected void serialize(final Person p);

    abstract protected void serialize(final StudyAt studyAt);

    abstract protected void serialize(final WorkAt workAt);

    abstract protected void serialize(final Person p, final Knows knows);
}
