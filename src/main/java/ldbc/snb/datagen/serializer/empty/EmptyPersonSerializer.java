package ldbc.snb.datagen.serializer.empty;

import ldbc.snb.datagen.objects.Knows;
import ldbc.snb.datagen.objects.Person;
import ldbc.snb.datagen.objects.StudyAt;
import ldbc.snb.datagen.objects.WorkAt;
import ldbc.snb.datagen.serializer.PersonSerializer;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by aprat on 30/01/15.
 */
public class EmptyPersonSerializer extends PersonSerializer {

    public void initialize(Configuration conf, int reducerId) {

    }

    public void close() {

    }

    protected void serialize(Person p) {

    }

    protected void serialize(StudyAt studyAt) {

    }

    protected void serialize(WorkAt workAt) {

    }

    protected void serialize(Knows knows) {

    }

}
