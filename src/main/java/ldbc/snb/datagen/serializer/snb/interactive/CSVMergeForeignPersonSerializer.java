package ldbc.snb.datagen.serializer.snb.interactive;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.objects.Knows;
import ldbc.snb.datagen.objects.Person;
import ldbc.snb.datagen.objects.StudyAt;
import ldbc.snb.datagen.objects.WorkAt;
import ldbc.snb.datagen.serializer.HDFSCSVWriter;
import ldbc.snb.datagen.serializer.PersonSerializer;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by aprat on 17/02/15.
 */
public class CSVMergeForeignPersonSerializer extends PersonSerializer {

    private HDFSCSVWriter[] writers;

    private enum FileNames {
        PERSON ("person"),
        PERSON_SPEAKS_LANGUAGE ("person_speaks_language"),
        PERSON_HAS_EMAIL ("person_email_emailaddress"),
        PERSON_HAS_INTEREST_TAG ("person_hasInterest_tag"),
        PERSON_WORK_AT ("person_workAt_organisation"),
        PERSON_STUDY_AT ("person_studyAt_organisation"),
        PERSON_KNOWS_PERSON("person_knows_person");

        private final String name;

        private FileNames( String name ) {
            this.name = name;
        }
        public String toString() {
            return name;
        }
    }

    public CSVMergeForeignPersonSerializer() {
    }

    public void initialize(Configuration conf, int reducerId) {
        int numFiles = FileNames.values().length;
        writers = new HDFSCSVWriter[numFiles];
        for( int i = 0; i < numFiles; ++i) {
            writers[i] = new HDFSCSVWriter(conf.get("ldbc.snb.datagen.serializer.socialNetworkDir"),FileNames.values()[i].toString()+"_"+reducerId,conf.getInt("ldbc.snb.datagen.numPartitions",1),conf.getBoolean("ldbc.snb.datagen.serializer.compressed",false),"|", conf.getBoolean("ldbc.snb.datagen.serializer.endlineSeparator",false));
        }

        ArrayList<String> arguments = new ArrayList<String>();
        arguments.add("id");
        arguments.add("firstName");
        arguments.add("lastName");
        arguments.add("gender");
        arguments.add("birthday");
        arguments.add("creationDate");
        arguments.add("locationIP");
        arguments.add("browserUsed");
        arguments.add("place");
        writers[FileNames.PERSON.ordinal()].writeEntry(arguments);

        arguments.clear();
        arguments.add("Person.id");
        arguments.add("language");
        writers[FileNames.PERSON_SPEAKS_LANGUAGE.ordinal()].writeEntry(arguments);

        arguments.clear();
        arguments.add("Person.id");
        arguments.add("email");
        writers[FileNames.PERSON_HAS_EMAIL.ordinal()].writeEntry(arguments);

        arguments.clear();
        arguments.add("Person.id");
        arguments.add("Tag.id");
        writers[FileNames.PERSON_HAS_INTEREST_TAG.ordinal()].writeEntry(arguments);

        arguments.clear();
        arguments.add("Person.id");
        arguments.add("Organisation.id");
        arguments.add("workFrom");
        writers[FileNames.PERSON_WORK_AT.ordinal()].writeEntry(arguments);

        arguments.clear();
        arguments.add("Person.id");
        arguments.add("Organisation.id");
        arguments.add("classYear");
        writers[FileNames.PERSON_STUDY_AT.ordinal()].writeEntry(arguments);

        arguments.clear();
        arguments.add("Person.id");
        arguments.add("Person.id");
        arguments.add("creationDate");
        writers[FileNames.PERSON_KNOWS_PERSON.ordinal()].writeEntry(arguments);

    }

    @Override
    public void close() {
        int numFiles = FileNames.values().length;
        for(int i = 0; i < numFiles; ++i) {
            writers[i].close();
        }
    }

    @Override
    protected void serialize(final Person p) {

        ArrayList<String> arguments = new ArrayList<String>();

        arguments.add(Long.toString(p.accountId()));
        arguments.add(p.firstName());
        arguments.add(p.lastName());
        if(p.gender() == 1) {
            arguments.add("male");
        } else {
            arguments.add("female");
        }

        String dateString = Dictionaries.dates.formatDate(p.birthDay());
        arguments.add(dateString);

        dateString = Dictionaries.dates.formatDateTime(p.creationDate());
        arguments.add(dateString);
        arguments.add(p.ipAddress().toString());
        arguments.add(Dictionaries.browsers.getName(p.browserId()));
        arguments.add(Integer.toString(p.cityId()));
        writers[FileNames.PERSON.ordinal()].writeEntry(arguments);

        ArrayList<Integer> languages = p.languages();
        for (int i = 0; i < languages.size(); i++) {
            arguments.clear();
            arguments.add(Long.toString(p.accountId()));
            arguments.add(Dictionaries.languages.getLanguageName(languages.get(i)));
            writers[FileNames.PERSON_SPEAKS_LANGUAGE.ordinal()].writeEntry(arguments);
        }

        Iterator<String> itString = p.emails().iterator();
        while (itString.hasNext()) {
            arguments.clear();
            String email = itString.next();
            arguments.add(Long.toString(p.accountId()));
            arguments.add(email);
            writers[FileNames.PERSON_HAS_EMAIL.ordinal()].writeEntry(arguments);
        }

        Iterator<Integer> itInteger = p.interests().iterator();
        while (itInteger.hasNext()) {
            arguments.clear();
            Integer interestIdx = itInteger.next();
            arguments.add(Long.toString(p.accountId()));
            arguments.add(Integer.toString(interestIdx));
            writers[FileNames.PERSON_HAS_INTEREST_TAG.ordinal()].writeEntry(arguments);
        }
    }

    @Override
    protected void serialize(final StudyAt studyAt) {
        ArrayList<String> arguments = new ArrayList<String>();
        String dateString = Dictionaries.dates.formatYear(studyAt.year);
        arguments.add(Long.toString(studyAt.user));
        arguments.add(Long.toString(studyAt.university));
        arguments.add(dateString);
        writers[FileNames.PERSON_STUDY_AT.ordinal()].writeEntry(arguments);
    }

    @Override
    protected void serialize(final WorkAt workAt) {
        ArrayList<String> arguments = new ArrayList<String>();
        String dateString = Dictionaries.dates.formatYear(workAt.year);
        arguments.add(Long.toString(workAt.user));
        arguments.add(Long.toString(workAt.company));
        arguments.add(dateString);
        writers[FileNames.PERSON_WORK_AT.ordinal()].writeEntry(arguments);
    }

    @Override
    protected void serialize( final Person p, Knows knows) {
        ArrayList<String> arguments = new ArrayList<String>();
        String dateString = Dictionaries.dates.formatDateTime(knows.creationDate());
        arguments.add(Long.toString(p.accountId()));
        arguments.add(Long.toString(knows.to().accountId()));
        arguments.add(dateString);
        writers[FileNames.PERSON_KNOWS_PERSON.ordinal()].writeEntry(arguments);
    }

    @Override
    public void reset() {

    }
}
