package ldbc.snb.datagen.serializer.small;

        import ldbc.snb.datagen.dictionary.Dictionaries;
        import ldbc.snb.datagen.objects.Organization;
        import ldbc.snb.datagen.objects.Place;
        import ldbc.snb.datagen.objects.Tag;
        import ldbc.snb.datagen.objects.TagClass;
        import ldbc.snb.datagen.serializer.HDFSCSVWriter;
        import ldbc.snb.datagen.serializer.InvariantSerializer;
        import ldbc.snb.datagen.vocabulary.DBP;
        import ldbc.snb.datagen.vocabulary.DBPOWL;
        import org.apache.hadoop.conf.Configuration;

        import java.util.ArrayList;

/**
 * Created by aprat on 12/17/14.
 */
public class CSVInvariantSerializer extends InvariantSerializer {

    private HDFSCSVWriter[] writers;

    private enum FileNames {
        TAG ("tag");

        private final String name;

        private FileNames( String name ) {
            this.name = name;
        }
        public String toString() {
            return name;
        }
    }

    public void initialize(Configuration conf, int reducerId) {
        int numFiles = FileNames.values().length;
        writers = new HDFSCSVWriter[numFiles];
        for( int i = 0; i < numFiles; ++i) {
            writers[i] = new HDFSCSVWriter(conf.get("ldbc.snb.datagen.serializer.socialNetworkDir"),FileNames.values()[i].toString()+"_"+reducerId,conf.getInt("ldbc.snb.datagen.numPartitions",1),conf.getBoolean("ldbc.snb.datagen.serializer.compressed",false),"|", conf.getBoolean("ldbc.snb.datagen.serializer.endlineSeparator",false));
        }

        ArrayList<String> arguments = new ArrayList<String>();
        arguments.add("id");
        arguments.add("name");
        writers[FileNames.TAG.ordinal()].writeEntry(arguments);
    }

    public void close() {
        int numFiles = FileNames.values().length;
        for(int i = 0; i < numFiles; ++i) {
            writers[i].close();
        }
    }

    protected void serialize(final Place place) {
    }

    protected void serialize(final Organization organization) {
    }

    protected void serialize(final TagClass tagClass) {
    }

    protected void serialize(final Tag tag) {
        ArrayList<String> arguments = new ArrayList<String>();
        arguments.add(Integer.toString(tag.id));
        arguments.add(tag.name);
        writers[FileNames.TAG.ordinal()].writeEntry(arguments);
        arguments.clear();
    }

    public void reset() {

    }
}
